use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::pin::Pin;

use futures::FutureExt;
use futures::stream::{self, StreamExt};
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{Body, body};
use gotham::mime;
use gotham::state::{FromState, State};
use http::StatusCode;
use idgenerator::IdInstance;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::data_access::{self, execute_sql_async, load_files_as_table};
use crate::data_contract::{
    CreateTable, FileDescriptor, IcebergColumnStats, IcebergFileStats, IcebergRowGroupStats,
    ServingPattern, ServingTableConfig, TableDescription,
};
use crate::elastic_search_endpoints::NamePathExtractor;
use crate::peers::CheckpointDescriptor;
use crate::schema_massager::{PowdrrDataType, PowdrrSchema};
use crate::search_runtime::batches_to_serde_value;
use crate::serving_plan::{ServingPredicate, ServingQueryClassification, ServingRequestPlan};
use crate::state_provider::STATE_PROVIDER;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const MAX_IN_VALUES: usize = 32;
const MAX_WARMUP_FILE_GROUPS_PER_PATTERN: usize = 2;
const MAX_WARMUP_FILES: usize = 8;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServingQueryResponse {
    pub table: String,
    pub classification: ServingQueryClassification,
    #[serde(default)]
    pub matched_pattern: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    pub files_considered: usize,
    pub files_selected: usize,
    #[serde(default)]
    pub row_groups_considered: usize,
    #[serde(default)]
    pub row_groups_selected: usize,
    pub estimated_bytes: u64,
    #[serde(default)]
    pub metadata_snapshot_cached: bool,
    #[serde(default)]
    pub metadata_files_cached: usize,
    #[serde(default)]
    pub metadata_row_groups_cached: usize,
    #[serde(default)]
    pub page_index_row_groups_selected: usize,
    #[serde(default)]
    pub bloom_filter_row_groups_selected: usize,
    #[serde(default)]
    pub bulk_cache: data_access::ServingBulkCacheStats,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub rows: Vec<Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServingConfigResponse {
    pub acknowledged: bool,
    pub table: String,
    pub serving: ServingTableConfig,
}

struct ServingExecutionContext {
    description: TableDescription,
    schema: PowdrrSchema,
    files: Vec<FileDescriptor>,
    delete_files: Vec<String>,
    file_stats: HashMap<String, IcebergFileStats>,
    snapshot_id: Option<String>,
    metadata_snapshot_cached: bool,
}

struct ServingPlan {
    classification: ServingQueryClassification,
    matched_pattern: Option<String>,
    reason: Option<String>,
    limit: usize,
    sql: String,
    selected_files: Vec<FileDescriptor>,
    files_considered: usize,
    files_selected: usize,
    row_groups_considered: usize,
    row_groups_selected: usize,
    estimated_bytes: u64,
    metadata_snapshot_cached: bool,
    metadata_files_cached: usize,
    metadata_row_groups_cached: usize,
    page_index_row_groups_selected: usize,
    bloom_filter_row_groups_selected: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct ServingWarmupStep {
    pub pattern_name: String,
    pub request: ServingRequestPlan,
    pub selected_files: Vec<FileDescriptor>,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ServingWarmupPlan {
    pub matched_patterns: Vec<String>,
    pub selected_files: Vec<FileDescriptor>,
    pub estimated_bytes: u64,
    pub steps: Vec<ServingWarmupStep>,
}

#[derive(Clone, Debug)]
struct OrderedFileGroup {
    files: Vec<FileDescriptor>,
    best_case_sort_value: Value,
}

#[derive(Clone, Debug, Default)]
struct PrunedFileSelection {
    selected_files: Vec<FileDescriptor>,
    files_selected: usize,
    row_groups_considered: usize,
    row_groups_selected: usize,
    estimated_bytes: u64,
    metadata_files_cached: usize,
    metadata_row_groups_cached: usize,
    page_index_row_groups_selected: usize,
    bloom_filter_row_groups_selected: usize,
}

pub fn get_serving_config(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state);
        match STATE_PROVIDER.describe_table(&path.name).await {
            Ok(Some(description)) => match description.serving {
                Some(serving) => {
                    let response = json_response(
                        &state,
                        StatusCode::OK,
                        &ServingConfigResponse {
                            acknowledged: true,
                            table: description.name,
                            serving,
                        },
                    );
                    Ok((state, response))
                }
                None => {
                    let response = json_response(
                        &state,
                        StatusCode::NOT_FOUND,
                        &json_error("No serving config declared for table"),
                    );
                    Ok((state, response))
                }
            },
            Ok(None) => {
                let response = json_response(
                    &state,
                    StatusCode::NOT_FOUND,
                    &json_error("Table not found"),
                );
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn put_serving_config(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let body = match parse_json_body::<ServingTableConfig>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        let (tags, dynamodb, mongodb) = match STATE_PROVIDER.describe_table(&path).await {
            Ok(Some(description)) => (description.tags, description.dynamodb, description.mongodb),
            Ok(None) => (HashMap::new(), None, None),
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                return Ok((state, response));
            }
        };

        let request = CreateTable {
            name: path.clone(),
            tags,
            serving: Some(body.clone()),
            dynamodb,
            mongodb,
        };

        match STATE_PROVIDER.upsert_table_metadata(&request).await {
            Ok(_) => {
                let response = json_response(
                    &state,
                    StatusCode::OK,
                    &ServingConfigResponse {
                        acknowledged: true,
                        table: path,
                        serving: body,
                    },
                );
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn serve_query(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let request = match parse_json_body::<ServingRequestPlan>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        match execute_serving_query(&path, request).await {
            Ok(response) => {
                let status = match response.classification {
                    ServingQueryClassification::FastPath => StatusCode::OK,
                    ServingQueryClassification::SlowPath => StatusCode::OK,
                    ServingQueryClassification::Rejected => StatusCode::UNPROCESSABLE_ENTITY,
                };
                let response = json_response(&state, status, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(&state, error.status, &json_error(&error.message));
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub async fn execute_serving_query(
    table_name: &str,
    request: ServingRequestPlan,
) -> Result<ServingQueryResponse, ServingQueryError> {
    let context = load_serving_context(table_name).await?;
    validate_request(&request, &context.schema)?;
    let bulk_cache = data_access::serving_bulk_cache_stats();

    let plan = plan_request(&context, &request)?;
    if request.explain {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: plan.classification,
            matched_pattern: plan.matched_pattern,
            snapshot_id: context.snapshot_id,
            reason: plan.reason,
            files_considered: plan.files_considered,
            files_selected: plan.files_selected,
            row_groups_considered: plan.row_groups_considered,
            row_groups_selected: plan.row_groups_selected,
            estimated_bytes: plan.estimated_bytes,
            metadata_snapshot_cached: plan.metadata_snapshot_cached,
            metadata_files_cached: plan.metadata_files_cached,
            metadata_row_groups_cached: plan.metadata_row_groups_cached,
            page_index_row_groups_selected: plan.page_index_row_groups_selected,
            bloom_filter_row_groups_selected: plan.bloom_filter_row_groups_selected,
            bulk_cache,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    if plan.classification == ServingQueryClassification::Rejected {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: plan.classification,
            matched_pattern: plan.matched_pattern,
            snapshot_id: context.snapshot_id,
            reason: plan.reason,
            files_considered: plan.files_considered,
            files_selected: plan.files_selected,
            row_groups_considered: plan.row_groups_considered,
            row_groups_selected: plan.row_groups_selected,
            estimated_bytes: plan.estimated_bytes,
            metadata_snapshot_cached: plan.metadata_snapshot_cached,
            metadata_files_cached: plan.metadata_files_cached,
            metadata_row_groups_cached: plan.metadata_row_groups_cached,
            page_index_row_groups_selected: plan.page_index_row_groups_selected,
            bloom_filter_row_groups_selected: plan.bloom_filter_row_groups_selected,
            bulk_cache,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    if plan.classification == ServingQueryClassification::SlowPath && !request.allow_slow_path {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: plan.classification,
            matched_pattern: plan.matched_pattern,
            snapshot_id: context.snapshot_id,
            reason: Some(
                "Query requires slow path. Set allow_slow_path=true to execute it".to_string(),
            ),
            files_considered: plan.files_considered,
            files_selected: plan.files_selected,
            row_groups_considered: plan.row_groups_considered,
            row_groups_selected: plan.row_groups_selected,
            estimated_bytes: plan.estimated_bytes,
            metadata_snapshot_cached: plan.metadata_snapshot_cached,
            metadata_files_cached: plan.metadata_files_cached,
            metadata_row_groups_cached: plan.metadata_row_groups_cached,
            page_index_row_groups_selected: plan.page_index_row_groups_selected,
            bloom_filter_row_groups_selected: plan.bloom_filter_row_groups_selected,
            bulk_cache,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    let mut rows = execute_plan(
        &plan.selected_files,
        &context.delete_files,
        &context.file_stats,
        &request,
        &plan.sql,
        plan.limit,
    )
    .await?;
    if request.order_by.is_empty() {
        rows.truncate(plan.limit);
    }
    let bulk_cache = data_access::serving_bulk_cache_stats();

    Ok(ServingQueryResponse {
        table: context.description.name,
        classification: plan.classification,
        matched_pattern: plan.matched_pattern,
        snapshot_id: context.snapshot_id,
        reason: plan.reason,
        files_considered: plan.files_considered,
        files_selected: plan.files_selected,
        row_groups_considered: plan.row_groups_considered,
        row_groups_selected: plan.row_groups_selected,
        estimated_bytes: plan.estimated_bytes,
        metadata_snapshot_cached: plan.metadata_snapshot_cached,
        metadata_files_cached: plan.metadata_files_cached,
        metadata_row_groups_cached: plan.metadata_row_groups_cached,
        page_index_row_groups_selected: plan.page_index_row_groups_selected,
        bloom_filter_row_groups_selected: plan.bloom_filter_row_groups_selected,
        bulk_cache,
        sql: Some(plan.sql),
        rows,
    })
}

async fn load_serving_context(
    table_name: &str,
) -> Result<ServingExecutionContext, ServingQueryError> {
    let description = match STATE_PROVIDER.describe_table(&table_name.to_string()).await {
        Ok(Some(description)) => description,
        Ok(None) => {
            return Err(ServingQueryError::new(
                StatusCode::NOT_FOUND,
                "Table not found",
            ));
        }
        Err(error) => {
            return Err(ServingQueryError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            ));
        }
    };

    let checkpoint_id = match STATE_PROVIDER
        .get_active_servable_checkpoint(&description.name)
        .await
    {
        Ok(Some(checkpoint_id)) => checkpoint_id,
        Ok(None) => {
            return Err(ServingQueryError::new(
                StatusCode::NOT_FOUND,
                "No checkpoint available for table",
            ));
        }
        Err(error) => {
            return Err(ServingQueryError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            ));
        }
    };

    let checkpoint = match STATE_PROVIDER
        .get_checkpoint(CheckpointDescriptor::new(
            description.name.clone(),
            checkpoint_id,
        ))
        .await
    {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => {
            return Err(ServingQueryError::new(
                StatusCode::NOT_FOUND,
                "Checkpoint metadata was not found",
            ));
        }
        Err(error) => {
            return Err(ServingQueryError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            ));
        }
    };

    let iceberg_metadata = match checkpoint.iceberg_metadata.clone() {
        Some(metadata) => metadata,
        None => {
            return Err(ServingQueryError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Serving queries currently require Iceberg-backed storage",
            ));
        }
    };

    let files = iceberg_metadata.files.as_file_tuples();
    let delete_files = checkpoint
        .deletes_metadata
        .as_ref()
        .map(|metadata| metadata.files.clone())
        .unwrap_or_default();
    let file_stats = iceberg_metadata
        .file_stats
        .iter()
        .cloned()
        .map(|stats| (stats.file_path.clone(), stats))
        .collect();
    let metadata_snapshot_cached = iceberg_metadata
        .snapshot_id
        .as_ref()
        .and_then(|snapshot_id| snapshot_id.parse::<i64>().ok())
        .map(|snapshot_id| {
            data_access::iceberg_table_metadata_cache_contains("default", table_name, snapshot_id)
        })
        .unwrap_or(false);
    Ok(ServingExecutionContext {
        description,
        schema: iceberg_metadata.table_schema.clone(),
        snapshot_id: iceberg_metadata.snapshot_id.clone(),
        files,
        delete_files,
        file_stats,
        metadata_snapshot_cached,
    })
}

fn plan_request(
    context: &ServingExecutionContext,
    request: &ServingRequestPlan,
) -> Result<ServingPlan, ServingQueryError> {
    let serving = context.description.serving.clone().unwrap_or_default();
    let limit = request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let sql = build_sql("{table}", request, limit, !context.delete_files.is_empty())?;

    if !request_supported(request) {
        return Ok(ServingPlan {
            classification: ServingQueryClassification::Rejected,
            matched_pattern: None,
            reason: Some("Unsupported serving query shape".to_string()),
            limit,
            sql,
            selected_files: vec![],
            files_considered: context.files.len(),
            files_selected: 0,
            row_groups_considered: 0,
            row_groups_selected: 0,
            estimated_bytes: 0,
            metadata_snapshot_cached: context.metadata_snapshot_cached,
            metadata_files_cached: 0,
            metadata_row_groups_cached: 0,
            page_index_row_groups_selected: 0,
            bloom_filter_row_groups_selected: 0,
        });
    }

    let pruned = prune_candidate_files(&context.files, &context.file_stats, request);

    if let Some(pattern) = serving
        .patterns
        .iter()
        .find(|pattern| request_matches_pattern(request, pattern, limit))
    {
        return Ok(ServingPlan {
            classification: ServingQueryClassification::FastPath,
            matched_pattern: Some(pattern.name.clone()),
            reason: None,
            limit,
            sql,
            selected_files: pruned.selected_files.clone(),
            files_considered: context.files.len(),
            files_selected: pruned.files_selected,
            row_groups_considered: pruned.row_groups_considered,
            row_groups_selected: pruned.row_groups_selected,
            estimated_bytes: pruned.estimated_bytes,
            metadata_snapshot_cached: context.metadata_snapshot_cached,
            metadata_files_cached: pruned.metadata_files_cached,
            metadata_row_groups_cached: pruned.metadata_row_groups_cached,
            page_index_row_groups_selected: pruned.page_index_row_groups_selected,
            bloom_filter_row_groups_selected: pruned.bloom_filter_row_groups_selected,
        });
    }

    Ok(ServingPlan {
        classification: ServingQueryClassification::SlowPath,
        matched_pattern: None,
        reason: Some("No declared serving pattern matched this query".to_string()),
        limit,
        sql,
        selected_files: pruned.selected_files.clone(),
        files_considered: context.files.len(),
        files_selected: pruned.files_selected,
        row_groups_considered: pruned.row_groups_considered,
        row_groups_selected: pruned.row_groups_selected,
        estimated_bytes: pruned.estimated_bytes,
        metadata_snapshot_cached: context.metadata_snapshot_cached,
        metadata_files_cached: pruned.metadata_files_cached,
        metadata_row_groups_cached: pruned.metadata_row_groups_cached,
        page_index_row_groups_selected: pruned.page_index_row_groups_selected,
        bloom_filter_row_groups_selected: pruned.bloom_filter_row_groups_selected,
    })
}

async fn execute_plan(
    files: &[FileDescriptor],
    delete_files: &[String],
    file_stats: &HashMap<String, IcebergFileStats>,
    request: &ServingRequestPlan,
    sql: &str,
    limit: usize,
) -> Result<Vec<Value>, ServingQueryError> {
    if limit == 0 {
        return Ok(vec![]);
    }

    if let Some(sort) = request.order_by.first() {
        if let Some(ordered_groups) =
            ordered_file_groups_for_top_k(files, file_stats, request, &sort.field, sort.descending)
        {
            return execute_ordered_top_k_plan(ordered_groups, delete_files, request, sql, limit)
                .await;
        }
    }

    execute_parallel_plan(files, delete_files, request, sql, limit).await
}

async fn execute_parallel_plan(
    files: &[FileDescriptor],
    delete_files: &[String],
    request: &ServingRequestPlan,
    sql: &str,
    limit: usize,
) -> Result<Vec<Value>, ServingQueryError> {
    let mut rows = vec![];
    let sql_template = sql.to_string();
    let file_groups = group_files_by_schema(files);
    let concurrency = file_groups.len().clamp(1, serving_file_parallelism());
    let delete_files = delete_files.to_vec();
    let mut results =
        stream::iter(
            file_groups.into_iter().map(|files| {
                let local_sql_template = sql_template.clone();
                let local_delete_files = delete_files.clone();
                async move {
                    execute_file_group_plan(files, &local_sql_template, &local_delete_files).await
                }
            }),
        )
        .buffer_unordered(concurrency);

    while let Some(result) = results.next().await {
        merge_rows(&mut rows, result?, request, limit);
    }

    Ok(rows)
}

async fn execute_ordered_top_k_plan(
    file_groups: Vec<OrderedFileGroup>,
    delete_files: &[String],
    request: &ServingRequestPlan,
    sql: &str,
    limit: usize,
) -> Result<Vec<Value>, ServingQueryError> {
    let mut rows = vec![];
    let Some(sort) = request.order_by.first() else {
        return Ok(rows);
    };
    let mut file_groups = file_groups.into_iter().peekable();
    let delete_files = delete_files.to_vec();

    while let Some(file_group) = file_groups.next() {
        let new_rows = execute_file_group_plan(file_group.files, sql, &delete_files).await?;
        merge_rows(&mut rows, new_rows, request, limit);

        if let Some(next_group) = file_groups.peek() {
            if remaining_groups_cannot_beat_kth_row(
                &rows,
                &next_group.best_case_sort_value,
                &sort.field,
                sort.descending,
                limit,
            ) {
                break;
            }
        }
    }

    Ok(rows)
}

fn merge_rows(
    rows: &mut Vec<Value>,
    new_rows: Vec<Value>,
    request: &ServingRequestPlan,
    limit: usize,
) {
    rows.extend(new_rows);

    if let Some(sort) = request.order_by.first() {
        rows.sort_by(|left, right| {
            compare_row_values(
                left.get(&sort.field),
                right.get(&sort.field),
                sort.descending,
            )
        });
        rows.truncate(limit);
        return;
    }

    // Bound unsorted fan-in while preserving enough slack for stable comparisons.
    let unsorted_cap = limit.saturating_mul(4).max(limit);
    if rows.len() > unsorted_cap {
        rows.truncate(unsorted_cap);
    }
}

fn serving_file_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().clamp(1, 8))
        .unwrap_or(4)
}

fn ordered_file_groups_for_top_k(
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    request: &ServingRequestPlan,
    sort_field: &str,
    descending: bool,
) -> Option<Vec<OrderedFileGroup>> {
    let mut groups = group_files_by_schema(files)
        .into_iter()
        .map(|group| {
            file_group_sort_bound(&group, file_stats, request, sort_field, descending).map(
                |bound| OrderedFileGroup {
                    files: group,
                    best_case_sort_value: bound,
                },
            )
        })
        .collect::<Option<Vec<_>>>()?;

    groups.sort_by(|left, right| {
        compare_sort_values(
            &left.best_case_sort_value,
            &right.best_case_sort_value,
            descending,
        )
    });
    Some(groups)
}

#[cfg(test)]
pub(crate) fn select_serving_warmup_files(
    serving: &ServingTableConfig,
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
) -> Vec<FileDescriptor> {
    build_serving_warmup_plan(serving, files, file_stats)
        .map(|plan| plan.selected_files)
        .unwrap_or_default()
}

pub(crate) fn build_serving_warmup_plan(
    serving: &ServingTableConfig,
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
) -> Option<ServingWarmupPlan> {
    let mut selected = vec![];
    let mut seen_paths = HashSet::new();
    let mut matched_patterns = vec![];
    let mut estimated_bytes = 0;
    let mut steps = vec![];

    for pattern in serving.patterns.iter() {
        let Some((request, limit)) = warmup_request_for_pattern(pattern) else {
            continue;
        };
        if !request_matches_pattern(&request, pattern, limit) {
            continue;
        }
        let pruned = prune_candidate_files(files, file_stats, &request);
        let Some(sort) = request.order_by.first() else {
            continue;
        };
        let Some(ordered_groups) = ordered_file_groups_for_top_k(
            &pruned.selected_files,
            file_stats,
            &request,
            &sort.field,
            sort.descending,
        ) else {
            continue;
        };
        let mut step_selected_files = vec![];
        let mut step_estimated_bytes = 0;

        for group in ordered_groups
            .into_iter()
            .take(MAX_WARMUP_FILE_GROUPS_PER_PATTERN)
        {
            for file in group.files {
                step_estimated_bytes += file.size;
                step_selected_files.push(file.clone());
                if seen_paths.insert(file.file_path.clone()) {
                    estimated_bytes += file.size;
                    selected.push(file);
                    if selected.len() >= MAX_WARMUP_FILES {
                        if !step_selected_files.is_empty() {
                            matched_patterns.push(pattern.name.clone());
                            steps.push(ServingWarmupStep {
                                pattern_name: pattern.name.clone(),
                                request: request.clone(),
                                selected_files: step_selected_files,
                                estimated_bytes: step_estimated_bytes,
                            });
                        }
                        return Some(ServingWarmupPlan {
                            matched_patterns,
                            selected_files: selected,
                            estimated_bytes,
                            steps,
                        });
                    }
                }
            }
        }

        if !step_selected_files.is_empty() {
            matched_patterns.push(pattern.name.clone());
            steps.push(ServingWarmupStep {
                pattern_name: pattern.name.clone(),
                request,
                selected_files: step_selected_files,
                estimated_bytes: step_estimated_bytes,
            });
        }
    }

    if selected.is_empty() {
        None
    } else {
        Some(ServingWarmupPlan {
            matched_patterns,
            selected_files: selected,
            estimated_bytes,
            steps,
        })
    }
}

pub(crate) async fn execute_serving_warmup_plan(
    plan: &ServingWarmupPlan,
    delete_files: &[String],
) -> Result<(), ServingQueryError> {
    for step in plan.steps.iter() {
        tracing::info!(
            pattern_name = step.pattern_name,
            estimated_bytes = step.estimated_bytes,
            files_selected = step.selected_files.len(),
            "Executing serving-shaped warmup query"
        );
        execute_serving_warmup_step(step, delete_files).await?;
    }

    Ok(())
}

async fn execute_serving_warmup_step(
    step: &ServingWarmupStep,
    delete_files: &[String],
) -> Result<(), ServingQueryError> {
    let limit = step.request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let sql = build_sql("{table}", &step.request, limit, !delete_files.is_empty())?;
    let file_groups = group_files_by_schema(&step.selected_files);
    let concurrency = file_groups.len().clamp(1, serving_file_parallelism());
    let delete_files = delete_files.to_vec();
    let mut results = stream::iter(file_groups.into_iter().map(|files| {
        let local_sql = sql.clone();
        let local_delete_files = delete_files.clone();
        async move { execute_file_group_warmup(files, &local_sql, &local_delete_files).await }
    }))
    .buffer_unordered(concurrency);

    while let Some(result) = results.next().await {
        result?;
    }

    Ok(())
}

fn file_group_sort_bound(
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    request: &ServingRequestPlan,
    sort_field: &str,
    descending: bool,
) -> Option<Value> {
    let mut best_bound: Option<Value> = None;

    for file in files {
        let candidate = file_sort_bound(file, file_stats, request, sort_field, descending)?;
        if best_bound
            .as_ref()
            .map(|best| compare_sort_values(&candidate, best, descending) == Ordering::Less)
            .unwrap_or(true)
        {
            best_bound = Some(candidate);
        }
    }

    best_bound
}

fn file_sort_bound(
    file: &FileDescriptor,
    file_stats: &HashMap<String, IcebergFileStats>,
    request: &ServingRequestPlan,
    sort_field: &str,
    descending: bool,
) -> Option<Value> {
    let stats = file_stats.get(&file.file_path)?;
    if !stats.row_groups.is_empty() {
        let matching_row_groups = stats
            .row_groups
            .iter()
            .filter(|row_group| row_group_may_match_request(row_group, request))
            .collect::<Vec<_>>();
        if matching_row_groups.is_empty() {
            return None;
        }

        let mut best_row_group_bound: Option<Value> = None;
        for row_group in matching_row_groups.iter() {
            let Some(candidate) = row_group_sort_bound(row_group, sort_field, descending) else {
                return file_level_sort_bound(stats, sort_field, descending);
            };
            if best_row_group_bound
                .as_ref()
                .map(|best| compare_sort_values(&candidate, best, descending) == Ordering::Less)
                .unwrap_or(true)
            {
                best_row_group_bound = Some(candidate);
            }
        }
        return best_row_group_bound;
    }

    file_level_sort_bound(stats, sort_field, descending)
}

fn file_level_sort_bound(
    stats: &IcebergFileStats,
    sort_field: &str,
    descending: bool,
) -> Option<Value> {
    let column_stats = stats
        .columns
        .iter()
        .find(|stats| stats.field_name == sort_field)?;

    if column_is_all_null(column_stats, stats.record_count) {
        return Some(Value::Null);
    }

    if descending {
        column_stats.upper_bound.clone()
    } else {
        column_stats.lower_bound.clone()
    }
}

fn row_group_sort_bound(
    row_group: &IcebergRowGroupStats,
    sort_field: &str,
    descending: bool,
) -> Option<Value> {
    let column_stats = row_group
        .columns
        .iter()
        .find(|stats| stats.field_name == sort_field)?;

    if column_is_all_null(column_stats, row_group.record_count) {
        return Some(Value::Null);
    }

    if descending {
        column_stats.upper_bound.clone()
    } else {
        column_stats.lower_bound.clone()
    }
}

fn remaining_groups_cannot_beat_kth_row(
    rows: &[Value],
    next_best_sort_value: &Value,
    sort_field: &str,
    descending: bool,
    limit: usize,
) -> bool {
    if limit == 0 || rows.len() < limit {
        return false;
    }

    let Some(kth_row_value) = rows.get(limit - 1).and_then(|row| row.get(sort_field)) else {
        return false;
    };

    compare_sort_values(next_best_sort_value, kth_row_value, descending) == Ordering::Greater
}

async fn execute_file_group_plan(
    files: Vec<FileDescriptor>,
    sql_template: &str,
    delete_files: &[String],
) -> Result<Vec<Value>, ServingQueryError> {
    let local_name = file_group_table_name(&files);
    let deletes_table_name = format!("serving_deletes_{}", IdInstance::next_id());
    let delete_local_tables = delete_files
        .iter()
        .map(|_| format!("serving_delete_file_{}", IdInstance::next_id()))
        .collect::<Vec<_>>();
    let file_paths = files
        .iter()
        .map(|file| file.file_path.clone())
        .collect::<Vec<_>>();
    let total_size = files.iter().map(|file| file.size).sum::<u64>();
    data_access::reserve(&local_name, total_size, vec![]).await;
    let mut created_delete_tables = vec![];
    let mut deletes_union_created = false;
    let result = async {
        load_files_as_table(&local_name, &file_paths, &files[0].schema.to_arrow_schema())
            .await
            .map_err(|error| {
                ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &error.to_string())
            })?;

        if !delete_files.is_empty() {
            let delete_schema = PowdrrSchema::deletes().to_arrow_schema();
            for (local_delete_table, delete_file_path) in
                delete_local_tables.iter().zip(delete_files.iter())
            {
                data_access::load_file_as_table(
                    local_delete_table,
                    delete_file_path,
                    false,
                    Some(delete_schema.clone()),
                )
                .await
                .map_err(|error| {
                    ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &error.to_string())
                })?;
                created_delete_tables.push(local_delete_table.clone());
            }
            let deletes_sql = create_serving_deletes_table_sql(&delete_local_tables);
            data_access::create_table(&deletes_table_name, &deletes_sql)
                .await
                .map_err(|error| {
                    ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &error.to_string())
                })?;
            deletes_union_created = true;
        }

        let local_sql = sql_template
            .replace("{table}", &local_name)
            .replace("{deletes_table}", &deletes_table_name);
        let batches = execute_sql_async(&local_sql).await.map_err(|error| {
            ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string())
        })?;
        let serde_result = batches_to_serde_value(&batches).await.map_err(|error| {
            ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.message)
        })?;
        Ok::<Vec<Value>, ServingQueryError>(serde_result.values)
    }
    .await;
    for table_name in &created_delete_tables {
        data_access::drop(table_name).await;
    }
    if deletes_union_created {
        data_access::drop(&deletes_table_name).await;
    }
    data_access::release(&local_name).await;
    result
}

async fn execute_file_group_warmup(
    files: Vec<FileDescriptor>,
    sql_template: &str,
    delete_files: &[String],
) -> Result<(), ServingQueryError> {
    let local_name = file_group_table_name(&files);
    let deletes_table_name = format!("serving_deletes_{}", IdInstance::next_id());
    let delete_local_tables = delete_files
        .iter()
        .map(|_| format!("serving_delete_file_{}", IdInstance::next_id()))
        .collect::<Vec<_>>();
    let file_paths = files
        .iter()
        .map(|file| file.file_path.clone())
        .collect::<Vec<_>>();
    let total_size = files.iter().map(|file| file.size).sum::<u64>();
    data_access::reserve(&local_name, total_size, vec![]).await;
    let mut created_delete_tables = vec![];
    let mut deletes_union_created = false;
    let result = async {
        load_files_as_table(&local_name, &file_paths, &files[0].schema.to_arrow_schema())
            .await
            .map_err(|error| {
                ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &error.to_string())
            })?;

        if !delete_files.is_empty() {
            let delete_schema = PowdrrSchema::deletes().to_arrow_schema();
            for (local_delete_table, delete_file_path) in
                delete_local_tables.iter().zip(delete_files.iter())
            {
                data_access::load_file_as_table(
                    local_delete_table,
                    delete_file_path,
                    false,
                    Some(delete_schema.clone()),
                )
                .await
                .map_err(|error| {
                    ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &error.to_string())
                })?;
                created_delete_tables.push(local_delete_table.clone());
            }
            let deletes_sql = create_serving_deletes_table_sql(&delete_local_tables);
            data_access::create_table(&deletes_table_name, &deletes_sql)
                .await
                .map_err(|error| {
                    ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &error.to_string())
                })?;
            deletes_union_created = true;
        }

        let local_sql = sql_template
            .replace("{table}", &local_name)
            .replace("{deletes_table}", &deletes_table_name);
        let _ = execute_sql_async(&local_sql).await.map_err(|error| {
            ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string())
        })?;
        Ok::<(), ServingQueryError>(())
    }
    .await;
    for table_name in &created_delete_tables {
        data_access::drop(table_name).await;
    }
    if deletes_union_created {
        data_access::drop(&deletes_table_name).await;
    }
    data_access::release(&local_name).await;
    result
}

fn create_serving_deletes_table_sql(local_names: &[String]) -> String {
    if local_names.is_empty() {
        "select null as _id_seq_no".to_string()
    } else {
        let union_selects = local_names
            .iter()
            .map(|table_name| format!("select * from {table_name}"))
            .collect::<Vec<_>>()
            .join(" union all ");
        format!("select * from ({union_selects})")
    }
}

fn group_files_by_schema(files: &[FileDescriptor]) -> Vec<Vec<FileDescriptor>> {
    let mut groups: Vec<Vec<FileDescriptor>> = vec![];

    for file in files.iter().cloned() {
        if let Some(existing_group) = groups.iter_mut().find(|group| {
            group
                .first()
                .map(|existing| existing.schema == file.schema)
                .unwrap_or(false)
        }) {
            existing_group.push(file);
        } else {
            groups.push(vec![file]);
        }
    }

    groups
}

fn file_group_table_name(files: &[FileDescriptor]) -> String {
    let mut file_paths = files
        .iter()
        .map(|file| file.file_path.clone())
        .collect::<Vec<_>>();
    file_paths.sort();

    let mut hasher = DefaultHasher::new();
    for file_path in file_paths.iter() {
        file_path.hash(&mut hasher);
    }

    format!("table_group_{:016x}", hasher.finish())
}

fn validate_request(
    request: &ServingRequestPlan,
    schema: &PowdrrSchema,
) -> Result<(), ServingQueryError> {
    let schema_map = schema.to_map();
    let mut seen_filter_fields = HashSet::new();

    if request.order_by.len() > 1 {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            "Only one ORDER BY field is supported in the serving MVP",
        ));
    }

    if let Some(select_fields) = normalized_select(request.select.clone()) {
        for field in select_fields.iter() {
            if !schema_map.contains_key(field) {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Unknown select field {}", field),
                ));
            }
        }
    }

    for sort in request.order_by.iter() {
        if !schema_map.contains_key(&sort.field) {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Unknown sort field {}", sort.field),
            ));
        }
    }

    for filter in request.filters.iter() {
        if !seen_filter_fields.insert(filter.field.clone()) {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                "Duplicate filters on the same field are not supported",
            ));
        }
        let field = match schema_map.get(&filter.field) {
            Some(field) => field,
            None => {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Unknown filter field {}", filter.field),
                ));
            }
        };
        validate_filter(filter, &field.data_type)?;
    }

    if request.limit.unwrap_or(DEFAULT_LIMIT) > MAX_LIMIT {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Limit must be <= {}", MAX_LIMIT),
        ));
    }

    Ok(())
}

fn validate_filter(
    filter: &ServingPredicate,
    data_type: &PowdrrDataType,
) -> Result<(), ServingQueryError> {
    let eq_like = usize::from(filter.eq.is_some()) + usize::from(filter.in_values.is_some());
    let range_like = usize::from(filter.gt.is_some())
        + usize::from(filter.gte.is_some())
        + usize::from(filter.lt.is_some())
        + usize::from(filter.lte.is_some());

    if eq_like == 0 && range_like == 0 {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Filter for {} has no operator", filter.field),
        ));
    }
    if eq_like > 1 || (eq_like > 0 && range_like > 0) {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Filter for {} mixes incompatible operators", filter.field),
        ));
    }
    if let Some(values) = filter.in_values.as_ref() {
        if values.is_empty() || values.len() > MAX_IN_VALUES {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!(
                    "IN filter for {} must have 1-{} values",
                    filter.field, MAX_IN_VALUES
                ),
            ));
        }
        for value in values.iter() {
            validate_literal(value, data_type, &filter.field)?;
        }
    }
    if let Some(value) = filter.eq.as_ref() {
        validate_literal(value, data_type, &filter.field)?;
    }
    for range_value in [&filter.gt, &filter.gte, &filter.lt, &filter.lte] {
        if let Some(value) = range_value.as_ref() {
            validate_literal(value, data_type, &filter.field)?;
        }
    }

    Ok(())
}

fn validate_literal(
    value: &Value,
    data_type: &PowdrrDataType,
    field_name: &str,
) -> Result<(), ServingQueryError> {
    match data_type {
        PowdrrDataType::Boolean => {
            if !value.is_boolean() {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Field {} expects a boolean literal", field_name),
                ));
            }
        }
        PowdrrDataType::Float | PowdrrDataType::Integer => {
            if !value.is_number() {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Field {} expects a numeric literal", field_name),
                ));
            }
        }
        PowdrrDataType::String => {
            if !value.is_string() {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Field {} expects a string literal", field_name),
                ));
            }
        }
        PowdrrDataType::Object(_) | PowdrrDataType::Array(_) | PowdrrDataType::Null => {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Field {} is not supported by the serving MVP", field_name),
            ));
        }
    }
    Ok(())
}

fn request_supported(request: &ServingRequestPlan) -> bool {
    request.order_by.len() <= 1
}

fn request_matches_pattern(
    request: &ServingRequestPlan,
    pattern: &ServingPattern,
    limit: usize,
) -> bool {
    if pattern
        .max_limit
        .map(|max_limit| limit > max_limit as usize)
        .unwrap_or(false)
    {
        return false;
    }

    let select_fields = normalized_select(request.select.clone());
    if let (Some(select_fields), Some(projection)) =
        (select_fields.as_ref(), pattern.projection.as_ref())
    {
        if !select_fields.iter().all(|field| projection.contains(field)) {
            return false;
        }
    } else if request.select.is_none() && pattern.projection.is_some() {
        return false;
    }

    if let Some(order_field) = pattern.order_field.as_ref() {
        match request.order_by.first() {
            Some(sort) if &sort.field == order_field && sort.descending == pattern.descending => {}
            _ => return false,
        }
    } else if !request.order_by.is_empty() {
        return false;
    }

    let eq_field_set = pattern.eq_fields.iter().cloned().collect::<HashSet<_>>();
    let mut seen_fields = HashSet::new();
    for filter in request.filters.iter() {
        seen_fields.insert(filter.field.clone());
        let is_eq = filter.eq.is_some() || filter.in_values.is_some();
        let is_range = filter.gt.is_some()
            || filter.gte.is_some()
            || filter.lt.is_some()
            || filter.lte.is_some();
        if is_eq && !eq_field_set.contains(&filter.field) {
            return false;
        }
        if is_range && pattern.range_field.as_deref() != Some(filter.field.as_str()) {
            return false;
        }
    }

    if !pattern
        .eq_fields
        .iter()
        .all(|field| seen_fields.contains(field))
    {
        return false;
    }
    if let Some(range_field) = pattern.range_field.as_ref() {
        if !seen_fields.contains(range_field) {
            return false;
        }
    }

    true
}

fn warmup_request_for_pattern(pattern: &ServingPattern) -> Option<(ServingRequestPlan, usize)> {
    let order_field = pattern.order_field.as_ref()?;
    if !pattern.eq_fields.is_empty() || pattern.range_field.is_some() {
        return None;
    }

    let limit = pattern
        .max_limit
        .unwrap_or(DEFAULT_LIMIT as u64)
        .clamp(1, MAX_LIMIT as u64) as usize;
    Some((
        ServingRequestPlan {
            select: pattern.projection.clone(),
            filters: vec![],
            order_by: vec![crate::serving_plan::ServingSort {
                field: order_field.clone(),
                descending: pattern.descending,
            }],
            limit: Some(limit),
            allow_slow_path: false,
            explain: false,
        },
        limit,
    ))
}

fn prune_candidate_files(
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    request: &ServingRequestPlan,
) -> PrunedFileSelection {
    let mut pruned = PrunedFileSelection::default();

    for file in files.iter().cloned() {
        let Some(stats) = file_stats.get(&file.file_path) else {
            pruned.estimated_bytes += file.size;
            pruned.files_selected += 1;
            record_metadata_cache_coverage(&mut pruned, &file.file_path);
            pruned.selected_files.push(file);
            continue;
        };

        if stats.row_groups.is_empty() {
            if file_may_match_request(stats, request) {
                pruned.estimated_bytes += file.size;
                pruned.files_selected += 1;
                record_metadata_cache_coverage(&mut pruned, &file.file_path);
                pruned.selected_files.push(file);
            }
            continue;
        }

        pruned.row_groups_considered += stats.row_groups.len();
        let matching_row_groups = stats
            .row_groups
            .iter()
            .filter(|row_group| row_group_may_match_request(row_group, request))
            .collect::<Vec<_>>();
        if matching_row_groups.is_empty() {
            continue;
        }

        pruned.files_selected += 1;
        pruned.row_groups_selected += matching_row_groups.len();
        pruned.estimated_bytes += matching_row_groups
            .iter()
            .map(|row_group| row_group.compressed_bytes)
            .sum::<u64>();
        record_page_pruning_coverage(&mut pruned, &matching_row_groups, request);
        record_metadata_cache_coverage(&mut pruned, &file.file_path);
        pruned.selected_files.push(file);
    }

    pruned
}

fn record_metadata_cache_coverage(pruned: &mut PrunedFileSelection, file_path: &str) {
    let coverage = data_access::cached_parquet_row_group_stats_coverage(&[file_path.to_string()]);
    pruned.metadata_files_cached += coverage.files_cached;
    pruned.metadata_row_groups_cached += coverage.row_groups_cached;
}

fn record_page_pruning_coverage(
    pruned: &mut PrunedFileSelection,
    matching_row_groups: &[&IcebergRowGroupStats],
    request: &ServingRequestPlan,
) {
    if request.filters.is_empty() {
        return;
    }

    pruned.page_index_row_groups_selected += matching_row_groups
        .iter()
        .filter(|row_group| row_group.page_index_present)
        .count();
    pruned.bloom_filter_row_groups_selected += matching_row_groups
        .iter()
        .filter(|row_group| row_group.bloom_filter_present)
        .count();
}

fn file_may_match_request(file_stats: &IcebergFileStats, request: &ServingRequestPlan) -> bool {
    request
        .filters
        .iter()
        .all(|predicate| predicate_may_match_file(file_stats, predicate))
}

fn row_group_may_match_request(
    row_group_stats: &IcebergRowGroupStats,
    request: &ServingRequestPlan,
) -> bool {
    request
        .filters
        .iter()
        .all(|predicate| predicate_may_match_row_group(row_group_stats, predicate))
}

fn predicate_may_match_file(file_stats: &IcebergFileStats, predicate: &ServingPredicate) -> bool {
    let Some(column_stats) = file_stats
        .columns
        .iter()
        .find(|stats| stats.field_name == predicate.field)
    else {
        return true;
    };

    predicate_may_match_stats(column_stats, file_stats.record_count, predicate)
}

fn predicate_may_match_row_group(
    row_group_stats: &IcebergRowGroupStats,
    predicate: &ServingPredicate,
) -> bool {
    let Some(column_stats) = row_group_stats
        .columns
        .iter()
        .find(|stats| stats.field_name == predicate.field)
    else {
        return true;
    };

    predicate_may_match_stats(column_stats, row_group_stats.record_count, predicate)
}

fn predicate_may_match_stats(
    column_stats: &IcebergColumnStats,
    record_count: Option<u64>,
    predicate: &ServingPredicate,
) -> bool {
    if let Some(eq) = predicate.eq.as_ref() {
        return equality_may_match(column_stats, record_count, eq);
    }
    if let Some(values) = predicate.in_values.as_ref() {
        return values
            .iter()
            .any(|value| equality_may_match(column_stats, record_count, value));
    }

    range_may_match(column_stats, record_count, predicate)
}

fn equality_may_match(
    column_stats: &IcebergColumnStats,
    record_count: Option<u64>,
    value: &Value,
) -> bool {
    if column_is_all_null(column_stats, record_count) {
        return false;
    }

    if let Some(lower_bound) = column_stats.lower_bound.as_ref() {
        if matches!(
            compare_scalar_values(value, lower_bound),
            Some(Ordering::Less)
        ) {
            return false;
        }
    }
    if let Some(upper_bound) = column_stats.upper_bound.as_ref() {
        if matches!(
            compare_scalar_values(value, upper_bound),
            Some(Ordering::Greater)
        ) {
            return false;
        }
    }

    true
}

fn range_may_match(
    column_stats: &IcebergColumnStats,
    record_count: Option<u64>,
    predicate: &ServingPredicate,
) -> bool {
    if column_is_all_null(column_stats, record_count) {
        return false;
    }

    if let Some(value) = predicate.gt.as_ref() {
        if let Some(upper_bound) = column_stats.upper_bound.as_ref() {
            if matches!(
                compare_scalar_values(upper_bound, value),
                Some(Ordering::Less | Ordering::Equal)
            ) {
                return false;
            }
        }
    }
    if let Some(value) = predicate.gte.as_ref() {
        if let Some(upper_bound) = column_stats.upper_bound.as_ref() {
            if matches!(
                compare_scalar_values(upper_bound, value),
                Some(Ordering::Less)
            ) {
                return false;
            }
        }
    }
    if let Some(value) = predicate.lt.as_ref() {
        if let Some(lower_bound) = column_stats.lower_bound.as_ref() {
            if matches!(
                compare_scalar_values(lower_bound, value),
                Some(Ordering::Greater | Ordering::Equal)
            ) {
                return false;
            }
        }
    }
    if let Some(value) = predicate.lte.as_ref() {
        if let Some(lower_bound) = column_stats.lower_bound.as_ref() {
            if matches!(
                compare_scalar_values(lower_bound, value),
                Some(Ordering::Greater)
            ) {
                return false;
            }
        }
    }

    true
}

fn column_is_all_null(column_stats: &IcebergColumnStats, record_count: Option<u64>) -> bool {
    match (column_stats.null_count, record_count) {
        (Some(null_count), Some(record_count)) => null_count >= record_count,
        _ => false,
    }
}

fn compare_scalar_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64()?.partial_cmp(&right.as_f64()?),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn build_sql(
    table_name: &str,
    request: &ServingRequestPlan,
    limit: usize,
    include_delete_filter: bool,
) -> Result<String, ServingQueryError> {
    let select = match normalized_select(request.select.clone()) {
        Some(select_fields) => select_fields
            .iter()
            .map(|field| format!("\"{}\"", escape_identifier(field)))
            .collect::<Vec<_>>()
            .join(", "),
        None => {
            if include_delete_filter {
                "t.*".to_string()
            } else {
                "*".to_string()
            }
        }
    };

    let mut where_clauses = request
        .filters
        .iter()
        .map(sql_for_filter)
        .collect::<Result<Vec<_>, _>>()?;
    let mut sql = format!("SELECT {} FROM {} t", select, table_name);
    if include_delete_filter {
        sql.push_str(" LEFT JOIN {deletes_table} dt ON dt._id_seq_no = t.\"_id_seq_no\"");
        where_clauses.push("dt._id_seq_no IS NULL".to_string());
    }
    if !where_clauses.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_clauses.join(" AND ")));
    }
    if let Some(sort) = request.order_by.first() {
        sql.push_str(&format!(
            " ORDER BY \"{}\" {}",
            escape_identifier(&sort.field),
            if sort.descending { "DESC" } else { "ASC" }
        ));
    }
    sql.push_str(&format!(" LIMIT {}", limit));
    Ok(sql)
}

fn sql_for_filter(filter: &ServingPredicate) -> Result<String, ServingQueryError> {
    let field = format!("\"{}\"", escape_identifier(&filter.field));
    if let Some(value) = filter.eq.as_ref() {
        return Ok(format!("{} = {}", field, sql_literal(value)?));
    }
    if let Some(values) = filter.in_values.as_ref() {
        let sql_values = values
            .iter()
            .map(sql_literal)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(format!("{} IN ({})", field, sql_values.join(", ")));
    }

    let mut parts = vec![];
    if let Some(value) = filter.gt.as_ref() {
        parts.push(format!("{} > {}", field, sql_literal(value)?));
    }
    if let Some(value) = filter.gte.as_ref() {
        parts.push(format!("{} >= {}", field, sql_literal(value)?));
    }
    if let Some(value) = filter.lt.as_ref() {
        parts.push(format!("{} < {}", field, sql_literal(value)?));
    }
    if let Some(value) = filter.lte.as_ref() {
        parts.push(format!("{} <= {}", field, sql_literal(value)?));
    }
    Ok(parts.join(" AND "))
}

fn sql_literal(value: &Value) -> Result<String, ServingQueryError> {
    match value {
        Value::String(text) => Ok(format!("'{}'", text.replace('\'', "''"))),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(if *boolean {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }),
        _ => Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            "Only scalar literals are supported in serving queries",
        )),
    }
}

fn normalized_select(select: Option<Vec<String>>) -> Option<Vec<String>> {
    match select {
        Some(fields) if fields.len() == 1 && fields[0] == "*" => None,
        Some(fields) => Some(fields),
        None => None,
    }
}

fn compare_row_values(left: Option<&Value>, right: Option<&Value>, descending: bool) -> Ordering {
    let ordering = compare_values(left.unwrap_or(&Value::Null), right.unwrap_or(&Value::Null));
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_sort_values(left: &Value, right: &Value, descending: bool) -> Ordering {
    compare_row_values(Some(left), Some(right), descending)
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => {
            if let (Some(left_number), Some(right_number)) = (left.as_f64(), right.as_f64()) {
                return left_number
                    .partial_cmp(&right_number)
                    .unwrap_or(Ordering::Equal);
            }
            if let (Some(left_string), Some(right_string)) = (left.as_str(), right.as_str()) {
                return left_string.cmp(right_string);
            }
            if let (Some(left_bool), Some(right_bool)) = (left.as_bool(), right.as_bool()) {
                return left_bool.cmp(&right_bool);
            }
            left.to_string().cmp(&right.to_string())
        }
    }
}

fn escape_identifier(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}

async fn parse_json_body<T: for<'de> Deserialize<'de>>(state: &mut State) -> Result<T, String> {
    let valid_body = body::to_bytes(Body::take_from(state))
        .await
        .map_err(|error| error.to_string())?;
    serde_json::from_slice::<T>(&valid_body).map_err(|error| error.to_string())
}

fn json_response<T: Serialize>(
    state: &State,
    status: StatusCode,
    body: &T,
) -> gotham::hyper::Response<Body> {
    create_response(
        state,
        status,
        mime::APPLICATION_JSON,
        serde_json::to_string(body).unwrap(),
    )
}

fn json_error(message: &str) -> Value {
    serde_json::json!({ "error": message })
}

#[derive(Debug)]
pub struct ServingQueryError {
    pub status: StatusCode,
    pub message: String,
}

impl ServingQueryError {
    fn new(status: StatusCode, message: &str) -> Self {
        Self {
            status,
            message: message.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ServingExecutionContext, build_serving_warmup_plan, build_sql, file_group_table_name,
        group_files_by_schema, ordered_file_groups_for_top_k, plan_request, prune_candidate_files,
        remaining_groups_cannot_beat_kth_row, request_matches_pattern, select_serving_warmup_files,
    };
    use crate::data_access::{
        prime_parquet_row_group_stats_cache_for_test, reset_serving_metadata_caches_for_test,
    };
    use crate::data_contract::{
        FileDescriptor, IcebergColumnStats, IcebergFileStats, IcebergRowGroupStats, ServingPattern,
        ServingTableConfig, TableDescription,
    };
    use crate::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema};
    use crate::serving_plan::{
        ServingPredicate, ServingQueryClassification, ServingRequestPlan, ServingSort,
    };
    use serde_json::json;
    use std::collections::HashMap;

    fn test_schema() -> PowdrrSchema {
        PowdrrSchema::from(&vec![
            PowdrrField {
                name: "score".to_string(),
                data_type: PowdrrDataType::Integer,
            },
            PowdrrField {
                name: "tenant".to_string(),
                data_type: PowdrrDataType::String,
            },
        ])
    }

    fn alternate_schema() -> PowdrrSchema {
        PowdrrSchema::from(&vec![
            PowdrrField {
                name: "score".to_string(),
                data_type: PowdrrDataType::Integer,
            },
            PowdrrField {
                name: "region".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "tenant".to_string(),
                data_type: PowdrrDataType::String,
            },
        ])
    }

    fn test_files(schema: &PowdrrSchema) -> Vec<FileDescriptor> {
        vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema: schema.clone(),
                size: 200,
            },
        ]
    }

    fn test_context(
        serving: ServingTableConfig,
        file_stats: Vec<IcebergFileStats>,
    ) -> ServingExecutionContext {
        let schema = test_schema();
        ServingExecutionContext {
            description: TableDescription {
                name: "events".to_string(),
                tags: HashMap::new(),
                serving: Some(serving),
                dynamodb: None,
                mongodb: None,
            },
            schema: schema.clone(),
            files: test_files(&schema),
            delete_files: vec![],
            file_stats: file_stats
                .into_iter()
                .map(|stats| (stats.file_path.clone(), stats))
                .collect(),
            snapshot_id: Some("snapshot_1".to_string()),
            metadata_snapshot_cached: false,
        }
    }

    fn column_stats(
        field_name: &str,
        null_count: Option<u64>,
        lower_bound: Option<serde_json::Value>,
        upper_bound: Option<serde_json::Value>,
    ) -> IcebergColumnStats {
        IcebergColumnStats {
            field_id: if field_name == "tenant" { 1 } else { 2 },
            field_name: field_name.to_string(),
            null_count,
            lower_bound,
            upper_bound,
        }
    }

    fn file_stats(
        file_path: &str,
        record_count: Option<u64>,
        columns: Vec<IcebergColumnStats>,
    ) -> IcebergFileStats {
        IcebergFileStats {
            file_path: file_path.to_string(),
            record_count,
            columns,
            row_groups: vec![],
        }
    }

    fn row_group_stats(
        row_group_index: usize,
        record_count: Option<u64>,
        compressed_bytes: u64,
        columns: Vec<IcebergColumnStats>,
    ) -> IcebergRowGroupStats {
        IcebergRowGroupStats {
            row_group_index,
            record_count,
            compressed_bytes,
            page_index_present: false,
            bloom_filter_present: false,
            columns,
        }
    }

    #[test]
    fn test_build_sql_for_range_top_n() {
        let sql = build_sql(
            "{table}",
            &ServingRequestPlan {
                select: Some(vec!["title".to_string()]),
                filters: vec![ServingPredicate {
                    field: "snippet".to_string(),
                    eq: Some(json!("hello")),
                    in_values: None,
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                }],
                order_by: vec![ServingSort {
                    field: "title".to_string(),
                    descending: false,
                }],
                limit: Some(5),
                allow_slow_path: false,
                explain: false,
            },
            5,
            false,
        )
        .unwrap();

        assert_eq!(
            sql,
            "SELECT \"title\" FROM {table} t WHERE \"snippet\" = 'hello' ORDER BY \"title\" ASC LIMIT 5"
        );
    }

    #[test]
    fn test_request_matches_pattern() {
        let request = ServingRequestPlan {
            select: Some(vec!["title".to_string()]),
            filters: vec![],
            order_by: vec![ServingSort {
                field: "title".to_string(),
                descending: false,
            }],
            limit: Some(3),
            allow_slow_path: false,
            explain: false,
        };
        let pattern = ServingPattern {
            name: "recent_titles".to_string(),
            eq_fields: vec![],
            range_field: None,
            order_field: Some("title".to_string()),
            descending: false,
            max_limit: Some(10),
            projection: Some(vec!["title".to_string()]),
        };

        assert!(request_matches_pattern(&request, &pattern, 3));
    }

    #[test]
    fn test_plan_request_prunes_files_for_fast_path_eq_query() {
        let context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_lookup".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: None,
                    descending: false,
                    max_limit: Some(25),
                    projection: None,
                }],
            },
            vec![
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "tenant",
                        Some(0),
                        Some(json!("acme")),
                        Some(json!("acme")),
                    )],
                ),
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "tenant",
                        Some(0),
                        Some(json!("omega")),
                        Some(json!("omega")),
                    )],
                ),
            ],
        );
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.classification, ServingQueryClassification::FastPath);
        assert_eq!(plan.files_considered, 2);
        assert_eq!(plan.files_selected, 1);
        assert_eq!(plan.row_groups_considered, 0);
        assert_eq!(plan.row_groups_selected, 0);
        assert_eq!(plan.estimated_bytes, 100);
        assert_eq!(
            plan.selected_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet"]
        );
    }

    #[test]
    fn test_plan_request_prunes_files_for_range_query() {
        let mut first = file_stats(
            "file://first.parquet",
            Some(20),
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(0)),
                Some(json!(100)),
            )],
        );
        first.row_groups = vec![
            row_group_stats(
                0,
                Some(10),
                25,
                vec![column_stats(
                    "score",
                    Some(0),
                    Some(json!(0)),
                    Some(json!(10)),
                )],
            ),
            row_group_stats(
                1,
                Some(10),
                25,
                vec![column_stats(
                    "score",
                    Some(0),
                    Some(json!(20)),
                    Some(json!(30)),
                )],
            ),
        ];
        let mut second = file_stats(
            "file://second.parquet",
            Some(10),
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(60)),
                Some(json!(100)),
            )],
        );
        second.row_groups = vec![row_group_stats(
            0,
            Some(10),
            40,
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(60)),
                Some(json!(100)),
            )],
        )];
        let context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "score_scan".to_string(),
                    eq_fields: vec![],
                    range_field: Some("score".to_string()),
                    order_field: None,
                    descending: false,
                    max_limit: None,
                    projection: None,
                }],
            },
            vec![first, second],
        );
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "score".to_string(),
                eq: None,
                in_values: None,
                gt: None,
                gte: Some(json!(50)),
                lt: None,
                lte: None,
            }],
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.classification, ServingQueryClassification::FastPath);
        assert_eq!(plan.files_selected, 1);
        assert_eq!(plan.row_groups_considered, 3);
        assert_eq!(plan.row_groups_selected, 1);
        assert_eq!(plan.estimated_bytes, 40);
        assert_eq!(
            plan.selected_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://second.parquet"]
        );
    }

    #[test]
    fn test_plan_request_reports_cached_row_group_metadata_coverage() {
        reset_serving_metadata_caches_for_test();

        let cached_row_groups = vec![row_group_stats(
            0,
            Some(10),
            25,
            vec![column_stats(
                "tenant",
                Some(0),
                Some(json!("acme")),
                Some(json!("acme")),
            )],
        )];
        prime_parquet_row_group_stats_cache_for_test("file://first.parquet", &cached_row_groups);

        let mut first = file_stats(
            "file://first.parquet",
            Some(10),
            vec![column_stats(
                "tenant",
                Some(0),
                Some(json!("acme")),
                Some(json!("acme")),
            )],
        );
        first.row_groups = cached_row_groups;
        let context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_lookup".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: None,
                    descending: false,
                    max_limit: Some(25),
                    projection: None,
                }],
            },
            vec![
                first,
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "tenant",
                        Some(0),
                        Some(json!("omega")),
                        Some(json!("omega")),
                    )],
                ),
            ],
        );
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.metadata_files_cached, 1);
        assert_eq!(plan.metadata_row_groups_cached, 1);

        reset_serving_metadata_caches_for_test();
    }

    #[test]
    fn test_plan_request_reports_page_and_bloom_coverage_for_selected_row_groups() {
        let mut first = file_stats(
            "file://first.parquet",
            Some(20),
            vec![column_stats(
                "tenant",
                Some(0),
                Some(json!("acme")),
                Some(json!("omega")),
            )],
        );
        first.row_groups = vec![
            IcebergRowGroupStats {
                row_group_index: 0,
                record_count: Some(10),
                compressed_bytes: 25,
                page_index_present: true,
                bloom_filter_present: true,
                columns: vec![column_stats(
                    "tenant",
                    Some(0),
                    Some(json!("acme")),
                    Some(json!("acme")),
                )],
            },
            IcebergRowGroupStats {
                row_group_index: 1,
                record_count: Some(10),
                compressed_bytes: 25,
                page_index_present: true,
                bloom_filter_present: false,
                columns: vec![column_stats(
                    "tenant",
                    Some(0),
                    Some(json!("omega")),
                    Some(json!("omega")),
                )],
            },
        ];
        let context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_lookup".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: None,
                    descending: false,
                    max_limit: Some(25),
                    projection: None,
                }],
            },
            vec![first],
        );
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.row_groups_selected, 1);
        assert_eq!(plan.page_index_row_groups_selected, 1);
        assert_eq!(plan.bloom_filter_row_groups_selected, 1);
    }

    #[test]
    fn test_prune_candidate_files_keeps_unknown_stats_and_drops_all_nulls() {
        let schema = test_schema();
        let files = test_files(&schema);
        let file_stats = HashMap::from([(
            "file://first.parquet".to_string(),
            file_stats(
                "file://first.parquet",
                Some(10),
                vec![column_stats("tenant", Some(10), None, None)],
            ),
        )]);
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let selected_files = prune_candidate_files(&files, &file_stats, &request);

        assert_eq!(
            selected_files
                .selected_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://second.parquet"]
        );
        assert_eq!(selected_files.files_selected, 1);
        assert_eq!(selected_files.row_groups_considered, 0);
        assert_eq!(selected_files.row_groups_selected, 0);
        assert_eq!(selected_files.estimated_bytes, 200);
    }

    #[test]
    fn test_group_files_by_schema_batches_compatible_files() {
        let schema = test_schema();
        let other_schema = alternate_schema();
        let files = vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema: schema.clone(),
                size: 200,
            },
            FileDescriptor {
                file_path: "file://third.parquet".to_string(),
                schema: other_schema,
                size: 300,
            },
        ];

        let groups = group_files_by_schema(&files);

        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0]
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet", "file://second.parquet"]
        );
        assert_eq!(
            groups[1]
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://third.parquet"]
        );
    }

    #[test]
    fn test_ordered_file_groups_for_top_k_sorts_groups_by_descending_upper_bound() {
        let schema = test_schema();
        let other_schema = alternate_schema();
        let files = vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema: schema.clone(),
                size: 200,
            },
            FileDescriptor {
                file_path: "file://third.parquet".to_string(),
                schema: other_schema,
                size: 300,
            },
        ];
        let mut first = file_stats(
            "file://first.parquet",
            Some(20),
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(0)),
                Some(json!(100)),
            )],
        );
        first.row_groups = vec![
            row_group_stats(
                0,
                Some(10),
                25,
                vec![
                    column_stats("tenant", Some(0), Some(json!("acme")), Some(json!("acme"))),
                    column_stats("score", Some(0), Some(json!(0)), Some(json!(20))),
                ],
            ),
            row_group_stats(
                1,
                Some(10),
                25,
                vec![
                    column_stats(
                        "tenant",
                        Some(0),
                        Some(json!("omega")),
                        Some(json!("omega")),
                    ),
                    column_stats("score", Some(0), Some(json!(90)), Some(json!(100))),
                ],
            ),
        ];
        let mut second = file_stats(
            "file://second.parquet",
            Some(10),
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(10)),
                Some(json!(80)),
            )],
        );
        second.row_groups = vec![row_group_stats(
            0,
            Some(10),
            40,
            vec![
                column_stats("tenant", Some(0), Some(json!("acme")), Some(json!("acme"))),
                column_stats("score", Some(0), Some(json!(10)), Some(json!(80))),
            ],
        )];
        let file_stats = HashMap::from([
            ("file://first.parquet".to_string(), first),
            ("file://second.parquet".to_string(), second),
            (
                "file://third.parquet".to_string(),
                file_stats(
                    "file://third.parquet",
                    Some(10),
                    vec![
                        column_stats("tenant", Some(0), Some(json!("acme")), Some(json!("acme"))),
                        column_stats("score", Some(0), Some(json!(70)), Some(json!(75))),
                    ],
                ),
            ),
        ]);
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            order_by: vec![ServingSort {
                field: "score".to_string(),
                descending: true,
            }],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let ordered_groups =
            ordered_file_groups_for_top_k(&files, &file_stats, &request, "score", true)
                .expect("score bounds should enable ordered top-k");

        assert_eq!(
            ordered_groups[0]
                .files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet", "file://second.parquet"]
        );
        assert_eq!(ordered_groups[0].best_case_sort_value, json!(80));
        assert_eq!(
            ordered_groups[1]
                .files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://third.parquet"]
        );
        assert_eq!(ordered_groups[1].best_case_sort_value, json!(75));
    }

    #[test]
    fn test_ordered_file_groups_for_top_k_returns_none_when_sort_bounds_are_missing() {
        let schema = test_schema();
        let files = test_files(&schema);
        let file_stats = HashMap::from([
            (
                "file://first.parquet".to_string(),
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(0)),
                        Some(json!(40)),
                    )],
                ),
            ),
            (
                "file://second.parquet".to_string(),
                file_stats("file://second.parquet", Some(10), vec![]),
            ),
        ]);
        let request = ServingRequestPlan {
            select: None,
            filters: vec![],
            order_by: vec![ServingSort {
                field: "score".to_string(),
                descending: true,
            }],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        assert!(
            ordered_file_groups_for_top_k(&files, &file_stats, &request, "score", true).is_none(),
            "missing score bounds should fall back to the existing parallel path"
        );
    }

    #[test]
    fn test_select_serving_warmup_files_prefers_order_only_patterns() {
        let schema = test_schema();
        let files = vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema,
                size: 200,
            },
        ];
        let file_stats = HashMap::from([
            (
                "file://first.parquet".to_string(),
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(0)),
                        Some(json!(50)),
                    )],
                ),
            ),
            (
                "file://second.parquet".to_string(),
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(60)),
                        Some(json!(100)),
                    )],
                ),
            ),
        ]);
        let selected = select_serving_warmup_files(
            &ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "top_scores".to_string(),
                    eq_fields: vec![],
                    range_field: None,
                    order_field: Some("score".to_string()),
                    descending: true,
                    max_limit: Some(10),
                    projection: None,
                }],
            },
            &files,
            &file_stats,
        );

        assert_eq!(
            selected
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet", "file://second.parquet"]
        );
    }

    #[test]
    fn test_select_serving_warmup_files_skips_patterns_that_need_filters() {
        let schema = test_schema();
        let files = test_files(&schema);
        let file_stats = HashMap::from([
            (
                "file://first.parquet".to_string(),
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(0)),
                        Some(json!(40)),
                    )],
                ),
            ),
            (
                "file://second.parquet".to_string(),
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(60)),
                        Some(json!(100)),
                    )],
                ),
            ),
        ]);
        let selected = select_serving_warmup_files(
            &ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_scores".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: Some("score".to_string()),
                    descending: true,
                    max_limit: Some(10),
                    projection: None,
                }],
            },
            &files,
            &file_stats,
        );

        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_serving_warmup_files_skips_missing_sort_bounds() {
        let schema = test_schema();
        let files = test_files(&schema);
        let file_stats = HashMap::from([(
            "file://first.parquet".to_string(),
            file_stats(
                "file://first.parquet",
                Some(10),
                vec![column_stats(
                    "score",
                    Some(0),
                    Some(json!(0)),
                    Some(json!(40)),
                )],
            ),
        )]);
        let selected = select_serving_warmup_files(
            &ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "top_scores".to_string(),
                    eq_fields: vec![],
                    range_field: None,
                    order_field: Some("score".to_string()),
                    descending: true,
                    max_limit: Some(10),
                    projection: None,
                }],
            },
            &files,
            &file_stats,
        );

        assert!(selected.is_empty());
    }

    #[test]
    fn test_build_serving_warmup_plan_reports_patterns_and_estimated_bytes() {
        let schema = test_schema();
        let files = vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema,
                size: 200,
            },
        ];
        let file_stats = HashMap::from([
            (
                "file://first.parquet".to_string(),
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(0)),
                        Some(json!(50)),
                    )],
                ),
            ),
            (
                "file://second.parquet".to_string(),
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(60)),
                        Some(json!(100)),
                    )],
                ),
            ),
        ]);

        let plan = build_serving_warmup_plan(
            &ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "top_scores".to_string(),
                    eq_fields: vec![],
                    range_field: None,
                    order_field: Some("score".to_string()),
                    descending: true,
                    max_limit: Some(10),
                    projection: None,
                }],
            },
            &files,
            &file_stats,
        )
        .expect("order-only pattern should produce a warmup plan");

        assert_eq!(plan.matched_patterns, vec!["top_scores"]);
        assert_eq!(plan.estimated_bytes, 300);
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].pattern_name, "top_scores");
        assert_eq!(plan.steps[0].estimated_bytes, 300);
        assert_eq!(plan.steps[0].request.order_by[0].field, "score".to_string());
        assert_eq!(
            plan.selected_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet", "file://second.parquet"]
        );
    }

    #[test]
    fn test_remaining_groups_cannot_beat_kth_row_descending() {
        let rows = vec![json!({ "score": 100 }), json!({ "score": 90 })];

        assert!(remaining_groups_cannot_beat_kth_row(
            &rows,
            &json!(89),
            "score",
            true,
            2,
        ));
        assert!(!remaining_groups_cannot_beat_kth_row(
            &rows,
            &json!(90),
            "score",
            true,
            2,
        ));
    }

    #[test]
    fn test_remaining_groups_cannot_beat_kth_row_ignores_zero_limit() {
        let rows = vec![json!({ "score": 100 })];

        assert!(!remaining_groups_cannot_beat_kth_row(
            &rows,
            &json!(99),
            "score",
            true,
            0,
        ));
    }

    #[test]
    fn test_file_group_table_name_is_order_independent() {
        let schema = test_schema();
        let forward = vec![
            FileDescriptor {
                file_path: "file://alpha.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://beta.parquet".to_string(),
                schema: schema.clone(),
                size: 200,
            },
        ];
        let reverse = vec![forward[1].clone(), forward[0].clone()];

        assert_eq!(
            file_group_table_name(&forward),
            file_group_table_name(&reverse)
        );
    }
}
