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
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::data_access::{self, execute_sql_async, load_files_as_table};
use crate::data_contract::{
    CreateTable, FileDescriptor, IcebergColumnStats, IcebergFileStats, ServingPattern,
    ServingTableConfig, TableDescription, TableMetadataCheckpoint,
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
    pub estimated_bytes: u64,
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
    checkpoint: TableMetadataCheckpoint,
    schema: PowdrrSchema,
    files: Vec<FileDescriptor>,
    file_stats: HashMap<String, IcebergFileStats>,
    snapshot_id: Option<String>,
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
    estimated_bytes: u64,
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

        let tags = match STATE_PROVIDER.describe_table(&path).await {
            Ok(Some(description)) => description.tags,
            Ok(None) => HashMap::new(),
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
            dynamodb: None,
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

    if context
        .checkpoint
        .deletes_metadata
        .as_ref()
        .map(|deletes| !deletes.files.is_empty())
        .unwrap_or(false)
    {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: ServingQueryClassification::Rejected,
            matched_pattern: None,
            snapshot_id: context.snapshot_id,
            reason: Some("Delete-aware serving is not implemented yet".to_string()),
            files_considered: context.files.len(),
            files_selected: 0,
            estimated_bytes: 0,
            sql: None,
            rows: vec![],
        });
    }

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
            estimated_bytes: plan.estimated_bytes,
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
            estimated_bytes: plan.estimated_bytes,
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
            estimated_bytes: plan.estimated_bytes,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    let mut rows = execute_plan(&plan.selected_files, &request, &plan.sql, plan.limit).await?;
    if request.order_by.is_empty() {
        rows.truncate(plan.limit);
    }

    Ok(ServingQueryResponse {
        table: context.description.name,
        classification: plan.classification,
        matched_pattern: plan.matched_pattern,
        snapshot_id: context.snapshot_id,
        reason: plan.reason,
        files_considered: plan.files_considered,
        files_selected: plan.files_selected,
        estimated_bytes: plan.estimated_bytes,
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
        .get_latest_checkpoint(&description.name, None)
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
    let file_stats = iceberg_metadata
        .file_stats
        .iter()
        .cloned()
        .map(|stats| (stats.file_path.clone(), stats))
        .collect();
    Ok(ServingExecutionContext {
        description,
        schema: iceberg_metadata.table_schema.clone(),
        snapshot_id: iceberg_metadata.snapshot_id.clone(),
        checkpoint,
        files,
        file_stats,
    })
}

fn plan_request(
    context: &ServingExecutionContext,
    request: &ServingRequestPlan,
) -> Result<ServingPlan, ServingQueryError> {
    let serving = context.description.serving.clone().unwrap_or_default();
    let limit = request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let sql = build_sql("{table}", request, limit)?;

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
            estimated_bytes: 0,
        });
    }

    let selected_files = prune_candidate_files(&context.files, &context.file_stats, request);
    let estimated_bytes = selected_files.iter().map(|file| file.size).sum::<u64>();

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
            selected_files: selected_files.clone(),
            files_considered: context.files.len(),
            files_selected: selected_files.len(),
            estimated_bytes,
        });
    }

    Ok(ServingPlan {
        classification: ServingQueryClassification::SlowPath,
        matched_pattern: None,
        reason: Some("No declared serving pattern matched this query".to_string()),
        limit,
        sql,
        selected_files: selected_files.clone(),
        files_considered: context.files.len(),
        files_selected: selected_files.len(),
        estimated_bytes,
    })
}

async fn execute_plan(
    files: &[FileDescriptor],
    request: &ServingRequestPlan,
    sql: &str,
    limit: usize,
) -> Result<Vec<Value>, ServingQueryError> {
    let mut rows = vec![];
    let sql_template = sql.to_string();
    let file_groups = group_files_by_schema(files);
    let concurrency = file_groups.len().clamp(1, serving_file_parallelism());
    let mut results = stream::iter(file_groups.into_iter().map(|files| {
        let local_sql_template = sql_template.clone();
        async move { execute_file_group_plan(files, &local_sql_template).await }
    }))
    .buffer_unordered(concurrency);

    while let Some(result) = results.next().await {
        merge_rows(&mut rows, result?, request, limit);
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

async fn execute_file_group_plan(
    files: Vec<FileDescriptor>,
    sql_template: &str,
) -> Result<Vec<Value>, ServingQueryError> {
    let local_name = file_group_table_name(&files);
    let file_paths = files
        .iter()
        .map(|file| file.file_path.clone())
        .collect::<Vec<_>>();
    let total_size = files.iter().map(|file| file.size).sum::<u64>();
    data_access::reserve(&local_name, total_size, vec![]).await;
    let result = async {
        load_files_as_table(&local_name, &file_paths, &files[0].schema.to_arrow_schema())
            .await
            .map_err(|error| {
                ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &error.to_string())
            })?;

        let local_sql = sql_template.replace("{table}", &local_name);
        let batches = execute_sql_async(&local_sql).await.map_err(|error| {
            ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string())
        })?;
        let serde_result = batches_to_serde_value(&batches).await.map_err(|error| {
            ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.message)
        })?;
        Ok::<Vec<Value>, ServingQueryError>(serde_result.values)
    }
    .await;
    data_access::release(&local_name).await;
    result
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

fn prune_candidate_files(
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    request: &ServingRequestPlan,
) -> Vec<FileDescriptor> {
    files
        .iter()
        .filter(|file| {
            file_stats
                .get(&file.file_path)
                .map(|stats| file_may_match_request(stats, request))
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

fn file_may_match_request(file_stats: &IcebergFileStats, request: &ServingRequestPlan) -> bool {
    request
        .filters
        .iter()
        .all(|predicate| predicate_may_match_file(file_stats, predicate))
}

fn predicate_may_match_file(file_stats: &IcebergFileStats, predicate: &ServingPredicate) -> bool {
    let Some(column_stats) = file_stats
        .columns
        .iter()
        .find(|stats| stats.field_name == predicate.field)
    else {
        return true;
    };

    if let Some(eq) = predicate.eq.as_ref() {
        return equality_may_match(column_stats, file_stats.record_count, eq);
    }
    if let Some(values) = predicate.in_values.as_ref() {
        return values
            .iter()
            .any(|value| equality_may_match(column_stats, file_stats.record_count, value));
    }

    range_may_match(column_stats, file_stats.record_count, predicate)
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
) -> Result<String, ServingQueryError> {
    let select = match normalized_select(request.select.clone()) {
        Some(select_fields) => select_fields
            .iter()
            .map(|field| format!("\"{}\"", escape_identifier(field)))
            .collect::<Vec<_>>()
            .join(", "),
        None => "*".to_string(),
    };

    let where_clauses = request
        .filters
        .iter()
        .map(sql_for_filter)
        .collect::<Result<Vec<_>, _>>()?;
    let mut sql = format!("SELECT {} FROM {} t", select, table_name);
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
        ServingExecutionContext, build_sql, file_group_table_name, group_files_by_schema,
        plan_request, prune_candidate_files, request_matches_pattern,
    };
    use crate::data_contract::{
        FileDescriptor, IcebergColumnStats, IcebergFileStats, ServingPattern, ServingTableConfig,
        TableDescription, TableMetadataCheckpoint,
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
            },
            checkpoint: TableMetadataCheckpoint::new(
                "events".to_string(),
                "checkpoint_1".to_string(),
                schema.clone(),
            ),
            schema: schema.clone(),
            files: test_files(&schema),
            file_stats: file_stats
                .into_iter()
                .map(|stats| (stats.file_path.clone(), stats))
                .collect(),
            snapshot_id: Some("snapshot_1".to_string()),
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
            vec![
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(0)),
                        Some(json!(10)),
                    )],
                ),
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
            ],
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
        assert_eq!(plan.estimated_bytes, 200);
        assert_eq!(
            plan.selected_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://second.parquet"]
        );
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
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://second.parquet"]
        );
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
