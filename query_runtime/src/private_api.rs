use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::arrow::{
    array::{
        Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array,
        Int64Array, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
        UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
};
use datafusion::error::DataFusionError;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    error::Error,
    fmt,
    sync::{LazyLock, Mutex},
};

use crate::data_access::{self, load_file_as_table};
use crate::data_contract::{
    ExtensionFile, ExtensionFileMetadata, FileDescriptor, IcebergFileStats, IcebergMetadata,
    SpeedboatMetadata, TableMetadataCheckpoint,
};
use crate::elastic_search_common::record_batches_to_ipc_stream_bytes;
use crate::elastic_search_index::create_index_inner;
use crate::elastic_search_responses::QueryResultHit;
use crate::lakehouse_serving::{
    ServingCacheManagerPlan, build_serving_cache_manager_plan,
    default_serving_cache_manager_request, execute_serving_cache_manager_plan,
};
use crate::peers::PrivatePrefetchInvocation;
use crate::peers::{
    CheckpointDescriptor, PrivateCompactionInvocation, PrivateExactConstraintGroup,
    PrivateExtensionInvocation, PrivateSearchAggregationFilterSpec,
    PrivateSearchAggregationPartial, PrivateSearchAggregationSpec,
    PrivateSearchHistogramBucketPartial, PrivateSearchInvocation, PrivateSearchRangeConstraint,
    PrivateSearchResult, PrivateSearchSortSpec, PrivateSearchTermsBucketPartial,
    PrivateSearchTermsOrderSpec, PrivateSqlInvocation,
};
use crate::prefetch::warm_iceberg_checkpoints;
use crate::query_execution::{
    QueryExecutionPlan, QueryExtensionFileSpec, QueryInputFile, QuerySqlTemplate, QueryStorageKind,
    execute_query_plan_batches,
};
use crate::query_path::{QueryPredicate, file_may_match_predicates};
use crate::runtime_bindings;
use crate::schema_massager::{PowdrrSchema, SqlQuery};
use crate::search_executor::typed_sort_projection_name;
use crate::search_runtime::array_value_to_json;
use crate::util::log_err;

static EXACT_PRUNING_SUMMARY_CACHE: LazyLock<Mutex<HashMap<String, ExactPruningSummary>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExtensionFileSpec {
    suffix: String,
    file_path: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ExactPruningFieldSummary {
    complete: bool,
    values: BTreeSet<String>,
}

type ExactPruningSummary = HashMap<String, ExactPruningFieldSummary>;

#[derive(Debug)]
pub struct PrivateApiError {
    pub message: String,
}

impl Error for PrivateApiError {}

impl fmt::Display for PrivateApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl PrivateApiError {
    fn from(source: DataFusionError) -> Self {
        PrivateApiError {
            message: format!("DataFusionError: {}", source),
        }
    }
}

#[derive(Debug)]
struct RequiredFiles {
    table_schema: PowdrrSchema,
    iceberg_files: Vec<FileDescriptor>,
    all_iceberg_files_count: usize,
    iceberg_file_extensions: Vec<Vec<ExtensionFileSpec>>,
    speedboat_files: Vec<FileDescriptor>,
    all_speedboat_files_count: usize,
    speedboat_file_extensions: Vec<Vec<ExtensionFileSpec>>,
    delete_files: Vec<String>,
}

struct FilteredFiles {
    files: Vec<FileDescriptor>,
    all_files_count: usize,
}

fn filter_iceberg(
    iceberg_metadata: &Option<IcebergMetadata>,
    index: u64,
    num: u64,
) -> FilteredFiles {
    match iceberg_metadata {
        Some(im) => {
            let filtered_files = im.files.as_selected_tuples(index, num);
            // TODO: apply filters
            FilteredFiles {
                files: filtered_files,
                all_files_count: im.files.len(),
            }
        }
        None => FilteredFiles {
            files: vec![],
            all_files_count: 0,
        },
    }
}

fn filter_speedboat(
    speedboat_metadata: &Option<SpeedboatMetadata>,
    index: u64,
    num: u64,
) -> FilteredFiles {
    match speedboat_metadata {
        Some(sm) => {
            let filtered_files = sm.files.as_selected_tuples(index, num);
            // TODO: apply filters
            FilteredFiles {
                files: filtered_files,
                all_files_count: sm.files.len(),
            }
        }
        None => FilteredFiles {
            files: vec![],
            all_files_count: 0,
        },
    }
}

async fn determine_required_files(
    required_extensions: &Vec<String>,
    checkpoints: &Vec<CheckpointDescriptor>,
    index: u64,
    num: u64,
) -> Result<RequiredFiles, PrivateApiError> {
    if required_extensions.len() > 1 || checkpoints.len() != 1 {
        return Err(PrivateApiError {
            message: "Only read for one table at a time please.".to_string(),
        });
    }

    let table_metadata = load_checkpoint_table_metadata(checkpoints).await?;
    required_files_from_table_metadata(required_extensions, &table_metadata, index, num)
}

async fn load_checkpoint_table_metadata(
    checkpoints: &Vec<CheckpointDescriptor>,
) -> Result<TableMetadataCheckpoint, PrivateApiError> {
    if checkpoints.len() != 1 {
        return Err(PrivateApiError {
            message: "Only read for one table at a time please.".to_string(),
        });
    }

    let target_checkpoint = &checkpoints[0];
    match runtime_bindings::get_checkpoint(target_checkpoint.clone()).await {
        Ok(tmc) => match tmc {
            Some(tmc) => Ok(tmc),
            None => panic!(
                "The table metadata was not found for a known checkpoint: {}",
                target_checkpoint
            ),
        },
        Err(_e) => log_err(PrivateApiError {
            message: "Error calling get checkpoint".to_string(),
        }),
    }
}

fn required_files_from_table_metadata(
    required_extensions: &Vec<String>,
    table_metadata: &TableMetadataCheckpoint,
    index: u64,
    num: u64,
) -> Result<RequiredFiles, PrivateApiError> {
    // TODO: add logic to select the iceberg and speedboat files for this host.

    let filtered_iceberg_files = filter_iceberg(&table_metadata.iceberg_metadata, index, num);
    let filtered_speedboat_files = filter_speedboat(&table_metadata.speedboat_metadata, index, num);
    let iceberg_file_extensions = filtered_iceberg_files
        .files
        .iter()
        .map(|f| get_extension_files(required_extensions, &table_metadata, &f.file_path))
        .collect::<Result<Vec<_>, _>>()?;
    let speedboat_file_extensions = filtered_speedboat_files
        .files
        .iter()
        .map(|f| get_extension_files(required_extensions, &table_metadata, &f.file_path))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RequiredFiles {
        table_schema: table_metadata.schema.clone(),
        iceberg_files: filtered_iceberg_files.files.clone(),
        all_iceberg_files_count: filtered_iceberg_files.all_files_count,
        iceberg_file_extensions,
        speedboat_files: filtered_speedboat_files.files.clone(),
        all_speedboat_files_count: filtered_speedboat_files.all_files_count,
        speedboat_file_extensions,
        delete_files: table_metadata
            .deletes_metadata
            .as_ref()
            .map_or_else(Vec::new, |d| d.files.clone()),
    })
}

async fn narrow_prefetch_files_for_serving_warmup(
    required_files: &mut RequiredFiles,
    table_metadata: &TableMetadataCheckpoint,
) -> Option<ServingCacheManagerPlan> {
    if required_files.iceberg_files.is_empty() {
        return None;
    }

    let description = match runtime_bindings::describe_table(&table_metadata.table_name).await {
        Ok(Some(description)) => description,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!(
                "Skipping serving warmup narrowing for {}: {}",
                table_metadata.table_name,
                error
            );
            return None;
        }
    };
    let Some(serving) = description.serving.as_ref() else {
        return None;
    };
    let Some(iceberg_metadata) = table_metadata.iceberg_metadata.as_ref() else {
        return None;
    };
    let file_stats = iceberg_metadata
        .file_stats
        .iter()
        .map(|stats| (stats.file_path.clone(), stats.clone()))
        .collect::<HashMap<_, _>>();
    let manager_request = default_serving_cache_manager_request(serving);
    let warmup_plan = build_serving_cache_manager_plan(
        &manager_request,
        serving,
        &required_files.iceberg_files,
        &file_stats,
        &iceberg_metadata.sort_order,
        &iceberg_metadata.access_artifacts,
    );
    if warmup_plan.warm_files.is_empty() {
        return None;
    }

    let selected_paths = warmup_plan
        .warm_files
        .iter()
        .map(|file| file.file_path.clone())
        .collect::<BTreeSet<_>>();
    let initial_selected = required_files.iceberg_files.len();
    let iceberg_files = std::mem::take(&mut required_files.iceberg_files);
    let iceberg_file_extensions = std::mem::take(&mut required_files.iceberg_file_extensions);
    for (file, extensions) in iceberg_files
        .into_iter()
        .zip(iceberg_file_extensions.into_iter())
    {
        if selected_paths.contains(&file.file_path) {
            required_files.iceberg_files.push(file);
            required_files.iceberg_file_extensions.push(extensions);
        }
    }

    tracing::info!(
        matched_patterns = ?warmup_plan.matched_patterns,
        matched_artifacts = ?warmup_plan.matched_artifacts,
        estimated_bytes = warmup_plan.estimated_warm_bytes,
        "Prefetch narrowed serving warmup for {} to {}/{} parquet files",
        table_metadata.table_name,
        required_files.iceberg_files.len(),
        initial_selected
    );
    Some(warmup_plan)
}
fn generate_required_files(
    invocation: &PrivateCompactionInvocation,
    index: u64,
    num: u64,
) -> RequiredFiles {
    let speedboat_files = invocation.speedboat_files.as_selected_tuples(index, num);

    RequiredFiles {
        table_schema: invocation.table_schema.clone(),
        iceberg_files: vec![],
        all_iceberg_files_count: 0,
        speedboat_files: speedboat_files.clone(),
        all_speedboat_files_count: speedboat_files.len(),
        iceberg_file_extensions: vec![],
        speedboat_file_extensions: speedboat_files.iter().map(|_| vec![]).collect(),
        delete_files: invocation.delete_files.clone(),
    }
}

fn get_extension_files(
    required_extensions: &Vec<String>,
    table_metadata: &TableMetadataCheckpoint,
    file_path: &String,
) -> Result<Vec<ExtensionFileSpec>, PrivateApiError> {
    if required_extensions.is_empty() {
        return Ok(vec![]);
    }

    let mut specs = vec![];
    for extension_name in required_extensions.iter() {
        let extension_files =
            get_extension_files_for_name(table_metadata, extension_name, file_path)?;
        specs.extend(extension_files.iter().map(extension_file_spec));
    }

    Ok(specs)
}

fn get_extension_files_for_name<'a>(
    table_metadata: &'a TableMetadataCheckpoint,
    extension_name: &String,
    file_path: &String,
) -> Result<&'a Vec<ExtensionFile>, PrivateApiError> {
    let descriptor = table_metadata.get_descriptor().full_name();
    let extension_metadata = table_metadata
        .extension_metadata
        .get(extension_name)
        .ok_or_else(|| PrivateApiError {
            message: format!(
                "Checkpoint {} is missing published metadata for required extension {}",
                descriptor, extension_name
            ),
        })?;

    extension_metadata
        .get(file_path)
        .ok_or_else(|| PrivateApiError {
            message: format!(
                "Checkpoint {} is missing published {} files for {}",
                descriptor, extension_name, file_path
            ),
        })
}

fn extension_file_spec(extension_file: &ExtensionFile) -> ExtensionFileSpec {
    ExtensionFileSpec {
        suffix: normalize_extension_suffix(&extension_file.suffix),
        file_path: extension_file.location.clone(),
    }
}

fn normalize_extension_suffix(suffix: &str) -> String {
    let trimmed = suffix.trim_start_matches('_');
    if trimmed.is_empty() {
        suffix.to_string()
    } else {
        trimmed.to_string()
    }
}

#[allow(dead_code)]
async fn ensure_loaded(
    file_path: &String,
    extension_files: &Vec<ExtensionFileSpec>,
    top_level_size: u64,
    parquet: bool,
    schema: Option<PowdrrSchema>,
) -> Result<String, DataFusionError> {
    let new_local_name = data_access::path_to_table_name(file_path);
    let extension_file_names = extension_files
        .iter()
        .map(|e| format!("{}_{}", &new_local_name, e.suffix))
        .collect::<Vec<String>>();
    // TODO: add in extension file sizes
    let total_size = top_level_size;

    data_access::reserve(&new_local_name, total_size, extension_file_names.clone()).await;
    // After this, on error we need to release, on OK we do not release

    match load_file_as_table(
        &new_local_name,
        file_path,
        parquet,
        schema.map(|s| s.to_arrow_schema()),
    )
    .await
    {
        Err(e) => {
            data_access::release(&new_local_name).await;
            return log_err(e);
        }
        Ok(nln) => nln,
    };

    for (spec, name) in extension_files.iter().zip(extension_file_names.iter()) {
        match load_file_as_table(&name, &spec.file_path, true, None).await {
            Err(e) => {
                data_access::release(&new_local_name).await;
                let error = format!("{}", e);
                println!("{}", error);
                return log_err(e);
            }
            _ => (),
        };
    }

    Ok(new_local_name.clone())
}

async fn ensure_loaded_extension_only(
    base_file_path: &String,
    extension_file: &ExtensionFileSpec,
    top_level_size: u64,
) -> Result<String, DataFusionError> {
    let local_name = format!(
        "{}_{}",
        data_access::path_to_table_name(base_file_path),
        extension_file.suffix
    );

    data_access::reserve(&local_name, top_level_size, vec![]).await;
    match load_file_as_table(&local_name, &extension_file.file_path, true, None).await {
        Ok(_) => Ok(local_name),
        Err(error) => {
            data_access::release(&local_name).await;
            log_err(error)
        }
    }
}

async fn execute_raw_sql(
    sql: &str,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if use_cpu_threadpool {
        data_access::execute_sql_async(&sql.to_string()).await
    } else {
        let results = data_access::execute_sql(&sql.to_string()).await?;
        match results.collect().await {
            Ok(batches) => Ok(batches),
            Err(error) => log_err(error),
        }
    }
}

fn exact_pruning_extension_file<'a>(
    extension_files: &'a [ExtensionFileSpec],
) -> Option<&'a ExtensionFileSpec> {
    extension_files
        .iter()
        .find(|extension| extension.suffix == "exact_pruning")
}

#[cfg(test)]
fn exact_pruning_summary_from_rows(rows: Vec<serde_json::Value>) -> ExactPruningSummary {
    let mut summary = ExactPruningSummary::new();
    for row in rows {
        let Some(value_map) = row.as_object() else {
            continue;
        };
        let Some(field_name) = value_map
            .get("field_name")
            .and_then(|value| value.as_str())
            .map(str::to_string)
        else {
            continue;
        };
        let complete = value_map
            .get("complete")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let entry = summary.entry(field_name).or_default();
        if entry.values.is_empty() && !entry.complete {
            entry.complete = complete;
        } else {
            entry.complete &= complete;
        }
        if let Some(field_value) = value_map
            .get("field_value")
            .and_then(|value| value.as_str())
            .map(str::to_string)
        {
            entry.values.insert(field_value);
        }
    }
    summary
}

fn exact_pruning_summary_from_batches(batches: &[RecordBatch]) -> ExactPruningSummary {
    let mut summary = ExactPruningSummary::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let Some(field_name) = batch_string_field(batch, row_index, "field_name") else {
                continue;
            };
            let complete = batch_bool_field(batch, row_index, "complete").unwrap_or(false);
            let entry = summary.entry(field_name).or_default();
            if entry.values.is_empty() && !entry.complete {
                entry.complete = complete;
            } else {
                entry.complete &= complete;
            }
            if let Some(field_value) = batch_string_field(batch, row_index, "field_value") {
                entry.values.insert(field_value);
            }
        }
    }
    summary
}

async fn load_exact_pruning_summary(
    base_file_path: &String,
    top_level_size: u64,
    extension_files: &Vec<ExtensionFileSpec>,
) -> Result<Option<ExactPruningSummary>, PrivateApiError> {
    let Some(extension_file) = exact_pruning_extension_file(extension_files) else {
        return Ok(None);
    };

    if let Some(cached) = EXACT_PRUNING_SUMMARY_CACHE
        .lock()
        .unwrap()
        .get(&extension_file.file_path)
        .cloned()
    {
        return Ok(Some(cached));
    }

    let pruning_local_name =
        ensure_loaded_extension_only(base_file_path, extension_file, top_level_size)
            .await
            .map_err(PrivateApiError::from)?;
    let sql = format!(
        "SELECT field_name, field_value, complete FROM {}",
        pruning_local_name
    );
    let batches = match execute_raw_sql(&sql, true).await {
        Ok(batches) => batches,
        Err(error) => {
            data_access::release(&pruning_local_name).await;
            return log_err(PrivateApiError::from(error));
        }
    };
    data_access::release(&pruning_local_name).await;

    let summary = exact_pruning_summary_from_batches(&batches);
    EXACT_PRUNING_SUMMARY_CACHE
        .lock()
        .unwrap()
        .insert(extension_file.file_path.clone(), summary.clone());
    Ok(Some(summary))
}

fn exact_pruning_summary_may_match(
    summary: &ExactPruningSummary,
    exact_constraints: &[PrivateExactConstraintGroup],
) -> bool {
    for constraint in exact_constraints {
        let Some(field_summary) = summary.get(&constraint.field) else {
            continue;
        };
        if field_summary.complete
            && !constraint
                .values
                .iter()
                .any(|value| field_summary.values.contains(value))
        {
            return false;
        }
    }
    true
}

fn search_query_predicates(
    exact_constraints: &[PrivateExactConstraintGroup],
    range_constraints: &[PrivateSearchRangeConstraint],
) -> Vec<QueryPredicate> {
    let mut predicates = exact_constraints
        .iter()
        .map(|constraint| QueryPredicate {
            field: constraint.field.clone(),
            eq: None,
            in_values: Some(
                constraint
                    .values
                    .iter()
                    .cloned()
                    .map(serde_json::Value::String)
                    .collect(),
            ),
            gt: None,
            gte: None,
            lt: None,
            lte: None,
        })
        .collect::<Vec<_>>();

    predicates.extend(range_constraints.iter().map(|constraint| QueryPredicate {
        field: constraint.field.clone(),
        eq: None,
        in_values: None,
        gt: constraint.gt.clone(),
        gte: constraint.gte.clone(),
        lt: constraint.lt.clone(),
        lte: constraint.lte.clone(),
    }));

    predicates
}

async fn file_may_match_search(
    file: &FileDescriptor,
    extension_files: &Vec<ExtensionFileSpec>,
    iceberg_file_stats: Option<&IcebergFileStats>,
    exact_constraints: &[PrivateExactConstraintGroup],
    range_constraints: &[PrivateSearchRangeConstraint],
    parquet: bool,
) -> Result<bool, PrivateApiError> {
    if let Some(file_stats) = iceberg_file_stats {
        if !file_may_match_predicates(
            file_stats,
            &search_query_predicates(exact_constraints, range_constraints),
        ) {
            return Ok(false);
        }
    }

    if exact_constraints.is_empty() {
        return Ok(true);
    }

    let Some(summary) = load_exact_pruning_summary(
        &file.file_path,
        if parquet { 1 } else { file.size },
        extension_files,
    )
    .await?
    else {
        return Ok(true);
    };

    Ok(exact_pruning_summary_may_match(&summary, exact_constraints))
}

async fn prune_required_files_for_search(
    required_files: &mut RequiredFiles,
    table_metadata: &TableMetadataCheckpoint,
    exact_constraints: &[PrivateExactConstraintGroup],
    range_constraints: &[PrivateSearchRangeConstraint],
) -> Result<(), PrivateApiError> {
    let iceberg_file_stats = table_metadata
        .iceberg_metadata
        .as_ref()
        .map(|metadata| {
            metadata
                .file_stats
                .iter()
                .map(|stats| (stats.file_path.clone(), stats.clone()))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();

    let mut retained_iceberg_files = vec![];
    let mut retained_iceberg_extensions = vec![];
    for (file, extensions) in required_files
        .iceberg_files
        .iter()
        .cloned()
        .zip(required_files.iceberg_file_extensions.iter().cloned())
    {
        if file_may_match_search(
            &file,
            &extensions,
            iceberg_file_stats.get(&file.file_path),
            exact_constraints,
            range_constraints,
            true,
        )
        .await?
        {
            retained_iceberg_files.push(file);
            retained_iceberg_extensions.push(extensions);
        }
    }
    required_files.iceberg_files = retained_iceberg_files;
    required_files.iceberg_file_extensions = retained_iceberg_extensions;

    let mut retained_speedboat_files = vec![];
    let mut retained_speedboat_extensions = vec![];
    for (file, extensions) in required_files
        .speedboat_files
        .iter()
        .cloned()
        .zip(required_files.speedboat_file_extensions.iter().cloned())
    {
        if file_may_match_search(
            &file,
            &extensions,
            None,
            exact_constraints,
            range_constraints,
            false,
        )
        .await?
        {
            retained_speedboat_files.push(file);
            retained_speedboat_extensions.push(extensions);
        }
    }
    required_files.speedboat_files = retained_speedboat_files;
    required_files.speedboat_file_extensions = retained_speedboat_extensions;

    Ok(())
}

pub async fn data_query(
    invocation: &PrivateSqlInvocation,
    index: u64,
    num: u64,
) -> Result<Vec<u8>, PrivateApiError> {
    let batches = data_query_batches(invocation, index, num).await?;
    record_batches_to_ipc_stream_bytes(&batches).map_err(|error| PrivateApiError {
        message: error.message,
    })
}

pub(crate) async fn data_query_batches(
    invocation: &PrivateSqlInvocation,
    index: u64,
    num: u64,
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    if invocation.checkpoints.len() == 0 {
        return Ok(vec![]);
    }

    let required_files = match determine_required_files(
        &invocation.required_extensions,
        &invocation.checkpoints,
        index,
        num,
    )
    .await
    {
        Ok(rf) => rf,
        Err(e) => return log_err(e),
    };

    let parquet_size = required_files
        .iceberg_files
        .iter()
        .map(|f| f.size)
        .sum::<u64>();
    let speedboat_size = required_files
        .speedboat_files
        .iter()
        .map(|f| f.size)
        .sum::<u64>();
    log_required_files("Query", &required_files, parquet_size, speedboat_size);

    data_query_batches_worker(&invocation.sql, &required_files, true, None).await
}

pub async fn search_query(
    invocation: &PrivateSearchInvocation,
    index: u64,
    num: u64,
) -> Result<PrivateSearchResult, PrivateApiError> {
    if invocation.checkpoints.len() == 0 {
        return Ok(PrivateSearchResult {
            hits: vec![],
            total_hits: 0,
            aggregations: vec![],
        });
    }

    let mut required_files = match determine_required_files(
        &invocation.required_extensions,
        &invocation.checkpoints,
        index,
        num,
    )
    .await
    {
        Ok(rf) => rf,
        Err(e) => {
            if invocation.exact_sql.is_some() && !invocation.calculate_score {
                match determine_required_files(&vec![], &invocation.checkpoints, index, num).await {
                    Ok(rf) => rf,
                    Err(_) => return log_err(e),
                }
            } else {
                return log_err(e);
            }
        }
    };

    if !invocation.exact_constraints.is_empty() || !invocation.range_constraints.is_empty() {
        let target_checkpoint = &invocation.checkpoints[0];
        let table_metadata = match runtime_bindings::get_checkpoint(target_checkpoint.clone()).await
        {
            Ok(Some(table_metadata)) => table_metadata,
            Ok(None) => {
                return Err(PrivateApiError {
                    message: format!("Checkpoint {} was not found", target_checkpoint),
                });
            }
            Err(error) => {
                return Err(PrivateApiError {
                    message: format!("Error loading checkpoint metadata: {}", error),
                });
            }
        };
        prune_required_files_for_search(
            &mut required_files,
            &table_metadata,
            &invocation.exact_constraints,
            &invocation.range_constraints,
        )
        .await?;
    }

    let use_exact_sql = invocation
        .exact_sql
        .as_ref()
        .is_some_and(|_| required_files_have_extension_suffix(&required_files, "exact_index"));

    let extension_suffixes = if use_exact_sql {
        Some(invocation.exact_extension_suffixes.clone())
    } else {
        Some(invocation.base_extension_suffixes.clone())
    };

    let parquet_size = required_files
        .iceberg_files
        .iter()
        .map(|f| f.size)
        .sum::<u64>();
    let speedboat_size = required_files
        .speedboat_files
        .iter()
        .map(|f| f.size)
        .sum::<u64>();
    log_required_files("Search", &required_files, parquet_size, speedboat_size);

    let sql = if use_exact_sql {
        invocation.exact_sql.as_ref().unwrap_or(&invocation.sql)
    } else {
        &invocation.sql
    };

    let batches =
        data_query_batches_worker(sql, &required_files, true, extension_suffixes.as_ref()).await?;
    let total_hits = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    if invocation.size == 0 && invocation.aggregations.is_empty() {
        return Ok(PrivateSearchResult {
            hits: vec![],
            total_hits,
            aggregations: vec![],
        });
    }

    if invocation.aggregations.is_empty() {
        let mut hit_refs = query_result_hit_refs_from_batches(
            &batches,
            invocation.calculate_score,
            &invocation.sorts,
        );

        if !invocation.sorts.is_empty() {
            if invocation.size < hit_refs.len() {
                hit_refs.select_nth_unstable_by(invocation.size, |left, right| {
                    compare_query_result_hit_refs_by_sort(left, right, &invocation.sorts)
                });
                hit_refs.truncate(invocation.size);
            }
            hit_refs.sort_by(|left, right| {
                compare_query_result_hit_refs_by_sort(left, right, &invocation.sorts)
            });
        } else if invocation.calculate_score {
            if invocation.size < hit_refs.len() {
                hit_refs
                    .select_nth_unstable_by(invocation.size, compare_query_result_hit_refs_desc);
                hit_refs.truncate(invocation.size);
            }
            hit_refs.sort_by(compare_query_result_hit_refs_desc);
        }

        hit_refs.truncate(invocation.size);
        let hits =
            query_result_hits_from_refs(&batches, &hit_refs, &Some(invocation.table.clone()))?;
        return Ok(PrivateSearchResult {
            hits,
            total_hits,
            aggregations: vec![],
        });
    }

    let aggregations =
        compute_search_aggregation_partials_from_batches(&batches, &invocation.aggregations);
    if invocation.size == 0 {
        return Ok(PrivateSearchResult {
            hits: vec![],
            total_hits,
            aggregations,
        });
    }

    let mut hit_refs =
        query_result_hit_refs_from_batches(&batches, invocation.calculate_score, &invocation.sorts);

    if !invocation.sorts.is_empty() {
        if invocation.size < hit_refs.len() {
            hit_refs.select_nth_unstable_by(invocation.size, |left, right| {
                compare_query_result_hit_refs_by_sort(left, right, &invocation.sorts)
            });
            hit_refs.truncate(invocation.size);
        }
        hit_refs.sort_by(|left, right| {
            compare_query_result_hit_refs_by_sort(left, right, &invocation.sorts)
        });
    } else if invocation.calculate_score {
        if invocation.size < hit_refs.len() {
            hit_refs.select_nth_unstable_by(invocation.size, compare_query_result_hit_refs_desc);
            hit_refs.truncate(invocation.size);
        }
        hit_refs.sort_by(compare_query_result_hit_refs_desc);
    }

    hit_refs.truncate(invocation.size);
    let hits = query_result_hits_from_refs(&batches, &hit_refs, &Some(invocation.table.clone()))?;

    Ok(PrivateSearchResult {
        hits,
        total_hits,
        aggregations,
    })
}

fn required_files_have_extension_suffix(required_files: &RequiredFiles, suffix: &str) -> bool {
    required_files
        .iceberg_file_extensions
        .iter()
        .chain(required_files.speedboat_file_extensions.iter())
        .all(|extensions| {
            extensions
                .iter()
                .any(|extension| extension.suffix.as_str() == suffix)
        })
}

#[derive(Clone)]
struct QueryResultHitRef {
    batch_index: usize,
    row_index: usize,
    id: Option<String>,
    version: u64,
    seq_no: u64,
    score: Option<f64>,
    sort: Option<Vec<SortScalar>>,
}

#[derive(Clone)]
enum SortScalar {
    Null,
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bool(bool),
}

fn query_result_hit_refs_from_batches(
    batches: &[RecordBatch],
    calculate_score: bool,
    sorts: &[PrivateSearchSortSpec],
) -> Vec<QueryResultHitRef> {
    let capacity = batches.iter().map(|batch| batch.num_rows()).sum();
    let mut hits = Vec::with_capacity(capacity);
    for (batch_index, batch) in batches.iter().enumerate() {
        for row_index in 0..batch.num_rows() {
            hits.push(query_result_hit_ref_from_batch_row(
                batch_index,
                batch,
                row_index,
                calculate_score,
                sorts,
            ));
        }
    }
    hits
}

fn query_result_hit_ref_from_batch_row(
    batch_index: usize,
    batch: &RecordBatch,
    row_index: usize,
    calculate_score: bool,
    sorts: &[PrivateSearchSortSpec],
) -> QueryResultHitRef {
    let id = batch_string_field(batch, row_index, "_id");
    let version = batch_u64_field(batch, row_index, "_version").unwrap_or_default();
    let seq_no = batch_u64_field(batch, row_index, "_seq_no").unwrap_or_default();
    let score = batch_numeric_field(batch, row_index, "score").or_else(|| {
        if calculate_score {
            batch_numeric_field(batch, row_index, "term_cnt").and_then(|term_cnt| {
                batch_numeric_field(batch, row_index, "word_cnt")
                    .map(|word_cnt| bm25_fallback_score(term_cnt, word_cnt))
            })
        } else {
            None
        }
    });
    let sort = batch_sort_values_for_row(batch, row_index, calculate_score, sorts);
    QueryResultHitRef {
        batch_index,
        row_index,
        id,
        version,
        seq_no,
        score,
        sort,
    }
}

fn query_result_hits_from_refs(
    batches: &[RecordBatch],
    hit_refs: &[QueryResultHitRef],
    index: &Option<String>,
) -> Result<Vec<QueryResultHit>, PrivateApiError> {
    hit_refs
        .iter()
        .map(|hit_ref| query_result_hit_from_ref(batches, hit_ref, index))
        .collect()
}

fn query_result_hit_from_ref(
    batches: &[RecordBatch],
    hit_ref: &QueryResultHitRef,
    index: &Option<String>,
) -> Result<QueryResultHit, PrivateApiError> {
    let batch = &batches[hit_ref.batch_index];
    let source = batch_source_value(batch, hit_ref.row_index)?;
    let has_id = hit_ref.id.is_some();
    Ok(QueryResultHit {
        _index: index.clone(),
        _id: hit_ref.id.clone(),
        _version: hit_ref.version,
        _seq_no: hit_ref.seq_no,
        _score: hit_ref.score,
        _primary_term: has_id.then_some(1),
        found: None,
        sort: hit_ref
            .sort
            .as_ref()
            .map(|values| values.iter().map(sort_scalar_to_json).collect()),
        _source: source,
    })
}

fn batch_sort_values_for_row(
    batch: &RecordBatch,
    row_index: usize,
    calculate_score: bool,
    sorts: &[PrivateSearchSortSpec],
) -> Option<Vec<SortScalar>> {
    if sorts.is_empty() {
        return None;
    }

    Some(
        sorts
            .iter()
            .map(|sort| {
                if sort.field == "_score" {
                    batch_numeric_field(batch, row_index, "score")
                        .map(SortScalar::Float)
                        .or_else(|| {
                            if calculate_score {
                                batch_numeric_field(batch, row_index, "term_cnt").and_then(
                                    |term_cnt| {
                                        batch_numeric_field(batch, row_index, "word_cnt").map(
                                            |word_cnt| {
                                                SortScalar::Float(bm25_fallback_score(
                                                    term_cnt, word_cnt,
                                                ))
                                            },
                                        )
                                    },
                                )
                            } else {
                                None
                            }
                        })
                        .unwrap_or(SortScalar::Null)
                } else {
                    batch_sort_scalar_field(
                        batch,
                        row_index,
                        &typed_sort_projection_name(&sort.field),
                    )
                    .or_else(|| batch_sort_scalar_field(batch, row_index, &sort.field))
                    .unwrap_or(SortScalar::Null)
                }
            })
            .collect(),
    )
}

fn batch_source_value(batch: &RecordBatch, row_index: usize) -> Result<Value, PrivateApiError> {
    if let Some(source) = batch_json_field(batch, row_index, "_source") {
        return match source {
            Value::String(source) => {
                serde_json::from_str(&source).map_err(|error| PrivateApiError {
                    message: format!("Could not parse _source payload: {}", error),
                })
            }
            other => Ok(other),
        };
    }

    let schema = batch.schema();
    let mut source_map = serde_json::Map::new();
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if field.name().starts_with("__powdrr_sort_")
            || matches!(
                field.name().as_str(),
                "_id" | "_version" | "_seq_no" | "_source" | "score" | "term_cnt" | "word_cnt"
            )
        {
            continue;
        }
        source_map.insert(
            field.name().clone(),
            array_value_to_json(column.as_ref(), row_index).map_err(|error| PrivateApiError {
                message: error.message,
            })?,
        );
    }
    Ok(Value::Object(source_map))
}

fn batch_json_field(batch: &RecordBatch, row_index: usize, field: &str) -> Option<Value> {
    let field_index = batch.schema().index_of(field).ok()?;
    array_value_to_json(batch.column(field_index).as_ref(), row_index).ok()
}

fn batch_sort_scalar_field(
    batch: &RecordBatch,
    row_index: usize,
    field: &str,
) -> Option<SortScalar> {
    let field_index = batch.schema().index_of(field).ok()?;
    sort_scalar_from_array(batch.column(field_index).as_ref(), row_index)
}

fn compare_query_result_hit_refs_by_sort(
    left: &QueryResultHitRef,
    right: &QueryResultHitRef,
    sorts: &[PrivateSearchSortSpec],
) -> std::cmp::Ordering {
    let left_values = left.sort.as_deref().unwrap_or(&[]);
    let right_values = right.sort.as_deref().unwrap_or(&[]);
    for (index, sort) in sorts.iter().enumerate() {
        let left_value = left_values.get(index).unwrap_or(&SortScalar::Null);
        let right_value = right_values.get(index).unwrap_or(&SortScalar::Null);
        let ordering = compare_sort_values(left_value, right_value);
        let ordering = if sort.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }

    right
        .seq_no
        .cmp(&left.seq_no)
        .then_with(|| left.id.cmp(&right.id))
}

fn compare_query_result_hit_refs_desc(
    left: &QueryResultHitRef,
    right: &QueryResultHitRef,
) -> std::cmp::Ordering {
    match (left.score, right.score) {
        (Some(left_score), Some(right_score)) => right_score
            .partial_cmp(&left_score)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
    .then_with(|| right.seq_no.cmp(&left.seq_no))
    .then_with(|| left.id.cmp(&right.id))
}

fn compare_sort_values(left: &SortScalar, right: &SortScalar) -> std::cmp::Ordering {
    match (left, right) {
        (SortScalar::Null, SortScalar::Null) => std::cmp::Ordering::Equal,
        (SortScalar::Null, _) => std::cmp::Ordering::Greater,
        (_, SortScalar::Null) => std::cmp::Ordering::Less,
        (SortScalar::Int(left_number), SortScalar::Int(right_number)) => {
            left_number.cmp(right_number)
        }
        (SortScalar::UInt(left_number), SortScalar::UInt(right_number)) => {
            left_number.cmp(right_number)
        }
        (SortScalar::Int(left_number), SortScalar::UInt(right_number)) => {
            if *left_number < 0 {
                std::cmp::Ordering::Less
            } else {
                (*left_number as u64).cmp(right_number)
            }
        }
        (SortScalar::UInt(left_number), SortScalar::Int(right_number)) => {
            if *right_number < 0 {
                std::cmp::Ordering::Greater
            } else {
                left_number.cmp(&(*right_number as u64))
            }
        }
        (left_number, right_number)
            if matches!(
                (left_number, right_number),
                (
                    SortScalar::Int(_) | SortScalar::UInt(_) | SortScalar::Float(_),
                    SortScalar::Int(_) | SortScalar::UInt(_) | SortScalar::Float(_)
                )
            ) =>
        {
            sort_scalar_numeric_as_f64(left_number)
                .partial_cmp(&sort_scalar_numeric_as_f64(right_number))
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        (SortScalar::String(left_string), SortScalar::String(right_string)) => {
            left_string.cmp(right_string)
        }
        (SortScalar::Bool(left_bool), SortScalar::Bool(right_bool)) => left_bool.cmp(right_bool),
        _ => std::cmp::Ordering::Equal,
    }
}

fn sort_scalar_from_array(array: &dyn Array, row_index: usize) -> Option<SortScalar> {
    if array.is_null(row_index) {
        return Some(SortScalar::Null);
    }
    if let Some(timestamp) = timestamp_array_value(array, row_index) {
        return Some(SortScalar::Int(timestamp));
    }
    match array.data_type() {
        DataType::Int64 => Some(SortScalar::Int(
            array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(row_index),
        )),
        DataType::Int32 => Some(SortScalar::Int(
            array
                .as_any()
                .downcast_ref::<Int32Array>()?
                .value(row_index) as i64,
        )),
        DataType::UInt64 => Some(SortScalar::UInt(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()?
                .value(row_index),
        )),
        DataType::UInt32 => Some(SortScalar::UInt(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()?
                .value(row_index) as u64,
        )),
        DataType::Float64 => Some(SortScalar::Float(
            array
                .as_any()
                .downcast_ref::<Float64Array>()?
                .value(row_index),
        )),
        DataType::Float32 => Some(SortScalar::Float(
            array
                .as_any()
                .downcast_ref::<Float32Array>()?
                .value(row_index) as f64,
        )),
        DataType::Boolean => Some(SortScalar::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()?
                .value(row_index),
        )),
        DataType::Utf8 | DataType::LargeUtf8 => {
            string_key_array_value(array, row_index).map(SortScalar::String)
        }
        DataType::Date32 => Some(SortScalar::Int(
            array
                .as_any()
                .downcast_ref::<Date32Array>()?
                .value(row_index) as i64
                * 86_400_000,
        )),
        DataType::Date64 => Some(SortScalar::Int(
            array
                .as_any()
                .downcast_ref::<Date64Array>()?
                .value(row_index),
        )),
        _ => None,
    }
}

fn sort_scalar_to_json(value: &SortScalar) -> Value {
    match value {
        SortScalar::Null => Value::Null,
        SortScalar::Int(number) => Value::from(*number),
        SortScalar::UInt(number) => Value::from(*number),
        SortScalar::Float(number) => Value::from(*number),
        SortScalar::String(string) => Value::String(string.clone()),
        SortScalar::Bool(boolean) => Value::Bool(*boolean),
    }
}

fn sort_scalar_numeric_as_f64(value: &SortScalar) -> f64 {
    match value {
        SortScalar::Int(number) => *number as f64,
        SortScalar::UInt(number) => *number as f64,
        SortScalar::Float(number) => *number,
        SortScalar::Null | SortScalar::String(_) | SortScalar::Bool(_) => {
            unreachable!("numeric helper called for non-numeric sort scalar")
        }
    }
}

fn bm25_fallback_score(term_cnt: f64, word_cnt: f64) -> f64 {
    let constant_k = 1.2;
    let constant_b = 0.75;
    let avgdl = 5.6;
    (term_cnt * (constant_k + 1.0))
        / (term_cnt + constant_k * (1.0 - constant_b + (constant_b * word_cnt / avgdl)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_contract::IcebergColumnStats;
    use std::collections::HashMap;

    fn checkpoint_with_extension_metadata(
        extension_metadata: HashMap<String, HashMap<String, Vec<ExtensionFile>>>,
    ) -> TableMetadataCheckpoint {
        TableMetadataCheckpoint {
            table_name: "table".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "checkpoint".to_string(),
            iceberg_metadata: None,
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata,
            schema: PowdrrSchema::minimal(),
        }
    }

    #[test]
    fn get_extension_files_uses_checkpoint_metadata_and_normalizes_suffixes() {
        let file_path = "s3://warehouse/table/data.parquet".to_string();
        let checkpoint = checkpoint_with_extension_metadata(HashMap::from([(
            "es".to_string(),
            HashMap::from([(
                file_path.clone(),
                vec![ExtensionFile {
                    suffix: "_search_index".to_string(),
                    location: "s3://warehouse/table/data.search_index.parquet".to_string(),
                }],
            )]),
        )]));

        let specs = get_extension_files(&vec!["es".to_string()], &checkpoint, &file_path).unwrap();

        assert_eq!(
            specs,
            vec![ExtensionFileSpec {
                suffix: "search_index".to_string(),
                file_path: "s3://warehouse/table/data.search_index.parquet".to_string(),
            }]
        );
    }

    #[test]
    fn get_extension_files_errors_when_checkpoint_lacks_required_metadata() {
        let file_path = "s3://warehouse/table/data.parquet".to_string();
        let checkpoint = checkpoint_with_extension_metadata(HashMap::new());

        let error =
            get_extension_files(&vec!["es".to_string()], &checkpoint, &file_path).unwrap_err();

        assert!(
            error
                .message
                .contains("missing published metadata for required extension es")
        );
    }

    #[test]
    fn iceberg_file_may_match_search_prunes_exact_and_range_filters() {
        let file_stats = IcebergFileStats {
            file_path: "s3://warehouse/table/data.parquet".to_string(),
            record_count: Some(10),
            columns: vec![
                IcebergColumnStats {
                    field_id: 1,
                    field_name: "service".to_string(),
                    null_count: Some(0),
                    lower_bound: Some(serde_json::Value::String("auth".to_string())),
                    upper_bound: Some(serde_json::Value::String("payments".to_string())),
                },
                IcebergColumnStats {
                    field_id: 2,
                    field_name: "@timestamp".to_string(),
                    null_count: Some(0),
                    lower_bound: Some(serde_json::Value::from(100_i64)),
                    upper_bound: Some(serde_json::Value::from(200_i64)),
                },
            ],
            partition_values: vec![],
            row_groups: vec![],
        };

        assert!(file_may_match_predicates(
            &file_stats,
            &search_query_predicates(
                &[PrivateExactConstraintGroup {
                    field: "service".to_string(),
                    values: vec!["billing".to_string(), "payments".to_string()],
                }],
                &[PrivateSearchRangeConstraint {
                    field: "@timestamp".to_string(),
                    gt: None,
                    gte: Some(serde_json::Value::from(150_i64)),
                    lt: None,
                    lte: Some(serde_json::Value::from(250_i64)),
                }],
            ),
        ));

        assert!(!file_may_match_predicates(
            &file_stats,
            &search_query_predicates(
                &[PrivateExactConstraintGroup {
                    field: "service".to_string(),
                    values: vec!["zzz".to_string()],
                }],
                &[],
            ),
        ));

        assert!(!file_may_match_predicates(
            &file_stats,
            &search_query_predicates(
                &[],
                &[PrivateSearchRangeConstraint {
                    field: "@timestamp".to_string(),
                    gt: None,
                    gte: Some(serde_json::Value::from(250_i64)),
                    lt: None,
                    lte: None,
                }],
            ),
        ));
    }

    #[test]
    fn exact_pruning_summary_may_match_only_prunes_complete_misses() {
        let summary = HashMap::from([
            (
                "service".to_string(),
                ExactPruningFieldSummary {
                    complete: true,
                    values: BTreeSet::from(["auth".to_string(), "api".to_string()]),
                },
            ),
            (
                "env".to_string(),
                ExactPruningFieldSummary {
                    complete: false,
                    values: BTreeSet::new(),
                },
            ),
        ]);

        assert!(exact_pruning_summary_may_match(
            &summary,
            &[PrivateExactConstraintGroup {
                field: "service".to_string(),
                values: vec!["api".to_string()],
            }]
        ));
        assert!(!exact_pruning_summary_may_match(
            &summary,
            &[PrivateExactConstraintGroup {
                field: "service".to_string(),
                values: vec!["payments".to_string()],
            }]
        ));
        assert!(exact_pruning_summary_may_match(
            &summary,
            &[PrivateExactConstraintGroup {
                field: "env".to_string(),
                values: vec!["prod".to_string()],
            }]
        ));
    }

    #[test]
    fn sort_scalar_to_json_preserves_integer_number_types() {
        assert_eq!(sort_scalar_to_json(&SortScalar::Int(7)), Value::from(7_i64));
        assert_eq!(
            sort_scalar_to_json(&SortScalar::UInt(9)),
            Value::from(9_u64)
        );
        assert_eq!(
            sort_scalar_to_json(&SortScalar::Float(1.5)),
            Value::from(1.5_f64)
        );
    }

    #[test]
    fn compare_sort_values_handles_mixed_integer_types() {
        assert_eq!(
            compare_sort_values(&SortScalar::Int(7), &SortScalar::UInt(8)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_sort_values(&SortScalar::UInt(8), &SortScalar::Int(7)),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_sort_values(&SortScalar::Int(-1), &SortScalar::UInt(0)),
            std::cmp::Ordering::Less
        );
    }
}

pub async fn compaction_query(
    invocation: &PrivateCompactionInvocation,
    index: u64,
    num: u64,
) -> Result<Vec<u8>, PrivateApiError> {
    let batches = compaction_query_batches(invocation, index, num).await?;
    record_batches_to_ipc_stream_bytes(&batches).map_err(|error| PrivateApiError {
        message: error.message,
    })
}

pub(crate) async fn compaction_query_batches(
    invocation: &PrivateCompactionInvocation,
    index: u64,
    num: u64,
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    let required_files = generate_required_files(invocation, index, num);
    data_query_batches_worker(&invocation.sql, &required_files, true, None).await
}

pub async fn extension_query(
    invocation: &PrivateExtensionInvocation,
    index: u64,
    num: u64,
) -> Result<ExtensionFileMetadata, PrivateApiError> {
    let iceberg_files = invocation.iceberg_files.as_selected_tuples(index, num);
    let speedboat_files = invocation.speedboat_files.as_selected_tuples(index, num);
    match create_index_inner(&iceberg_files, &speedboat_files).await {
        Ok(result) => Ok(result),
        Err(e) => Err(PrivateApiError {
            message: format!("{}", e),
        }),
    }
}

pub async fn prefetch_query(
    invocation: &PrivatePrefetchInvocation,
    index: u64,
    num: u64,
) -> Result<(), PrivateApiError> {
    if invocation.required_extensions.is_empty() {
        match warm_iceberg_checkpoints(&invocation.checkpoints).await {
            Ok(_) => {}
            Err(error) => {
                return Err(PrivateApiError {
                    message: format!("Unable to warm iceberg metadata: {}", error),
                });
            }
        }
    }

    let table_metadata = match load_checkpoint_table_metadata(&invocation.checkpoints).await {
        Ok(metadata) => metadata,
        Err(e) => return log_err(e),
    };
    let mut required_files = match required_files_from_table_metadata(
        &invocation.required_extensions,
        &table_metadata,
        index,
        num,
    ) {
        Ok(rf) => rf,
        Err(e) => return log_err(e),
    };
    let files_considered = required_files.iceberg_files.len();
    let targeted_warmup_plan = if invocation.required_extensions.is_empty() {
        narrow_prefetch_files_for_serving_warmup(&mut required_files, &table_metadata).await
    } else {
        None
    };
    if let Some(plan) = targeted_warmup_plan.as_ref() {
        execute_serving_cache_manager_plan(plan, &required_files.delete_files)
            .await
            .map_err(|error| PrivateApiError {
                message: error.message,
            })?;
    } else {
        data_query_batches_worker(&SqlQuery::dummy(), &required_files, false, None).await?;
    }
    data_access::flush_serving_bulk_cache()
        .await
        .map_err(|message| PrivateApiError { message })?;
    let snapshot_id = table_metadata
        .iceberg_metadata
        .as_ref()
        .and_then(|metadata| metadata.snapshot_id.clone());
    if let Some(plan) = targeted_warmup_plan.as_ref() {
        data_access::record_serving_cache_manager_operation(
            data_access::ServingCacheManagerOperationStats {
                table: table_metadata.table_name.clone(),
                snapshot_id: snapshot_id.clone(),
                warmed_files: plan.warm_files.len(),
                evicted_files: plan.files_to_evict.len(),
                targeted_ranges: plan.targeted_ranges,
                matched_patterns: plan.matched_patterns.clone(),
                matched_artifacts: plan.matched_artifacts.clone(),
                metadata_refreshed: plan.metadata_refreshed,
                bulk_cache_flushed: true,
                bulk_cache_reset: plan.bulk_cache_reset,
            },
        );
    }
    data_access::record_serving_bulk_cache_warmup(data_access::ServingBulkCacheWarmupStats {
        table: table_metadata.table_name,
        snapshot_id,
        targeted: targeted_warmup_plan.is_some(),
        matched_patterns: targeted_warmup_plan
            .as_ref()
            .map(|plan| plan.matched_patterns.clone())
            .unwrap_or_default(),
        shaped_queries: targeted_warmup_plan
            .as_ref()
            .map(|plan| plan.warmup_steps.len())
            .unwrap_or(0),
        files_considered,
        files_selected: required_files.iceberg_files.len(),
        estimated_bytes: targeted_warmup_plan
            .as_ref()
            .map(|plan| plan.estimated_warm_bytes)
            .unwrap_or_else(|| {
                required_files
                    .iceberg_files
                    .iter()
                    .map(|file| file.size)
                    .sum()
            }),
    });
    Ok(())
}

fn log_required_files(
    label: &str,
    required_files: &RequiredFiles,
    parquet_size: u64,
    speedboat_size: u64,
) {
    tracing::info!(
        "{}: parquet = {}/{}, {}, speedboat = {}/{}, {}",
        label,
        required_files.iceberg_files.len(),
        required_files.all_iceberg_files_count,
        parquet_size,
        required_files.speedboat_files.len(),
        required_files.all_speedboat_files_count,
        speedboat_size
    );
}

#[derive(Clone, Copy)]
struct BatchRowRef {
    batch_index: usize,
    row_index: usize,
}

fn compute_search_aggregation_partials_from_batches(
    batches: &[RecordBatch],
    specs: &[PrivateSearchAggregationSpec],
) -> Vec<PrivateSearchAggregationPartial> {
    let rows = all_batch_rows(batches);
    compute_search_aggregation_partials(batches, &rows, specs)
}

fn all_batch_rows(batches: &[RecordBatch]) -> Vec<BatchRowRef> {
    let capacity = batches.iter().map(|batch| batch.num_rows()).sum();
    let mut rows = Vec::with_capacity(capacity);
    for (batch_index, batch) in batches.iter().enumerate() {
        for row_index in 0..batch.num_rows() {
            rows.push(BatchRowRef {
                batch_index,
                row_index,
            });
        }
    }
    rows
}

fn compute_search_aggregation_partials(
    batches: &[RecordBatch],
    rows: &[BatchRowRef],
    specs: &[PrivateSearchAggregationSpec],
) -> Vec<PrivateSearchAggregationPartial> {
    specs
        .iter()
        .map(|spec| compute_search_aggregation_partial(batches, rows, spec))
        .collect()
}

fn compute_search_aggregation_partial(
    batches: &[RecordBatch],
    rows: &[BatchRowRef],
    spec: &PrivateSearchAggregationSpec,
) -> PrivateSearchAggregationPartial {
    match spec {
        PrivateSearchAggregationSpec::Average { name, field } => {
            let mut sum = 0.0;
            let mut count = 0_u64;
            for row in rows.iter() {
                if let Some(value) = extract_numeric_field_from_batch_row(
                    &batches[row.batch_index],
                    row.row_index,
                    field,
                ) {
                    sum += value;
                    count += 1;
                }
            }
            PrivateSearchAggregationPartial::Average {
                name: name.clone(),
                sum,
                count,
            }
        }
        PrivateSearchAggregationSpec::Cardinality { name, field } => {
            let values = rows
                .iter()
                .filter_map(|row| {
                    extract_term_key_from_batch_row(&batches[row.batch_index], row.row_index, field)
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            PrivateSearchAggregationPartial::Cardinality {
                name: name.clone(),
                values,
            }
        }
        PrivateSearchAggregationSpec::DateHistogram {
            name,
            field,
            fixed_interval,
            min_doc_count: _,
            extended_bounds: _,
            sub_aggregations,
        } => {
            let Some(interval_ms) = parse_fixed_interval_millis(fixed_interval) else {
                return PrivateSearchAggregationPartial::DateHistogram {
                    name: name.clone(),
                    buckets: vec![],
                };
            };

            if sub_aggregations.is_empty() {
                let mut bucket_counts = BTreeMap::<i64, u64>::new();
                for row in rows.iter() {
                    if let Some(timestamp_ms) = extract_timestamp_millis_from_batch_row(
                        &batches[row.batch_index],
                        row.row_index,
                        field,
                    ) {
                        let bucket_key = timestamp_ms - timestamp_ms.rem_euclid(interval_ms);
                        *bucket_counts.entry(bucket_key).or_default() += 1;
                    }
                }

                let buckets = bucket_counts
                    .into_iter()
                    .map(
                        |(bucket_key, doc_count)| PrivateSearchHistogramBucketPartial {
                            key: bucket_key,
                            key_as_string: timestamp_millis_to_key_as_string(bucket_key),
                            doc_count,
                            sub_aggregations: vec![],
                        },
                    )
                    .collect::<Vec<_>>();

                return PrivateSearchAggregationPartial::DateHistogram {
                    name: name.clone(),
                    buckets,
                };
            }

            let mut buckets = BTreeMap::<i64, Vec<BatchRowRef>>::new();
            for row in rows.iter() {
                if let Some(timestamp_ms) = extract_timestamp_millis_from_batch_row(
                    &batches[row.batch_index],
                    row.row_index,
                    field,
                ) {
                    let bucket_key = timestamp_ms - timestamp_ms.rem_euclid(interval_ms);
                    buckets.entry(bucket_key).or_default().push(*row);
                }
            }

            let buckets = buckets
                .into_iter()
                .map(
                    |(bucket_key, bucket_rows)| PrivateSearchHistogramBucketPartial {
                        key: bucket_key,
                        key_as_string: timestamp_millis_to_key_as_string(bucket_key),
                        doc_count: bucket_rows.len() as u64,
                        sub_aggregations: compute_search_aggregation_partials(
                            batches,
                            &bucket_rows,
                            sub_aggregations,
                        ),
                    },
                )
                .collect::<Vec<_>>();

            PrivateSearchAggregationPartial::DateHistogram {
                name: name.clone(),
                buckets,
            }
        }
        PrivateSearchAggregationSpec::Terms {
            name,
            field,
            size: _,
            order,
            missing,
            sub_aggregations,
        } => {
            if sub_aggregations.is_empty() {
                let mut bucket_counts = std::collections::HashMap::<String, u64>::new();
                for row in rows.iter() {
                    if let Some(key) = extract_term_key_from_batch_row(
                        &batches[row.batch_index],
                        row.row_index,
                        field,
                    )
                    .or_else(|| missing.as_ref().and_then(render_missing_term_key))
                    {
                        *bucket_counts.entry(key).or_default() += 1;
                    }
                }

                let mut bucket_partials = bucket_counts
                    .into_iter()
                    .map(|(key, doc_count)| PrivateSearchTermsBucketPartial {
                        doc_count,
                        sub_aggregations: vec![],
                        key,
                    })
                    .collect::<Vec<_>>();
                bucket_partials.sort_by(|left, right| {
                    compare_terms_bucket_partials(left, right, order.as_ref())
                });

                return PrivateSearchAggregationPartial::Terms {
                    name: name.clone(),
                    buckets: bucket_partials,
                };
            }

            let mut buckets = std::collections::HashMap::<String, Vec<BatchRowRef>>::new();
            for row in rows.iter() {
                if let Some(key) =
                    extract_term_key_from_batch_row(&batches[row.batch_index], row.row_index, field)
                        .or_else(|| missing.as_ref().and_then(render_missing_term_key))
                {
                    buckets.entry(key).or_default().push(*row);
                }
            }

            let mut bucket_partials = buckets
                .into_iter()
                .map(|(key, bucket_rows)| PrivateSearchTermsBucketPartial {
                    doc_count: bucket_rows.len() as u64,
                    sub_aggregations: compute_search_aggregation_partials(
                        batches,
                        &bucket_rows,
                        sub_aggregations,
                    ),
                    key,
                })
                .collect::<Vec<_>>();
            bucket_partials
                .sort_by(|left, right| compare_terms_bucket_partials(left, right, order.as_ref()));

            PrivateSearchAggregationPartial::Terms {
                name: name.clone(),
                buckets: bucket_partials,
            }
        }
        PrivateSearchAggregationSpec::Filter {
            name,
            filter,
            sub_aggregations,
        } => {
            if sub_aggregations.is_empty() {
                let doc_count = rows
                    .iter()
                    .filter(|row| {
                        row_matches_aggregation_filter_batch_row(
                            &batches[row.batch_index],
                            row.row_index,
                            filter,
                        )
                    })
                    .count() as u64;

                return PrivateSearchAggregationPartial::Filter {
                    name: name.clone(),
                    doc_count,
                    sub_aggregations: vec![],
                };
            }

            let filtered_rows = rows
                .iter()
                .copied()
                .filter(|row| {
                    row_matches_aggregation_filter_batch_row(
                        &batches[row.batch_index],
                        row.row_index,
                        filter,
                    )
                })
                .collect::<Vec<_>>();
            let sub_aggregations =
                compute_search_aggregation_partials(batches, &filtered_rows, sub_aggregations);
            PrivateSearchAggregationPartial::Filter {
                name: name.clone(),
                doc_count: filtered_rows.len() as u64,
                sub_aggregations,
            }
        }
    }
}

fn row_matches_aggregation_filter_batch_row(
    batch: &RecordBatch,
    row_index: usize,
    filter: &PrivateSearchAggregationFilterSpec,
) -> bool {
    match filter {
        PrivateSearchAggregationFilterSpec::Term { field, value } => {
            extract_term_key_from_batch_row(batch, row_index, field)
                .map(|field_value| field_value == *value)
                .unwrap_or(false)
        }
    }
}

fn extract_numeric_field_from_batch_row(
    batch: &RecordBatch,
    row_index: usize,
    field: &str,
) -> Option<f64> {
    let field_index = batch.schema().index_of(field).ok()?;
    numeric_array_value(batch.column(field_index).as_ref(), row_index)
}

fn extract_timestamp_millis_from_batch_row(
    batch: &RecordBatch,
    row_index: usize,
    field: &str,
) -> Option<i64> {
    let field_index = batch.schema().index_of(field).ok()?;
    timestamp_array_value(batch.column(field_index).as_ref(), row_index)
}

fn parse_fixed_interval_millis(interval: &str) -> Option<i64> {
    if interval.len() < 2 {
        return None;
    }
    let (value, unit) = interval.split_at(interval.len() - 1);
    let quantity = value.parse::<i64>().ok()?;
    let multiplier = match unit {
        "s" => 1_000,
        "m" => 60 * 1_000,
        "h" => 60 * 60 * 1_000,
        "d" => 24 * 60 * 60 * 1_000,
        "w" => 7 * 24 * 60 * 60 * 1_000,
        _ => return None,
    };
    quantity.checked_mul(multiplier)
}

fn render_missing_term_key(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(|text| text.to_string())
        .or_else(|| value.as_i64().map(|numeric| numeric.to_string()))
        .or_else(|| value.as_u64().map(|numeric| numeric.to_string()))
        .or_else(|| value.as_f64().map(|numeric| numeric.to_string()))
        .or_else(|| value.as_bool().map(|boolean| boolean.to_string()))
}

fn compare_terms_bucket_partials(
    left: &PrivateSearchTermsBucketPartial,
    right: &PrivateSearchTermsBucketPartial,
    order: Option<&PrivateSearchTermsOrderSpec>,
) -> std::cmp::Ordering {
    match order.unwrap_or(&PrivateSearchTermsOrderSpec::CountDesc) {
        PrivateSearchTermsOrderSpec::CountAsc => left
            .doc_count
            .cmp(&right.doc_count)
            .then_with(|| left.key.cmp(&right.key)),
        PrivateSearchTermsOrderSpec::CountDesc => right
            .doc_count
            .cmp(&left.doc_count)
            .then_with(|| left.key.cmp(&right.key)),
        PrivateSearchTermsOrderSpec::KeyAsc => left.key.cmp(&right.key),
        PrivateSearchTermsOrderSpec::KeyDesc => right.key.cmp(&left.key),
    }
}

fn timestamp_millis_to_key_as_string(timestamp_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn extract_term_key_from_batch_row(
    batch: &RecordBatch,
    row_index: usize,
    field: &str,
) -> Option<String> {
    let field_index = batch.schema().index_of(field).ok()?;
    string_key_array_value(batch.column(field_index).as_ref(), row_index)
}

fn batch_string_field(batch: &RecordBatch, row_index: usize, field: &str) -> Option<String> {
    let field_index = batch.schema().index_of(field).ok()?;
    string_key_array_value(batch.column(field_index).as_ref(), row_index)
}

fn batch_bool_field(batch: &RecordBatch, row_index: usize, field: &str) -> Option<bool> {
    let field_index = batch.schema().index_of(field).ok()?;
    let array = batch.column(field_index).as_ref();
    if array.is_null(row_index) {
        return None;
    }
    match array.data_type() {
        DataType::Boolean => Some(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()?
                .value(row_index),
        ),
        _ => None,
    }
}

fn batch_u64_field(batch: &RecordBatch, row_index: usize, field: &str) -> Option<u64> {
    let field_index = batch.schema().index_of(field).ok()?;
    u64_array_value(batch.column(field_index).as_ref(), row_index)
}

fn batch_numeric_field(batch: &RecordBatch, row_index: usize, field: &str) -> Option<f64> {
    let field_index = batch.schema().index_of(field).ok()?;
    numeric_array_value(batch.column(field_index).as_ref(), row_index)
}

fn numeric_array_value(array: &dyn Array, row_index: usize) -> Option<f64> {
    if array.is_null(row_index) {
        return None;
    }
    match array.data_type() {
        DataType::Float64 => Some(
            array
                .as_any()
                .downcast_ref::<Float64Array>()?
                .value(row_index),
        ),
        DataType::Float32 => Some(
            array
                .as_any()
                .downcast_ref::<Float32Array>()?
                .value(row_index) as f64,
        ),
        DataType::Int64 => Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(row_index) as f64,
        ),
        DataType::Int32 => Some(
            array
                .as_any()
                .downcast_ref::<Int32Array>()?
                .value(row_index) as f64,
        ),
        DataType::UInt64 => Some(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()?
                .value(row_index) as f64,
        ),
        DataType::UInt32 => Some(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()?
                .value(row_index) as f64,
        ),
        _ => None,
    }
}

fn u64_array_value(array: &dyn Array, row_index: usize) -> Option<u64> {
    if array.is_null(row_index) {
        return None;
    }
    match array.data_type() {
        DataType::UInt64 => Some(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()?
                .value(row_index),
        ),
        DataType::UInt32 => Some(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()?
                .value(row_index) as u64,
        ),
        DataType::Int64 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(row_index),
        )
        .ok(),
        DataType::Int32 => u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()?
                .value(row_index),
        )
        .ok(),
        _ => None,
    }
}

fn timestamp_array_value(array: &dyn Array, row_index: usize) -> Option<i64> {
    if array.is_null(row_index) {
        return None;
    }
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()?
                .value(row_index)
                * 1_000,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()?
                .value(row_index),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?
                .value(row_index)
                / 1_000,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()?
                .value(row_index)
                / 1_000_000,
        ),
        DataType::Date64 => Some(
            array
                .as_any()
                .downcast_ref::<Date64Array>()?
                .value(row_index),
        ),
        DataType::Date32 => Some(
            array
                .as_any()
                .downcast_ref::<Date32Array>()?
                .value(row_index) as i64
                * 86_400_000,
        ),
        DataType::Int64 => Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(row_index),
        ),
        DataType::UInt64 => i64::try_from(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()?
                .value(row_index),
        )
        .ok(),
        DataType::Utf8 => DateTime::parse_from_rfc3339(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(row_index),
        )
        .ok()
        .map(|datetime| datetime.with_timezone(&Utc).timestamp_millis()),
        DataType::LargeUtf8 => DateTime::parse_from_rfc3339(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()?
                .value(row_index),
        )
        .ok()
        .map(|datetime| datetime.with_timezone(&Utc).timestamp_millis()),
        _ => None,
    }
}

fn string_key_array_value(array: &dyn Array, row_index: usize) -> Option<String> {
    if array.is_null(row_index) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(row_index)
                .to_string(),
        ),
        DataType::LargeUtf8 => Some(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()?
                .value(row_index)
                .to_string(),
        ),
        DataType::Int64 => Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(row_index)
                .to_string(),
        ),
        DataType::Int32 => Some(
            array
                .as_any()
                .downcast_ref::<Int32Array>()?
                .value(row_index)
                .to_string(),
        ),
        DataType::UInt64 => Some(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()?
                .value(row_index)
                .to_string(),
        ),
        DataType::UInt32 => Some(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()?
                .value(row_index)
                .to_string(),
        ),
        DataType::Float64 => Some(
            array
                .as_any()
                .downcast_ref::<Float64Array>()?
                .value(row_index)
                .to_string(),
        ),
        DataType::Float32 => Some(
            array
                .as_any()
                .downcast_ref::<Float32Array>()?
                .value(row_index)
                .to_string(),
        ),
        DataType::Boolean => Some(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()?
                .value(row_index)
                .to_string(),
        ),
        _ => None,
    }
}

async fn data_query_batches_worker(
    sql: &SqlQuery,
    required_files: &RequiredFiles,
    use_cpu_threadpool: bool,
    extension_suffixes: Option<&Vec<String>>,
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    let iceberg_query_files = required_files
        .iceberg_files
        .iter()
        .cloned()
        .zip(required_files.iceberg_file_extensions.iter().cloned())
        .map(|(file, extensions)| QueryInputFile {
            file,
            storage: QueryStorageKind::Iceberg,
            extensions: extensions
                .into_iter()
                .map(|extension| QueryExtensionFileSpec {
                    suffix: extension.suffix,
                    file_path: extension.file_path,
                })
                .collect(),
        });
    let speedboat_query_files = required_files
        .speedboat_files
        .iter()
        .cloned()
        .zip(required_files.speedboat_file_extensions.iter().cloned())
        .map(|(file, extensions)| QueryInputFile {
            file,
            storage: QueryStorageKind::Speedboat,
            extensions: extensions
                .into_iter()
                .map(|extension| QueryExtensionFileSpec {
                    suffix: extension.suffix,
                    file_path: extension.file_path,
                })
                .collect(),
        });

    let plan = QueryExecutionPlan {
        sql: QuerySqlTemplate::Structured {
            sql: sql.clone(),
            table_schema: required_files.table_schema.clone(),
        },
        files: iceberg_query_files.chain(speedboat_query_files).collect(),
        delete_files: required_files.delete_files.clone(),
        extension_suffixes: extension_suffixes.cloned(),
        use_cpu_threadpool,
    };

    execute_query_plan_batches(plan)
        .await
        .map_err(PrivateApiError::from)
}
