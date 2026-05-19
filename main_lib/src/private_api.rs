use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use futures_util::StreamExt;
use futures_util::future::try_join_all;
use idgenerator::IdInstance;
use prost::Message;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    error::Error,
    fmt,
};

use crate::data_access::{self, load_file_as_table};
use crate::data_contract::{
    ExtensionFile, ExtensionFileMetadata, FileDescriptor, IcebergMetadata, SpeedboatMetadata,
    TableMetadataCheckpoint,
};
use crate::elastic_search_index::create_index_inner;
use crate::elastic_search_responses::{QueryResultHit, compare_query_result_hits_desc};
use crate::lakehouse_serving::{
    ServingCacheManagerPlan, build_serving_cache_manager_plan,
    default_serving_cache_manager_request, execute_serving_cache_manager_plan,
};
use crate::peers::{
    CheckpointDescriptor, PrivateCompactionInvocation, PrivateExactConstraintGroup,
    PrivateExtensionInvocation, PrivatePrefetchInvocation, PrivateSearchAggregationFilterSpec,
    PrivateSearchAggregationPartial, PrivateSearchAggregationSpec,
    PrivateSearchHistogramBucketPartial, PrivateSearchInvocation, PrivateSearchResult,
    PrivateSearchSortSpec, PrivateSearchTermsBucketPartial, PrivateSearchTermsOrderSpec,
    PrivateSqlInvocation,
};
use crate::prefetch::warm_iceberg_checkpoints;
use crate::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema, SqlQuery};
use crate::search_executor::typed_sort_projection_name;
use crate::search_runtime::batches_to_serde_value;
use crate::state_provider::*;
use crate::util::log_err;

const EXACT_CANDIDATE_DOC_ID_FETCH_THRESHOLD: usize = 128;

#[derive(Debug, PartialEq, Eq)]
struct ExtensionFileSpec {
    suffix: String,
    file_path: String,
}

#[derive(Debug)]
pub(crate) struct PrivateApiError {
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

pub(crate) struct DataQueryResult {
    #[allow(dead_code)]
    pub(crate) num: u32,
    pub(crate) result: Vec<Vec<u8>>,
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
    match STATE_PROVIDER
        .get_checkpoint(target_checkpoint.clone())
        .await
    {
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

    let description = match STATE_PROVIDER
        .describe_table(&table_metadata.table_name)
        .await
    {
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

async fn execute_sql(
    sql_template: &String,
    local_name: &String,
    deletes_local_name: &String,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    // create a plan to run a SQL query
    let final_sql = sql_template
        .replace("{target_table}", local_name)
        .replace("{deletes_table}", deletes_local_name);
    if use_cpu_threadpool {
        match data_access::execute_sql_async(&final_sql).await {
            Ok(val) => Ok(val),
            Err(e) => log_err(e),
        }
    } else {
        let results = match data_access::execute_sql(&final_sql).await {
            Ok(val) => val,
            Err(e) => return log_err(e),
        };
        match results.collect().await {
            Ok(r) => Ok(r),
            Err(e) => log_err(e),
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

async fn create_all_deletes_table(local_names: &Vec<String>) -> Result<String, PrivateApiError> {
    let table_name = format!("table_{}", IdInstance::next_id());
    let ddl_stmt;
    if local_names.len() == 0 {
        ddl_stmt = "select null as _id_seq_no".to_string();
    } else {
        let union_selects = local_names
            .iter()
            .map(|x| format!("select * from {x}"))
            .collect::<Vec<String>>()
            .join(" union all ");
        ddl_stmt = format!("select * from ({union_selects})");
    }
    match data_access::create_table(&table_name, &ddl_stmt).await {
        Ok(_) => Ok(table_name.clone()),
        Err(e) => return log_err(PrivateApiError::from(e)),
    }
}

async fn process_iceberg_file(
    sql: &SqlQuery,
    iceberg_file: &FileDescriptor,
    iceberg_file_extensions: &Vec<ExtensionFileSpec>,
    table_schema: &PowdrrSchema,
    deletes_table_name: &String,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    let local_name = match ensure_loaded(
        &iceberg_file.file_path,
        iceberg_file_extensions,
        1,
        true,
        Some(table_schema.clone()),
    )
    .await
    {
        Ok(ln) => ln,
        Err(e) => return Err(PrivateApiError::from(e)),
    };

    let local_results = match execute_sql(
        &sql.build(table_schema, &iceberg_file.schema),
        &local_name,
        deletes_table_name,
        use_cpu_threadpool,
    )
    .await
    {
        Ok(vrb) => vrb,
        Err(e) => {
            data_access::release(&local_name).await;
            return log_err(PrivateApiError::from(e));
        }
    };
    data_access::release(&local_name).await;
    Ok(local_results)
}

async fn process_speedboat_file(
    sql: &SqlQuery,
    speedboat_file: &FileDescriptor,
    speedboat_file_extensions: &Vec<ExtensionFileSpec>,
    table_schema: &PowdrrSchema,
    deletes_table_name: &String,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    let local_name = match ensure_loaded(
        &speedboat_file.file_path,
        speedboat_file_extensions,
        speedboat_file.size,
        false,
        Some(speedboat_file.schema.clone()),
    )
    .await
    {
        Ok(ln) => ln,
        Err(e) => return log_err(PrivateApiError::from(e)),
    };
    let sql = sql.build(table_schema, &speedboat_file.schema);
    let local_results =
        match execute_sql(&sql, &local_name, &deletes_table_name, use_cpu_threadpool).await {
            Ok(vrb) => vrb,
            Err(e) => {
                return {
                    data_access::release(&local_name).await;
                    log_err(PrivateApiError::from(e))
                };
            }
        };
    data_access::release(&local_name).await;
    Ok(local_results)
}

fn exact_extension_file<'a>(
    extension_files: &'a [ExtensionFileSpec],
) -> Option<&'a ExtensionFileSpec> {
    extension_files
        .iter()
        .find(|extension| extension.suffix == "exact_index")
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn exact_candidate_doc_ids_sql(
    table_name: &str,
    constraints: &[PrivateExactConstraintGroup],
) -> Option<String> {
    if constraints.is_empty() {
        return None;
    }

    let clause = constraints
        .iter()
        .map(|constraint| {
            let mut values = constraint.values.clone();
            values.sort();
            values.dedup();
            let values = values
                .iter()
                .map(|value| sql_string_literal(value))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "(field_name = {} AND field_value IN ({}))",
                sql_string_literal(&constraint.field),
                values
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");

    if constraints.len() == 1 {
        Some(format!(
            "SELECT DISTINCT doc_id FROM {table_name} WHERE {clause}"
        ))
    } else {
        Some(format!(
            "SELECT doc_id FROM {table_name} WHERE {clause} GROUP BY doc_id HAVING COUNT(DISTINCT field_name) = {}",
            constraints.len()
        ))
    }
}

async fn exact_candidate_doc_ids(
    base_file_path: &String,
    top_level_size: u64,
    extension_files: &Vec<ExtensionFileSpec>,
    constraints: &[PrivateExactConstraintGroup],
    use_cpu_threadpool: bool,
) -> Result<Vec<serde_json::Value>, PrivateApiError> {
    let Some(extension_file) = exact_extension_file(extension_files) else {
        return Ok(vec![]);
    };
    let exact_local_name =
        ensure_loaded_extension_only(base_file_path, extension_file, top_level_size)
            .await
            .map_err(PrivateApiError::from)?;

    let sql = match exact_candidate_doc_ids_sql(&exact_local_name, constraints) {
        Some(sql) => sql,
        None => {
            data_access::release(&exact_local_name).await;
            return Ok(vec![]);
        }
    };

    let batches = match execute_raw_sql(&sql, use_cpu_threadpool).await {
        Ok(batches) => batches,
        Err(error) => {
            data_access::release(&exact_local_name).await;
            return log_err(PrivateApiError::from(error));
        }
    };

    let serde_result = match batches_to_serde_value(&batches).await {
        Ok(result) => result,
        Err(error) => {
            data_access::release(&exact_local_name).await;
            return Err(PrivateApiError {
                message: error.message,
            });
        }
    };
    data_access::release(&exact_local_name).await;

    let mut doc_ids = serde_result
        .values
        .into_iter()
        .filter_map(|row| row.get("doc_id").cloned())
        .filter(|value| !value.is_null())
        .collect::<Vec<_>>();
    doc_ids.sort_by(|left, right| left.to_string().cmp(&right.to_string()));
    doc_ids.dedup_by(|left, right| left == right);
    Ok(doc_ids)
}

async fn process_file_with_exact_candidates(
    sql: &SqlQuery,
    exact_sql: &SqlQuery,
    file: &FileDescriptor,
    extension_files: &Vec<ExtensionFileSpec>,
    table_schema: &PowdrrSchema,
    deletes_table_name: &String,
    use_cpu_threadpool: bool,
    exact_constraints: &[PrivateExactConstraintGroup],
    exact_doc_id_field_name: &str,
    parquet: bool,
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    let candidate_doc_ids = exact_candidate_doc_ids(
        &file.file_path,
        if parquet { 1 } else { file.size },
        extension_files,
        exact_constraints,
        use_cpu_threadpool,
    )
    .await?;
    if candidate_doc_ids.is_empty() {
        return Ok(vec![]);
    }
    if candidate_doc_ids.len() > EXACT_CANDIDATE_DOC_ID_FETCH_THRESHOLD {
        return if parquet {
            process_iceberg_file(
                exact_sql,
                file,
                extension_files,
                table_schema,
                deletes_table_name,
                use_cpu_threadpool,
            )
            .await
        } else {
            process_speedboat_file(
                exact_sql,
                file,
                extension_files,
                table_schema,
                deletes_table_name,
                use_cpu_threadpool,
            )
            .await
        };
    }

    let Some(filtered_sql) =
        sql.with_doc_id_filter_values(exact_doc_id_field_name, &candidate_doc_ids)
    else {
        return Ok(vec![]);
    };
    let empty_extensions = vec![];
    let load_schema = if parquet {
        table_schema.clone()
    } else {
        file.schema.clone()
    };
    let local_name = ensure_loaded(
        &file.file_path,
        &empty_extensions,
        if parquet { 1 } else { file.size },
        parquet,
        Some(load_schema),
    )
    .await
    .map_err(PrivateApiError::from)?;

    let built_sql = filtered_sql.build(table_schema, &file.schema);
    let local_results = match execute_sql(
        &built_sql,
        &local_name,
        deletes_table_name,
        use_cpu_threadpool,
    )
    .await
    {
        Ok(results) => results,
        Err(error) => {
            data_access::release(&local_name).await;
            return log_err(PrivateApiError::from(error));
        }
    };
    data_access::release(&local_name).await;
    Ok(local_results)
}

pub(crate) async fn data_query(
    invocation: &PrivateSqlInvocation,
    index: u64,
    num: u64,
) -> Result<DataQueryResult, PrivateApiError> {
    if invocation.checkpoints.len() == 0 {
        return Ok(DataQueryResult {
            num: 0,
            result: vec![],
        });
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

    data_query_worker(&invocation.sql, &required_files, true).await
}

pub(crate) async fn search_query(
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

    let use_exact_candidates = !invocation.exact_constraints.is_empty()
        && invocation.exact_doc_id_field_name.is_some()
        && invocation
            .exact_sql
            .as_ref()
            .is_some_and(|_| required_files_have_extension_suffix(&required_files, "exact_index"));
    let use_exact_sql = invocation
        .exact_sql
        .as_ref()
        .is_some_and(|_| required_files_have_extension_suffix(&required_files, "exact_index"));

    if use_exact_candidates || use_exact_sql {
        retain_required_file_extension_suffixes(&mut required_files, &["exact_index"]);
    } else if invocation.calculate_score {
        retain_required_file_extension_suffixes(&mut required_files, &["search_index"]);
    } else {
        clear_required_file_extensions(&mut required_files);
    }

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

    let batches = if use_exact_candidates {
        data_query_batches_exact_worker(
            &invocation.sql,
            invocation.exact_sql.as_ref().unwrap_or(&invocation.sql),
            &required_files,
            &invocation.exact_constraints,
            invocation.exact_doc_id_field_name.as_deref().unwrap(),
            true,
        )
        .await?
    } else {
        data_query_batches_worker(sql, &required_files, true).await?
    };
    let serde_result = match batches_to_serde_value(&batches).await {
        Ok(result) => result,
        Err(e) => return Err(PrivateApiError { message: e.message }),
    };

    let total_hits = serde_result.values.len();
    let aggregations =
        compute_search_aggregation_partials(&serde_result.values, &invocation.aggregations);
    if invocation.size == 0 {
        return Ok(PrivateSearchResult {
            hits: vec![],
            total_hits,
            aggregations,
        });
    }

    let mut hits = serde_result
        .values
        .iter()
        .map(|value| {
            let score =
                search_sort_values_for_row(value, invocation.calculate_score, &invocation.sorts);
            QueryResultHit::from_record_with_sort(
                &Some(invocation.table.clone()),
                value,
                None,
                score,
            )
        })
        .collect::<Vec<QueryResultHit>>();

    if !invocation.sorts.is_empty() {
        hits.sort_by(|left, right| {
            compare_query_result_hits_by_sort(left, right, &invocation.sorts)
        });
    } else if invocation.calculate_score {
        hits.sort_by(compare_query_result_hits_desc);
    }

    hits.truncate(invocation.size);

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

fn retain_required_file_extension_suffixes(required_files: &mut RequiredFiles, suffixes: &[&str]) {
    for extensions in required_files.iceberg_file_extensions.iter_mut() {
        extensions.retain(|extension| suffixes.iter().any(|suffix| extension.suffix == *suffix));
    }
    for extensions in required_files.speedboat_file_extensions.iter_mut() {
        extensions.retain(|extension| suffixes.iter().any(|suffix| extension.suffix == *suffix));
    }
}

fn clear_required_file_extensions(required_files: &mut RequiredFiles) {
    for extensions in required_files.iceberg_file_extensions.iter_mut() {
        extensions.clear();
    }
    for extensions in required_files.speedboat_file_extensions.iter_mut() {
        extensions.clear();
    }
}

fn search_sort_values_for_row(
    row: &serde_json::Value,
    calculate_score: bool,
    sorts: &[PrivateSearchSortSpec],
) -> Option<Vec<serde_json::Value>> {
    if sorts.is_empty() {
        return None;
    }

    let value_map = row.as_object().unwrap();
    Some(
        sorts
            .iter()
            .map(|sort| {
                if sort.field == "_score" {
                    value_map
                        .get("score")
                        .and_then(|value| value.as_f64())
                        .or_else(|| {
                            if calculate_score {
                                value_map.get("term_cnt").and_then(|term_cnt| {
                                    value_map.get("word_cnt").map(|word_cnt| {
                                        bm25_fallback_score_from_values(term_cnt, word_cnt)
                                    })
                                })
                            } else {
                                None
                            }
                        })
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    sort_value_for_field(row, &sort.field)
                }
            })
            .collect(),
    )
}

fn sort_value_for_field(row: &serde_json::Value, field: &str) -> serde_json::Value {
    let value_map = row.as_object().unwrap();
    let projection_name = typed_sort_projection_name(field);
    if let Some(value) = value_map.get(&projection_name) {
        return value.clone();
    }
    if let Some(value) = value_map.get(field) {
        return value.clone();
    }

    value_map
        .get("_source")
        .and_then(|source| sort_value_from_source(source, field))
        .unwrap_or(serde_json::Value::Null)
}

fn sort_value_from_source(source: &serde_json::Value, field: &str) -> Option<serde_json::Value> {
    let parsed_source = match source {
        serde_json::Value::String(source) => {
            serde_json::from_str::<serde_json::Value>(source).ok()?
        }
        other => other.clone(),
    };

    if let Some(value) = parsed_source.get(field) {
        return Some(value.clone());
    }

    let mut current = &parsed_source;
    for segment in field.split('.') {
        current = current.get(segment)?;
    }

    Some(current.clone())
}

fn compare_query_result_hits_by_sort(
    left: &QueryResultHit,
    right: &QueryResultHit,
    sorts: &[PrivateSearchSortSpec],
) -> std::cmp::Ordering {
    let left_values = left.sort.as_deref().unwrap_or(&[]);
    let right_values = right.sort.as_deref().unwrap_or(&[]);
    for (index, sort) in sorts.iter().enumerate() {
        let left_value = left_values.get(index).unwrap_or(&serde_json::Value::Null);
        let right_value = right_values.get(index).unwrap_or(&serde_json::Value::Null);
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
        ._seq_no
        .cmp(&left._seq_no)
        .then_with(|| left._id.cmp(&right._id))
}

fn compare_sort_values(left: &serde_json::Value, right: &serde_json::Value) -> std::cmp::Ordering {
    match (left, right) {
        (serde_json::Value::Null, serde_json::Value::Null) => std::cmp::Ordering::Equal,
        (serde_json::Value::Null, _) => std::cmp::Ordering::Greater,
        (_, serde_json::Value::Null) => std::cmp::Ordering::Less,
        _ => {
            if let (Some(left_number), Some(right_number)) = (left.as_f64(), right.as_f64()) {
                return left_number
                    .partial_cmp(&right_number)
                    .unwrap_or(std::cmp::Ordering::Equal);
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

fn bm25_fallback_score_from_values(
    term_cnt: &serde_json::Value,
    word_cnt: &serde_json::Value,
) -> f64 {
    let term_cnt = term_cnt.as_f64().unwrap_or(0.0);
    let word_cnt = word_cnt.as_f64().unwrap_or(0.0);
    let constant_k = 1.2;
    let constant_b = 0.75;
    let avgdl = 5.6;
    (term_cnt * (constant_k + 1.0))
        / (term_cnt + constant_k * (1.0 - constant_b + (constant_b * word_cnt / avgdl)))
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn retain_required_file_extension_suffixes_keeps_only_selected_suffixes() {
        let mut required_files = RequiredFiles {
            table_schema: PowdrrSchema::minimal(),
            iceberg_files: vec![],
            all_iceberg_files_count: 0,
            iceberg_file_extensions: vec![vec![
                ExtensionFileSpec {
                    suffix: "search_index".to_string(),
                    file_path: "search.parquet".to_string(),
                },
                ExtensionFileSpec {
                    suffix: "exact_index".to_string(),
                    file_path: "exact.parquet".to_string(),
                },
            ]],
            speedboat_files: vec![],
            all_speedboat_files_count: 0,
            speedboat_file_extensions: vec![],
            delete_files: vec![],
        };

        retain_required_file_extension_suffixes(&mut required_files, &["exact_index"]);

        assert_eq!(
            required_files.iceberg_file_extensions,
            vec![vec![ExtensionFileSpec {
                suffix: "exact_index".to_string(),
                file_path: "exact.parquet".to_string(),
            }]]
        );
    }

    #[test]
    fn clear_required_file_extensions_removes_all_sidecars() {
        let mut required_files = RequiredFiles {
            table_schema: PowdrrSchema::minimal(),
            iceberg_files: vec![],
            all_iceberg_files_count: 0,
            iceberg_file_extensions: vec![vec![ExtensionFileSpec {
                suffix: "search_index".to_string(),
                file_path: "search.parquet".to_string(),
            }]],
            speedboat_files: vec![],
            all_speedboat_files_count: 0,
            speedboat_file_extensions: vec![vec![ExtensionFileSpec {
                suffix: "exact_index".to_string(),
                file_path: "exact.parquet".to_string(),
            }]],
            delete_files: vec![],
        };

        clear_required_file_extensions(&mut required_files);

        assert_eq!(required_files.iceberg_file_extensions, vec![vec![]]);
        assert_eq!(required_files.speedboat_file_extensions, vec![vec![]]);
    }
}

pub(crate) async fn compaction_query(
    invocation: &PrivateCompactionInvocation,
    index: u64,
    num: u64,
) -> Result<DataQueryResult, PrivateApiError> {
    let required_files = generate_required_files(invocation, index, num);

    data_query_worker(&invocation.sql, &required_files, true).await
}

pub(crate) async fn extension_query(
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

pub(crate) async fn prefetch_query(
    invocation: &PrivatePrefetchInvocation,
    index: u64,
    num: u64,
) -> Result<DataQueryResult, PrivateApiError> {
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
    )
    {
        Ok(rf) => rf,
        Err(e) => return log_err(e),
    };
    let files_considered = required_files.iceberg_files.len();
    let targeted_warmup_plan = if invocation.required_extensions.is_empty() {
        narrow_prefetch_files_for_serving_warmup(&mut required_files, &table_metadata).await
    } else {
        None
    };
    let result = if let Some(plan) = targeted_warmup_plan.as_ref() {
        execute_serving_cache_manager_plan(plan, &required_files.delete_files)
            .await
            .map_err(|error| PrivateApiError {
                message: error.message,
            })?;
        DataQueryResult {
            num: 0,
            result: vec![],
        }
    } else {
        data_query_worker(&SqlQuery::dummy(), &required_files, false).await?
    };
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
    Ok(result)
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

fn compute_search_aggregation_partials(
    rows: &[serde_json::Value],
    specs: &[PrivateSearchAggregationSpec],
) -> Vec<PrivateSearchAggregationPartial> {
    specs
        .iter()
        .map(|spec| compute_search_aggregation_partial(rows, spec))
        .collect()
}

fn compute_search_aggregation_partial(
    rows: &[serde_json::Value],
    spec: &PrivateSearchAggregationSpec,
) -> PrivateSearchAggregationPartial {
    match spec {
        PrivateSearchAggregationSpec::Average { name, field } => {
            let mut sum = 0.0;
            let mut count = 0_u64;
            for row in rows.iter() {
                if let Some(value) = extract_numeric_field(row, field) {
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
                .filter_map(|row| extract_term_key(row, field))
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

            let mut buckets = BTreeMap::<i64, Vec<serde_json::Value>>::new();
            for row in rows.iter() {
                if let Some(timestamp_ms) = extract_timestamp_millis(row, field) {
                    let bucket_key = timestamp_ms - timestamp_ms.rem_euclid(interval_ms);
                    buckets.entry(bucket_key).or_default().push(row.clone());
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
            let mut buckets = std::collections::HashMap::<String, Vec<serde_json::Value>>::new();
            for row in rows.iter() {
                if let Some(key) = extract_term_key(row, field)
                    .or_else(|| missing.as_ref().and_then(render_missing_term_key))
                {
                    buckets.entry(key).or_default().push(row.clone());
                }
            }

            let mut bucket_partials = buckets
                .into_iter()
                .map(|(key, bucket_rows)| PrivateSearchTermsBucketPartial {
                    doc_count: bucket_rows.len() as u64,
                    sub_aggregations: compute_search_aggregation_partials(
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
            let filtered_rows = rows
                .iter()
                .filter(|row| row_matches_aggregation_filter(row, filter))
                .cloned()
                .collect::<Vec<_>>();
            let sub_aggregations =
                compute_search_aggregation_partials(&filtered_rows, sub_aggregations);
            PrivateSearchAggregationPartial::Filter {
                name: name.clone(),
                doc_count: filtered_rows.len() as u64,
                sub_aggregations,
            }
        }
    }
}

fn row_matches_aggregation_filter(
    row: &serde_json::Value,
    filter: &PrivateSearchAggregationFilterSpec,
) -> bool {
    match filter {
        PrivateSearchAggregationFilterSpec::Term { field, value } => row
            .get(field)
            .and_then(|field_value| field_value.as_str())
            .map(|field_value| field_value == value)
            .unwrap_or(false),
    }
}

fn extract_numeric_field(row: &serde_json::Value, field: &str) -> Option<f64> {
    let value = row.get(field)?;
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|numeric| numeric as f64))
        .or_else(|| value.as_u64().map(|numeric| numeric as f64))
}

fn extract_timestamp_millis(row: &serde_json::Value, field: &str) -> Option<i64> {
    let value = row.get(field)?;
    if let Some(timestamp_ms) = value.as_i64() {
        return Some(timestamp_ms);
    }
    if let Some(timestamp_ms) = value.as_u64() {
        return i64::try_from(timestamp_ms).ok();
    }
    let timestamp = value.as_str()?;
    DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|datetime| datetime.with_timezone(&Utc).timestamp_millis())
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

fn extract_term_key(row: &serde_json::Value, field: &str) -> Option<String> {
    let value = row.get(field)?;
    value
        .as_str()
        .map(|text| text.to_string())
        .or_else(|| value.as_i64().map(|numeric| numeric.to_string()))
        .or_else(|| value.as_u64().map(|numeric| numeric.to_string()))
        .or_else(|| value.as_f64().map(|numeric| numeric.to_string()))
        .or_else(|| value.as_bool().map(|boolean| boolean.to_string()))
}

async fn data_query_batches_worker(
    sql: &SqlQuery,
    required_files: &RequiredFiles,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    let mut delete_local_names = vec![];
    let delete_schema = PowdrrSchema::from(&vec![PowdrrField {
        name: "_id_seq_no".to_string(),
        data_type: PowdrrDataType::String,
    }]);
    let extension_file_vecs = vec![];
    for delete_file_path in required_files.delete_files.iter() {
        let local_name = match ensure_loaded(
            &delete_file_path,
            &extension_file_vecs,
            1,
            false,
            Some(delete_schema.clone()),
        )
        .await
        {
            Ok(ln) => ln,
            Err(e) => return log_err(PrivateApiError::from(e)),
        };
        delete_local_names.push(local_name);
    }
    // TODO: need to make a stable name here and skip this if it is already loaded
    let all_deletes_local_name = create_all_deletes_table(&delete_local_names).await?;

    let iceberg_calls = required_files
        .iceberg_files
        .iter()
        .zip(required_files.iceberg_file_extensions.iter())
        .map(|(iceberg_file, iceberg_file_extensions)| {
            process_iceberg_file(
                sql,
                iceberg_file,
                iceberg_file_extensions,
                &required_files.table_schema,
                &all_deletes_local_name,
                use_cpu_threadpool,
            )
        });
    let speedboat_calls = required_files
        .speedboat_files
        .iter()
        .zip(required_files.speedboat_file_extensions.iter())
        .map(|(speedboat_file, speedboat_file_extensions)| {
            process_speedboat_file(
                sql,
                speedboat_file,
                speedboat_file_extensions,
                &required_files.table_schema,
                &all_deletes_local_name,
                use_cpu_threadpool,
            )
        });

    let iceberg_results: Vec<Result<RecordBatch, FlightError>> =
        match try_join_all(iceberg_calls).await {
            Ok(ar) => ar
                .iter()
                .flatten()
                .map(|x| Ok(x.clone()))
                .collect::<Vec<Result<RecordBatch, FlightError>>>(),
            Err(e) => {
                let error = format!("{}", e.message);
                println!("{}", error);
                panic!("dude")
            }
        };

    let speedboat_results: Vec<Result<RecordBatch, FlightError>> =
        match try_join_all(speedboat_calls).await {
            Ok(ar) => ar
                .iter()
                .flatten()
                .map(|x| Ok(x.clone()))
                .collect::<Vec<Result<RecordBatch, FlightError>>>(),
            Err(e) => {
                let error = format!("{}", e.message);
                println!("{}", error);
                panic!("dude")
            }
        };

    data_access::drop(&all_deletes_local_name).await;

    Ok(iceberg_results
        .into_iter()
        .chain(speedboat_results.into_iter())
        .map(|result| result.unwrap())
        .collect())
}

async fn data_query_batches_exact_worker(
    sql: &SqlQuery,
    exact_sql: &SqlQuery,
    required_files: &RequiredFiles,
    exact_constraints: &[PrivateExactConstraintGroup],
    exact_doc_id_field_name: &str,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    let mut delete_local_names = vec![];
    let delete_schema = PowdrrSchema::from(&vec![PowdrrField {
        name: "_id_seq_no".to_string(),
        data_type: PowdrrDataType::String,
    }]);
    let extension_file_vecs = vec![];
    for delete_file_path in required_files.delete_files.iter() {
        let local_name = ensure_loaded(
            &delete_file_path,
            &extension_file_vecs,
            1,
            false,
            Some(delete_schema.clone()),
        )
        .await
        .map_err(PrivateApiError::from)?;
        delete_local_names.push(local_name);
    }
    let all_deletes_local_name = create_all_deletes_table(&delete_local_names).await?;

    let iceberg_calls = required_files
        .iceberg_files
        .iter()
        .zip(required_files.iceberg_file_extensions.iter())
        .map(|(iceberg_file, iceberg_file_extensions)| {
            process_file_with_exact_candidates(
                sql,
                exact_sql,
                iceberg_file,
                iceberg_file_extensions,
                &required_files.table_schema,
                &all_deletes_local_name,
                use_cpu_threadpool,
                exact_constraints,
                exact_doc_id_field_name,
                true,
            )
        });
    let speedboat_calls = required_files
        .speedboat_files
        .iter()
        .zip(required_files.speedboat_file_extensions.iter())
        .map(|(speedboat_file, speedboat_file_extensions)| {
            process_file_with_exact_candidates(
                sql,
                exact_sql,
                speedboat_file,
                speedboat_file_extensions,
                &required_files.table_schema,
                &all_deletes_local_name,
                use_cpu_threadpool,
                exact_constraints,
                exact_doc_id_field_name,
                false,
            )
        });

    let iceberg_results = try_join_all(iceberg_calls)
        .await
        .map_err(|error| PrivateApiError {
            message: error.message,
        })?;
    let speedboat_results =
        try_join_all(speedboat_calls)
            .await
            .map_err(|error| PrivateApiError {
                message: error.message,
            })?;

    data_access::drop(&all_deletes_local_name).await;

    Ok(iceberg_results
        .into_iter()
        .chain(speedboat_results.into_iter())
        .flatten()
        .collect())
}

async fn data_query_worker(
    sql: &SqlQuery,
    required_files: &RequiredFiles,
    use_cpu_threadpool: bool,
) -> Result<DataQueryResult, PrivateApiError> {
    let batches = data_query_batches_worker(sql, required_files, use_cpu_threadpool).await?;

    let mut retval = Vec::new();
    let input_stream =
        futures::stream::iter(batches.into_iter().map(Ok::<RecordBatch, FlightError>));
    let mut flight_data_stream = FlightDataEncoderBuilder::new().build(input_stream);
    while let Some(value) = flight_data_stream.next().await {
        let mut buf = Vec::new();
        match value {
            Ok(v) => match v.encode(&mut buf) {
                Ok(_) => (),
                Err(e) => {
                    let error = format!("Error encoding data: {:?}", e);
                    panic!("{}", error);
                }
            },
            Err(e) => {
                let error = format!("Error streaming data: {:?}", e);
                panic!("{}", error);
            }
        };
        retval.push(buf);
    }
    Ok(DataQueryResult {
        num: 0,
        result: retval,
    })
}
