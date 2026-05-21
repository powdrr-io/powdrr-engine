use crate::data_access::load_file_as_table;
use crate::data_contract::{
    ExtensionFileMetadata, ExtensionWorkItem, FileDescriptor, TableMetadataCheckpoint,
    checkpoint_extension_metadata_key,
};
use crate::elastic_search_common::call_peers;
use crate::elastic_table_validation::{ElasticTableValidationError, validate_elastic_table_files};
use crate::peers::{
    CheckpointDescriptor, PrivateExtensionInvocation, PrivateInvocation, PrivateInvocationResult,
};
use crate::query_execution::{
    QueryExecutionPlan, QueryInputFile, QuerySqlTemplate, QueryStorageKind,
    execute_query_plan_batches,
};
use crate::runtime_bindings;
use crate::schema_massager::PowdrrSchema;
use crate::search_runtime::batches_to_serde_value;
use crate::{
    data_access,
    data_access::{execute_sql, file_exists},
    data_contract::{ExtensionCommit, ExtensionFile},
    util::add_file_suffix,
};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::{arrow::datatypes::DataType, dataframe::DataFrameWriteOptions};
use idgenerator::IdInstance;
use std::error::Error;
use std::fmt::Display;

const EXACT_PRUNING_VALUE_LIMIT: usize = 64;
const SNAPSHOT_EXACT_LOOKUP_SUFFIX: &str = "snapshot_exact_lookup";
const SNAPSHOT_LOOKUP_SUFFIX: &str = "snapshot_lookup";

#[derive(Debug)]
pub struct IndexError {
    pub message: String,
}

impl Error for IndexError {}
unsafe impl Send for IndexError {}
unsafe impl Sync for IndexError {}

impl IndexError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn from(data_fusion_error: DataFusionError) -> Self {
        IndexError {
            message: format!("{}", data_fusion_error),
        }
    }
}

impl Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)?;
        Ok(())
    }
}

impl From<ElasticTableValidationError> for IndexError {
    fn from(value: ElasticTableValidationError) -> Self {
        Self::new(value.to_string())
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    data_type.equals_datatype(&DataType::Utf8View)
        || data_type.equals_datatype(&DataType::Utf8)
        || data_type.equals_datatype(&DataType::LargeUtf8)
}

fn is_exact_index_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8View
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

async fn drop_all(tables: &Vec<String>) -> () {
    for table in tables {
        data_access::drop(table).await;
    }
}

async fn create_index_worker(
    table_name: &String,
    doc_id_field_name: &String,
    target_file_path: &String,
) -> Result<(), IndexError> {
    let new_local_name = table_name;
    let doc_id_field_name_local = doc_id_field_name;
    let mut created_tables = vec![];

    let raw_table = match execute_sql(&format!("select * from {new_local_name}").to_string()).await
    {
        Err(e) => return Err(IndexError::from(e)),
        Ok(rt) => rt,
    };

    let fields_without_doc_id_field: Vec<&String> = raw_table
        .schema()
        .iter()
        .filter(|c| is_string_type(c.1.data_type()) && c.1.name() != doc_id_field_name)
        .map(|c| c.1.name())
        .collect();

    let field_normalization_queries: Vec<String> = fields_without_doc_id_field.iter().map(
        |field_name| format!("SELECT {doc_id_field_name_local} as doc_id, '{field_name}' as field_name, {new_local_name}.\"{field_name}\" as field_value from {new_local_name}")
    ).collect();

    if field_normalization_queries.is_empty() {
        return Err(IndexError::new(format!(
            "Table {} has no searchable top-level string columns besides {}",
            new_local_name, doc_id_field_name
        )));
    }

    let field_normalization_queries_union = field_normalization_queries.join(" UNION ");

    match data_access::create_table(
        &format!("{new_local_name}_fields"),
        &field_normalization_queries_union,
    )
    .await
    {
        Err(e) => return Err(IndexError::from(e)),
        _ => (),
    };
    created_tables.push(format!("{new_local_name}_fields"));

    match data_access::create_table(&format!("{new_local_name}_split"), &format!("SELECT doc_id, field_name, string_to_array(field_value, ' ') as field_terms from {new_local_name}_fields")).await {
        Err(e) => {
            drop_all(&created_tables).await;
            return Err(IndexError::from(e))
        },
        _ => ()
    };
    created_tables.push(format!("{new_local_name}_split"));

    match data_access::create_table(&format!("{new_local_name}_split_unnest"), &format!("SELECT doc_id, field_name, unnest(field_terms) as field_term from {new_local_name}_split")).await {
        Err(e) => {
            drop_all(&created_tables).await;
            return Err(IndexError::from(e))
        },
        _ => ()
    };
    created_tables.push(format!("{new_local_name}_split_unnest"));

    match data_access::create_table(&format!("{new_local_name}_term_frequency"), &format!("SELECT doc_id, field_name, cast(doc_id as string) || '_' || field_name as doc_id_field_name, field_term, count(1) as term_cnt from {new_local_name}_split_unnest group by doc_id, field_name, field_term")).await {
        Err(e)  => {
            drop_all(&created_tables).await;
            return Err(IndexError::from(e))
        },
        _ => ()
    };
    created_tables.push(format!("{new_local_name}_term_frequency"));

    match data_access::create_table(&format!("{new_local_name}_field_size"), &format!("SELECT doc_id, field_name, cast(doc_id as string) || '_' || field_name as doc_id_field_name, array_length(field_terms) as word_cnt from {new_local_name}_split")).await {
        Err(e) => {
            drop_all(&created_tables).await;
            return Err(IndexError::from(e))
        },
        _ => ()
    };
    created_tables.push(format!("{new_local_name}_field_size"));

    // TODO: need to think about multiple term search still
    // TODO: can you search multiple fields at once?
    // TODO: split into a file per term? then we don't even move the data or load it for terms we don't search for and
    //       we do one less filter at query time. might make for very small files though.

    // f(Qi, D) = select doc_id, field_name, doc_id_field_name, field_term, term_cnt from term_frequency where field_name = '{target_field}' and field_term = '{target_term} and term_cnt > 0'
    // |D| = select doc_id, field_name, doc_id_field_name, word_cnt from field_size where field_name = '{target_field}'
    // (THIS SHOULD BE IN THE METADATA) avgdl = select field_name, sum(word_cnt) as total_word_cnt from field_size group by field_name
    // (THIS SHOUDL BE IN THE METADATA) N = select count(1) from base_table
    // n(qi) = num results in f(Qi, D)

    match data_access::create_table(&format!("{new_local_name}_joined"), &format!("SELECT tf.doc_id, tf.field_name, tf.field_term, tf.term_cnt, fs.word_cnt from {new_local_name}_term_frequency tf INNER JOIN {new_local_name}_field_size fs ON tf.doc_id_field_name = fs.doc_id_field_name WHERE tf.term_cnt > 0")).await {
        Err(e) => {
            drop_all(&created_tables).await;
            return Err(IndexError::from(e))
        },
        _ => ()
    };
    created_tables.push(format!("{new_local_name}_joined"));

    let joined_table =
        match execute_sql(&format!("SELECT * FROM {new_local_name}_joined").to_string()).await {
            Err(e) => {
                drop_all(&created_tables).await;
                return Err(IndexError::from(e));
            }
            Ok(tft) => tft,
        };

    match joined_table
        .write_parquet(
            target_file_path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await
    {
        Err(e) => {
            drop_all(&created_tables).await;
            return Err(IndexError::from(e));
        }
        _ => (),
    };
    drop_all(&created_tables).await;
    Ok(())
}

async fn create_exact_index_worker(
    table_name: &String,
    doc_id_field_name: &String,
    target_file_path: &String,
) -> Result<(), IndexError> {
    let new_local_name = table_name;
    let doc_id_field_name_local = doc_id_field_name;

    let raw_table = match execute_sql(&format!("select * from {new_local_name}").to_string()).await
    {
        Err(e) => return Err(IndexError::from(e)),
        Ok(rt) => rt,
    };

    let exact_fields: Vec<&String> = raw_table
        .schema()
        .iter()
        .filter(|c| is_exact_index_type(c.1.data_type()) && c.1.name() != doc_id_field_name)
        .map(|c| c.1.name())
        .collect();

    if exact_fields.is_empty() {
        return Ok(());
    }

    let field_queries: Vec<String> = exact_fields
        .iter()
        .map(|field_name| {
            format!(
                "SELECT {doc_id_field_name_local} as doc_id, '{field_name}' as field_name, CAST({new_local_name}.\"{field_name}\" as string) as field_value FROM {new_local_name} WHERE {new_local_name}.\"{field_name}\" IS NOT NULL"
            )
        })
        .collect();

    let union_query = field_queries.join(" UNION ALL ");
    let exact_table = match execute_sql(&union_query).await {
        Err(e) => return Err(IndexError::from(e)),
        Ok(table) => table,
    };

    match exact_table
        .write_parquet(
            target_file_path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await
    {
        Err(e) => Err(IndexError::from(e)),
        Ok(_) => Ok(()),
    }
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn exact_pruning_field_values(
    table_name: &str,
    field_name: &str,
) -> Result<Vec<String>, IndexError> {
    let sql = format!(
        "SELECT CAST({table_name}.\"{field_name}\" as string) as field_value \
         FROM {table_name} \
         WHERE {table_name}.\"{field_name}\" IS NOT NULL \
         GROUP BY {table_name}.\"{field_name}\" \
         LIMIT {}",
        EXACT_PRUNING_VALUE_LIMIT + 1
    );
    let dataframe = execute_sql(&sql).await.map_err(IndexError::from)?;
    let batches = dataframe.collect().await.map_err(IndexError::from)?;
    let values = batches_to_serde_value(&batches)
        .await
        .map_err(|error| IndexError::new(error.message))?
        .values
        .into_iter()
        .filter_map(|row| {
            row.get("field_value")
                .and_then(|value| value.as_str().map(ToString::to_string))
        })
        .collect::<Vec<_>>();
    Ok(values)
}

async fn create_exact_pruning_worker(
    table_name: &String,
    doc_id_field_name: &String,
    target_file_path: &String,
) -> Result<bool, IndexError> {
    let raw_table = execute_sql(&format!("select * from {table_name}").to_string())
        .await
        .map_err(IndexError::from)?;

    let exact_fields: Vec<&String> = raw_table
        .schema()
        .iter()
        .filter(|c| is_exact_index_type(c.1.data_type()) && c.1.name() != doc_id_field_name)
        .map(|c| c.1.name())
        .collect();

    if exact_fields.is_empty() {
        return Ok(false);
    }

    let mut row_queries = vec![];
    for field_name in exact_fields {
        let values = exact_pruning_field_values(table_name, field_name).await?;
        if values.len() > EXACT_PRUNING_VALUE_LIMIT {
            row_queries.push(format!(
                "SELECT {} as field_name, CAST(NULL as string) as field_value, false as complete",
                sql_string_literal(field_name)
            ));
            continue;
        }

        if values.is_empty() {
            row_queries.push(format!(
                "SELECT {} as field_name, CAST(NULL as string) as field_value, true as complete",
                sql_string_literal(field_name)
            ));
            continue;
        }

        row_queries.extend(values.into_iter().map(|value| {
            format!(
                "SELECT {} as field_name, {} as field_value, true as complete",
                sql_string_literal(field_name),
                sql_string_literal(&value)
            )
        }));
    }

    if row_queries.is_empty() {
        return Ok(false);
    }

    let summary_table = execute_sql(&row_queries.join(" UNION ALL "))
        .await
        .map_err(IndexError::from)?;
    summary_table
        .write_parquet(
            target_file_path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await
        .map_err(IndexError::from)?;
    Ok(true)
}

pub(crate) async fn create_index_jsonl(
    file_path: &String,
    doc_id_field_name: &String,
    schema: &PowdrrSchema,
) -> Result<Option<String>, IndexError> {
    let target_file_path = add_file_suffix(
        file_path,
        &"search_index".to_string(),
        Some(&".parquet".to_string()),
    );
    if file_exists(&target_file_path).await {
        return Ok(None);
    }

    let top_level_name = data_access::path_to_table_name(file_path);
    // TODO: pass in real size
    data_access::reserve(&top_level_name, 1000, vec![]).await;

    let result = data_access::load_file_as_table(
        &top_level_name,
        file_path,
        false,
        Some(schema.to_arrow_schema()),
    )
    .await;
    match result {
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(IndexError::from(e));
        }
        Ok(_) => (),
    };

    match create_index_worker(&top_level_name, doc_id_field_name, &target_file_path).await {
        Ok(_) => (),
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(e);
        }
    }

    data_access::release(&top_level_name).await;
    Ok(Some(target_file_path))
}

pub(crate) async fn create_exact_index_jsonl(
    file_path: &String,
    doc_id_field_name: &String,
    schema: &PowdrrSchema,
) -> Result<Option<String>, IndexError> {
    let target_file_path = add_file_suffix(
        file_path,
        &"exact_index".to_string(),
        Some(&".parquet".to_string()),
    );
    if file_exists(&target_file_path).await {
        return Ok(None);
    }

    let top_level_name = data_access::path_to_table_name(file_path);
    data_access::reserve(&top_level_name, 1000, vec![]).await;

    let result = data_access::load_file_as_table(
        &top_level_name,
        file_path,
        false,
        Some(schema.to_arrow_schema()),
    )
    .await;
    match result {
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(IndexError::from(e));
        }
        Ok(_) => (),
    };

    match create_exact_index_worker(&top_level_name, doc_id_field_name, &target_file_path).await {
        Ok(_) => (),
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(e);
        }
    }

    data_access::release(&top_level_name).await;
    Ok(Some(target_file_path))
}

pub async fn create_index_parquet(
    file_path: &String,
    doc_id_field_name: &String,
) -> Result<Option<String>, IndexError> {
    let target_file_path = add_file_suffix(
        file_path,
        &"search_index".to_string(),
        Some(&".parquet".to_string()),
    );
    if file_exists(&target_file_path).await {
        return Ok(None);
    }

    let top_level_name = data_access::path_to_table_name(file_path);
    // TODO: pass in real size
    data_access::reserve(&top_level_name, 1000, vec![]).await;

    let result = load_file_as_table(&top_level_name, file_path, true, None).await;
    match result {
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(IndexError::from(e));
        }
        Ok(_) => (),
    };

    match create_index_worker(&top_level_name, doc_id_field_name, &target_file_path).await {
        Ok(_) => (),
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(e);
        }
    }
    data_access::release(&top_level_name).await;
    Ok(Some(target_file_path))
}

pub(crate) async fn create_exact_index_parquet(
    file_path: &String,
    doc_id_field_name: &String,
) -> Result<Option<String>, IndexError> {
    let target_file_path = add_file_suffix(
        file_path,
        &"exact_index".to_string(),
        Some(&".parquet".to_string()),
    );
    if file_exists(&target_file_path).await {
        return Ok(None);
    }

    let top_level_name = data_access::path_to_table_name(file_path);
    data_access::reserve(&top_level_name, 1000, vec![]).await;

    let result = load_file_as_table(&top_level_name, file_path, true, None).await;
    match result {
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(IndexError::from(e));
        }
        Ok(_) => (),
    };

    match create_exact_index_worker(&top_level_name, doc_id_field_name, &target_file_path).await {
        Ok(_) => (),
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(e);
        }
    }
    data_access::release(&top_level_name).await;
    Ok(Some(target_file_path))
}

pub(crate) async fn create_exact_pruning_index_jsonl(
    file_path: &String,
    doc_id_field_name: &String,
    schema: &PowdrrSchema,
) -> Result<Option<String>, IndexError> {
    let target_file_path = add_file_suffix(
        file_path,
        &"exact_pruning".to_string(),
        Some(&".parquet".to_string()),
    );
    if file_exists(&target_file_path).await {
        return Ok(None);
    }

    let top_level_name = data_access::path_to_table_name(file_path);
    data_access::reserve(&top_level_name, 1000, vec![]).await;

    let result = data_access::load_file_as_table(
        &top_level_name,
        file_path,
        false,
        Some(schema.to_arrow_schema()),
    )
    .await;
    match result {
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(IndexError::from(e));
        }
        Ok(_) => (),
    };

    let created =
        match create_exact_pruning_worker(&top_level_name, doc_id_field_name, &target_file_path)
            .await
        {
            Ok(created) => created,
            Err(e) => {
                data_access::release(&top_level_name).await;
                return Err(e);
            }
        };

    data_access::release(&top_level_name).await;
    if created {
        Ok(Some(target_file_path))
    } else {
        Ok(None)
    }
}

pub(crate) async fn create_exact_pruning_index_parquet(
    file_path: &String,
    doc_id_field_name: &String,
) -> Result<Option<String>, IndexError> {
    let target_file_path = add_file_suffix(
        file_path,
        &"exact_pruning".to_string(),
        Some(&".parquet".to_string()),
    );
    if file_exists(&target_file_path).await {
        return Ok(None);
    }

    let top_level_name = data_access::path_to_table_name(file_path);
    data_access::reserve(&top_level_name, 1000, vec![]).await;

    let result = load_file_as_table(&top_level_name, file_path, true, None).await;
    match result {
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(IndexError::from(e));
        }
        Ok(_) => (),
    };

    let created =
        match create_exact_pruning_worker(&top_level_name, doc_id_field_name, &target_file_path)
            .await
        {
            Ok(created) => created,
            Err(e) => {
                data_access::release(&top_level_name).await;
                return Err(e);
            }
        };
    data_access::release(&top_level_name).await;
    if created {
        Ok(Some(target_file_path))
    } else {
        Ok(None)
    }
}

fn checkpoint_query_inputs(files: Vec<FileDescriptor>) -> Vec<QueryInputFile> {
    files.into_iter()
        .map(|file| QueryInputFile {
            file,
            storage: QueryStorageKind::Iceberg,
            extensions: vec![],
        })
        .collect()
}

fn snapshot_lookup_full_scan_sql(include_delete_filter: bool) -> String {
    if include_delete_filter {
        "SELECT t.* FROM {target_table} t LEFT JOIN {deletes_table} dt ON dt._id_seq_no = t.\"_id_seq_no\" WHERE dt._id_seq_no IS NULL".to_string()
    } else {
        "SELECT * FROM {target_table}".to_string()
    }
}

async fn write_snapshot_lookup_parquet(
    batches: &Vec<RecordBatch>,
    target_file_path: &String,
) -> Result<bool, IndexError> {
    let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    if row_count == 0 {
        return Ok(false);
    }

    let local_name = format!("snapshot_lookup_{}", IdInstance::next_id());
    data_access::load_memtable_with_name(&local_name, batches)
        .await
        .map_err(IndexError::from)?;
    let dataframe = execute_sql(&format!("SELECT * FROM {local_name}"))
        .await
        .map_err(IndexError::from)?;
    let write_result = dataframe
        .write_parquet(
            target_file_path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await
        .map_err(IndexError::from);
    data_access::drop(&local_name).await;
    write_result.map(|_| true)
}

async fn create_snapshot_lookup_artifact(
    checkpoint: &TableMetadataCheckpoint,
) -> Result<Option<String>, IndexError> {
    let Some(iceberg_metadata) = checkpoint.iceberg_metadata.as_ref() else {
        return Ok(None);
    };
    let files = iceberg_metadata.files.as_file_tuples();
    if files.is_empty() {
        return Ok(None);
    }

    let delete_files = checkpoint
        .deletes_metadata
        .as_ref()
        .map(|metadata| metadata.files.clone())
        .unwrap_or_default();
    let target_file_path = add_file_suffix(
        &files[0].file_path,
        &format!("{SNAPSHOT_LOOKUP_SUFFIX}_{}", checkpoint.checkpoint_id),
        Some(&".parquet".to_string()),
    );
    if file_exists(&target_file_path).await {
        return Ok(Some(target_file_path));
    }

    let batches = execute_query_plan_batches(QueryExecutionPlan {
        sql: QuerySqlTemplate::Built(snapshot_lookup_full_scan_sql(!delete_files.is_empty())),
        files: checkpoint_query_inputs(files),
        delete_files: delete_files.clone(),
        use_deletes_table: !delete_files.is_empty(),
        extension_suffixes: None,
        use_cpu_threadpool: true,
    })
    .await
    .map_err(IndexError::from)?;

    if write_snapshot_lookup_parquet(&batches, &target_file_path).await? {
        Ok(Some(target_file_path))
    } else {
        Ok(None)
    }
}

async fn create_snapshot_exact_lookup_artifact(
    checkpoint: &TableMetadataCheckpoint,
) -> Result<Option<String>, IndexError> {
    let Some(iceberg_metadata) = checkpoint.iceberg_metadata.as_ref() else {
        return Ok(None);
    };
    let schema_map = checkpoint.schema.to_map();
    for required in ["_id", "_id_seq_no", "_seq_no", "_version", "_source"] {
        if !schema_map.contains_key(required) {
            return Ok(None);
        }
    }

    let files = iceberg_metadata.files.as_file_tuples();
    if files.is_empty() {
        return Ok(None);
    }

    let delete_files = checkpoint
        .deletes_metadata
        .as_ref()
        .map(|metadata| metadata.files.clone())
        .unwrap_or_default();
    let target_file_path = add_file_suffix(
        &files[0].file_path,
        &format!("{SNAPSHOT_EXACT_LOOKUP_SUFFIX}_{}", checkpoint.checkpoint_id),
        Some(&".parquet".to_string()),
    );
    if file_exists(&target_file_path).await {
        return Ok(Some(target_file_path));
    }

    let sql = if delete_files.is_empty() {
        "SELECT t.\"_id\", t.\"_id_seq_no\", t.\"_seq_no\", t.\"_version\", t.\"_source\" FROM {target_table} t WHERE t.\"_id\" IS NOT NULL".to_string()
    } else {
        "SELECT t.\"_id\", t.\"_id_seq_no\", t.\"_seq_no\", t.\"_version\", t.\"_source\" FROM {target_table} t LEFT JOIN {deletes_table} dt ON dt._id_seq_no = t.\"_id_seq_no\" WHERE dt._id_seq_no IS NULL AND t.\"_id\" IS NOT NULL".to_string()
    };

    let batches = execute_query_plan_batches(QueryExecutionPlan {
        sql: QuerySqlTemplate::Built(sql),
        files: checkpoint_query_inputs(files),
        delete_files: delete_files.clone(),
        use_deletes_table: !delete_files.is_empty(),
        extension_suffixes: None,
        use_cpu_threadpool: true,
    })
    .await
    .map_err(IndexError::from)?;

    if write_snapshot_lookup_parquet(&batches, &target_file_path).await? {
        Ok(Some(target_file_path))
    } else {
        Ok(None)
    }
}

async fn lookup_work_item_checkpoint(
    work_item: &ExtensionWorkItem,
) -> Result<Option<TableMetadataCheckpoint>, IndexError> {
    let Some(checkpoint_id) = work_item.checkpoint_id.as_ref() else {
        return Ok(None);
    };
    runtime_bindings::get_checkpoint(CheckpointDescriptor::new(
        work_item.table_name.clone(),
        checkpoint_id.clone(),
    ))
    .await
    .map_err(|error| {
        IndexError::new(format!(
            "Unable to load checkpoint {} for {}: {}",
            checkpoint_id, work_item.table_name, error
        ))
    })
}

pub(crate) async fn create_index(work_item: &ExtensionWorkItem) -> Result<(), IndexError> {
    let invocation = PrivateInvocation::Extension(PrivateExtensionInvocation {
        extension_name: work_item.extension_type.clone(),
        speedboat_files: work_item.speedboat_files.clone(),
        iceberg_files: work_item.iceberg_files.clone(),
    });

    let results = match call_peers(&invocation).await {
        Ok(output) => output
            .iter()
            .map(|r| match r {
                PrivateInvocationResult::Data(_) => {
                    panic!("Unexpected result from peer calls while indexing")
                }
                PrivateInvocationResult::Extension(files) => files.clone(),
                PrivateInvocationResult::Prefetch => {
                    panic!("Unexpected result from peer calls while indexing")
                }
            })
            .collect::<Vec<ExtensionFileMetadata>>(),
        Err(e) => {
            return Err(IndexError {
                message: e.message.clone(),
            });
        }
    };

    assert!(results.len() > 0);

    let mut final_result = ExtensionFileMetadata::new();
    for result in results {
        final_result.extend(result);
    }
    if let Some(checkpoint) = lookup_work_item_checkpoint(work_item).await? {
        let checkpoint_extension_key = checkpoint_extension_metadata_key(&checkpoint.checkpoint_id);
        let checkpoint_entry = final_result
            .entry(checkpoint_extension_key)
            .or_insert_with(Vec::new);
        if let Some(snapshot_exact_lookup_path) =
            create_snapshot_exact_lookup_artifact(&checkpoint).await?
        {
            checkpoint_entry.push(ExtensionFile {
                suffix: SNAPSHOT_EXACT_LOOKUP_SUFFIX.to_string(),
                location: snapshot_exact_lookup_path,
            });
        }
        if let Some(snapshot_lookup_path) = create_snapshot_lookup_artifact(&checkpoint).await? {
            checkpoint_entry.push(ExtensionFile {
                suffix: SNAPSHOT_LOOKUP_SUFFIX.to_string(),
                location: snapshot_lookup_path,
            });
        }
    }

    match runtime_bindings::extension_commit(
        &work_item.table_name,
        &ExtensionCommit {
            id: work_item.id.clone(),
            extension: "es".to_string(),
            files: final_result.clone(),
        },
    )
    .await
    {
        Ok(_) => (),
        Err(e) => {
            return Err(IndexError {
                message: format!("{}", e),
            });
        }
    }

    Ok(())
}

pub(crate) async fn create_index_inner_with_doc_id(
    iceberg_files: &Vec<FileDescriptor>,
    speedboat_files: &Vec<FileDescriptor>,
    doc_id_field_name: &String,
) -> Result<ExtensionFileMetadata, IndexError> {
    let all_files = iceberg_files
        .iter()
        .chain(speedboat_files.iter())
        .cloned()
        .collect::<Vec<FileDescriptor>>();
    if !all_files.is_empty() {
        validate_elastic_table_files(&all_files, doc_id_field_name)?;
    }

    let mut files = ExtensionFileMetadata::new();

    for file_desc in iceberg_files {
        match create_index_parquet(&file_desc.file_path, doc_id_field_name).await {
            Ok(output) => match output {
                Some(extension_file_path) => {
                    files
                        .entry(file_desc.file_path.clone())
                        .or_insert_with(Vec::new)
                        .push(ExtensionFile {
                            suffix: "search_index".to_string(),
                            location: extension_file_path.clone(),
                        });
                }
                None => (),
            },
            Err(e) => {
                return Err(IndexError::new(format!(
                    "Failed to build elastic sidecar for {}: {}",
                    file_desc.file_path, e
                )));
            }
        }

        match create_exact_index_parquet(&file_desc.file_path, doc_id_field_name).await {
            Ok(output) => match output {
                Some(extension_file_path) => {
                    files
                        .entry(file_desc.file_path.clone())
                        .or_insert_with(Vec::new)
                        .push(ExtensionFile {
                            suffix: "exact_index".to_string(),
                            location: extension_file_path.clone(),
                        });
                }
                None => (),
            },
            Err(e) => {
                return Err(IndexError::new(format!(
                    "Failed to build exact elastic sidecar for {}: {}",
                    file_desc.file_path, e
                )));
            }
        }

        match create_exact_pruning_index_parquet(&file_desc.file_path, doc_id_field_name).await {
            Ok(output) => match output {
                Some(extension_file_path) => {
                    files
                        .entry(file_desc.file_path.clone())
                        .or_insert_with(Vec::new)
                        .push(ExtensionFile {
                            suffix: "exact_pruning".to_string(),
                            location: extension_file_path.clone(),
                        });
                }
                None => (),
            },
            Err(e) => {
                return Err(IndexError::new(format!(
                    "Failed to build exact pruning sidecar for {}: {}",
                    file_desc.file_path, e
                )));
            }
        }
    }

    for file_desc in speedboat_files {
        match create_index_jsonl(&file_desc.file_path, doc_id_field_name, &file_desc.schema).await {
            Ok(output) => match output {
                Some(extension_file_path) => {
                    files
                        .entry(file_desc.file_path.clone())
                        .or_insert_with(Vec::new)
                        .push(ExtensionFile {
                            suffix: "search_index".to_string(),
                            location: extension_file_path.clone(),
                        });
                }
                None => (),
            },
            Err(e) => {
                return Err(IndexError::new(format!(
                    "Failed to build elastic sidecar for {}: {}",
                    file_desc.file_path, e
                )));
            }
        }

        match create_exact_index_jsonl(&file_desc.file_path, doc_id_field_name, &file_desc.schema)
            .await
        {
            Ok(output) => match output {
                Some(extension_file_path) => {
                    files
                        .entry(file_desc.file_path.clone())
                        .or_insert_with(Vec::new)
                        .push(ExtensionFile {
                            suffix: "exact_index".to_string(),
                            location: extension_file_path.clone(),
                        });
                }
                None => (),
            },
            Err(e) => {
                return Err(IndexError::new(format!(
                    "Failed to build exact elastic sidecar for {}: {}",
                    file_desc.file_path, e
                )));
            }
        }

        match create_exact_pruning_index_jsonl(
            &file_desc.file_path,
            doc_id_field_name,
            &file_desc.schema,
        )
        .await
        {
            Ok(output) => match output {
                Some(extension_file_path) => {
                    files
                        .entry(file_desc.file_path.clone())
                        .or_insert_with(Vec::new)
                        .push(ExtensionFile {
                            suffix: "exact_pruning".to_string(),
                            location: extension_file_path.clone(),
                        });
                }
                None => (),
            },
            Err(e) => {
                return Err(IndexError::new(format!(
                    "Failed to build exact pruning sidecar for {}: {}",
                    file_desc.file_path, e
                )));
            }
        }
    }
    Ok(files)
}

pub(crate) async fn create_index_inner(
    iceberg_files: &Vec<FileDescriptor>,
    speedboat_files: &Vec<FileDescriptor>,
) -> Result<ExtensionFileMetadata, IndexError> {
    create_index_inner_with_doc_id(iceberg_files, speedboat_files, &"_id_seq_no".to_string()).await
}

#[cfg(test)]
mod tests {
    use crate::elastic_search_index::create_index_parquet;
    use gotham::test::{Server as GothamServer, TestServer};
    use powdrr_query_server::router::router;
    use std::env;

    #[test]
    fn test_simple_create_index_parquet() {
        let test_server = TestServer::with_timeout(router(true), 1000).unwrap();

        test_server.run_future(async {
            match create_index_parquet(
                &format!(
                    "file://{}/testdata/flights.parquet",
                    env::current_dir().unwrap().to_str().unwrap()
                ),
                &"index_col".to_string(),
            )
            .await
            {
                Err(_) => panic!("failed"),
                Ok(_) => (),
            }
        });
    }
    /*
        #[test]
        fn test_simple_create_index_json() {
            let test_server = &*crate::router::tests::TEST_SERVER;

            test_server.run_future(async {
                match create_index_jsonl(&format!("file://{}/testdata/logs.json", env::current_dir().unwrap().to_str().unwrap()), &"index_col".to_string()).await {
                    Err(_) => panic!("failed"),
                    Ok(_) => ()
                }
            });
        }
    */
}
