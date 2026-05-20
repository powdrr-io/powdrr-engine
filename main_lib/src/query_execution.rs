use crate::data_access::{self, load_file_as_table, load_files_as_table};
use crate::data_contract::FileDescriptor;
use crate::schema_massager::{PowdrrSchema, SqlQuery};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use futures_util::future::try_join_all;
use idgenerator::IdInstance;
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueryExtensionFileSpec {
    pub suffix: String,
    pub file_path: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryStorageKind {
    Iceberg,
    Speedboat,
}

#[derive(Clone, Debug)]
pub(crate) struct QueryInputFile {
    pub file: FileDescriptor,
    pub storage: QueryStorageKind,
    pub extensions: Vec<QueryExtensionFileSpec>,
}

#[derive(Clone, Debug)]
pub(crate) enum QuerySqlTemplate {
    Built(String),
    Structured {
        sql: SqlQuery,
        table_schema: PowdrrSchema,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct QueryExecutionPlan {
    // This is the protocol-neutral mixed-source read plan consumed by both
    // lakehouse serving and Elasticsearch/private reads.
    pub sql: QuerySqlTemplate,
    pub files: Vec<QueryInputFile>,
    pub delete_files: Vec<String>,
    pub extension_suffixes: Option<Vec<String>>,
    pub use_cpu_threadpool: bool,
}

pub(crate) fn group_query_input_files_by_schema(
    files: Vec<QueryInputFile>,
) -> Vec<Vec<QueryInputFile>> {
    let mut groups: Vec<Vec<QueryInputFile>> = vec![];

    for file in files {
        let mut suffixes = file
            .extensions
            .iter()
            .map(|extension| extension.suffix.clone())
            .collect::<Vec<_>>();
        suffixes.sort();
        if let Some(existing_group) = groups.iter_mut().find(|group| {
            let Some(existing) = group.first() else {
                return false;
            };
            let mut existing_suffixes = existing
                .extensions
                .iter()
                .map(|extension| extension.suffix.clone())
                .collect::<Vec<_>>();
            existing_suffixes.sort();
            existing.storage == file.storage
                && existing.file.schema == file.file.schema
                && existing_suffixes == suffixes
        }) {
            existing_group.push(file);
        } else {
            groups.push(vec![file]);
        }
    }

    groups
}

impl QuerySqlTemplate {
    fn build_for_schema(&self, target_schema: &PowdrrSchema) -> String {
        match self {
            QuerySqlTemplate::Built(sql) => sql.clone(),
            QuerySqlTemplate::Structured { sql, table_schema } => {
                sql.build(table_schema, target_schema)
            }
        }
    }
}

pub(crate) async fn execute_query_plan_batches(
    plan: QueryExecutionPlan,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let QueryExecutionPlan {
        sql,
        files,
        delete_files,
        extension_suffixes,
        use_cpu_threadpool,
    } = plan;

    if files.is_empty() {
        return Ok(vec![]);
    }

    let files = filter_query_input_extensions(files, extension_suffixes.as_ref());
    let delete_local_tables = load_delete_local_tables(&delete_files).await?;
    let deletes_table_name = create_deletes_union_table(&delete_local_tables).await?;
    let grouped_files = group_query_input_files_by_schema(files);

    // Iceberg and speedboat share one execution entry point here; the only
    // per-storage difference is how a grouped file set is loaded into local
    // DataFusion tables before the shared SQL runs.
    let grouped_calls = grouped_files.into_iter().map(|group| {
        let sql_template = sql.build_for_schema(&group[0].file.schema);
        let deletes_table_name = deletes_table_name.clone();
        async move {
            match group[0].storage {
                QueryStorageKind::Iceberg => {
                    execute_query_file_group_batches_with_deletes_table(
                        group,
                        &sql_template,
                        &deletes_table_name,
                        "{target_table}",
                        use_cpu_threadpool,
                    )
                    .await
                }
                QueryStorageKind::Speedboat => {
                    execute_speedboat_group_batches(
                        group,
                        &sql_template,
                        &deletes_table_name,
                        use_cpu_threadpool,
                    )
                    .await
                }
            }
        }
    });

    let grouped_results = try_join_all(grouped_calls).await;

    for local_table in delete_local_tables.iter() {
        data_access::drop(local_table).await;
    }
    data_access::drop(&deletes_table_name).await;

    grouped_results.map(|results| results.into_iter().flatten().collect())
}

fn filter_query_input_extensions(
    files: Vec<QueryInputFile>,
    suffixes: Option<&Vec<String>>,
) -> Vec<QueryInputFile> {
    let Some(suffixes) = suffixes else {
        return files;
    };

    files
        .into_iter()
        .map(|mut file| {
            file.extensions
                .retain(|extension| suffixes.iter().any(|suffix| suffix == &extension.suffix));
            file
        })
        .collect()
}

pub(crate) async fn execute_query_file_group_batches(
    files: Vec<QueryInputFile>,
    sql_template: &str,
    delete_files: &[String],
    table_placeholder: &str,
    _deletes_placeholder: &str,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if files.is_empty() {
        return Ok(vec![]);
    }

    let delete_local_tables = load_delete_local_tables(delete_files).await?;
    let deletes_table_name = create_deletes_union_table(&delete_local_tables).await?;
    let result = execute_query_file_group_batches_with_deletes_table(
        files,
        sql_template,
        &deletes_table_name,
        table_placeholder,
        use_cpu_threadpool,
    )
    .await;

    for local_table in delete_local_tables.iter() {
        data_access::drop(local_table).await;
    }
    data_access::drop(&deletes_table_name).await;
    result
}

async fn execute_query_file_group_batches_with_deletes_table(
    files: Vec<QueryInputFile>,
    sql_template: &str,
    deletes_table_name: &str,
    table_placeholder: &str,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let local_name = file_group_table_name(&files);
    let extension_groups = extension_groups_by_suffix(&files);
    let related_names = extension_groups
        .keys()
        .map(|suffix| format!("{local_name}_{suffix}"))
        .collect::<Vec<_>>();
    let base_file_paths = files
        .iter()
        .map(|file| file.file.file_path.clone())
        .collect::<Vec<_>>();
    let total_size = files.iter().map(|file| file.file.size).sum::<u64>();
    data_access::reserve(&local_name, total_size, related_names).await;

    let result = async {
        load_files_as_table(
            &local_name,
            &base_file_paths,
            &files[0].file.schema.to_arrow_schema(),
        )
        .await?;

        for (suffix, file_paths) in extension_groups.iter() {
            load_files_as_table(
                &format!("{local_name}_{suffix}"),
                file_paths,
                &extension_schema_for_suffix(suffix)?,
            )
            .await?;
        }

        let local_sql = sql_template
            .replace(table_placeholder, &local_name)
            .replace("{deletes_table}", deletes_table_name);
        execute_group_sql(&local_sql, use_cpu_threadpool).await
    }
    .await;

    data_access::release(&local_name).await;
    result
}

async fn execute_speedboat_group_batches(
    files: Vec<QueryInputFile>,
    sql_template: &str,
    deletes_table_name: &str,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let per_file_calls = files.into_iter().map(|file| {
        let sql_template = sql_template.to_string();
        let deletes_table_name = deletes_table_name.to_string();
        async move {
            execute_single_file_batches(
                file,
                &sql_template,
                &deletes_table_name,
                use_cpu_threadpool,
            )
            .await
        }
    });
    let results = try_join_all(per_file_calls).await?;
    Ok(results.into_iter().flatten().collect())
}

async fn execute_single_file_batches(
    file: QueryInputFile,
    sql_template: &str,
    deletes_table_name: &str,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let local_name = query_input_local_name(&file);
    let extension_table_names = file
        .extensions
        .iter()
        .map(|extension| format!("{}_{}", local_name, extension.suffix))
        .collect::<Vec<_>>();
    data_access::reserve(&local_name, file.file.size, extension_table_names).await;

    let result = async {
        load_file_as_table(
            &local_name,
            &file.file.file_path,
            matches!(file.storage, QueryStorageKind::Iceberg),
            Some(file.file.schema.to_arrow_schema()),
        )
        .await?;

        for extension in file.extensions.iter() {
            let extension_table_name = format!("{}_{}", local_name, extension.suffix);
            load_file_as_table(&extension_table_name, &extension.file_path, true, None).await?;
        }

        let local_sql = sql_template
            .replace("{target_table}", &local_name)
            .replace("{deletes_table}", deletes_table_name);
        execute_group_sql(&local_sql, use_cpu_threadpool).await
    }
    .await;

    data_access::release(&local_name).await;
    result
}

fn file_group_table_name(files: &[QueryInputFile]) -> String {
    let mut file_paths = files
        .iter()
        .map(|file| file.file.file_path.clone())
        .collect::<Vec<_>>();
    file_paths.sort();

    let mut hasher = DefaultHasher::new();
    for file_path in file_paths.iter() {
        file_path.hash(&mut hasher);
    }

    format!("table_group_{:016x}", hasher.finish())
}

fn extension_groups_by_suffix(files: &[QueryInputFile]) -> BTreeMap<String, Vec<String>> {
    let mut grouped = BTreeMap::<String, Vec<String>>::new();
    for file in files {
        for extension in &file.extensions {
            grouped
                .entry(extension.suffix.clone())
                .or_default()
                .push(extension.file_path.clone());
        }
    }
    grouped
}

fn query_input_local_name(file: &QueryInputFile) -> String {
    data_access::path_to_table_name(&file.file.file_path)
}

async fn load_delete_local_tables(delete_files: &[String]) -> Result<Vec<String>, DataFusionError> {
    let delete_schema = PowdrrSchema::deletes().to_arrow_schema();
    let mut local_tables = vec![];
    for delete_file in delete_files.iter() {
        let local_table = format!("query_delete_file_{}", IdInstance::next_id());
        load_file_as_table(
            &local_table,
            delete_file,
            false,
            Some(delete_schema.clone()),
        )
        .await?;
        local_tables.push(local_table);
    }
    Ok(local_tables)
}

async fn create_deletes_union_table(
    delete_local_tables: &[String],
) -> Result<String, DataFusionError> {
    let deletes_table_name = format!("query_deletes_{}", IdInstance::next_id());
    data_access::create_table(
        &deletes_table_name,
        &create_deletes_table_sql(delete_local_tables),
    )
    .await?;
    Ok(deletes_table_name)
}

fn extension_schema_for_suffix(suffix: &str) -> Result<Schema, DataFusionError> {
    let schema = match suffix {
        "search_index" => Schema::new(vec![
            Field::new("doc_id", DataType::Utf8, true),
            Field::new("field_name", DataType::Utf8, false),
            Field::new("field_term", DataType::Utf8, true),
            Field::new("term_cnt", DataType::UInt64, true),
            Field::new("word_cnt", DataType::UInt64, true),
        ]),
        "exact_index" => Schema::new(vec![
            Field::new("doc_id", DataType::Utf8, true),
            Field::new("field_name", DataType::Utf8, false),
            Field::new("field_value", DataType::Utf8, true),
        ]),
        "exact_pruning" => Schema::new(vec![
            Field::new("field_name", DataType::Utf8, false),
            Field::new("field_value", DataType::Utf8, true),
            Field::new("complete", DataType::Boolean, false),
        ]),
        other => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported grouped extension suffix {}",
                other
            )));
        }
    };
    Ok(schema)
}

fn create_deletes_table_sql(local_names: &[String]) -> String {
    if local_names.is_empty() {
        "select null as _id_seq_no".to_string()
    } else {
        let union_selects = local_names
            .iter()
            .map(|name| format!("select * from {name}"))
            .collect::<Vec<_>>()
            .join(" union all ");
        format!("select * from ({union_selects})")
    }
}

async fn execute_group_sql(
    sql: &str,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if use_cpu_threadpool {
        data_access::execute_sql_async(&sql.to_string()).await
    } else {
        let results = data_access::execute_sql(&sql.to_string()).await?;
        results.collect().await
    }
}
