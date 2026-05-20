use crate::data_access::{self, load_file_as_table, load_files_as_table};
use crate::data_contract::FileDescriptor;
use crate::schema_massager::PowdrrSchema;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use idgenerator::IdInstance;
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueryExtensionFileSpec {
    pub suffix: String,
    pub file_path: String,
}

#[derive(Clone, Debug)]
pub(crate) struct QueryInputFile {
    pub file: FileDescriptor,
    pub extensions: Vec<QueryExtensionFileSpec>,
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
            existing.file.schema == file.file.schema && existing_suffixes == suffixes
        }) {
            existing_group.push(file);
        } else {
            groups.push(vec![file]);
        }
    }

    groups
}

pub(crate) async fn execute_query_file_group_batches(
    files: Vec<QueryInputFile>,
    sql_template: &str,
    delete_files: &[String],
    table_placeholder: &str,
    deletes_placeholder: &str,
    use_cpu_threadpool: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if files.is_empty() {
        return Ok(vec![]);
    }

    let local_name = file_group_table_name(&files);
    let deletes_table_name = format!("query_deletes_{}", IdInstance::next_id());
    let delete_local_tables = delete_files
        .iter()
        .map(|_| format!("query_delete_file_{}", IdInstance::next_id()))
        .collect::<Vec<_>>();
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

    let mut created_delete_tables = vec![];
    let mut deletes_union_created = false;
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

        if !delete_files.is_empty() {
            let delete_schema = PowdrrSchema::deletes().to_arrow_schema();
            for (local_delete_table, delete_file_path) in
                delete_local_tables.iter().zip(delete_files.iter())
            {
                load_file_as_table(
                    local_delete_table,
                    delete_file_path,
                    false,
                    Some(delete_schema.clone()),
                )
                .await?;
                created_delete_tables.push(local_delete_table.clone());
            }
        }
        data_access::create_table(
            &deletes_table_name,
            &create_deletes_table_sql(&delete_local_tables),
        )
        .await?;
        deletes_union_created = true;

        let local_sql = sql_template
            .replace(table_placeholder, &local_name)
            .replace(deletes_placeholder, &deletes_table_name);
        execute_group_sql(&local_sql, use_cpu_threadpool).await
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
