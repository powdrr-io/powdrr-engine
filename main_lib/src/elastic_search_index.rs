use crate::data_access::load_file_as_table;
use crate::data_contract::{ExtensionFileMetadata, ExtensionWorkItem, FileDescriptor};
use crate::elastic_search_common::call_peers;
use crate::elastic_table_validation::{ElasticTableValidationError, validate_elastic_table_files};
use crate::peers::{PrivateExtensionInvocation, PrivateInvocation, PrivateInvocationResult};
use crate::schema_massager::PowdrrSchema;
use crate::state_provider::STATE_PROVIDER;
use crate::{
    data_access,
    data_access::{execute_sql, file_exists},
    data_contract::{ExtensionCommit, ExtensionFile},
    util::add_file_suffix,
};
use datafusion::common::config::{ParquetColumnOptions, TableParquetOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::col;
use datafusion::{arrow::datatypes::DataType, dataframe::DataFrameWriteOptions};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;

#[derive(Debug)]
pub(crate) struct IndexError {
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

fn sidecar_parquet_options() -> TableParquetOptions {
    TableParquetOptions {
        column_specific_options: HashMap::from([
            (
                "field_name".to_string(),
                ParquetColumnOptions {
                    bloom_filter_enabled: Some(true),
                    ..ParquetColumnOptions::default()
                },
            ),
            (
                "field_term".to_string(),
                ParquetColumnOptions {
                    bloom_filter_enabled: Some(true),
                    ..ParquetColumnOptions::default()
                },
            ),
        ]),
        ..TableParquetOptions::default()
    }
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
            DataFrameWriteOptions::new()
                .with_single_file_output(true)
                .with_sort_by(vec![
                    col("field_name").sort(true, true),
                    col("field_term").sort(true, true),
                    col("doc_id").sort(true, true),
                ]),
            Some(sidecar_parquet_options()),
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

pub(crate) async fn create_index_parquet(
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

    match STATE_PROVIDER
        .extension_commit(
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
    use crate::util::add_file_suffix;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use gotham::test::Server;
    use parquet_55::file::reader::FileReader;
    use parquet_55::file::serialized_reader::SerializedFileReader;
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn write_test_parquet(path: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("doc_id", DataType::Int64, false),
            Field::new("message", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2_i64, 1_i64, 3_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec!["beta alpha", "alpha beta", "alpha"])) as ArrayRef,
            ],
        )
        .unwrap();

        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_sidecar_parquet_enables_bloom_filters_and_sorting_metadata() {
        let test_server = &*crate::router::tests::TEST_SERVER;

        test_server.run_future(async {
            let temp_dir = TempDir::new().unwrap();
            let parquet_path = temp_dir.path().join("events.parquet");
            write_test_parquet(&parquet_path);

            let source_path = format!("file://{}", parquet_path.display());
            let sidecar_path = add_file_suffix(
                &source_path,
                &"search_index".to_string(),
                Some(&".parquet".to_string()),
            );

            let result = create_index_parquet(&source_path, &"doc_id".to_string())
                .await
                .unwrap();
            assert_eq!(result.as_deref(), Some(sidecar_path.as_str()));

            let sidecar_local_path = sidecar_path.strip_prefix("file://").unwrap();
            let file = fs::File::open(sidecar_local_path).unwrap();
            let reader = SerializedFileReader::new(file).unwrap();
            let metadata = reader.metadata();
            let row_group = metadata.row_group(0);

            let sorting_columns = row_group.sorting_columns().unwrap();
            assert_eq!(sorting_columns.len(), 3);

            let bloom_enabled_columns = row_group
                .columns()
                .iter()
                .filter(|column| column.bloom_filter_offset().is_some())
                .map(|column| column.column_path().string())
                .collect::<Vec<String>>();
            assert!(bloom_enabled_columns.contains(&"field_name".to_string()));
            assert!(bloom_enabled_columns.contains(&"field_term".to_string()));
        });
    }
    /*
        #[test]
        fn test_simple_create_index_json() {
            let test_server = &*crate::router::tests::TEST_SERVER;

            test_server.run_future(async {
                match create_index_jsonl(&format!("file://{}/tests/data/logs.json", env::current_dir().unwrap().to_str().unwrap()), &"index_col".to_string()).await {
                    Err(_) => panic!("failed"),
                    Ok(_) => ()
                }
            });
        }
    */
}
