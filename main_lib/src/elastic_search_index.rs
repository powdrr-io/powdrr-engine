use std::error::Error;
use std::fmt::Display;
use datafusion::{arrow::datatypes::DataType, dataframe::DataFrameWriteOptions};
use datafusion::error::DataFusionError;
use crate::{data_access, data_access::{execute_sql, exists}, data_contract::{ExtensionCommit, ExtensionFile}, util::add_file_suffix};
use crate::data_access::load_file_as_table;
use crate::elastic_search_common::call_peers;
use crate::schema_massager::PowdrrSchema;
use crate::data_contract::{ExtensionFileMetadata, ExtensionWorkItem, FileDescriptor};
use crate::peers::{PrivateExtensionInvocation, PrivateInvocation, PrivateInvocationResult};
use crate::state_provider::STATE_PROVIDER;


#[derive(Debug)]
pub(crate) struct IndexError {
    pub message: String,
}

impl Error for IndexError {}
unsafe impl Send for IndexError {}
unsafe impl Sync for IndexError {}

impl IndexError {
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


fn is_string_type(data_type: &DataType) -> bool {
    data_type.equals_datatype(&DataType::Utf8View) || data_type.equals_datatype(&DataType::Utf8) || data_type.equals_datatype(&DataType::LargeUtf8)
}


async fn drop_all(tables: &Vec<String>) -> () {
    for table in tables {
        data_access::drop(table).await;
    }
}


async fn create_index_worker(table_name: &String, doc_id_field_name: &String, target_file_path: &String) -> Result<(), IndexError> {
    let new_local_name = table_name;
    let doc_id_field_name_local = doc_id_field_name;
    let mut created_tables = vec!();

    let raw_table = match execute_sql(&format!("select * from {new_local_name}").to_string()).await {
        Err(e) => return Err(IndexError::from(e)),
        Ok(rt) => rt,        
    };

    let fields_without_doc_id_field: Vec<&String> = raw_table.schema().iter()
        .filter(|c| is_string_type(c.1.data_type()) && c.1.name() != doc_id_field_name)
        .map(|c| c.1.name()).collect();

    let field_normalization_queries: Vec<String> = fields_without_doc_id_field.iter().map(
        |field_name| format!("SELECT {doc_id_field_name_local} as doc_id, '{field_name}' as field_name, {new_local_name}.\"{field_name}\" as field_value from {new_local_name}")
    ).collect();

    if field_normalization_queries.len() == 0 {
        // There are no string fields so there is nothing to index
        return Ok(())
    }

    let field_normalization_queries_union = field_normalization_queries.join(" UNION ");

    match data_access::create_table(&format!("{new_local_name}_fields"), &field_normalization_queries_union).await {
        Err(e) => {
            return Err(IndexError::from(e))
        },
        _ => ()
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

    match data_access::create_table(&format!("{new_local_name}_term_frequency"), &format!("SELECT doc_id, field_name, doc_id || '_' || field_name as doc_id_field_name, field_term, count(1) as term_cnt from {new_local_name}_split_unnest group by doc_id, field_name, field_term")).await {
        Err(e)  => {
            drop_all(&created_tables).await;
            return Err(IndexError::from(e))
        },
        _ => ()
    }; 
    created_tables.push(format!("{new_local_name}_term_frequency"));

    match data_access::create_table(&format!("{new_local_name}_field_size"), &format!("SELECT doc_id, field_name, doc_id || '_' || field_name as doc_id_field_name, array_length(field_terms) as word_cnt from {new_local_name}_split")).await {
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

    let joined_table = match execute_sql(&format!("SELECT * FROM {new_local_name}_joined").to_string()).await {
        Err(e) => {
            drop_all(&created_tables).await;
            return Err(IndexError::from(e))
        },
        Ok(tft) => tft,
    };

    match joined_table.write_parquet(
        target_file_path,
        DataFrameWriteOptions::new().with_single_file_output(true),
        None).await {
        Err(e) => {
            drop_all(&created_tables).await;
            return Err(IndexError::from(e))
        },
        _ => (),
    };
    drop_all(&created_tables).await;
    Ok(())
}

pub(crate) async fn create_index_jsonl(file_path: &String, doc_id_field_name: &String, schema: &PowdrrSchema) -> Result<Option<String>, IndexError> {
    let target_file_path = add_file_suffix(file_path, &"search_index".to_string(), Some(&".parquet".to_string()));
    if exists(&target_file_path).await {
        return Ok(None)
    }

    let top_level_name = data_access::path_to_table_name(file_path);
    // TODO: pass in real size
    data_access::reserve(&top_level_name, 1000, vec!()).await;
    
    let result = data_access::load_file_as_table(&top_level_name, file_path, false, Some(schema.to_arrow_schema())).await;
    match result {
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(IndexError::from(e))
        },
        Ok(_) => ()
    };

    match create_index_worker(&top_level_name, doc_id_field_name, &target_file_path).await {
        Ok(_) => (),
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(e)
        },
    }

    data_access::release(&top_level_name).await;
    Ok(Some(target_file_path))
}

pub(crate) async fn create_index_parquet(file_path: &String, doc_id_field_name: &String) -> Result<Option<String>, IndexError> {
    let target_file_path = add_file_suffix(file_path, &"search_index".to_string(), Some(&".parquet".to_string()));
    if exists(&target_file_path).await {
        return Ok(None)
    }

    let top_level_name = data_access::path_to_table_name(file_path);
    // TODO: pass in real size   
    data_access::reserve(&top_level_name, 1000, vec!()).await;

    let result = load_file_as_table(&top_level_name, file_path, true, None).await;
    match result {
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(IndexError::from(e))
        },
        Ok(_) => ()
    };

    match create_index_worker(&top_level_name, doc_id_field_name, &target_file_path).await {
        Ok(_) => (),
        Err(e) => {
            data_access::release(&top_level_name).await;
            return Err(e)
        },
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
        Ok(output) => {
            output.iter().map(|r| {
                match r {
                    PrivateInvocationResult::Data(_) => panic!("Unexpected result from peer calls while indexing"),
                    PrivateInvocationResult::Extension(files) => files.clone(),
                    PrivateInvocationResult::Prefetch => panic!("Unexpected result from peer calls while indexing"),
                }
            }).collect::<Vec<ExtensionFileMetadata>>()
        },
        Err(e) => {
            return Err(IndexError{ message: e.message.clone() })
        }
    };

    assert!(results.len() > 0);

    let mut final_result = ExtensionFileMetadata::new();
    for result in results {
        final_result.extend(result);
    }

    match crate::state_provider::StateProviderProxy::extension_commit(
        &work_item.table_name,
        &ExtensionCommit {
            id: work_item.id.clone(),
            extension: "es".to_string(),
            files: final_result.clone()
        }
    ).await {
        Ok(_) => (),
        Err(e) => return Err(IndexError{ message: format!("{}", e)}),
    }

    Ok(())
}

pub(crate) async fn create_index_inner(iceberg_files: &Vec<FileDescriptor>, speedboat_files: &Vec<FileDescriptor>) -> Result<ExtensionFileMetadata, IndexError> {
    let mut files = ExtensionFileMetadata::new();

    for file_desc in iceberg_files {
        match create_index_parquet(&file_desc.file_path, &"_id_seq_no".to_string()).await {
            Ok(output) => match output {
                Some(extension_file_path) => {
                    files.insert(
                        file_desc.file_path.clone(),
                        vec!(ExtensionFile{ suffix: "_search_index".to_string(), location: extension_file_path.clone() }),
                    );
                },
                None => (),
            },
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("nope");
            },
        }
    }

    for file_desc in speedboat_files {
        match create_index_jsonl(&file_desc.file_path, &"_id_seq_no".to_string(), &file_desc.schema).await {
            Ok(output) => match output {
                Some(extension_file_path) => {
                    files.insert(
                        file_desc.file_path.clone(),
                        vec!(ExtensionFile{ suffix: "_search_index".to_string(), location: extension_file_path.clone() }),
                    );
                },
                None => (),
            },
            Err(_) => panic!("nope"),
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::env;
    use gotham::test::Server;
    use crate::elastic_search_index::create_index_parquet;

    #[tokio::test]
    async fn test_simple_create_index_parquet() {
        match create_index_parquet(&format!("file://{}/tests/data/flights.parquet", env::current_dir().unwrap().to_str().unwrap()), &"index_col".to_string()).await {
            Err(_) => panic!("failed"),
            Ok(_) => ()
        }
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
