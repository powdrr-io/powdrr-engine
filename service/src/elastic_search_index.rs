use std::error::Error;

use datafusion::{arrow::datatypes::DataType, dataframe::DataFrameWriteOptions};

use crate::{data_access::{execute_sql, exists}, state_hosted_service::{ExtensionCommit, ExtensionFile, ExtensionFileMetadata, ExtensionMetadata, TableMetadataCheckpoint, API_SERVICE_CLIENT}, util::add_file_suffix};
use crate::data_access::load_file_as_table;

#[allow(dead_code)]
pub(crate) struct IndexError {
    pub message: String,
}


fn is_string_type(data_type: &DataType) -> bool {
    data_type.equals_datatype(&DataType::Utf8View) || data_type.equals_datatype(&DataType::Utf8) || data_type.equals_datatype(&DataType::LargeUtf8)
}


async fn create_index_worker(table_name: &String, doc_id_field_name: &String, target_file_path: &String) -> Result<(), Box<dyn Error>> {
    let new_local_name = table_name;
    let doc_id_field_name_local = doc_id_field_name;

    let raw_table = match execute_sql(&format!("select * from {new_local_name}").to_string()).await {
        Err(e) => return Err(Box::new(e)),
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

    match execute_sql(&format!("CREATE TABLE {new_local_name}_fields AS {field_normalization_queries_union}")).await {
        Err(e) => {
            return Err(Box::new(e))
        },
        _ => ()
    };

    match execute_sql(&format!("CREATE TABLE {new_local_name}_split AS SELECT doc_id, field_name, string_to_array(field_value, ' ') as field_terms from {new_local_name}_fields")).await {
        Err(e) => return Err(Box::new(e)),
        _ => ()
    };    

    match execute_sql(&format!("CREATE TABLE {new_local_name}_split_unnest AS SELECT doc_id, field_name, unnest(field_terms) as field_term from {new_local_name}_split")).await {
        Err(e) => return Err(Box::new(e)),
        _ => ()
    };

    match execute_sql(&format!("CREATE TABLE {new_local_name}_term_frequency AS SELECT doc_id, field_name, doc_id || '_' || field_name as doc_id_field_name, field_term, count(1) as term_cnt from {new_local_name}_split_unnest group by doc_id, field_name, field_term")).await {
        Err(e)  => {
            return Err(Box::new(e))
        },
        _ => ()
    }; 

    match execute_sql(&format!("CREATE TABLE {new_local_name}_field_size AS SELECT doc_id, field_name, doc_id || '_' || field_name as doc_id_field_name, array_length(field_terms) as word_cnt from {new_local_name}_split")).await {
        Err(e) => return Err(Box::new(e)),
        _ => ()
    }; 

    // TODO: need to think about multiple term search still
    // TODO: can you search multiple fields at once?
    // TODO: split into a file per term? then we don't even move the data or load it for terms we don't search for and
    //       we do one less filter at query time. might make for very small files though.

    // f(Qi, D) = select doc_id, field_name, doc_id_field_name, field_term, term_cnt from term_frequency where field_name = '{target_field}' and field_term = '{target_term} and term_cnt > 0'
    // |D| = select doc_id, field_name, doc_id_field_name, word_cnt from field_size where field_name = '{target_field}'
    // (THIS SHOULD BE IN THE METADATA) avgdl = select field_name, sum(word_cnt) as total_word_cnt from field_size group by field_name
    // (THIS SHOUDL BE IN THE METADATA) N = select count(1) from base_table
    // n(qi) = num results in f(Qi, D)

    match execute_sql(&format!("CREATE TABLE {new_local_name}_joined AS SELECT tf.doc_id, tf.field_name, tf.field_term, tf.term_cnt, fs.word_cnt from {new_local_name}_term_frequency tf INNER JOIN {new_local_name}_field_size fs ON tf.doc_id_field_name = fs.doc_id_field_name WHERE tf.term_cnt > 0")).await {
        Err(e) => {
            return Err(Box::new(e))
        },
        _ => ()
    };     

    if target_file_path.starts_with("s3:") {
        let joined_table = match execute_sql(&format!("SELECT * FROM {new_local_name}_joined").to_string()).await {
            Err(e) => {
                return Err(Box::new(e))
            },
            Ok(tft) => tft,
        }; 

        match joined_table.write_parquet(
            target_file_path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None).await {
            Err(e) => {
                return Err(Box::new(e))
            },
            _ => (),
        };
    } else {
        let joined_table = match execute_sql(&format!("SELECT * FROM {new_local_name}_joined").to_string()).await {
            Err(e) => {
                return Err(Box::new(e))
            },
            Ok(tft) => tft,
        }; 

        match joined_table.write_parquet(
            target_file_path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None).await {
            Err(e) => {
                return Err(Box::new(e))
            },
            _ => (),
        };
    }
    
    // TODO: make sure to drop all tables
    Ok(())

}

pub(crate) async fn create_index_jsonl(file_path: &String, doc_id_field_name: &String) -> Result<Option<String>, Box<dyn Error>> {
    let target_file_path = add_file_suffix(file_path, &"search_index".to_string(), Some(&".parquet".to_string()));
    if exists(&target_file_path).await {
        return Ok(None)
    }
    
    let result = load_file_as_table(file_path, 0, None, false).await;
    let new_local_name = match result {
        Err(e) => {
            let error = format!("{}", e);
            println!("{}", error);
            return Err(Box::new(e))
        },
        Ok(name) => name,
    };

    match create_index_worker(&new_local_name, doc_id_field_name, &target_file_path).await {
        Ok(_) => (),
        Err(e) => return Err(e),
    }
    Ok(Some(target_file_path))
}

pub(crate) async fn create_index_parquet(file_path: &String, doc_id_field_name: &String) -> Result<Option<String>, Box<dyn Error>> {
    let target_file_path = add_file_suffix(file_path, &"search_index".to_string(), Some(&".parquet".to_string()));
    if exists(&target_file_path).await {
        return Ok(None)
    }

    let result = load_file_as_table(file_path, 0, None, true).await;
    let new_local_name = match result {
        Err(e) => return Err(Box::new(e)),
        Ok(name) => name,
    };

    match create_index_worker(&new_local_name, doc_id_field_name, &target_file_path).await {
        Ok(_) => (),
        Err(e) => return Err(e),
    }
    Ok(Some(target_file_path))
}


pub(crate) async fn create_index(table_metadata: &TableMetadataCheckpoint) -> Result<(), Box<dyn Error>> {
    let files = create_index_inner(table_metadata).await?;
    if files.len() > 0 {
        match API_SERVICE_CLIENT.extension_commit(
            &table_metadata.table_name,
            &ExtensionCommit {
                extension: "es".to_string(),
                checkpoint_id: table_metadata.checkpoint_id.clone(),
                partial_metadata: ExtensionMetadata {
                    files: files,
                },
            }
        ).await {
            Ok(_) => (),
            Err(e) => return Err(Box::new(e)),
        }
    }

    Ok(())
}

pub(crate) async fn create_index_inner(table_metadata: &TableMetadataCheckpoint) -> Result<Vec<ExtensionFileMetadata>, Box<dyn Error>> {
    let mut files: Vec<ExtensionFileMetadata> = vec!();

    // TODO: does just "_id" work for everything?

    match &table_metadata.iceberg_metadata {
        Some(im) => {
            for file_path in im.files.iter() {
                match create_index_parquet(file_path, &"_id".to_string()).await {
                    Ok(output) => match output {
                        Some(extension_file_path) => files.push(ExtensionFileMetadata {
                            data_file_location: file_path.clone(),
                            extension_file_locations: vec!(ExtensionFile{ suffix: "_search_index".to_string(), location: extension_file_path.clone() }),
                        }),
                        None => (),
                    },
                    Err(e) => {
                        let error = format!("{}", e);
                        println!("{}", error);
                        panic!("nope");
                    },
                }
            }
        },
        None => (),
    };

    match &table_metadata.speedboat_metadata {
        Some(im) => {
            for file_path in im.files.iter() {
                match create_index_jsonl(file_path, &"_id".to_string()).await {
                    Ok(output) => match output {
                        Some(extension_file_path) => files.push(ExtensionFileMetadata {
                            data_file_location: file_path.clone(),
                            extension_file_locations: vec!(ExtensionFile{ suffix: "_search_index".to_string(), location: extension_file_path.clone() }),
                        }),
                        None => (),
                    },
                    Err(_) => panic!("nope"),
                }
            }
        },
        None => (),
    };
    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::env;
    use gotham::test::Server;
    use crate::elastic_search_index::{create_index_jsonl, create_index_parquet};

    #[test]
    fn test_simple_create_index_parquet() {
        let test_server = &*crate::router::tests::TEST_SERVER;

        test_server.run_future(async {
            match create_index_parquet(&format!("file://{}/tests/data/flights.parquet", env::current_dir().unwrap().to_str().unwrap()), &"index_col".to_string()).await {
                Err(_) => panic!("failed"),
                Ok(_) => ()
            }
        });
    }

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

}