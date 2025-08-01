use std::{error::Error, fmt};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use futures_util::future::try_join_all;
use futures_util::StreamExt;
use idgenerator::IdInstance;
use prost::Message;

use crate::data_access::{self, load_file_as_table};
use crate::data_contract::{ExtensionFileMetadata, FileDescriptor, IcebergMetadata, SpeedboatMetadata};
use crate::elastic_search_index::create_index_inner;
use crate::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema, SqlQuery};
use crate::peers::{CheckpointDescriptor, PrivateCompactionInvocation, PrivateExtensionInvocation, PrivatePrefetchInvocation, PrivateSqlInvocation};
use crate::state_provider::*;
use crate::util::{add_file_suffix, log_err};


#[derive(Debug)]
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
        PrivateApiError { message: format!("DataFusionError: {}", source) }
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
    iceberg_file_extensions: Vec<Vec<ExtensionFileSpec>>,
    speedboat_files: Vec<FileDescriptor>,
    speedboat_file_extensions: Vec<Vec<ExtensionFileSpec>>,
    delete_files: Vec<String>,
}


fn filter_iceberg<'a>(iceberg_metadata: &'a Option<IcebergMetadata>, index: u64, num: u64) -> Vec<FileDescriptor> {
    match iceberg_metadata {
        Some(im) => {
            let filtered_files = im.files.as_selected_tuples(index, num);
            // TODO: apply filters
            filtered_files
        },
        None => vec!()
    }    
}

fn filter_speedboat(speedboat_metadata: &Option<SpeedboatMetadata>, index: u64, num: u64) -> Vec<FileDescriptor> {
    match speedboat_metadata {
        Some(sm) => {
            let files = sm.files.as_selected_tuples(index, num);
            // TODO: apply filters
            files
        },
        None => vec!()
    }
}


async fn determine_required_files(required_extensions: &Vec<String>, checkpoints: &Vec<CheckpointDescriptor>, index: u64, num: u64) -> Result<RequiredFiles, PrivateApiError> {
    if required_extensions.len() > 1 || checkpoints.len() != 1 {
        return Err(PrivateApiError{ message: "Only read for one table at a time please.".to_string() })
    }

    let target_checkpoint = &checkpoints[0];
    let table_metadata = match STATE_PROVIDER.get_checkpoint(target_checkpoint.clone()).await {
        Ok(tmc) => {
            match tmc {
                Some(tmc) => tmc,
                None => panic!("The table metadata was not found for a known checkpoint: {}", target_checkpoint)
            }
        },
        Err(_e) => return log_err(PrivateApiError{ message: "Error calling get checkpoint".to_string() }),
    };

    // TODO: add logic to select the iceberg and speedboat files for this host.

    let filtered_iceberg_files = filter_iceberg(&table_metadata.iceberg_metadata, index, num);
    let filtered_speedboat_files = filter_speedboat(&table_metadata.speedboat_metadata, index, num);
    Ok(RequiredFiles {
        table_schema: table_metadata.schema.clone(),
        iceberg_files: filtered_iceberg_files.to_vec(),
        iceberg_file_extensions: filtered_iceberg_files.iter().map(|f|get_extension_files(required_extensions, &f.file_path)).collect(),
        speedboat_files: filtered_speedboat_files.to_vec(),
        speedboat_file_extensions: filtered_speedboat_files.iter().map(|f|get_extension_files(required_extensions, &f.file_path)).collect(),
        delete_files: table_metadata.deletes_metadata.map_or_else(|| vec!(), |d|d.files.clone()),
    })
}

fn generate_required_files(invocation: &PrivateCompactionInvocation, index: u64, num: u64) -> RequiredFiles {
    let speedboat_files = invocation.speedboat_files.as_selected_tuples(index, num);

    RequiredFiles {
        table_schema: invocation.table_schema.clone(),
        iceberg_files: vec![],
        speedboat_files: speedboat_files.clone(),
        iceberg_file_extensions: vec![],
        speedboat_file_extensions: speedboat_files.iter().map(|_|vec![]).collect(),
        delete_files: invocation.delete_files.clone(),
    }
}


fn get_extension_files(required_extensions: &Vec<String>, file_path: &String) -> Vec<ExtensionFileSpec> {
    // TODO - need to look at the actual extension metadata and figure out the file required
    if required_extensions.len() == 0 {
        vec!()
    } else {
        vec!(ExtensionFileSpec {
            suffix: "search_index".to_string(),
            file_path: add_file_suffix(file_path, &"search_index".to_string(), Some(&".parquet".to_string())),
        })
    }
}


async fn ensure_loaded(
    file_path: &String,
    extension_files: &Vec<ExtensionFileSpec>,
    top_level_size: u64,
    parquet: bool,
    schema: Option<PowdrrSchema>
) -> Result<String, DataFusionError> {
    let new_local_name = data_access::path_to_table_name(file_path);
    let extension_file_names = extension_files.iter().map(|e| format!("{}_{}", &new_local_name, e.suffix)).collect::<Vec<String>>();
    // TODO: add in extension file sizes
    let total_size = top_level_size;

    data_access::reserve(&new_local_name, total_size, extension_file_names.clone()).await;
    // After this, on error we need to release, on OK we do not release

    match load_file_as_table(&new_local_name, file_path, parquet, schema.map(|s|s.to_arrow_schema())).await {
        Err(e) => {
            data_access::release(&new_local_name).await;
            return log_err(e)
        },
        Ok(nln) => nln,
    };

    for (spec, name) in extension_files.iter().zip(extension_file_names.iter()) {
        match load_file_as_table(&name, &spec.file_path, true, None).await {
            Err(e) => {
                data_access::release(&new_local_name).await;
                let error = format!("{}", e);
                println!("{}", error);
                return log_err(e)
            },
            _ => ()
        };
    }

    Ok(new_local_name.clone())
}


async fn execute_sql(sql_template: &String, local_name: &String, deletes_local_name: &String, use_cpu_threadpool: bool) -> Result<Vec<RecordBatch>, DataFusionError> {
    // create a plan to run a SQL query
    let final_sql = sql_template.replace("{target_table}", local_name).replace("{deletes_table}", deletes_local_name);
    if use_cpu_threadpool {
        match data_access::execute_sql_async(&final_sql).await {
            Ok(val) => Ok(val),
            Err(e) => log_err(e)
        }
    } else {
        let results = match data_access::execute_sql(&final_sql).await {
            Ok(val) => val,
            Err(e) => return log_err(e)
        };
        match results.collect().await {
            Ok(r) => Ok(r),
            Err(e) => log_err(e)
        }
    }
}

async fn create_all_deletes_table(local_names: &Vec<String>) -> Result<String, PrivateApiError> {
    let table_name = format!("table_{}", IdInstance::next_id());
    let ddl_stmt;
    if local_names.len() == 0 {
        ddl_stmt = "select null as _id_seq_no".to_string();
    } else {
        let union_selects = local_names.iter().map(|x|format!("select * from {x}")).collect::<Vec<String>>().join(" union all ");
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
    let local_name = match ensure_loaded(&iceberg_file.file_path, iceberg_file_extensions,1,true, None).await {
        Ok(ln) => ln,
        Err(e) => return Err(PrivateApiError::from(e)),
    };
    let local_results = match execute_sql(&sql.build(table_schema, &iceberg_file.schema), &local_name, deletes_table_name, use_cpu_threadpool).await {
        Ok(vrb) => vrb,
        Err(e) => {
            data_access::release(&local_name).await;
            return log_err(PrivateApiError::from(e))
        },
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
    let local_name = match ensure_loaded(&speedboat_file.file_path, speedboat_file_extensions, speedboat_file.size, false, Some(speedboat_file.schema.clone())).await {
        Ok(ln) => ln,
        Err(e) => {
            return log_err(PrivateApiError::from(e))
        },
    };
    let sql = sql.build(table_schema, &speedboat_file.schema);
    let local_results = match execute_sql(&sql, &local_name, &deletes_table_name, use_cpu_threadpool).await {
        Ok(vrb) => vrb,
        Err(e) => return {
            data_access::release(&local_name).await;
            log_err(PrivateApiError::from(e))
        },
    };
    data_access::release(&local_name).await;
    Ok(local_results)
}


pub(crate) async fn data_query(invocation: &PrivateSqlInvocation, index: u64, num: u64) -> Result<DataQueryResult, PrivateApiError> {
    if invocation.checkpoints.len() == 0 {
        return Ok(DataQueryResult {
            num: 0,
            result: vec![],
        })        
    }

    let required_files = match determine_required_files(&invocation.required_extensions, &invocation.checkpoints, index, num).await {
        Ok(rf) => rf,
        Err(e) => return log_err(e),
    };

    let parquet_size = required_files.iceberg_files.iter().map(|f|f.size).sum::<u64>();
    let speedboat_size = required_files.speedboat_files.iter().map(|f|f.size).sum::<u64>();
    tracing::info!("Query: parquet = {}, {}, speedboat = {}, {}", required_files.iceberg_files.len(), parquet_size, required_files.speedboat_files.len(), speedboat_size);

    data_query_worker(
        &invocation.sql,
        &required_files,
        true,
    ).await
}


pub(crate) async fn compaction_query(invocation: &PrivateCompactionInvocation, index: u64, num: u64) -> Result<DataQueryResult, PrivateApiError> {
    let required_files = generate_required_files(invocation, index, num);
    
    data_query_worker(
        &invocation.sql,
        &required_files,
        true
    ).await
}

pub(crate) async fn extension_query(invocation: &PrivateExtensionInvocation, index: u64, num: u64) -> Result<ExtensionFileMetadata, PrivateApiError> {
    let iceberg_files = invocation.iceberg_files.as_selected_tuples(index, num);
    let speedboat_files = invocation.speedboat_files.as_selected_tuples(index, num);
    match create_index_inner(&iceberg_files, &speedboat_files).await {
        Ok(result) => Ok(result),
        Err(e) => Err(PrivateApiError{ message: format!("{}", e) }),
    }
}

pub(crate) async fn prefetch_query(invocation: &PrivatePrefetchInvocation, index: u64, num: u64) -> Result<DataQueryResult, PrivateApiError> {
    let required_files = match determine_required_files(&invocation.required_extensions, &invocation.checkpoints, index, num).await {
        Ok(rf) => rf,
        Err(e) => return log_err(e),
    };

    data_query_worker(
        &SqlQuery::dummy(),
        &required_files,
        false
    ).await
}


async fn data_query_worker(sql: &SqlQuery, required_files: &RequiredFiles, use_cpu_threadpool: bool) -> Result<DataQueryResult, PrivateApiError> {
    let mut delete_local_names = vec!();
    let delete_schema = PowdrrSchema::from(&vec!(
        PowdrrField{ name: "_id_seq_no".to_string(), data_type: PowdrrDataType::String },
    ));
    let extension_file_vecs = vec!();
    for delete_file_path in required_files.delete_files.iter() {
        let local_name = match ensure_loaded(&delete_file_path, &extension_file_vecs, 1, false, Some(delete_schema.clone())).await {
            Ok(ln) => ln,
            Err(e) => return log_err(PrivateApiError::from(e)),
        };
        delete_local_names.push(local_name);
    }
    // TODO: need to make a stable name here and skip this if it is already loaded
    let all_deletes_local_name = create_all_deletes_table(&delete_local_names).await?;

    let iceberg_calls = required_files.iceberg_files.iter().zip(required_files.iceberg_file_extensions.iter()).map(
        |(iceberg_file, iceberg_file_extensions)| process_iceberg_file(sql, iceberg_file, iceberg_file_extensions, &required_files.table_schema, &all_deletes_local_name, use_cpu_threadpool));
    let speedboat_calls = required_files.speedboat_files.iter().zip(required_files.speedboat_file_extensions.iter()).map(
        |(speedboat_file, speedboat_file_extensions)| process_speedboat_file(sql, speedboat_file, speedboat_file_extensions, &required_files.table_schema, &all_deletes_local_name, use_cpu_threadpool));

    let iceberg_results: Vec<Result<RecordBatch, FlightError>> = match try_join_all(iceberg_calls).await {
        Ok(ar) => ar.iter().flatten().map(|x|Ok(x.clone())).collect::<Vec<Result<RecordBatch, FlightError>>>(),
        Err(e) => {
            let error = format!("{}", e.message);
            println!("{}", error);
            panic!("dude")
        },
    };

    let speedboat_results: Vec<Result<RecordBatch, FlightError>> = match try_join_all(speedboat_calls).await {
        Ok(ar) => ar.iter().flatten().map(|x|Ok(x.clone())).collect::<Vec<Result<RecordBatch, FlightError>>>(),
        Err(e) => {
            let error = format!("{}", e.message);
            println!("{}", error);
            panic!("dude")
        },
    };    

    data_access::drop(&all_deletes_local_name).await;

    let mut retval = Vec::new();
    let input_stream = futures::stream::iter(iceberg_results).chain(futures::stream::iter(speedboat_results));
    let mut flight_data_stream = FlightDataEncoderBuilder::new()
        .build(input_stream);
    while let Some(value) = flight_data_stream.next().await {
        let mut buf = Vec::new();
        match value {
            Ok(v) => match v.encode(&mut buf) {
                Ok(_) => (),
                Err(e) => {
                    let error = format!("Error encoding data: {:?}", e);
                    panic!("{}", error);
                },
            }
            Err(e) => {
                let error = format!("Error streaming data: {:?}", e);
                panic!("{}", error);
            },
        };
        retval.push(buf);
    }
    Ok(DataQueryResult { num: 0, result: retval })
}
