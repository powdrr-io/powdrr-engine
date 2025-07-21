use std::hash::{DefaultHasher, Hash, Hasher};
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
use crate::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema, SqlQuery};
use crate::state_peers::{PrivateCompactionInvocation, PrivateSqlInvocation};
use crate::state_hosted_service::*;
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


#[derive(Clone)]
struct FileDescriptor {
    file_path: String,
    schema: PowdrrSchema,
    size: u64,
}

struct RequiredFiles {
    table_schema: PowdrrSchema,
    iceberg_files: Vec<FileDescriptor>,
    iceberg_file_extensions: Vec<Vec<ExtensionFileSpec>>,
    speedboat_files: Vec<FileDescriptor>,
    speedboat_file_extensions: Vec<Vec<ExtensionFileSpec>>,
    delete_files: Vec<String>,
}


fn selected_file(file_path: &String, index: u64, num: u64) -> bool {
    // TODO: validate this is a stable hash (aka it will give the same value on every machine on every run)
    let mut hasher = DefaultHasher::new();
    file_path.hash(&mut hasher);
    let hash_val = hasher.finish();
    hash_val % num == index
}


fn matches_filter(_iceberg_metadata: &IcebergMetadata, _index: usize, _file_path: &String) -> bool {
    true
}


fn filter_iceberg<'a>(iceberg_metadata: &'a Option<IcebergMetadata>, index: u64, num: u64) -> Vec<FileDescriptor> {
    match iceberg_metadata {
        Some(im) => {
            let mut filtered_files = vec!();
            for (idx, file_path) in im.files.iter().enumerate() {
                if !selected_file(file_path, index, num) {
                    continue;
                }
                if matches_filter(im, idx, file_path) {
                    let schema_index = *im.file_schemas.get(idx).unwrap() as usize;
                    let schema = im.schemas.get(schema_index).unwrap().clone();
                    filtered_files.push(FileDescriptor{ file_path: file_path.clone(), schema, size: 1 });
                }
            }
        
            filtered_files
        },
        None => vec!()
    }    
}

fn filter_speedboat<'a>(_invocation: &'a PrivateSqlInvocation, speedboat_metadata: &'a Option<SpeedboatMetadata>) -> Vec<FileDescriptor> {
    match speedboat_metadata {
        Some(sm) => {
            sm.files.iter().enumerate().map(|(index, file_path)|FileDescriptor{
                file_path: file_path.clone(),
                schema: sm.schemas.get(*sm.file_schemas.get(index).unwrap() as usize).unwrap().clone(),
                size: sm.sizes.get(index).unwrap().clone(),
            }).collect()
        },
        None => vec!()
    }
}


async fn determine_required_files(invocation: &PrivateSqlInvocation, index: u64, num: u64) -> Result<RequiredFiles, PrivateApiError> {
    if invocation.required_extensions.len() > 1 || invocation.snapshots.len() != 1 {
        return Err(PrivateApiError{ message: "Only read for one table at a time please.".to_string() })
    }

    let target_snapshot = &invocation.snapshots[0];
    let table_metadata = match API_SERVICE_CLIENT.get_checkpoint(target_snapshot.clone()).await {
        Ok(tmc) => tmc,
        Err(_e) => return log_err(PrivateApiError{ message: "Error calling get checkpoint".to_string() }),
    };

    // TODO: add logic to select the iceberg and speedboat files for this host.

    let filtered_iceberg_files = filter_iceberg(&table_metadata.iceberg_metadata, index, num);
    let filtered_speedboat_files = filter_speedboat(invocation, &table_metadata.speedboat_metadata);
    Ok(RequiredFiles {
        table_schema: table_metadata.schema.clone(),
        iceberg_files: filtered_iceberg_files.to_vec(),
        iceberg_file_extensions: filtered_iceberg_files.iter().map(|_|vec!()).collect(),
        speedboat_files: filtered_speedboat_files.to_vec(),
        speedboat_file_extensions: filtered_speedboat_files.iter().map(|f|get_extension_files(invocation, &f.file_path)).collect(),
        delete_files: table_metadata.deletes_metadata.map_or_else(|| vec!(), |d|d.files.clone()),
    })
}

fn generate_required_files(invocation: &PrivateCompactionInvocation, index: u64, num: u64) -> RequiredFiles {
    let speedboat_files = invocation.speedboat_files.iter().zip(invocation.file_schemas.iter()).map(
        |(file_path, schema_index)| {
            if selected_file(file_path, index, num) {
                Some(FileDescriptor {
                    file_path: file_path.clone(),
                    schema: invocation.schemas.get(*schema_index as usize).unwrap().clone(),
                    size: 1,
                })
            } else {
                None
            }
        }
    ).flatten().collect::<Vec<FileDescriptor>>();

    RequiredFiles {
        table_schema: invocation.table_schema.clone(),
        iceberg_files: vec![],
        speedboat_files: speedboat_files.clone(),
        iceberg_file_extensions: vec![],
        speedboat_file_extensions: speedboat_files.iter().map(|_|vec![]).collect(),
        delete_files: invocation.delete_files.clone(),
    }
}


fn get_extension_files(invocation: &PrivateSqlInvocation, file_path: &String) -> Vec<ExtensionFileSpec> {
    // TODO - need to look at the actual extension metadata and figure out the file required
    if invocation.required_extensions.len() == 0 {
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


async fn execute_sql(sql_template: &String, local_name: &String, deletes_local_name: &String) -> Result<Vec<RecordBatch>, DataFusionError> {
    // create a plan to run a SQL query
    let final_sql = sql_template.replace("{target_table}", local_name).replace("{deletes_table}", deletes_local_name);
    let results = match data_access::execute_sql(&final_sql).await {
        Ok(df) => df,
        Err(e) => return log_err(e),
    };
    match results.collect().await {
        Ok(r) => Ok(r),
        Err(e) => log_err(e)
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
    deletes_table_name: &String
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    let local_name = match ensure_loaded(&iceberg_file.file_path, iceberg_file_extensions,1,true, None).await {
        Ok(ln) => ln,
        Err(e) => return Err(PrivateApiError::from(e)),
    };
    let local_results = match execute_sql(&sql.build(table_schema, &iceberg_file.schema), &local_name, deletes_table_name).await {
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
    deletes_table_name: &String
) -> Result<Vec<RecordBatch>, PrivateApiError> {
    let local_name = match ensure_loaded(&speedboat_file.file_path, speedboat_file_extensions, speedboat_file.size, false, Some(speedboat_file.schema.clone())).await {
        Ok(ln) => ln,
        Err(e) => {
            return log_err(PrivateApiError::from(e))
        },
    };
    let sql = sql.build(table_schema, &speedboat_file.schema);
    let local_results = match execute_sql(&sql, &local_name, &deletes_table_name).await {
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
    if invocation.snapshots.len() == 0 {
        return Ok(DataQueryResult {
            num: 0,
            result: vec![],
        })        
    }

    let required_files = match determine_required_files(invocation, index, num).await {
        Ok(rf) => rf,
        Err(e) => return log_err(e),
    };

    data_query_worker(
        &invocation.sql,
        &required_files,
    ).await
}


pub(crate) async fn compaction_query(invocation: &PrivateCompactionInvocation, index: u64, num: u64) -> Result<DataQueryResult, PrivateApiError> {
    let required_files = generate_required_files(invocation, index, num);
    
    data_query_worker(
        &invocation.sql,
        &required_files
    ).await
}


async fn data_query_worker(sql: &SqlQuery, required_files: &RequiredFiles) -> Result<DataQueryResult, PrivateApiError> {
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
        |(iceberg_file, iceberg_file_extensions)| process_iceberg_file(sql, iceberg_file, iceberg_file_extensions, &required_files.table_schema, &all_deletes_local_name));
    let speedboat_calls = required_files.speedboat_files.iter().zip(required_files.speedboat_file_extensions.iter()).map(
        |(speedboat_file, speedboat_file_extensions)| process_speedboat_file(sql, speedboat_file, speedboat_file_extensions, &required_files.table_schema, &all_deletes_local_name));

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
