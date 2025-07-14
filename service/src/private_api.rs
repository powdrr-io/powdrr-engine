use std::hash::{DefaultHasher, Hash, Hasher};
use std::{error::Error, fmt};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use idgenerator::IdInstance;
use serde::Serialize;

use crate::data_access::{self, load_file_as_table};
use crate::schema_massager::PowdrrSchema;
use crate::state_peers::PrivateSqlInvocation;
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


#[derive(Serialize)]
pub(crate) struct DataQueryResult {
    pub(crate) num: u32,
    pub(crate) result: String,
}


#[derive(Clone)]
struct FileDescriptor {
    file_path: String,
    schema: PowdrrSchema,
}

struct RequiredFiles {
    table_schema: PowdrrSchema,
    iceberg_files: Vec<FileDescriptor>,
    speedboat_files: Vec<FileDescriptor>,
    delete_files: Vec<String>,
}


fn selected_file(invocation: &PrivateSqlInvocation, file_path: &String) -> bool {
    let mut hasher = DefaultHasher::new();
    file_path.hash(&mut hasher);
    let hash_val = hasher.finish();
    hash_val % invocation.num == invocation.index
}


fn matches_filter(_iceberg_metadata: &IcebergMetadata, _index: usize, _file_path: &String) -> bool {
    true
}


fn filter_iceberg<'a>(invocation: &'a PrivateSqlInvocation, iceberg_metadata: &'a Option<IcebergMetadata>) -> Vec<FileDescriptor> {
    match iceberg_metadata {
        Some(im) => {
            let mut filtered_files = vec!();
            for (idx, file_path) in im.files.iter().enumerate() {
                if !selected_file(invocation, file_path) {
                    continue;
                }
                if matches_filter(im, idx, file_path) {
                    let schema_index = *im.file_schemas.get(idx).unwrap() as usize;
                    let schema = im.schemas.get(schema_index).unwrap().clone();
                    filtered_files.push(FileDescriptor{ file_path: file_path.clone(), schema });
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
            }).collect()
        },
        None => vec!()
    }
}


async fn determine_required_files(invocation: &PrivateSqlInvocation) -> Result<RequiredFiles, PrivateApiError> {
    if invocation.required_extensions.len() > 1 || invocation.snapshots.len() != 1 {
        return Err(PrivateApiError{ message: "Only read for one table at a time please.".to_string() })
    }

    let target_snapshot = &invocation.snapshots[0];
    let table_metadata = match API_SERVICE_CLIENT.get_checkpoint(target_snapshot.clone()).await {
        Ok(tmc) => tmc,
        Err(_e) => return log_err(PrivateApiError{ message: "Error calling get checkpoint".to_string() }),
    };

    let filtered_iceberg_files = filter_iceberg(invocation, &table_metadata.iceberg_metadata);
    let filtered_speedboat_files = filter_speedboat(invocation, &table_metadata.speedboat_metadata);
    Ok(RequiredFiles {
        table_schema: table_metadata.schema.clone(),
        iceberg_files: filtered_iceberg_files.to_vec(),
        speedboat_files: filtered_speedboat_files.to_vec(),
        delete_files: table_metadata.deletes_metadata.map_or_else(|| vec!(), |d|d.files.clone()),
    })
}


fn get_extension_files(invocation: &PrivateSqlInvocation, file_path: &String) -> Vec<ExtensionFileSpec> {
    // TODO - need to look at the actual extension and figure out the file required
    if invocation.required_extensions.len() == 0 {
        vec!()
    } else {
        vec!(ExtensionFileSpec {
            suffix: "search_index".to_string(),
            file_path: add_file_suffix(file_path, &"search_index".to_string(), Some(&".parquet".to_string())),
        })
    }
}


async fn ensure_loaded(invocation: &PrivateSqlInvocation, file_path: &String, parquet: bool) -> Result<String, DataFusionError> {
    let new_local_name = data_access::path_to_table_name(file_path);
    let extension_files = get_extension_files(invocation, file_path);
    let extension_file_names = extension_files.iter().map(|e| format!("{}_{}", &new_local_name, e.suffix)).collect::<Vec<String>>();
    let total_size = 0;

    data_access::reserve(&new_local_name, total_size, extension_file_names.clone()).await;

    match load_file_as_table(&new_local_name, file_path, parquet).await {
        Err(e) => {
            data_access::release(&new_local_name).await;
            return Err(e)
        },
        Ok(nln) => nln,
    };

    let extension_files = get_extension_files(invocation, file_path);
    for (spec, name) in extension_files.iter().zip(extension_file_names.iter()) {
        match load_file_as_table(&name, &spec.file_path, true).await {
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                data_access::release(&new_local_name).await;
                return Err(e)
            },
            _ => ()
        };

    }

    data_access::release(&new_local_name).await;

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
        ddl_stmt = format!("create table {table_name} as select null as _id, null as _seq_no");
    } else {
        let union_selects = local_names.iter().map(|x|format!("select * from {x}")).collect::<Vec<String>>().join(" union all ");
        ddl_stmt = format!("create table {table_name} as select _id, max(_seq_no) as _seq_no from ({union_selects}) group by _id");
    }
    match data_access::execute_sql(&ddl_stmt).await {
        Ok(_) => Ok(table_name.clone()),
        Err(e) => return log_err(PrivateApiError::from(e)),
    }
}


pub(crate) async fn data_query(invocation: &PrivateSqlInvocation) -> Result<DataQueryResult, PrivateApiError> {
    let required_files = match determine_required_files(invocation).await {
        Ok(rf) => rf,
        Err(e) => return log_err(e),
    };

    let mut delete_local_names = vec!();
    for delete_file_path in required_files.delete_files {
        let local_name = match ensure_loaded(invocation, &delete_file_path, false).await {
            Ok(ln) => ln,
            Err(e) => return log_err(PrivateApiError::from(e)),
        };
        delete_local_names.push(local_name);
    }
    // TODO: need to make a stable name here and skip this if it is already loaded
    let all_deletes_local_name = create_all_deletes_table(&delete_local_names).await?;     

    let mut all_results: Vec<RecordBatch> = vec!();
    for iceberg_file in required_files.iceberg_files.iter() {
        let local_name = match ensure_loaded(invocation, &iceberg_file.file_path, true).await {
            Ok(ln) => ln,
            Err(e) => return Err(PrivateApiError::from(e)),
        };
        let local_results = match execute_sql(&invocation.sql.build(&required_files.table_schema, &iceberg_file.schema), &local_name, &all_deletes_local_name).await {
            Ok(vrb) => vrb,
            Err(e) => return log_err(PrivateApiError::from(e)),
        };
        all_results.extend(local_results);
    }
    for speedboat_file in required_files.speedboat_files.iter() {
        let local_name = match ensure_loaded(invocation, &speedboat_file.file_path, false).await {
            Ok(ln) => ln,
            Err(e) => return log_err(PrivateApiError::from(e)),
        };
        let local_results = match execute_sql(&invocation.sql.build(&required_files.table_schema, &speedboat_file.schema), &local_name, &all_deletes_local_name).await {
            Ok(vrb) => vrb,
            Err(e) => return log_err(PrivateApiError::from(e)),
        };
        all_results.extend(local_results);
    }
         
    let all_results_refs: Vec<&RecordBatch> = all_results.iter().map(|f| f).collect();
    let total_num_rows = match all_results_refs.len() {
        0 => 0,
        _ => all_results.iter().map(|v| v.num_rows()).reduce(|l, r| l + r).unwrap()
    };
    // TODO: need to convert this whole thing into arrow flight
    let buf = Vec::new();
    let mut writer = arrow_json::LineDelimitedWriter::new(buf);
    writer.write_batches(all_results_refs.as_slice()).unwrap();
    writer.finish().unwrap();
    
    // Get the underlying buffer back,
    let buf = writer.into_inner();

    let result = String::from_utf8(buf).unwrap();

    Ok(DataQueryResult { num: total_num_rows as u32, result: result })
}
