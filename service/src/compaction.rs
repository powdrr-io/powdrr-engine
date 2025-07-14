
use std::{error::Error, fmt};
use iceberg_lib::iceberg::{compact_logs, load_table_metadata, CompactionResult};
use idgenerator::IdInstance;
use tokio::sync::oneshot::error::RecvError;

use crate::{state_hosted_service::{CompactionCommit, IcebergCommit, IcebergMetadata, SpeedboatCommit, SpeedboatCommitTableInfo, API_SERVICE_CLIENT}, util::log_err};
use crate::schema_massager::PowdrrSchema;
use crate::state_hosted_service::CompactionWorkItem;

#[derive(Debug)]
pub(crate) struct CompactionError {
    pub message: String,
}

impl Error for CompactionError {}

impl fmt::Display for CompactionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}


async fn do_iceberg_commit(table_name: &String, last_snapshot_id: i64) -> Result<i64, RecvError> {
    let lib_metadata = match load_table_metadata(
        &"default".to_string(), 
        table_name, 
        last_snapshot_id
    ).await {
        Ok(m) => m,
        Err(_) => panic!("nope"),
    };

    match API_SERVICE_CLIENT.iceberg_commit(
        table_name,
        &IcebergCommit {
            metadata: IcebergMetadata {
                snapshot_id: lib_metadata.snapshot_id.to_string(),
                files: lib_metadata.files,
                column_names: lib_metadata.column_names,
                column_stats: lib_metadata.column_stats,
                // TODO: need to get a real schema here
                schemas: vec!(),
                file_schemas: vec!(),
            },
            compactions: lib_metadata.compactions,
        }
    ).await {
        Ok(_) => (),
        Err(e) => return Err(e)
    };

    return Ok(lib_metadata.snapshot_id)    
}


async fn do_speedboat_commit(table_name: &String, file_path: &String, compaction_id: &String, num_records: u64, schema: &PowdrrSchema) -> Result<(), RecvError> {
    match API_SERVICE_CLIENT.speedboat_commit(
        &SpeedboatCommit{
            commit_type: "commit".to_string(),
            type_files: vec!(SpeedboatCommitTableInfo { 
                table_name: table_name.clone(), 
                files: vec!(file_path.to_string()),
                sizes: vec!(num_records),
                schema: Some(schema.clone()),
            }),
            compactions: vec!(compaction_id.clone()),
        }
    ).await {
        Ok(_) => Ok(()),
        Err(e) => Err(e)
    }
}


pub(crate) async fn perform_compaction(work_items: Vec<(String, CompactionWorkItem)>, last_snapshot_id: i64) -> Result<i64, CompactionError> {
    let mut new_last_snapshot_id = 0;
    for (table_name, work_item) in work_items.iter() {
        // if table_group.0 != "logs" {
        //    panic!("Only logs supported");
        //}

        let files = work_item.files.clone();
        let compaction_id = IdInstance::next_id().to_string();

        // NOTE: the api commit must happen before the iceberg commit. The service is designed to understand that
        // a compaction commit might get committed to it but fail afterwards. If we commit to Iceberg and fail to
        // record that in the service then that leads to correctness errors that aren't really possible to fix.
        match API_SERVICE_CLIENT.compaction_commit(
            table_name,
            &CompactionCommit {
                removed_file_locations: files.iter().cloned().collect(),
                compaction_id: compaction_id.clone(),
            }
        ).await {
            Ok(_) => (),
            Err(_) => return Err(CompactionError { message: "api call failed".to_string() }),
        }   

        match compact_logs(
            &"default".to_string(),
            &table_name,
            &compaction_id,
            &files,
            &vec!(),
            10_000,
        ).await {
            Ok(result) => match result {
                CompactionResult::None => (),
                CompactionResult::Iceberg{ num_records} => {
                    tracing::info!("Iceberg compaction: {} speedboat files, {} records", files.len(), num_records);
                    match do_iceberg_commit(&table_name, last_snapshot_id).await {
                        Ok(s) => { new_last_snapshot_id = s },
                        Err(e) => return log_err(CompactionError{ message: format!("{}", e) }),
                    }
                },
                CompactionResult::Speedboat{ file_location, num_records } => {
                    tracing::info!("Speedboat compaction: {} speedboat files, {} records", files.len(), num_records);
                    for file in files {
                        tracing::info!("compaction file: {}", file);
                    }
                    // TODO: need to get a real schema here
                    let schema = PowdrrSchema{ fields: vec!() };
                    match do_speedboat_commit(&table_name, &file_location, &compaction_id, num_records.try_into().unwrap(), &schema).await {
                        Ok(_) => (),
                        Err(_) => return log_err(CompactionError{ message: "dunno".to_string() })
                    }
                },
            },
            Err(e) => return log_err(CompactionError{ message: format!("{}", e) })
        };
    }
   
    Ok(new_last_snapshot_id)
}
