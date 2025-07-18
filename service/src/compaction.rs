
use std::{error::Error, fmt};
use std::fs::read_to_string;
use std::pin::Pin;
use async_trait::async_trait;
use futures_util::FutureExt;
use gotham::mime;
use http::StatusCode;
use idgenerator::IdInstance;
use tokio::sync::oneshot::error::RecvError;

use crate::{data_access, state_hosted_service::{CompactionCommit, SpeedboatCommit, SpeedboatCommitTableInfo, API_SERVICE_CLIENT}, util::log_err};
use crate::data_access::execute_sql;
use crate::elastic_search_common::{Command, ElasticSearchResponse, ResultGeneratorFuture};
use crate::schema_massager::{extract_powdrr_schema_str, PowdrrSchema, SqlBuilder};
use crate::state_hosted_service::CompactionWorkItem;
use crate::state_peers::{PrivateCompactionInvocation, PrivateInvocation};

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

#[allow(dead_code)]
pub enum CompactionResult {
    None,
    Iceberg {
        num_records: usize,
    },
    Speedboat {
        file_location: String,
        num_records: usize,
    }
}

struct CompactionCommand {
    table: String,
    work_item: CompactionWorkItem,
    #[allow(dead_code)]
    compaction_id: String,
}

#[async_trait]
impl Command for CompactionCommand {
    async fn get_private_invocation(&self) -> PrivateInvocation {
        assert_eq!(self.work_item.iceberg_files.len(), 0, "Iceberg file compaction is not yet implemented");
        PrivateInvocation::Compaction(PrivateCompactionInvocation {
            sql: SqlBuilder::for_compaction().build(),
            speedboat_files: self.work_item.speedboat_files.clone(),
            schemas: self.work_item.schemas.clone(),
            file_schemas: self.work_item.file_schemas.clone(),
            table_schema: self.work_item.table_schema.clone(),
            delete_files: self.work_item.delete_files.clone(),
        })
    }

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>> {
        let _table = self.table.clone();
        async move {
            // TODO: Need to come up with a real result here.
            let result = Ok(ElasticSearchResponse {
                status: StatusCode::OK,
                mime: mime::TEXT_PLAIN,
                body: "Success".to_string(),
                headers: vec![],
            });

            let table_name = match result_table_name {
                Some(t) => t,
                None => {
                    // TODO: Need to commit that after this compaction there is....nothing?
                    // Maybe this should panic since it shouldn't be possible to get here.
                    return result
                }
            };
            let _remaining_deletes_data_frame = match execute_sql(&format!("select * from {table_name} where t._id = null")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };
            let _results_data_frame = match execute_sql(&format!("select * from {table_name} where t._id = null")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };

            // TODO 1: Write remaining delete to a file
            // TODO 2: Write results to a file
            // TODO 3: Commit the update to Iceberg as necessary
            // TODO 4: Commit the update to Speedboat as necessary
            data_access::drop(&table_name).await;
            result
        }.boxed()
    }
}


async fn compact_logs(_command: &CompactionCommand) -> Result<CompactionResult, CompactionError>{
    Ok(CompactionResult::None)
}


async fn do_iceberg_commit(_table_name: &String, _last_snapshot_id: i64) -> Result<i64, RecvError> {
    /*
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
    */

    todo!("Need to implement this")
}


async fn do_speedboat_commit(table_name: &String, file_path: &String, compaction_id: &String, total_size: u64, schema: &PowdrrSchema) -> Result<(), RecvError> {
    match API_SERVICE_CLIENT.speedboat_commit(
        &SpeedboatCommit{
            commit_type: "commit".to_string(),
            type_files: vec!(SpeedboatCommitTableInfo { 
                table_name: table_name.clone(), 
                files: vec!(file_path.to_string()),
                sizes: vec!(total_size),
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
        assert_eq!(work_item.iceberg_files.len(), 0, "Iceberg file compaction is not yet implemented");

        // TODO: this is all wrong
        let files = work_item.speedboat_files.clone();

        let compaction_id = IdInstance::next_id().to_string();

        // NOTE: the api commit must happen before the iceberg commit. The service is designed to understand that
        // a compaction commit might get committed to it but fail afterwards. If we commit to Iceberg and fail to
        // record that in the service then that leads to correctness errors that aren't really possible to fix.
        match API_SERVICE_CLIENT.compaction_commit(
            table_name,
            &CompactionCommit {
                removed_speedboat_files: work_item.speedboat_files.clone(),
                removed_iceberg_files: work_item.iceberg_files.clone(),
                compaction_id: compaction_id.clone(),
                removed_delete_files: work_item.delete_files.clone(),
            }
        ).await {
            Ok(_) => (),
            Err(_) => return Err(CompactionError { message: "api call failed".to_string() }),
        }

        let command = CompactionCommand {
            table: table_name.clone(),
            work_item: work_item.clone(),
            compaction_id: compaction_id.clone(),
        };

        match compact_logs(&command).await {
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
                    // TODO: need the compactor to tell me the schema eventually
                    let content = read_to_string(&file_location).unwrap();
                    let schema = extract_powdrr_schema_str(content.as_str());

                    match do_speedboat_commit(&table_name, &file_location, &compaction_id, content.len() as u64, &schema).await {
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
