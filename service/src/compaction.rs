
use std::{error::Error, fmt};
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use futures_util::FutureExt;
use gotham::mime;
use http::StatusCode;
use idgenerator::IdInstance;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::error::RecvError;

use crate::{elastic_search_ingest, state_hosted_service::{CompactionCommit, API_SERVICE_CLIENT}};
use crate::data_access::execute_sql;
use crate::elastic_search_commands::to_serde_value;
use crate::elastic_search_common::{execute_command, Command, CommandContext, ElasticSearchResponse, ResultGeneratorFuture};
use crate::elastic_search_ingest::WriteBuffer;
use crate::schema_massager::SqlBuilder;
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
        let table = self.table.clone();
        let compactions = vec!(self.compaction_id.clone());
        let schema = self.work_item.table_schema.clone();
        async move {
            let table_name = match result_table_name {
                Some(t) => t,
                None => {
                    // TODO: Need to commit that after this compaction there is....nothing?
                    // Maybe this should panic since it shouldn't be possible to get here.
                    let none = CompactionResult::None;
                    return Ok(ElasticSearchResponse {
                        status: StatusCode::OK,
                        mime: mime::APPLICATION_JSON,
                        body: serde_json::to_string(&none).unwrap(),
                        headers: vec![],
                    });
                }
            };
            let remaining_deletes_data_frame = match execute_sql(&format!("select _dt_id as _id, _dt_seq_no from {table_name} where _id is null and _dt_id is not null")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };
            let results_data_frame = match execute_sql(&format!("select * from {table_name} where _id is not null and _dt_id is null")).await {
                Ok(df) => {
                    match df.drop_columns(&["_dt_id", "_dt_seq_no"]) {
                        Ok(df) => df,
                        Err(_) => panic!("nope")
                    }
                },
                Err(_) => panic!("nope")
            };

            let (compacted_deletes, _) = to_serde_value(&remaining_deletes_data_frame).await;
            let (compacted_results, _) = to_serde_value(&results_data_frame).await;

            let mut result_buffer = WriteBuffer::new();
            result_buffer.schema = Some(schema);
            result_buffer.push_many(compacted_results.iter().map(|x|serde_json::to_string(x).unwrap()).collect());
            match elastic_search_ingest::commit_general_compactions(&result_buffer, &table, &"compact".to_string(), &compactions).await {
                Ok(_) => (),
                Err(_) => panic!("nope"),
            };

            if compacted_deletes.len() != 0 {
                let mut deletes_buffer = WriteBuffer::new();
                deletes_buffer.push_many(compacted_deletes.iter().map(|x| serde_json::to_string(x).unwrap()).collect());
                match elastic_search_ingest::commit_general_compactions(&deletes_buffer, &table, &"delete".to_string(), &compactions).await {
                    Ok(_) => (),
                    Err(_) => panic!("nope"),
                };
            }

            Ok(ElasticSearchResponse {
                status: StatusCode::OK,
                mime: mime::TEXT_PLAIN,
                body: "success".to_string(),
                headers: vec![],
            })
        }.boxed()
    }
}


async fn compact_logs(command: Arc<dyn Command>) -> Result<(), CompactionError>{
    let _response = execute_command(CommandContext{}, command).await;
    // TODO: look at response to see if there are errors?
    Ok(())
}

#[allow(dead_code)]
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


pub(crate) async fn perform_compaction(work_items: Vec<(String, CompactionWorkItem)>, last_snapshot_id: i64) -> Result<i64, CompactionError> {
    let new_last_snapshot_id = last_snapshot_id;
    for (table_name, work_item) in work_items.iter() {
        assert_eq!(work_item.iceberg_files.len(), 0, "Iceberg file compaction is not yet implemented");

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

        compact_logs(Arc::new(command)).await?;
        // TODO: need to figure out the last snapshot stuff when iceberg is implemented
    }
   
    Ok(new_last_snapshot_id)
}
