use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex},
};

use serde_json::Value;

use powdrr_query_lib::data_contract::TableMetadataCheckpoint;
use powdrr_query_lib::serving_plan::ServingRequestPlan;
use powdrr_query_runtime::lakehouse_serving::{
    execute_checkpoint_exact_id_lookup_rows, execute_checkpoint_exact_lookup_batch_rows,
};
use powdrr_query_runtime::peers::CheckpointDescriptor;
use powdrr_query_runtime::read_plan::ReadPlan;
use powdrr_query_runtime::state_provider::{STATE_PROVIDER, ServiceApiError};

#[derive(Clone)]
struct CachedActiveCheckpoint {
    checkpoint_id: String,
    checkpoint: Arc<TableMetadataCheckpoint>,
}

static ACTIVE_CHECKPOINT_CACHE: LazyLock<Mutex<HashMap<String, CachedActiveCheckpoint>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
pub(crate) enum ActiveCheckpointLookupError {
    NotFound(String),
    Internal(String),
}

pub(crate) async fn execute_active_checkpoint_exact_lookup_batch_rows(
    table_name: &str,
    requests: &[ServingRequestPlan],
) -> Result<Option<Vec<Vec<Value>>>, ActiveCheckpointLookupError> {
    let checkpoint = load_active_checkpoint(table_name).await?;
    let read_plans = requests.iter().map(ReadPlan::from).collect::<Vec<_>>();
    execute_checkpoint_exact_lookup_batch_rows(checkpoint.as_ref(), &read_plans)
        .await
        .map_err(ActiveCheckpointLookupError::Internal)
}

pub(crate) async fn execute_active_checkpoint_exact_id_lookup_rows(
    table_name: &str,
    doc_ids: &[String],
) -> Result<Option<Vec<Value>>, ActiveCheckpointLookupError> {
    let checkpoint = load_active_checkpoint(table_name).await?;
    execute_checkpoint_exact_id_lookup_rows(checkpoint.as_ref(), doc_ids)
        .await
        .map_err(ActiveCheckpointLookupError::Internal)
}

pub(crate) async fn load_active_checkpoint(
    table_name: &str,
) -> Result<Arc<TableMetadataCheckpoint>, ActiveCheckpointLookupError> {
    let checkpoint_id = STATE_PROVIDER
        .get_published_active_servable_checkpoint(&table_name.to_string())
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            ActiveCheckpointLookupError::NotFound(format!(
                "No checkpoint was available for table {}",
                table_name
            ))
        })?;

    if let Some(cached) = ACTIVE_CHECKPOINT_CACHE
        .lock()
        .unwrap()
        .get(table_name)
        .cloned()
    {
        if cached.checkpoint_id == checkpoint_id {
            return Ok(cached.checkpoint);
        }
    }

    STATE_PROVIDER
        .get_checkpoint(CheckpointDescriptor::new(
            table_name.to_string(),
            checkpoint_id,
        ))
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            ActiveCheckpointLookupError::NotFound(format!(
                "Checkpoint metadata was not found for table {}",
                table_name
            ))
        })
        .map(Arc::new)
        .inspect(|checkpoint| {
            ACTIVE_CHECKPOINT_CACHE.lock().unwrap().insert(
                table_name.to_string(),
                CachedActiveCheckpoint {
                    checkpoint_id: checkpoint.checkpoint_id.clone(),
                    checkpoint: checkpoint.clone(),
                },
            );
        })
}

fn service_error(error: ServiceApiError) -> ActiveCheckpointLookupError {
    ActiveCheckpointLookupError::Internal(error.to_string())
}
