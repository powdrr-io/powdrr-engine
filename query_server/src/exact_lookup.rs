use serde_json::Value;

use powdrr_query_lib::data_contract::TableMetadataCheckpoint;
use powdrr_query_lib::serving_plan::ServingRequestPlan;
use powdrr_query_runtime::lakehouse_serving::execute_checkpoint_exact_lookup_batch_rows;
use powdrr_query_runtime::peers::CheckpointDescriptor;
use powdrr_query_runtime::read_plan::ReadPlan;
use powdrr_query_runtime::state_provider::{STATE_PROVIDER, ServiceApiError};

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
    execute_checkpoint_exact_lookup_batch_rows(&checkpoint, &read_plans)
        .await
        .map_err(ActiveCheckpointLookupError::Internal)
}

pub(crate) async fn load_active_checkpoint(
    table_name: &str,
) -> Result<TableMetadataCheckpoint, ActiveCheckpointLookupError> {
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
}

fn service_error(error: ServiceApiError) -> ActiveCheckpointLookupError {
    ActiveCheckpointLookupError::Internal(error.to_string())
}
