use std::collections::HashSet;

use crate::data_access;
use crate::elastic_search_common::call_peers;
use crate::peers::{CheckpointDescriptor, PrivateInvocation, PrivatePrefetchInvocation};
use crate::state_provider::STATE_PROVIDER;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct PrefetchCommand {
    required_extensions: Vec<String>,
    checkpoints: Vec<CheckpointDescriptor>,
}

pub(crate) async fn warm_iceberg_checkpoints(
    checkpoints: &Vec<CheckpointDescriptor>,
) -> Result<(), std::io::Error> {
    let mut warmed_tables = HashSet::new();

    for checkpoint in checkpoints.iter() {
        if !warmed_tables.insert(checkpoint.table_name.clone()) {
            continue;
        }

        let table_metadata = match STATE_PROVIDER.get_checkpoint(checkpoint.clone()).await {
            Ok(Some(table_metadata)) => table_metadata,
            Ok(None) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Checkpoint {} was not found", checkpoint),
                ));
            }
            Err(error) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    error.to_string(),
                ));
            }
        };

        let Some(iceberg_metadata) = table_metadata.iceberg_metadata.as_ref() else {
            continue;
        };
        let last_snapshot_id = iceberg_metadata
            .snapshot_id
            .as_ref()
            .and_then(|snapshot_id| snapshot_id.parse::<i64>().ok())
            .unwrap_or_default();

        data_access::load_iceberg_table_metadata(
            &"default".to_string(),
            &table_metadata.table_name,
            last_snapshot_id,
        )
        .await
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error.to_string()))?;
    }

    Ok(())
}

pub(crate) async fn perform_prefetch(
    required_extensions: &Vec<String>,
    checkpoints: &Vec<CheckpointDescriptor>,
) -> Result<(), std::io::Error> {
    let prefetch_invocation = PrivateInvocation::Prefetch(PrivatePrefetchInvocation {
        required_extensions: required_extensions.clone(),
        checkpoints: checkpoints.clone(),
    });
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Prefetching Start !!!!!!!!!!!!!!!!!!!!!!!");
    match call_peers(&prefetch_invocation).await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error during prefetching: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
        }
    }
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Prefetching End !!!!!!!!!!!!!!!!!!!!!!!");
    match STATE_PROVIDER
        .set_prefetch_checkpoints(
            checkpoints,
            if required_extensions.len() == 0 {
                None
            } else {
                Some(required_extensions[0].clone())
            },
        )
        .await
    {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error during prefetching: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
        }
    }
    Ok(())
}
