use serde::{Deserialize, Serialize};
use crate::elastic_search_common::call_peers;
use crate::state_hosted_service::API_SERVICE_CLIENT;
use crate::state_peers::{CheckpointDescriptor, PrivateInvocation, PrivatePrefetchInvocation};


#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct PrefetchCommand {
    required_extensions: Vec<String>,
    checkpoints: Vec<CheckpointDescriptor>
}

pub(crate) async fn perform_prefetch(required_extensions: &Vec<String>, checkpoints: &Vec<CheckpointDescriptor>) -> Result<(), std::io::Error> {
    let prefetch_invocation = PrivateInvocation::Prefetch(
        PrivatePrefetchInvocation {
            required_extensions: required_extensions.clone(),
            checkpoints: checkpoints.clone()
        }
    );
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Prefetching Start !!!!!!!!!!!!!!!!!!!!!!!");
    match call_peers(&prefetch_invocation).await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error during prefetching: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
        }
    }
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Prefetching End !!!!!!!!!!!!!!!!!!!!!!!");
    match API_SERVICE_CLIENT.set_prefetch_checkpoints(checkpoints, if required_extensions.len() == 0 { None } else { Some(required_extensions[0].clone()) }).await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error during prefetching: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
        }
    }
    Ok(())
}
