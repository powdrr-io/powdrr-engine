use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use futures_util::FutureExt;
use gotham::mime;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use crate::elastic_search_common::{execute_command, Command, CommandContext, ElasticSearchResponse, ResultGeneratorFuture};
use crate::state_peers::{CheckpointDescriptor, PrivateInvocation, PrivatePrefetchInvocation};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct PrefetchCommand {
    required_extensions: Vec<String>,
    checkpoints: Vec<CheckpointDescriptor>
}

#[async_trait]
impl Command for PrefetchCommand {
    async fn get_private_invocation(&self) -> PrivateInvocation {
        PrivateInvocation::Prefetch(
            PrivatePrefetchInvocation {
                required_extensions: self.required_extensions.clone(),
                checkpoints: self.checkpoints.clone()
            }
        )
    }

    fn result_generator(&self, _result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>> {
        async {
            Ok(ElasticSearchResponse {
                status: StatusCode::OK,
                mime: mime::TEXT_PLAIN,
                body: "success".to_string(),
                headers: vec![],
            })
        }.boxed()
    }
}


pub(crate) async fn perform_prefetch(required_extensions: &Vec<String>, checkpoints: &Vec<CheckpointDescriptor>) -> Result<(), std::io::Error> {
    let prefetch_command = PrefetchCommand {
        required_extensions: required_extensions.clone(),
        checkpoints: checkpoints.clone()
    };
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Prefetching Start !!!!!!!!!!!!!!!!!!!!!!!");
    let _response = execute_command(CommandContext{}, Arc::new(prefetch_command)).await;
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Prefetching End !!!!!!!!!!!!!!!!!!!!!!!");
    Ok(())
}
