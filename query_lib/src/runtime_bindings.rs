use crate::data_contract::TableDescription;
use crate::data_contract::{ExtensionCommit, TableMetadataCheckpoint};
use crate::peers::{CheckpointDescriptor, PeerClient};
use crate::state_provider::STATE_PROVIDER;
use crate::test_api::PeerModeType;
use async_trait::async_trait;
use powdrr_control_plane::service_api_error::ServiceApiError;
use std::sync::LazyLock;

#[async_trait]
pub(crate) trait QueryRuntimeBindings: Send + Sync {
    async fn set_peer_mode(&self, mode: &PeerModeType);

    async fn add_checkpoint(&self, checkpoint: &TableMetadataCheckpoint);

    async fn get_published_active_servable_checkpoint(
        &self,
        table_name: &String,
    ) -> Result<Option<String>, ServiceApiError>;

    async fn get_checkpoint(
        &self,
        checkpoint: CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError>;

    async fn describe_table(
        &self,
        table_name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError>;

    async fn extension_commit(
        &self,
        table_name: &String,
        extension_commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError>;

    async fn get_peer_clients(&self) -> Vec<Box<dyn PeerClient>>;
}

struct DefaultQueryRuntimeBindings;

#[async_trait]
impl QueryRuntimeBindings for DefaultQueryRuntimeBindings {
    async fn set_peer_mode(&self, mode: &PeerModeType) {
        STATE_PROVIDER.set_peer_mode(mode).await;
    }

    async fn add_checkpoint(&self, checkpoint: &TableMetadataCheckpoint) {
        STATE_PROVIDER.add_checkpoint(checkpoint).await;
    }

    async fn get_published_active_servable_checkpoint(
        &self,
        table_name: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        STATE_PROVIDER
            .get_published_active_servable_checkpoint(table_name)
            .await
    }

    async fn get_checkpoint(
        &self,
        checkpoint: CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        STATE_PROVIDER.get_checkpoint(checkpoint).await
    }

    async fn describe_table(
        &self,
        table_name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError> {
        STATE_PROVIDER.describe_table(table_name).await
    }

    async fn extension_commit(
        &self,
        table_name: &String,
        extension_commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError> {
        STATE_PROVIDER
            .extension_commit(table_name, extension_commit)
            .await
    }

    async fn get_peer_clients(&self) -> Vec<Box<dyn PeerClient>> {
        STATE_PROVIDER.get_peer_clients().await
    }
}

static QUERY_RUNTIME_BINDINGS: LazyLock<DefaultQueryRuntimeBindings> =
    LazyLock::new(|| DefaultQueryRuntimeBindings);

pub(crate) async fn set_peer_mode(mode: &PeerModeType) {
    QUERY_RUNTIME_BINDINGS.set_peer_mode(mode).await;
}

pub(crate) async fn add_checkpoint(checkpoint: &TableMetadataCheckpoint) {
    QUERY_RUNTIME_BINDINGS.add_checkpoint(checkpoint).await;
}

pub(crate) async fn get_published_active_servable_checkpoint(
    table_name: &String,
) -> Result<Option<String>, ServiceApiError> {
    QUERY_RUNTIME_BINDINGS
        .get_published_active_servable_checkpoint(table_name)
        .await
}

pub(crate) async fn get_checkpoint(
    checkpoint: CheckpointDescriptor,
) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
    QUERY_RUNTIME_BINDINGS.get_checkpoint(checkpoint).await
}

pub(crate) async fn describe_table(
    table_name: &String,
) -> Result<Option<TableDescription>, ServiceApiError> {
    QUERY_RUNTIME_BINDINGS.describe_table(table_name).await
}

pub(crate) async fn extension_commit(
    table_name: &String,
    extension_commit: &ExtensionCommit,
) -> Result<bool, ServiceApiError> {
    QUERY_RUNTIME_BINDINGS
        .extension_commit(table_name, extension_commit)
        .await
}

pub(crate) async fn get_peer_clients() -> Vec<Box<dyn PeerClient>> {
    QUERY_RUNTIME_BINDINGS.get_peer_clients().await
}
