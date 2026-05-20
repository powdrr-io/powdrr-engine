use crate::data_contract::{
    CleanupWorkItem, CompactionWorkItem, ExtensionWorkItem, OrgInfo, TableMetadataCheckpoint,
};
use crate::peers::CheckpointDescriptor;
use crate::state_provider::ServiceApiError;
use async_trait::async_trait;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishedCheckpointRole {
    Active,
    Target,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PublishedCheckpointSelector {
    pub table_name: String,
    pub extension: Option<String>,
    pub role: PublishedCheckpointRole,
}

impl PublishedCheckpointSelector {
    pub fn new(table_name: String, extension: Option<String>) -> Self {
        Self::active(table_name, extension)
    }

    pub fn active(table_name: String, extension: Option<String>) -> Self {
        Self {
            table_name,
            extension,
            role: PublishedCheckpointRole::Active,
        }
    }

    pub fn target(table_name: String, extension: Option<String>) -> Self {
        Self {
            table_name,
            extension,
            role: PublishedCheckpointRole::Target,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PublishedCheckpointRecord {
    pub selector: PublishedCheckpointSelector,
    pub checkpoint_id: String,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct CutoverEpoch(pub u64);

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCutoverState {
    pub selector: PublishedCheckpointSelector,
    pub epoch: CutoverEpoch,
    pub active_checkpoint_id: Option<String>,
    pub target_checkpoint_id: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ServingNodeLease {
    pub node_id: String,
    pub membership_epoch: CutoverEpoch,
    pub observed_at_ms: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ServingNodeActivationAck {
    pub selector: PublishedCheckpointSelector,
    pub node_id: String,
    pub epoch: CutoverEpoch,
    pub checkpoint_id: String,
    pub activated_at_ms: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CutoverMembershipView {
    pub selector: PublishedCheckpointSelector,
    pub epoch: CutoverEpoch,
    pub target_checkpoint_id: String,
    pub required_node_ids: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCutoverRequest {
    pub org_id: String,
    pub selector: PublishedCheckpointSelector,
    pub target_checkpoint_id: String,
}

impl CheckpointCutoverRequest {
    pub fn new(
        org_id: String,
        table_name: String,
        extension: Option<String>,
        target_checkpoint_id: String,
    ) -> Self {
        Self {
            org_id,
            selector: PublishedCheckpointSelector::target(table_name, extension),
            target_checkpoint_id,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CheckpointUpdateRequest {
    pub org_id: String,
    pub table_name: String,
}

impl CheckpointUpdateRequest {
    pub fn new(org_id: String, table_name: String) -> Self {
        Self { org_id, table_name }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MetadataClaimKind {
    Leased,
    ProcessLocal,
}

#[derive(Debug, Clone)]
pub struct ClaimedExtensionWorkItem {
    pub claim: MetadataClaimKind,
    pub work_item: ExtensionWorkItem,
}

#[derive(Debug, Clone)]
pub struct ClaimedCompactionWorkItem {
    pub claim: MetadataClaimKind,
    pub table_name: String,
    pub work_item: CompactionWorkItem,
}

#[derive(Debug, Clone)]
pub struct ClaimedCleanupWorkItem {
    pub claim: MetadataClaimKind,
    pub work_item: CleanupWorkItem,
}

#[async_trait]
pub trait MetadataStore {
    async fn queue_checkpoint_publication(
        &mut self,
        request: &CheckpointUpdateRequest,
    ) -> Result<(), ServiceApiError>;

    async fn get_published_checkpoint_record(
        &mut self,
        org_info: &OrgInfo,
        selector: &PublishedCheckpointSelector,
    ) -> Result<Option<PublishedCheckpointRecord>, ServiceApiError>;

    async fn get_checkpoint_metadata(
        &mut self,
        org_info: &OrgInfo,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError>;

    async fn claim_extension_work_items(
        &mut self,
        org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ClaimedExtensionWorkItem>, ServiceApiError>;

    async fn claim_compaction_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<ClaimedCompactionWorkItem>, ServiceApiError>;

    async fn claim_cleanup_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<ClaimedCleanupWorkItem>, ServiceApiError>;

    async fn advance_published_checkpoints(&mut self) -> Result<bool, ServiceApiError>;

    async fn plan_checkpoint_cutover(
        &mut self,
        request: &CheckpointCutoverRequest,
    ) -> Result<(), ServiceApiError> {
        self.queue_checkpoint_publication(&CheckpointUpdateRequest::new(
            request.org_id.clone(),
            request.selector.table_name.clone(),
        ))
        .await
    }

    async fn get_checkpoint_cutover_state(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceApiError> {
        let active_selector =
            PublishedCheckpointSelector::active(table_name.clone(), extension.clone());
        let target_selector = PublishedCheckpointSelector::target(table_name.clone(), extension);
        let active_checkpoint_id = self
            .get_published_checkpoint_record(org_info, &active_selector)
            .await?
            .map(|record| record.checkpoint_id);
        let target_checkpoint_id = self
            .get_published_checkpoint_record(org_info, &target_selector)
            .await?
            .map(|record| record.checkpoint_id)
            .or_else(|| active_checkpoint_id.clone());
        Ok(CheckpointCutoverState {
            selector: target_selector,
            epoch: CutoverEpoch::default(),
            active_checkpoint_id,
            target_checkpoint_id,
        })
    }

    async fn heartbeat_serving_node(
        &mut self,
        _org_info: &OrgInfo,
        _lease: &ServingNodeLease,
    ) -> Result<(), ServiceApiError> {
        Ok(())
    }

    async fn record_serving_node_activation(
        &mut self,
        _org_info: &OrgInfo,
        _ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceApiError> {
        Ok(())
    }

    async fn list_serving_node_activations(
        &mut self,
        _org_info: &OrgInfo,
        _table_name: &String,
        _extension: Option<String>,
    ) -> Result<Vec<ServingNodeActivationAck>, ServiceApiError> {
        Ok(vec![])
    }

    async fn get_latest_committed_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        let selector = PublishedCheckpointSelector::active(table_name.clone(), extension);
        Ok(self
            .get_published_checkpoint_record(org_info, &selector)
            .await?
            .map(|record| record.checkpoint_id))
    }

    async fn get_latest_target_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        let selector = PublishedCheckpointSelector::target(table_name.clone(), extension);
        Ok(self
            .get_published_checkpoint_record(org_info, &selector)
            .await?
            .map(|record| record.checkpoint_id))
    }

    async fn get_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        self.get_checkpoint_metadata(org_info, checkpoint).await
    }

    async fn get_extension_work_items(
        &mut self,
        org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        Ok(self
            .claim_extension_work_items(org_info, extension_type)
            .await?
            .into_iter()
            .map(|claimed| claimed.work_item)
            .collect())
    }

    async fn get_compaction_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        Ok(self
            .claim_compaction_work_items(org_info)
            .await?
            .into_iter()
            .map(|claimed| (claimed.table_name, claimed.work_item))
            .collect())
    }

    async fn get_cleanup_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        Ok(self
            .claim_cleanup_work_items(org_info)
            .await?
            .into_iter()
            .map(|claimed| claimed.work_item)
            .collect())
    }

    async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        self.advance_published_checkpoints().await
    }
}
