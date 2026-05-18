use async_trait::async_trait;
use crate::data_contract::{
    CleanupWorkItem,
    CompactionWorkItem,
    ExtensionWorkItem,
    OrgInfo,
    TableMetadataCheckpoint,
};
use crate::peers::CheckpointDescriptor;
use crate::state_provider::ServiceApiError;

#[derive(Debug, Clone)]
pub struct PublishedCheckpointSelector {
    pub table_name: String,
    pub extension: Option<String>,
}

impl PublishedCheckpointSelector {
    pub fn new(table_name: String, extension: Option<String>) -> Self {
        Self {
            table_name,
            extension,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PublishedCheckpointRecord {
    pub selector: PublishedCheckpointSelector,
    pub checkpoint_id: String,
}

#[derive(Debug, Clone)]
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

    async fn get_latest_committed_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        let selector = PublishedCheckpointSelector::new(table_name.clone(), extension);
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
