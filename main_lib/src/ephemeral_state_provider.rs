use crate::elastic_search_ingest::CreateIndexTemplateBody;
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::ephemeral_service_impl::EphemeralServiceImpl;
use crate::pipeline::PipelineDefinition;
use crate::data_contract::{CompactionCommit, CompactionWorkItem, CreateTable, ExtensionCommit, ExtensionWorkItem, IcebergCommit, SpeedboatCommit, TableDescription, TableMetadataCheckpoint};
use crate::state_provider::ServiceApiError;
use crate::peers::{CheckpointDescriptor, PeerClient};
use crate::test_api::{TestProcessingMode};

pub struct EphemeralStateProvider {
    service_impl: EphemeralServiceImpl
}

impl EphemeralStateProvider {
    pub fn new() -> Self {
        EphemeralStateProvider{
            service_impl: EphemeralServiceImpl::new()
        }
    }

    pub async fn clear_and_set(&mut self, mode: TestProcessingMode) -> () {
        self.service_impl.clear_and_set(mode).await.unwrap();
    }

    pub(crate) async fn add_checkpoint(&mut self, checkpoint: &TableMetadataCheckpoint) -> () {
        self.service_impl.add_checkpoint(checkpoint).await.unwrap();
    }

    pub(crate) async fn get_latest_target_checkpoint(&self, _table_name: &String, _extension: Option<String>) -> Result<Option<String>, ServiceApiError>{
        todo!()
    }

    pub(crate) async fn set_prefetch_checkpoints(&self, _descriptors: &Vec<CheckpointDescriptor>, _extension: Option<String>) -> Result<(), ServiceApiError> {
        todo!()
    }

    #[allow(dead_code)]
    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        self.service_impl.get_all_iceberg_tables().await
    }

    pub async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), ServiceApiError> {
        self.service_impl.create_table(create_table).await
    }

    pub async fn describe_table(&mut self, name: &String) -> Result<Option<TableDescription>, ServiceApiError> {
        self.service_impl.describe_table(name).await
    }

    pub async fn add_alias(&mut self, table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        self.service_impl.add_alias(table_name, alias).await
    }

    pub async fn remove_alias(&mut self, _table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        self.service_impl.remove_alias(_table_name, alias).await
    }

    pub async fn create_table_template(&mut self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceApiError> {
        self.service_impl.create_table_template(name, template).await
    }

    pub async fn describe_table_template(&mut self, name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        self.service_impl.describe_table_template(name).await
    }

    pub async fn create_pipeline(&mut self, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceApiError> {
        self.service_impl.create_pipeline(name, pipeline).await
    }

    pub async fn describe_pipeline(&mut self, name: &String) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        self.service_impl.describe_pipeline(name).await
    }

    pub async fn create_lifetime_policy(&mut self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), ServiceApiError> {
        self.service_impl.create_lifetime_policy(name, policy).await
    }

    pub async fn describe_lifetime_policy(&mut self, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        self.service_impl.describe_lifetime_policy(name).await
    }


    pub async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), ServiceApiError> {
        self.service_impl.speedboat_commit(commit).await
    }

    pub async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceApiError> {
        self.service_impl.iceberg_commit(table_name, iceberg_commit).await
    }

    pub async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), ServiceApiError> {
        self.service_impl.extension_commit(table_name, commit).await
    }

    pub async fn compaction_commit(&mut self, _table_name: &String, commit: &CompactionCommit) -> Result<(), ServiceApiError> {
        self.service_impl.compaction_commit(_table_name, commit).await
    }

    pub async fn get_latest_committed_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, ServiceApiError> {
        self.service_impl.get_latest_committed_checkpoint(table_name, extensions).await
    }

    pub async fn get_checkpoint(&mut self, snapshot: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        self.service_impl.get_checkpoint(snapshot).await
    }

    pub async fn get_extension_work_items(&mut self, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        self.service_impl.get_extension_work_items(extension_type).await
    }

    pub async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        self.service_impl.get_compaction_work_items().await
    }

    pub async fn get_peer_clients(&mut self) -> Vec<Box<dyn PeerClient>> {
        self.service_impl.get_peer_clients().await
    }

    pub async fn get_next_prefetch_checkpoints(&mut self, extensions: Option<String>) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        self.service_impl.get_next_prefetch_checkpoints(extensions).await
    }
}
