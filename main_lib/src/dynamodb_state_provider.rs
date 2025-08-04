use aws_sdk_dynamodb::Client;
use crate::data_contract::CreateIndexTemplateBody;
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::pipeline::PipelineDefinition;
use crate::data_contract::{CompactionCommit, CompactionWorkItem, CreateTable, ExtensionCommit, ExtensionWorkItem, IcebergCommit, SpeedboatCommit, TableDescription, TableMetadataCheckpoint};
use crate::dynamodb_service_impl::DynamoDBServiceImpl;
use crate::ephemeral_fetch_tracker::EphemeralFetchTracker;
use crate::state_provider::ServiceApiError;
use crate::peers::{CheckpointDescriptor, PeerClient, SelfPeer};
use crate::test_api::{CompactionMode};

pub struct DynamoDbStateProvider {
    pub service_impl: DynamoDBServiceImpl,
    pub fetch_tracker: EphemeralFetchTracker,
}

impl DynamoDbStateProvider {
    #[allow(dead_code)]
    pub fn new() -> Self {
        DynamoDbStateProvider{
            service_impl: DynamoDBServiceImpl::new(),
            fetch_tracker: EphemeralFetchTracker::new(),
        }
    }

    pub fn with_client(client: Client) -> Self {
        DynamoDbStateProvider{
            service_impl: DynamoDBServiceImpl::with_client(client),
            fetch_tracker: EphemeralFetchTracker::new(),
        }
    }

    pub(crate) async fn add_checkpoint(&mut self, checkpoint: &TableMetadataCheckpoint) -> () {
        self.service_impl.add_checkpoint(checkpoint).await.unwrap();
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
        match self.service_impl.speedboat_commit(commit).await {
            Ok(_) => {
                for table_info in commit.type_files.iter() {
                    let checkpoint_id = self.get_latest_committed_checkpoint(&table_info.table_name, None).await?;
                    self.fetch_tracker.set_next_prefetch_checkpoints(&table_info.table_name, None, &checkpoint_id.unwrap()).await?;
                }
                Ok(())
            },
            Err(e) => Err(e)
        }
    }

    pub async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceApiError> {
        match self.service_impl.iceberg_commit(table_name, iceberg_commit).await {
            Ok(_) => {
                let checkpoint_id = self.get_latest_committed_checkpoint(table_name, None).await?;
                self.fetch_tracker.set_next_prefetch_checkpoints(table_name, None, &checkpoint_id.unwrap()).await?;
                Ok(())
            },
            Err(e) => Err(e)
        }
    }

    pub async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), ServiceApiError> {
        match self.service_impl.extension_commit(table_name, commit).await {
            Ok(_) => {
                let checkpoint_id = self.get_latest_committed_checkpoint(table_name, None).await?;
                self.fetch_tracker.set_next_prefetch_checkpoints(table_name, Some(commit.extension.clone()), &checkpoint_id.unwrap()).await?;
                Ok(())
            },
            Err(e) => Err(e)
        }
    }

    pub async fn compaction_commit(&mut self, table_name: &String, commit: &CompactionCommit) -> Result<(), ServiceApiError> {
        match self.service_impl.compaction_commit(table_name, commit).await {
            Ok(_) => {
                let checkpoint_id = self.get_latest_committed_checkpoint(table_name, None).await?;
                self.fetch_tracker.set_next_prefetch_checkpoints(table_name, None, &checkpoint_id.unwrap()).await?;
                Ok(())
            },
            Err(e) => Err(e)
        }
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
        vec!(Box::new(SelfPeer::new(CompactionMode::Async)))
    }

    pub(crate) async fn get_latest_target_checkpoint(&mut self, table_name: &String, extension: Option<String>) -> Result<Option<String>, ServiceApiError>{
        self.fetch_tracker.get_latest_target_checkpoint(table_name, extension).await
    }

    pub(crate) async fn set_target_checkpoints(&mut self, descriptors: &Vec<CheckpointDescriptor>, extension: Option<String>) -> Result<(), ServiceApiError> {
        self.fetch_tracker.set_target_checkpoints(descriptors, extension).await
    }

    pub async fn get_next_prefetch_checkpoints(&mut self, extensions: Option<String>) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        self.fetch_tracker.get_next_prefetch_checkpoints(extensions).await
    }
}
