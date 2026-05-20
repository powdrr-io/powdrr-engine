use crate::data_contract::{
    CleanupCommit, CleanupWorkItem, CreateIndexTemplateBody, LicenseType, OrgInfo, OrgSettings,
};
use crate::data_contract::{
    CompactionCommit, CompactionWorkItem, CreateTable, ExtensionCommit, ExtensionWorkItem,
    IcebergCommit, SpeedboatCommit, TableDescription, TableMetadataCheckpoint,
};
use crate::dynamodb_service_impl::DynamoDBServiceImpl;
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::ephemeral_fetch_tracker::EphemeralFetchTracker;
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::state_provider::ServiceApiError;
use crate::test_api::TestProcessingMode;

pub struct DynamoDbStateProvider {
    pub service_impl: DynamoDBServiceImpl,
    pub fetch_tracker: EphemeralFetchTracker,
    pub fake_org_info: OrgInfo,
}

impl DynamoDbStateProvider {
    #[allow(dead_code)]
    pub fn new(mode: TestProcessingMode) -> Self {
        DynamoDbStateProvider {
            service_impl: DynamoDBServiceImpl::new(mode.clone()),
            fetch_tracker: EphemeralFetchTracker::new(mode),
            fake_org_info: OrgInfo {
                org_id: "fake_org_id".to_string(),
                license_type: LicenseType::Free,
            },
        }
    }

    pub async fn test(mode: TestProcessingMode) -> Self {
        DynamoDbStateProvider {
            service_impl: DynamoDBServiceImpl::test(mode.clone()).await,
            fetch_tracker: EphemeralFetchTracker::new(mode),
            fake_org_info: OrgInfo {
                org_id: "fake_org_id".to_string(),
                license_type: LicenseType::Free,
            },
        }
    }

    pub(crate) async fn add_checkpoint(&mut self, checkpoint: &TableMetadataCheckpoint) -> () {
        self.service_impl
            .add_checkpoint(&self.fake_org_info, checkpoint)
            .await
            .unwrap();
    }

    #[allow(dead_code)]
    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        let entities = self
            .service_impl
            .connector
            .fetch_entities(
                &self.fake_org_info.org_id,
                &"powdrr_table".to_string(),
                None,
            )
            .await
            .map_err(|error| ServiceApiError::new(error.to_string()))?;
        Ok(entities
            .entities
            .into_iter()
            .map(|entity| entity.entity_id)
            .collect())
    }

    pub async fn create_table(
        &mut self,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        self.service_impl
            .create_table(&self.fake_org_info, create_table)
            .await
    }

    pub async fn upsert_table_metadata(
        &mut self,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        self.service_impl
            .upsert_table_metadata(&self.fake_org_info, create_table)
            .await
    }

    pub async fn create_org(&mut self, settings: &OrgSettings) -> Result<(), ServiceApiError> {
        self.service_impl.create_org(settings).await
    }

    pub async fn lookup_secret_access_key(
        &mut self,
        access_key: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        self.service_impl.lookup_secret_access_key(access_key).await
    }

    pub async fn describe_table(
        &mut self,
        name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError> {
        self.service_impl
            .describe_table(&self.fake_org_info, name)
            .await
    }

    pub async fn add_alias(
        &mut self,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        self.service_impl
            .add_alias(&self.fake_org_info, table_name, alias)
            .await
    }

    pub async fn remove_alias(
        &mut self,
        _table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        self.service_impl
            .remove_alias(&self.fake_org_info, _table_name, alias)
            .await
    }

    pub async fn create_table_template(
        &mut self,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceApiError> {
        self.service_impl
            .create_table_template(&self.fake_org_info, name, template)
            .await
    }

    pub async fn describe_table_template(
        &mut self,
        name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        self.service_impl
            .describe_table_template(&self.fake_org_info, name)
            .await
    }

    pub async fn create_pipeline(
        &mut self,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceApiError> {
        self.service_impl
            .create_pipeline(&self.fake_org_info, name, pipeline)
            .await
    }

    pub async fn describe_pipeline(
        &mut self,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        self.service_impl
            .describe_pipeline(&self.fake_org_info, name)
            .await
    }

    pub async fn create_lifetime_policy(
        &mut self,
        name: &String,
        policy: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceApiError> {
        self.service_impl
            .create_lifetime_policy(&self.fake_org_info, name, policy)
            .await
    }

    pub async fn describe_lifetime_policy(
        &mut self,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        self.service_impl
            .describe_lifetime_policy(&self.fake_org_info, name)
            .await
    }

    pub async fn speedboat_commit(
        &mut self,
        commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceApiError> {
        match self
            .service_impl
            .speedboat_commit(&self.fake_org_info, commit)
            .await
        {
            Ok(val) => {
                if val {
                    for table_info in commit.type_files.iter() {
                        let checkpoint_id = self
                            .get_latest_committed_checkpoint(&table_info.table_name, None)
                            .await?;
                        if checkpoint_id.is_some() {
                            self.fetch_tracker
                                .set_next_prefetch_checkpoints(
                                    &table_info.table_name,
                                    None,
                                    &checkpoint_id.unwrap(),
                                )
                                .await?;
                        }
                    }
                }
                Ok(val)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn iceberg_commit(
        &mut self,
        table_name: &String,
        iceberg_commit: &IcebergCommit,
    ) -> Result<bool, ServiceApiError> {
        match self
            .service_impl
            .iceberg_commit(&self.fake_org_info, table_name, iceberg_commit)
            .await
        {
            Ok(val) => {
                if val {
                    let checkpoint_id = self
                        .get_latest_committed_checkpoint(table_name, None)
                        .await?;
                    self.fetch_tracker
                        .set_next_prefetch_checkpoints(table_name, None, &checkpoint_id.unwrap())
                        .await?;
                }
                Ok(val)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn extension_commit(
        &mut self,
        table_name: &String,
        commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError> {
        match self
            .service_impl
            .extension_commit(&self.fake_org_info, table_name, commit)
            .await
        {
            Ok(val) => {
                if val {
                    let checkpoint_id = self
                        .get_latest_committed_checkpoint(table_name, None)
                        .await?;
                    self.fetch_tracker
                        .set_next_prefetch_checkpoints(
                            table_name,
                            Some(commit.extension.clone()),
                            &checkpoint_id.unwrap(),
                        )
                        .await?;
                }
                Ok(val)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn compaction_commit(
        &mut self,
        table_name: &String,
        commit: &CompactionCommit,
    ) -> Result<bool, ServiceApiError> {
        match self
            .service_impl
            .compaction_commit(&self.fake_org_info, table_name, commit)
            .await
        {
            Ok(val) => {
                if val {
                    let checkpoint_id = self
                        .get_latest_committed_checkpoint(table_name, None)
                        .await?;
                    self.fetch_tracker
                        .set_next_prefetch_checkpoints(table_name, None, &checkpoint_id.unwrap())
                        .await?;
                }
                Ok(val)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn cleanup_commit(
        &mut self,
        commit: &CleanupCommit,
    ) -> Result<bool, ServiceApiError> {
        self.service_impl
            .cleanup_commit(&self.fake_org_info, commit)
            .await
    }

    pub async fn get_latest_committed_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        self.service_impl
            .get_latest_committed_checkpoint(&self.fake_org_info, table_name, extensions)
            .await
    }

    pub async fn get_published_active_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        self.service_impl
            .get_latest_committed_checkpoint(&self.fake_org_info, table_name, extensions)
            .await
    }

    pub async fn get_checkpoint(
        &mut self,
        snapshot: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        self.service_impl
            .get_checkpoint(&self.fake_org_info, snapshot)
            .await
    }

    pub async fn get_extension_work_items(
        &mut self,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        self.service_impl
            .get_extension_work_items(&self.fake_org_info, extension_type)
            .await
    }

    pub async fn get_compaction_work_items(
        &mut self,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        self.service_impl
            .get_compaction_work_items(&self.fake_org_info)
            .await
    }

    pub async fn get_cleanup_work_items(
        &mut self,
    ) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        self.service_impl
            .get_cleanup_work_items(&self.fake_org_info)
            .await
    }

    pub(crate) async fn get_latest_target_checkpoint(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        self.fetch_tracker
            .get_latest_target_checkpoint(table_name, extension)
            .await
    }

    pub(crate) async fn set_target_checkpoints(
        &mut self,
        descriptors: &Vec<CheckpointDescriptor>,
        extension: Option<String>,
    ) -> Result<(), ServiceApiError> {
        self.fetch_tracker
            .set_target_checkpoints(descriptors, extension)
            .await
    }

    pub async fn get_next_prefetch_checkpoints(
        &mut self,
        extensions: Option<String>,
    ) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        self.fetch_tracker
            .get_next_prefetch_checkpoints(extensions)
            .await
    }

    pub async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        self.service_impl.update_all_checkpoints().await
    }
}
