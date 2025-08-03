use std::collections::HashMap;
use idgenerator::IdInstance;
use powdrr_lib::data_contract::{TableDescription, CreateIndexTemplateBody, CompactionWorkItem, ExtensionWorkItem, CompactionCommit, TableMetadataCheckpoint, ExtensionCommit, CreateTable, SpeedboatCommit, IcebergCommit};
use powdrr_lib::elastic_search_lifetime_policy::ILMPolicyDefinition;
use powdrr_lib::peers::CheckpointDescriptor;
use powdrr_lib::pipeline::PipelineDefinition;
use powdrr_lib::state_provider::ServiceApiError;
use crate::dynamodb::{DynamoDbConnector, PowdrrNamedCompactionCommitCache, PowdrrNamedExtensionCommitCache, PowdrrNamedIcebergCommitCache, PowdrrNamedSpeedboatCommitCache, PowdrrNamedTableMetadataCheckpointCache, TableBody};
use crate::service_impl_provider::SERVICE_IMPL;


fn from_modyne(e: modyne::Error) -> ServiceApiError {
    ServiceApiError::new(e.to_string())
}


pub struct DynamoDBServiceImpl {
    connector: DynamoDbConnector,

    compactions_cache: PowdrrNamedCompactionCommitCache,
    checkpoints_cache: PowdrrNamedTableMetadataCheckpointCache,
    speedboat_cache: PowdrrNamedSpeedboatCommitCache,
    iceberg_cache: PowdrrNamedIcebergCommitCache,
    extension_cache: PowdrrNamedExtensionCommitCache,
}


static ORG_ID: &'static str = "fake_org_id";

impl DynamoDBServiceImpl {
    pub fn new() -> Self {
        let aws_config = aws_sdk_dynamodb::Config::builder().build();
        DynamoDBServiceImpl{
            connector: DynamoDbConnector::new(aws_sdk_dynamodb::Client::from_conf(aws_config)),
            compactions_cache: PowdrrNamedCompactionCommitCache::new(),
            checkpoints_cache: PowdrrNamedTableMetadataCheckpointCache::new(),
            speedboat_cache: PowdrrNamedSpeedboatCommitCache::new(),
            iceberg_cache: PowdrrNamedIcebergCommitCache::new(),
            extension_cache: PowdrrNamedExtensionCommitCache::new(),
        }
    }

    pub async fn add_checkpoint(&mut self, _metadata: &TableMetadataCheckpoint) -> Result<(), ServiceApiError> {
        unimplemented!()
    }

    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        todo!()
    }

    pub async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), ServiceApiError> {
        self.connector.create_powdrr_table(&ORG_ID.to_string(), &create_table.name, &TableBody{ tags: create_table.tags.clone() }).await.map_err(from_modyne)
    }

    pub async fn describe_table(&mut self, name: &String) -> Result<Option<TableDescription>, ServiceApiError> {
        self.connector.describe_powdrr_table(&ORG_ID.to_string(), name).await
            .map(|x| x.map(|x| TableDescription{ name: name.clone(), tags: x.tags.clone() }))
            .map_err(from_modyne)
    }

    pub async fn add_alias(&mut self, table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        self.connector.create_alias(&ORG_ID.to_string(), alias, table_name).await.map_err(from_modyne)
    }

    pub async fn remove_alias(&mut self, _table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        self.connector.delete_alias(&ORG_ID.to_string(), alias).await.map_err(from_modyne)
    }

    pub async fn create_table_template(&mut self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceApiError> {
        self.connector.create_table_template(&ORG_ID.to_string(), name, template).await.map_err(from_modyne)
    }

    pub async fn describe_table_template(&mut self, name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        self.connector.describe_table_template(&ORG_ID.to_string(), name).await.map_err(from_modyne)
    }

    pub async fn create_pipeline(&mut self, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceApiError> {
        self.connector.create_pipeline(&ORG_ID.to_string(), name, pipeline).await.map_err(from_modyne)
    }

    pub async fn describe_pipeline(&mut self, name: &String) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        self.connector.describe_pipeline(&ORG_ID.to_string(), name).await.map_err(from_modyne)
    }

    pub async fn create_lifetime_policy(&mut self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), ServiceApiError> {
        self.connector.create_lifetime_policy(&ORG_ID.to_string(), name, policy).await.map_err(from_modyne)
    }

    pub async fn describe_lifetime_policy(&mut self, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        self.connector.describe_lifetime_policy(&ORG_ID.to_string(), name).await.map_err(from_modyne)
    }

    pub async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), ServiceApiError> {
        let tables: Vec<String> = commit.type_files.iter().map(|x|x.table_name.clone()).collect();
        let result = self.connector.create_speedboat_commit(&mut self.speedboat_cache, &ORG_ID.to_string(), &tables[0], &IdInstance::next_id().to_string(), commit).await.map_err(from_modyne);
        tokio::spawn(Self::update_all_checkpoints(tables));
        // TODO: fill compaction work item
        // TODO: fill extension work item
        result
    }

    pub async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceApiError> {
        let result = self.connector.create_iceberg_commit(&mut self.iceberg_cache, &ORG_ID.to_string(), table_name, &IdInstance::next_id().to_string(), iceberg_commit).await.map_err(from_modyne);
        tokio::spawn(Self::update_all_checkpoints(vec!(table_name.clone())));
        // TODO: fill extension work item
        result
    }

    pub async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), ServiceApiError> {
        let result = self.connector.create_extension_commit(&mut self.extension_cache, &ORG_ID.to_string(), table_name, &IdInstance::next_id().to_string(), commit).await.map_err(from_modyne);
        // TODO: extension updates will be a little different
        result
    }

    pub async fn compaction_commit(&mut self, _table_name: &String, commit: &CompactionCommit) -> Result<(), ServiceApiError> {
        // NOTE: this just notes what the compactor is saying. We don't generate the new checkpoint
        // until we see an iceberg or speedboat commit with the new info.
        self.connector.create_compaction(&mut self.compactions_cache, &ORG_ID.to_string(), &commit.compaction_id, commit).await.map_err(from_modyne)
    }

    pub async fn get_latest_committed_checkpoint(&mut self, _table_name: &String, _extensions: Option<String>) -> Result<Option<String>, ServiceApiError> {
        todo!()
    }

    pub async fn get_checkpoint(&mut self, snapshot: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        self.connector.describe_checkpoint(&mut self.checkpoints_cache, &ORG_ID.to_string(), &snapshot.full_name()).await.map_err(from_modyne)
    }

    pub async fn get_extension_work_items(&mut self, _extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        todo!()
    }

    pub async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        todo!()
    }

    fn update_all_checkpoints(tables: Vec<String>) -> impl Future<Output=()> {
        async move {
            for table in tables {
                match SERVICE_IMPL.update_checkpoint(&table).await {
                    Ok(_) => (),
                    Err(e) => tracing::error!("Error updating checkpoint for table {}: {}", table, e)
                }
            }
        }
    }

    pub async fn update_checkpoint(&mut self, table_name: &String) -> Result<(), ServiceApiError> {
        // TODO: on table create, we need to create an empty checkpoint and update the latest checkpoint
        // TODO: need a bulk fetcher

        println!("Updating checkpoint for table {}", table_name);

        let latest_speedboat_trackers = self.connector.oldest_available_speedboat_commit(&ORG_ID.to_string(), table_name, None).await.map_err(from_modyne)?;
        if latest_speedboat_trackers.len() == 0 {
            return Ok(());
        }

        let latest_checkpoint_info = self.connector.describe_latest(&ORG_ID.to_string(), table_name).await.map_err(from_modyne)?.unwrap();
        let latest_checkpoint = self.connector.describe_checkpoint(&mut self.checkpoints_cache, &ORG_ID.to_string(), &latest_checkpoint_info.checkpoint_id).await.map_err(from_modyne)?.unwrap();

        let mut latest_speedboats = vec!();
        for speedboat_tracker in latest_speedboat_trackers.iter() {
            latest_speedboats.push(self.connector.describe_speedboat_commit(&mut self.speedboat_cache, &ORG_ID.to_string(), &speedboat_tracker.name).await.map_err(from_modyne)?.unwrap());
        }

        let new_checkpoint = latest_checkpoint.clone_and_apply(
            &latest_speedboats,
            &vec!(),
            &vec!(),
            &HashMap::new()
        );

        match self.connector.commit_checkpoint(
            &ORG_ID.to_string(),
            &latest_checkpoint_info,
            &latest_speedboat_trackers,
            &new_checkpoint,
        ).await.map_err(from_modyne) {
            Ok(val) => {
                if !val {
                    tracing::info!("Contention detected, not committing checkpoint");
                }
            },
            Err(e) => {
                tracing::error!("Error committing checkpoint: {}", e);
            }
        }

        Ok(())
    }

}
