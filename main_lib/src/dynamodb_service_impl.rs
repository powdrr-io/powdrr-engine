use std::collections::HashMap;
use idgenerator::IdInstance;
use crate::data_contract::{TableDescription, CreateIndexTemplateBody, CompactionWorkItem, ExtensionWorkItem, CompactionCommit, TableMetadataCheckpoint, ExtensionCommit, CreateTable, SpeedboatCommit, IcebergCommit, FileSetPayload};
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::state_provider::ServiceApiError;
use crate::dynamodb::{DynamoDbConnector, EntityVersionInfo, PowdrrNamedCompactionCommitCache, PowdrrNamedCompactionWorkItemCache, PowdrrNamedExtensionCommitCache, PowdrrNamedExtensionWorkItemCache, PowdrrNamedIcebergCommitCache, PowdrrNamedSpeedboatCommitCache, PowdrrNamedTableMetadataCheckpointCache, TableBody};
use crate::schema_massager::PowdrrSchema;

fn from_modyne(e: modyne::Error) -> ServiceApiError {
    ServiceApiError::new(e.to_string())
}


pub struct DynamoDBServiceImpl {
    pub(crate) connector: DynamoDbConnector,

    compactions_cache: PowdrrNamedCompactionCommitCache,
    checkpoints_cache: PowdrrNamedTableMetadataCheckpointCache,
    speedboat_cache: PowdrrNamedSpeedboatCommitCache,
    iceberg_cache: PowdrrNamedIcebergCommitCache,
    extension_cache: PowdrrNamedExtensionCommitCache,
    extension_work_item_cache: PowdrrNamedExtensionWorkItemCache,
    compaction_work_item_cache: PowdrrNamedCompactionWorkItemCache,
}


static ORG_ID: &'static str = "fake_org_id";

impl DynamoDBServiceImpl {
    pub fn new() -> Self {
        let aws_config = aws_sdk_dynamodb::Config::builder().build();
        Self::with_client(aws_sdk_dynamodb::Client::from_conf(aws_config))
    }

    pub fn with_client(client: aws_sdk_dynamodb::Client) -> Self {
        DynamoDBServiceImpl{
            connector: DynamoDbConnector::new(client),
            compactions_cache: PowdrrNamedCompactionCommitCache::new(),
            checkpoints_cache: PowdrrNamedTableMetadataCheckpointCache::new(),
            speedboat_cache: PowdrrNamedSpeedboatCommitCache::new(),
            iceberg_cache: PowdrrNamedIcebergCommitCache::new(),
            extension_cache: PowdrrNamedExtensionCommitCache::new(),
            extension_work_item_cache: PowdrrNamedExtensionWorkItemCache::new(),
            compaction_work_item_cache: PowdrrNamedCompactionWorkItemCache::new(),
        }
    }

    pub fn seed(&self) -> Result<(), ServiceApiError> {
        Ok(())
    }

    fn latest_checkpoint_key(table_name: &String, extension: &Option<String>) -> String {
        match extension {
            Some(x) => format!("checkpoint#{}#{}", table_name, x),
            None => format!("checkpoint#{}", table_name)
        }
    }

    fn latest_extension_work_item_key(table_name: &String, extension: &String) -> String {
        format!("extension_work_item#{}#{}", table_name, extension)
    }

    fn latest_compaction_work_item_key(table_name: &String) -> String {
        format!("compaction_work_item#{}", table_name)
    }

    const NO_WORK_ITEM: &'static str = "-1";

    pub async fn add_checkpoint(&mut self, metadata: &TableMetadataCheckpoint) -> Result<(), ServiceApiError> {
        self.connector.create_table(&ORG_ID.to_string(), &metadata.table_name, &TableBody{ tags: Default::default() }).await.map_err(from_modyne)?;
        let latest_checkpoint_info = self.connector.describe_latest(&ORG_ID.to_string(), &Self::latest_checkpoint_key(&metadata.table_name, &None)).await.map_err(from_modyne)?;
        assert!(latest_checkpoint_info.is_some());
        match self.connector.commit_checkpoint(
            latest_checkpoint_info.as_ref().unwrap(),
            &vec!(),
            &metadata,
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

            /*
        match ,
            connector.describe_powdrr_table(&ORG_ID.to_string(), &metadata.table_name).await.map_err(from_modyne)? {
            Some(_) => (),
            None => self.connector.create_powdrr_table(&ORG_ID.to_string(), &metadata.table_name, &TableBody::new()).await.map_err(from_modyne)?
        }

        let descriptor = CheckpointDescriptor{ table_name: metadata.table_name.clone(), checkpoint_id: metadata.checkpoint_id.to_string() };
        self.connector.create_checkpoint(&mut self.checkpoints_cache, &ORG_ID.to_string(), &descriptor.full_name(), &metadata).await.map_err(from_modyne)?;
        // TODO: change create to return a bool for collision on create?
        self.connector.create_latest(&ORG_ID.to_string(), &Self::latest_checkpoint_key(&metadata.table_name, &None), &EntityVersionInfo::new(&metadata.checkpoint_id)).await.map_err(from_modyne)?;
        self.connector.create_latest(&ORG_ID.to_string(), &Self::latest_checkpoint_key(&metadata.table_name, &Some("es".to_string())), &EntityVersionInfo::new(&metadata.checkpoint_id)).await.map_err(from_modyne)?;
        self.connector.create_latest(&ORG_ID.to_string(), &Self::latest_compaction_work_item_key(&metadata.table_name), &EntityVersionInfo::new(&Self::NO_WORK_ITEM.to_owned())).await.map_err(from_modyne)?;
        let extension_work_item = ExtensionWorkItem {
            extension_type: "es".to_string(),
            table_name: metadata.table_name.clone(),
            table_schema: metadata.schema.clone(),
            speedboat_files: metadata.speedboat_metadata.as_ref().map_or(FileSetPayload::new(), |x| x.files.clone()),
            iceberg_files: metadata.iceberg_metadata.as_ref().map_or(FileSetPayload::new(), |x| x.files.clone())
        };
        let new_id = IdInstance::next_id().to_string();
        self.connector.create_extension_work_item(&mut self.extension_work_item_cache, &ORG_ID.to_string(), &new_id, &extension_work_item).await.map_err(from_modyne)?;
        self.connector.create_latest(&ORG_ID.to_string(), &Self::latest_extension_work_item_key(&metadata.table_name, &"es".to_string()), &EntityVersionInfo::new(&new_id)).await.map_err(from_modyne)?;
        */
        Ok(())
    }

    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        todo!()
    }

    pub async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), ServiceApiError> {
        self.connector.create_table(&ORG_ID.to_string(), &create_table.name, &TableBody{ tags: create_table.tags.clone() }).await.map_err(from_modyne)
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
        let id = &IdInstance::next_id().to_string();
        self.connector.create_speedboat_commit(&mut self.speedboat_cache, &ORG_ID.to_string(), id, commit).await.map_err(from_modyne)?;
        self.connector.create_speedboat_commit_checkpointed(&ORG_ID.to_string(), &tables[0], id).await.map_err(from_modyne)?;

        // Everything in this loop just to create an ES work item. Yikes.
        loop {
            let latest_key = &Self::latest_extension_work_item_key(&tables[0], &"es".to_string());
            let latest_es = self.connector.describe_latest(&ORG_ID.to_string(), latest_key).await.map_err(from_modyne)?;
            assert!(latest_es.is_some());
            let mut work_item = if latest_es.as_ref().unwrap().entity_id == Self::NO_WORK_ITEM {
                ExtensionWorkItem {
                    extension_type: "es".to_string(),
                    table_name: tables[0].to_string(),
                    table_schema: PowdrrSchema{ fields: vec![] },
                    speedboat_files: FileSetPayload::new(),
                    iceberg_files: FileSetPayload::new()
                }
            } else {
                self.connector.describe_extension_work_item(&mut self.extension_work_item_cache, &ORG_ID.to_string(), &latest_es.as_ref().unwrap().entity_id).await.map_err(from_modyne)?.unwrap()
            };
            work_item.merge_speedboat(commit);
            let new_id = &IdInstance::next_id().to_string();
            self.connector.create_extension_work_item(&mut self.extension_work_item_cache, &ORG_ID.to_string(),new_id, &work_item).await.map_err(from_modyne)?;
            match self.connector.commit_work_item(latest_es.as_ref().unwrap(), new_id).await.map_err(from_modyne)? {
                true => break,
                false => ()
            }
        }

        // Everything in this loop just to create a compaction work item. Yikes.
        loop {
            let latest_key = &Self::latest_compaction_work_item_key(&tables[0]);
            let latest_es = self.connector.describe_latest(&ORG_ID.to_string(), latest_key).await.map_err(from_modyne)?;
            assert!(latest_es.is_some());
            let mut work_item = if latest_es.as_ref().unwrap().entity_id == Self::NO_WORK_ITEM {
                CompactionWorkItem {
                    table_schema: PowdrrSchema { fields: vec![] },
                    speedboat_files: FileSetPayload::new(),
                    delete_files: vec![],
                }
            } else {
                self.connector.describe_compaction_work_item(&mut self.compaction_work_item_cache, &ORG_ID.to_string(), &latest_es.as_ref().unwrap().entity_id).await.map_err(from_modyne)?.unwrap()
            };
            work_item.merge_speedboat(commit);
            let new_id = &IdInstance::next_id().to_string();
            self.connector.create_compaction_work_item(&mut self.compaction_work_item_cache, &ORG_ID.to_string(),new_id, &work_item).await.map_err(from_modyne)?;
            match self.connector.commit_work_item(latest_es.as_ref().unwrap(), new_id).await.map_err(from_modyne)? {
                true => break,
                false => ()
            }
        }


        Ok(())
    }

    pub async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceApiError> {
        let id = &IdInstance::next_id().to_string();
        self.connector.create_iceberg_commit(&mut self.iceberg_cache, &ORG_ID.to_string(), id, iceberg_commit).await.map_err(from_modyne)?;
        self.connector.create_iceberg_commit_checkpointed(&ORG_ID.to_string(), table_name, id).await.map_err(from_modyne)?;
        // TODO: fill extension work item
        Ok(())
    }

    pub async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), ServiceApiError> {
        let id = &IdInstance::next_id().to_string();
        self.connector.create_extension_commit(&mut self.extension_cache, &ORG_ID.to_string(), id, commit).await.map_err(from_modyne)?;
        self.connector.create_extension_commit_checkpointed(&ORG_ID.to_string(), table_name, id).await.map_err(from_modyne)?;
        // TODO: extension updates will be a little different
        Ok(())
    }

    pub async fn compaction_commit(&mut self, _table_name: &String, commit: &CompactionCommit) -> Result<(), ServiceApiError> {
        // NOTE: this just notes what the compactor is saying. We don't generate the new checkpoint
        // until we see an iceberg or speedboat commit with the new info.
        self.connector.create_compaction(&mut self.compactions_cache, &ORG_ID.to_string(), &commit.compaction_id, commit).await.map_err(from_modyne)
    }

    pub async fn get_latest_committed_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, ServiceApiError> {
        let value = self.connector.describe_latest(&ORG_ID.to_string(), &Self::latest_checkpoint_key(table_name, &extensions)).await.map_err(from_modyne)?;
        match value {
            Some(val) => {
                tracing::info!("Latest checkpoint for {}: {}", table_name, val.entity_id);
                Ok(Some(CheckpointDescriptor::from_full_name(&val.entity_id).checkpoint_id))
            },
            None => Ok(None)
        }
    }

    pub async fn get_checkpoint(&mut self, checkpoint: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        tracing::info!("Getting checkpoint for {}", checkpoint.full_name());
        match self.connector.describe_checkpoint(&mut self.checkpoints_cache, &ORG_ID.to_string(), &checkpoint.full_name()).await.map_err(from_modyne) {
            Ok(val) => {
                tracing::info!("Got checkpoint for {}: {}", checkpoint.full_name(), val.is_some());
                Ok(val)
            },
            Err(e) => Err(e)
        }
    }

    pub async fn get_extension_work_items(&mut self, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        let all_tables = self.connector.fetch_entities(&ORG_ID.to_string(), &"powdrr_table".to_string(), None).await.map_err(from_modyne)?;
        let mut work_items = vec!();
        let mut used_latest = vec!();
        for table_entity in all_tables.entities {
            let latest_es_key = &Self::latest_extension_work_item_key(&table_entity.entity_id, extension_type);
            let latest_entity_info = self.connector.describe_latest(&ORG_ID.to_string(), latest_es_key).await.map_err(from_modyne)?;
            match latest_entity_info {
                Some(latest_entity_info) => {
                    if latest_entity_info.entity_id != Self::NO_WORK_ITEM.to_owned() {
                        let work_item = self.connector.describe_extension_work_item(&mut self.extension_work_item_cache, &ORG_ID.to_string(), &latest_entity_info.entity_id).await.map_err(from_modyne)?;
                        assert!(work_item.is_some());
                        work_items.push(work_item.unwrap());
                        used_latest.push(latest_entity_info);
                    }
                },
                None => {
                    tracing::error!("Unable to find latest extension work item for table {}", table_entity.entity_id);
                }
            }
        }
        match self.connector.commit_work_item_taken(&used_latest, &Self::NO_WORK_ITEM.to_string()).await.map_err(from_modyne)? {
            true => Ok(work_items),
            false => Ok(vec!())
        }
    }

    pub async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        let all_tables = self.connector.fetch_entities(&ORG_ID.to_string(), &"powdrr_table".to_string(), None).await.map_err(from_modyne)?;
        let mut work_items = vec!();
        let mut used_latest = vec!();
        for table_entity in all_tables.entities {
            let latest_entity_info = self.connector.describe_latest(&ORG_ID.to_string(), &Self::latest_compaction_work_item_key(&table_entity.entity_id)).await.map_err(from_modyne)?;
            match latest_entity_info {
                Some(latest_entity_info) => {
                    if latest_entity_info.entity_id != Self::NO_WORK_ITEM.to_owned() {
                        let work_item = self.connector.describe_compaction_work_item(&mut self.compaction_work_item_cache, &ORG_ID.to_string(), &latest_entity_info.entity_id).await.map_err(from_modyne)?;
                        assert!(work_item.is_some());
                        let compaction = work_item.unwrap();
                        tracing::info!("Compaction work item stats: size = {}/{}, files = {}/200",
                            compaction.speedboat_files.sizes.iter().sum::<u64>(),
                            30 * 1024 * 1024,
                            compaction.speedboat_files.sizes.len()
                        );
                        let do_compaction = compaction.speedboat_files.sizes.iter().sum::<u64>() > 30 * 1024 * 1024 || compaction.speedboat_files.sizes.len() > 200;
                        if do_compaction {
                            work_items.push((table_entity.entity_id.clone(), compaction));
                            used_latest.push(latest_entity_info);
                        }
                    }
                },
                None => {
                    tracing::error!("Unable to find latest extension work item for table {}", table_entity.entity_id);
                }
            }
        }
        match self.connector.commit_work_item_taken(&used_latest, &Self::NO_WORK_ITEM.to_string()).await.map_err(from_modyne)? {
            true => Ok(work_items),
            false => Ok(vec!())
        }
    }

    pub async fn update_all_checkpoints(&mut self) -> Result<(), ServiceApiError> {
        let all_tables = self.connector.fetch_entities(&ORG_ID.to_string(), &"powdrr_table".to_string(), None).await.map_err(from_modyne)?;
        for table_entity in all_tables.entities {
            self.update_checkpoint(&table_entity.entity_id).await?
        }
        Ok(())
    }

    pub async fn update_checkpoint(&mut self, table_name: &String) -> Result<(), ServiceApiError> {
        // TODO: need a bulk fetcher

        println!("Updating checkpoint for table {}", table_name);

        let latest_speedboat_trackers = self.connector.oldest_available_speedboat_commit_checkpointed(&ORG_ID.to_string(), table_name, None).await.map_err(from_modyne)?;
        if latest_speedboat_trackers.len() == 0 {
            return Ok(());
        }

        let latest_checkpoint_info = self.connector.describe_latest(&ORG_ID.to_string(), &Self::latest_checkpoint_key(table_name, &None)).await.map_err(from_modyne)?.unwrap();
        let latest_checkpoint = self.connector.describe_checkpoint(&mut self.checkpoints_cache, &ORG_ID.to_string(), &latest_checkpoint_info.entity_id).await.map_err(from_modyne)?.unwrap();

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
