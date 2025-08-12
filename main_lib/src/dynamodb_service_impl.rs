use std::collections::HashMap;
use modyne::model::TransactWrite;
use crate::data_contract::{TableDescription, CreateIndexTemplateBody, CompactionWorkItem, ExtensionWorkItem, CompactionCommit, TableMetadataCheckpoint, ExtensionCommit, CreateTable, SpeedboatCommit, IcebergCommit, SpeedboatCommitTableInfo, CleanupWorkItem, CleanupCommit};
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::state_provider::ServiceApiError;
use crate::dynamodb::{DynamoDbConnector, PowdrrNamedCleanupWorkItemCache, PowdrrNamedCompactionCommitCache, PowdrrNamedCompactionWorkItemCache, PowdrrNamedExtensionCommitCache, PowdrrNamedExtensionWorkItemCache, PowdrrNamedIcebergCommitCache, PowdrrNamedSpeedboatCommitCache, PowdrrNamedTableMetadataCheckpointCache, TableBody};
use crate::test_api::TestProcessingMode;


const LEASE_LENGTH_MS: i64 = 60 * 1000; // 1 minute


fn from_modyne(e: modyne::Error) -> ServiceApiError {
    ServiceApiError::new(e.to_string())
}


pub struct DynamoDBServiceImpl {
    mode: TestProcessingMode,
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
    pub fn new(mode: TestProcessingMode) -> Self {
        let aws_config = aws_sdk_dynamodb::Config::builder().build();
        Self::with_client(aws_sdk_dynamodb::Client::from_conf(aws_config), mode)
    }

    pub fn with_client(client: aws_sdk_dynamodb::Client, mode: TestProcessingMode) -> Self {
        DynamoDBServiceImpl{
            mode: mode,
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
        if metadata.speedboat_metadata.is_some() {
            self.speedboat_commit(&SpeedboatCommit {
                type_files: vec!(SpeedboatCommitTableInfo {
                    commit_type: "commit".to_string(),
                    table_name: metadata.table_name.clone(),
                    files: metadata.speedboat_metadata.as_ref().unwrap().files.file_paths.clone(),
                    sizes: metadata.speedboat_metadata.as_ref().unwrap().files.sizes.clone(),
                    schema: Some(metadata.schema.clone()),
                }),
                compaction: None
            }).await?;
        }
        if metadata.iceberg_metadata.is_some() {
            self.iceberg_commit(
                &metadata.table_name,
                &IcebergCommit {
                    metadata: metadata.iceberg_metadata.as_ref().unwrap().clone(),
                    deletes_table_info: None,
                    compactions: vec![],
                },
            ).await?;
        }
        if metadata.deletes_metadata.is_some() {
            todo!("Need to implement this now")
        }

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
        //assert_eq!(tables.len(), 1, "Only really support single table commits right now");
        // TODO: change to return result bool
        let retval = self.connector.commit_speedboat(&ORG_ID.to_string(), &tables[0], commit).await.map_err(from_modyne)?;
        assert!(retval);
        Ok(())
    }

    async fn clone_and_apply(
        &mut self,
        metadata: &TableMetadataCheckpoint,
        speedboat_commits: &Vec<SpeedboatCommit>,
        iceberg_commits: &Vec<IcebergCommit>,
        extension_commits: &Vec<ExtensionCommit>,
    ) -> (TableMetadataCheckpoint, bool) {
        let compactions = self.gather_compactions(speedboat_commits, iceberg_commits).await.unwrap();
        metadata.clone_and_apply(
            speedboat_commits,
            iceberg_commits,
            extension_commits,
            &compactions
        )
    }

    async fn gather_compactions(&mut self, speedboat_commits: &Vec<SpeedboatCommit>, iceberg_commits: &Vec<IcebergCommit>) -> Result<HashMap<String, CompactionCommit>, ServiceApiError> {
        let mut compactions = vec!();
        for speedboat_commit in speedboat_commits {
            if speedboat_commit.compaction.is_some() {
                compactions.push(speedboat_commit.compaction.as_ref().unwrap().clone());
            }
        }

        for iceberg_commit in iceberg_commits {
            compactions.extend(iceberg_commit.compactions.iter().map(|x| x.clone()));
        }

        // TODO: Need bulk loading!
        let mut compaction_commits = HashMap::new();
        for compaction in compactions {
            compaction_commits.insert(
                compaction.clone(),
                self.connector.describe_compaction(&mut self.compactions_cache, &ORG_ID.to_string(), &compaction).await.map_err(from_modyne)?.unwrap()
            );
        }

        Ok(compaction_commits)
    }

    pub async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceApiError> {
        // TODO: change to return result bool
        let retval = self.connector.commit_iceberg(&ORG_ID.to_string(), table_name, iceberg_commit).await.map_err(from_modyne)?;
        assert!(retval);
        Ok(())
    }

    pub async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), ServiceApiError> {
        match self.connector.commit_extension_work_item_completed(&ORG_ID.to_string(), table_name, &commit).await.map_err(from_modyne)? {
            true => Ok(()),
            false => {
                Err(ServiceApiError{ message: "Unable to commit, conflict detected".to_string() })
            }
        }
    }

    pub async fn compaction_commit(&mut self, _table_name: &String, commit: &CompactionCommit) -> Result<(), ServiceApiError> {
        // NOTE: this just notes what the compactor is saying. We don't generate the new checkpoint
        // until we see an iceberg or speedboat commit with the new info.
        assert!(commit.compaction_id.len() > 0);
        self.connector.create_compaction(&mut self.compactions_cache, &ORG_ID.to_string(), &commit.compaction_id, commit).await.map_err(from_modyne)
    }

    pub async fn cleanup_commit(&mut self, commit: &CleanupCommit) -> Result<(), ServiceApiError> {
        let mut transaction = TransactWrite::new();
        transaction = DynamoDbConnector::mark_done_cleanup_work_item_lease_inner(transaction, &ORG_ID.to_string(), &commit.table_name, &commit.id, None);
        self.connector.commit_conditional_transaction(transaction).await.map_err(from_modyne)?;
        Ok(())
    }

    pub async fn get_latest_committed_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, ServiceApiError> {
        let value = self.connector.describe_latest(&ORG_ID.to_string(), &Self::latest_checkpoint_key(table_name, &extensions)).await.map_err(from_modyne)?;
        match value {
            Some(val) => {
                tracing::info!("Latest checkpoint for {}: {}", table_name, val.entity_id);
                Ok(Some(CheckpointDescriptor::from_full_name(&val.entity_id).full_checkpoint_id()))
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
        match self.connector.commit_extension_work_item_taken(&used_latest, &Self::NO_WORK_ITEM.to_string()).await.map_err(from_modyne)? {
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
                        // TODO: for now leases never expire
                        let leases = self.connector.oldest_available_compaction_work_item_lease(&ORG_ID.to_string(), &latest_entity_info.entity_id, None, Some(0)).await.map_err(from_modyne)?;
                        if leases.len() == 0 {
                            let work_item = self.connector.describe_compaction_work_item(&mut self.compaction_work_item_cache, &ORG_ID.to_string(), &latest_entity_info.entity_id).await.map_err(from_modyne)?;
                            assert!(work_item.is_some());
                            let compaction = work_item.unwrap();
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
        match self.connector.commit_compaction_work_item_taken(&used_latest, &Self::NO_WORK_ITEM.to_string()).await.map_err(from_modyne)? {
            true => Ok(work_items),
            false => Ok(vec!())
        }
    }

    pub async fn get_cleanup_work_items(&mut self) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        let all_tables = self.connector.fetch_entities(&ORG_ID.to_string(), &"powdrr_table".to_string(), None).await.map_err(from_modyne)?;
        let mut work_items = vec!();
        let mut transaction = TransactWrite::new();
        for table_entity in all_tables.entities {
            let available_infos = self.connector.oldest_available_cleanup_work_item_lease(&ORG_ID.to_string(), &table_entity.entity_id, None, Some(LEASE_LENGTH_MS)).await.map_err(from_modyne)?;
            for available_info in available_infos.iter() {
                let work_item = self.connector.describe_cleanup_work_item(&mut PowdrrNamedCleanupWorkItemCache::new(), &ORG_ID.to_string(), &available_info.name).await.map_err(from_modyne)?;
                assert!(work_item.is_some());
                work_items.push(work_item.unwrap());
                transaction = self.connector.claim_cleanup_work_item_lease(transaction, available_info);
            }
        }

        if work_items.len() > 0 {
            match self.connector.commit_conditional_transaction(transaction).await.map_err(from_modyne)? {
                true => Ok(work_items),
                false => Ok(vec!())
            }
        } else {
            Ok(vec!())
        }
    }

    pub async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        let all_tables = self.connector.fetch_entities(&ORG_ID.to_string(), &"powdrr_table".to_string(), None).await.map_err(from_modyne)?;
        let mut work_done = false;
        for table_entity in all_tables.entities {
            work_done = work_done | self.update_standard_checkpoint(&table_entity.entity_id).await?;
            work_done = work_done | self.update_extension_checkpoint(&table_entity.entity_id).await?;
        }
        Ok(work_done)
    }

    async fn update_standard_checkpoint(&mut self, table_name: &String) -> Result<bool, ServiceApiError> {
        // TODO: need a bulk fetcher

        let latest_speedboat_trackers = self.connector.oldest_available_speedboat_commit_checkpointed(&ORG_ID.to_string(), table_name, None, None).await.map_err(from_modyne)?;
        if latest_speedboat_trackers.len() == 0 {
            return Ok(false);
        }

        let latest_checkpoint_info = self.connector.describe_latest(&ORG_ID.to_string(), &Self::latest_checkpoint_key(table_name, &None)).await.map_err(from_modyne)?.unwrap();
        let latest_checkpoint = self.connector.describe_checkpoint(&mut self.checkpoints_cache, &ORG_ID.to_string(), &latest_checkpoint_info.entity_id).await.map_err(from_modyne)?.unwrap();

        let mut latest_speedboats = vec!();
        for speedboat_tracker in latest_speedboat_trackers.iter() {
            latest_speedboats.push(self.connector.describe_speedboat_commit(&mut self.speedboat_cache, &ORG_ID.to_string(), &speedboat_tracker.name).await.map_err(from_modyne)?.unwrap());
        }

        let (new_checkpoint, changed) = self.clone_and_apply(
            &latest_checkpoint,
            &latest_speedboats,
            &vec!(),
            &vec!(),
        ).await;

        assert!(changed);

        let mut compaction_latest = None;
        let mut compaction_work_item = None;
        if new_checkpoint.speedboat_metadata.is_some() {
            let speedboat_files = &new_checkpoint.speedboat_metadata.as_ref().unwrap().files;
            // TODO: do the real policy here
            let compact = speedboat_files.file_paths.len() as u64 >= 2 || speedboat_files.sizes.iter().sum::<u64>() > 30 * 1024 * 1024;
            if compact {
                let latest_compaction = self.connector.describe_latest(&ORG_ID.to_string(), &Self::latest_compaction_work_item_key(table_name)).await.map_err(from_modyne)?.unwrap();
                if latest_compaction.entity_id == Self::NO_WORK_ITEM.to_owned() {
                    compaction_latest = Some(latest_compaction);
                    compaction_work_item = Some(CompactionWorkItem::from_checkpoint(&new_checkpoint, &vec!()));
                }
            }
        }

        match self.connector.commit_checkpoint(
            &latest_checkpoint_info,
            &latest_speedboat_trackers,
            &new_checkpoint,
            &compaction_latest,
            &compaction_work_item
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

        Ok(true)
    }

    async fn update_extension_checkpoint(&mut self, table_name: &String) -> Result<bool, ServiceApiError> {
        let latest_extension_trackers = self.connector.oldest_available_extension_commit_checkpointed(&ORG_ID.to_string(), table_name, None, None).await.map_err(from_modyne)?;
        if latest_extension_trackers.len() == 0 {
            return Ok(false);
        }

        let mut extension_commits = vec!();
        for tracker in latest_extension_trackers.iter() {
            extension_commits.push(self.connector.describe_extension_commit(&mut self.extension_cache, &ORG_ID.to_string(), &tracker.name).await.map_err(from_modyne)?.unwrap());
        }

        let waiting_checkpoints = self.connector.oldest_available_checkpoint_waiting_for_extension(&ORG_ID.to_string(), table_name, None, None).await.map_err(from_modyne)?;
        let mut work_done = false;
        for checkpoint_tracker in waiting_checkpoints.iter() {
            // TODO: all commits per loop should be in a transaction
            work_done = true;

            let old_checkpoint = self.connector.describe_checkpoint(&mut self.checkpoints_cache, &ORG_ID.to_string(), &checkpoint_tracker.name).await.map_err(from_modyne)?.unwrap();
            let (mut new_checkpoint, changed) = self.clone_and_apply(
                &old_checkpoint,
                &vec!(),
                &vec!(),
                &extension_commits,
            ).await;
            new_checkpoint.original_checkpoint_id = Some(match old_checkpoint.original_checkpoint_id {
                Some(original_checkpoint_id) => original_checkpoint_id.clone(),
                None => old_checkpoint.checkpoint_id.clone()
            });

            if new_checkpoint.fully_covered_for_extension(&"es".to_string()) {
                let latest_checkpoint = self.connector.describe_latest(&ORG_ID.to_string(), &Self::latest_checkpoint_key(table_name, &Some("es".to_string()))).await.map_err(from_modyne)?.unwrap();
                let commit = latest_checkpoint.entity_id < new_checkpoint.get_descriptor().full_name();
                //let commit = true;
                if commit {
                    let retval = self.connector.commit_checkpoint(&latest_checkpoint, &vec!(), &new_checkpoint, &None, &None).await.map_err(from_modyne)?;
                    assert!(retval);
                }
            }

            if changed {
                self.connector.mark_done_checkpoint_waiting_for_extension(checkpoint_tracker).await.map_err(from_modyne)?;
            }
        }

        // TODO: need to figure out protocol on when to mark an extension commit as done

        Ok(work_done)
    }


}
