use std::collections::{HashMap, HashSet};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::Client;
use modyne::model::TransactWrite;
use modyne::TestTableExt;
use crate::data_contract::{TableDescription, CreateIndexTemplateBody, CompactionWorkItem, ExtensionWorkItem, CompactionCommit, TableMetadataCheckpoint, ExtensionCommit, CreateTable, SpeedboatCommit, IcebergCommit, SpeedboatCommitTableInfo, CleanupWorkItem, CleanupCommit, OrgSettings, OrgInfo};
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::state_provider::ServiceApiError;
use crate::dynamodb::{DynamoDbConnector, PowdrrNamedCleanupWorkItemCache, PowdrrNamedCompactionCommitCache, PowdrrNamedCompactionWorkItemCache, PowdrrNamedExtensionCommitCache, PowdrrNamedExtensionWorkItemCache, PowdrrNamedIcebergCommitCache, PowdrrNamedOrgInfoCache, PowdrrNamedSpeedboatCommitCache, PowdrrNamedTableMetadataCheckpointCache, TableBody};
use crate::test_api::{StateMode, TestProcessingMode};


const LEASE_LENGTH_MS: i64 = 60 * 1000; // 1 minute


fn from_modyne(e: modyne::Error) -> ServiceApiError {
    ServiceApiError::new(e.to_string())
}


#[allow(dead_code)]
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
    org_cache: PowdrrNamedOrgInfoCache,
    update_cache: Vec<(String, String)>,
}


static MANAGEMENT_ORG_ID: &'static str = "MANAGEMENT_ORG";


impl DynamoDBServiceImpl {
    pub fn new(mode: TestProcessingMode) -> Self {
        let aws_config = aws_sdk_dynamodb::Config::builder().build();
        Self::with_client(aws_sdk_dynamodb::Client::from_conf(aws_config), mode)
    }

    pub async fn test(mode: TestProcessingMode) -> Self {
        assert!(mode.state_mode.is_testing());
        let address_option = match &mode.state_mode {
            StateMode::TestingDynamoDb(address) => address,
            _ => panic!("Invalid state mode for testing")
        };
        let address = address_option.as_ref().unwrap_or(&"http://localhost:4566".to_owned()).clone();
        tracing::info!("Testing with DynamoDB at {}", address);
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("us-east-1")) // Region doesn't matter for local, but required
            .endpoint_url(address)
            .credentials_provider(aws_credential_types::Credentials::new(
                "test", "test", None, None, "static",
            ))
            .load()
            .await;

        let client = Client::new(&config);
        let impl_obj = Self::with_client(client, mode);

        let _ = impl_obj.connector.delete_table().send().await;
        let _create_table = match impl_obj.connector.create_table().send().await {
            Ok(_) => (),
            Err(e) => {
                panic!("Failed during initialization, is Docker running?: {:?}", e)
            },
        };
        impl_obj
    }

    fn with_client(client: Client, mode: TestProcessingMode) -> Self {
        DynamoDBServiceImpl {
            mode,
            connector: DynamoDbConnector::new(client),
            compactions_cache: PowdrrNamedCompactionCommitCache::new(),
            checkpoints_cache: PowdrrNamedTableMetadataCheckpointCache::new(),
            speedboat_cache: PowdrrNamedSpeedboatCommitCache::new(),
            iceberg_cache: PowdrrNamedIcebergCommitCache::new(),
            extension_cache: PowdrrNamedExtensionCommitCache::new(),
            extension_work_item_cache: PowdrrNamedExtensionWorkItemCache::new(),
            compaction_work_item_cache: PowdrrNamedCompactionWorkItemCache::new(),
            org_cache: PowdrrNamedOrgInfoCache::new(),
            update_cache: Vec::new(),
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

    pub async fn add_checkpoint(&mut self, org_info: &OrgInfo, metadata: &TableMetadataCheckpoint) -> Result<(), ServiceApiError> {
        self.create_table(org_info, &CreateTable { name: metadata.table_name.clone(), tags: Default::default(), serving: None }).await?;
        if metadata.speedboat_metadata.is_some() {
            self.speedboat_commit(
                org_info,
                &SpeedboatCommit {
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
                org_info,
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

    pub async fn create_table(&mut self, org_info: &OrgInfo, create_table: &CreateTable) -> Result<bool, ServiceApiError> {
        self.connector.create_table_helper(&org_info.org_id.to_string(), &create_table.name, &TableBody { tags: create_table.tags.clone(), serving: create_table.serving.clone() }).await.map_err(from_modyne)
    }

    pub async fn describe_table(&mut self, org_info: &OrgInfo, name: &String) -> Result<Option<TableDescription>, ServiceApiError> {
        let result = self.connector.describe_powdrr_table(&org_info.org_id.to_string(), name).await
            .map(|x| x.map(|x| TableDescription { name: name.clone(), tags: x.tags.clone(), serving: x.serving.clone() }))
            .map_err(from_modyne)?;

        match result {
            Some(x) => Ok(Some(x)),
            None => {
                match self.connector.describe_alias(&org_info.org_id.to_string(), name).await.map_err(from_modyne)? {
                    None => Ok(None),
                    Some(table_name) => {
                        self.connector.describe_powdrr_table(&org_info.org_id.to_string(), &table_name).await
                            .map(|x| x.map(|x| TableDescription { name: table_name.clone(), tags: x.tags.clone(), serving: x.serving.clone() }))
                            .map_err(from_modyne)
                    }
                }
            }
        }
    }

    pub async fn add_alias(&mut self, org_info: &OrgInfo, table_name: &String, alias: &String) -> Result<bool, ServiceApiError> {
        self.connector.create_alias(&org_info.org_id.to_string(), alias, table_name).await.map_err(from_modyne)
    }

    pub async fn remove_alias(&mut self, org_info: &OrgInfo, _table_name: &String, alias: &String) -> Result<bool, ServiceApiError> {
        self.connector.delete_alias(&org_info.org_id.to_string(), alias).await.map_err(from_modyne)
    }

    pub async fn create_table_template(&mut self, org_info: &OrgInfo, name: &String, template: &CreateIndexTemplateBody) -> Result<bool, ServiceApiError> {
        self.connector.create_table_template(&org_info.org_id.to_string(), name, template).await.map_err(from_modyne)
    }

    pub async fn describe_table_template(&mut self, org_info: &OrgInfo, name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        self.connector.describe_table_template(&org_info.org_id.clone(), name).await.map_err(from_modyne)
    }

    pub async fn create_pipeline(&mut self, org_info: &OrgInfo, name: &String, pipeline: &PipelineDefinition) -> Result<bool, ServiceApiError> {
        self.connector.create_pipeline(&org_info.org_id.clone(), name, pipeline).await.map_err(from_modyne)
    }

    pub async fn describe_pipeline(&mut self, org_info: &OrgInfo, name: &String) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        self.connector.describe_pipeline(&org_info.org_id.clone(), name).await.map_err(from_modyne)
    }

    pub async fn create_lifetime_policy(&mut self, org_info: &OrgInfo, name: &String, policy: &ILMPolicyDefinition) -> Result<bool, ServiceApiError> {
        self.connector.create_lifetime_policy(&org_info.org_id.clone(), name, policy).await.map_err(from_modyne)
    }

    pub async fn describe_lifetime_policy(&mut self, org_info: &OrgInfo, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        self.connector.describe_lifetime_policy(&org_info.org_id.clone(), name).await.map_err(from_modyne)
    }

    pub async fn speedboat_commit(&mut self, org_info: &OrgInfo, commit: &SpeedboatCommit) -> Result<bool, ServiceApiError> {
        let tables: HashSet<String> = commit.type_files.iter().map(|x| x.table_name.clone()).collect();
        assert_eq!(tables.len(), 1, "Only really support single table commits right now");
        let retval = self.connector.commit_speedboat(&org_info.org_id.clone(), &commit.type_files[0].table_name, commit).await.map_err(from_modyne)?;
        if retval {
            self.update_cache.push((org_info.org_id.clone(), commit.type_files[0].table_name.clone()));
        }
        Ok(retval)
    }

    async fn clone_and_apply(
        &mut self,
        org_id: &String,
        metadata: &TableMetadataCheckpoint,
        speedboat_commits: &Vec<SpeedboatCommit>,
        iceberg_commits: &Vec<IcebergCommit>,
        extension_commits: &Vec<ExtensionCommit>,
    ) -> (TableMetadataCheckpoint, bool) {
        let compactions = self.gather_compactions(org_id, speedboat_commits, iceberg_commits).await.unwrap();
        metadata.clone_and_apply(
            speedboat_commits,
            iceberg_commits,
            extension_commits,
            &compactions
        )
    }

    async fn gather_compactions(&mut self, org_id: &String, speedboat_commits: &Vec<SpeedboatCommit>, iceberg_commits: &Vec<IcebergCommit>) -> Result<HashMap<String, CompactionCommit>, ServiceApiError> {
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
                self.connector.describe_compaction(&mut self.compactions_cache, org_id, &compaction).await.map_err(from_modyne)?.unwrap()
            );
        }

        Ok(compaction_commits)
    }

    pub async fn iceberg_commit(&mut self, org_info: &OrgInfo, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<bool, ServiceApiError> {
        self.connector.commit_iceberg(&org_info.org_id.clone(), table_name, iceberg_commit).await.map_err(from_modyne)
    }

    pub async fn extension_commit(&mut self, org_info: &OrgInfo, table_name: &String, commit: &ExtensionCommit) -> Result<bool, ServiceApiError> {
        self.connector.commit_extension_work_item_completed(&org_info.org_id, table_name, &commit).await.map_err(from_modyne)
    }

    pub async fn compaction_commit(&mut self, org_info: &OrgInfo, _table_name: &String, commit: &CompactionCommit) -> Result<bool, ServiceApiError> {
        // NOTE: this just notes what the compactor is saying. We don't generate the new checkpoint
        // until we see an iceberg or speedboat commit with the new info.
        assert!(commit.compaction_id.len() > 0);
        self.connector.create_compaction(&mut self.compactions_cache, &org_info.org_id, &commit.compaction_id, commit).await.map_err(from_modyne)
    }

    pub async fn cleanup_commit(&mut self, org_info: &OrgInfo, commit: &CleanupCommit) -> Result<bool, ServiceApiError> {
        let mut transaction = TransactWrite::new();
        transaction = DynamoDbConnector::mark_done_cleanup_work_item_lease_inner(transaction, &org_info.org_id, &commit.table_name, &commit.id, None);
        self.connector.commit_conditional_transaction(transaction).await.map_err(from_modyne)
    }

    pub async fn get_latest_committed_checkpoint(&mut self, org_info: &OrgInfo, table_name: &String, extensions: Option<String>) -> Result<Option<String>, ServiceApiError> {
        let value = self.connector.describe_latest(&org_info.org_id, &Self::latest_checkpoint_key(table_name, &extensions)).await.map_err(from_modyne)?;
        match value {
            Some(val) => {
                tracing::info!("Latest checkpoint for {}: {}", table_name, val.entity_id);
                Ok(Some(CheckpointDescriptor::from_full_name(&val.entity_id).full_checkpoint_id()))
            },
            None => Ok(None)
        }
    }

    pub async fn get_checkpoint(&mut self, org_info: &OrgInfo, checkpoint: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        tracing::info!("Getting checkpoint for {}", checkpoint.full_name());
        match self.connector.describe_checkpoint(&mut self.checkpoints_cache, &org_info.org_id, &checkpoint.full_name()).await.map_err(from_modyne) {
            Ok(val) => {
                tracing::info!("Got checkpoint for {}: {}", checkpoint.full_name(), val.is_some());
                Ok(val)
            },
            Err(e) => Err(e)
        }
    }

    pub async fn get_extension_work_items(&mut self, org_info: &OrgInfo, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        let all_tables = self.connector.fetch_entities(&org_info.org_id, &"powdrr_table".to_string(), None).await.map_err(from_modyne)?;
        let mut work_items = vec!();
        let mut used_latest = vec!();
        for table_entity in all_tables.entities {
            let latest_es_key = &Self::latest_extension_work_item_key(&table_entity.entity_id, extension_type);
            let latest_entity_info = self.connector.describe_latest(&org_info.org_id, latest_es_key).await.map_err(from_modyne)?;
            match latest_entity_info {
                Some(latest_entity_info) => {
                    if latest_entity_info.entity_id != Self::NO_WORK_ITEM.to_owned() {
                        let lease = self.connector.valid_leases_extension_work_item_lease(&org_info.org_id, &latest_entity_info.entity_id, None, Some(LEASE_LENGTH_MS)).await.map_err(from_modyne)?;
                        if lease.len() == 0 {
                            let work_item = self.connector.describe_extension_work_item(&mut self.extension_work_item_cache, &org_info.org_id, &latest_entity_info.entity_id).await.map_err(from_modyne)?;
                            assert!(work_item.is_some());
                            work_items.push(work_item.unwrap());
                            used_latest.push(latest_entity_info);
                        }
                    }
                },
                None => {
                    tracing::error!("Unable to find latest extension work item for table {}", table_entity.entity_id);
                }
            }
        }
        match self.connector.commit_extension_work_item_taken(&used_latest, &Self::NO_WORK_ITEM.to_string()).await.map_err(from_modyne)? {
            true => {
                tracing::info!("Extensions: returning {} items", work_items.len());
                Ok(work_items)
            },
            false => {
                tracing::info!("Extensions: returning 0 items");
                Ok(vec!())
            }
        }
    }

    pub async fn get_compaction_work_items(&mut self, org_info: &OrgInfo, ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        let all_tables = self.connector.fetch_entities(&org_info.org_id, &"powdrr_table".to_string(), None).await.map_err(from_modyne)?;
        let mut work_items = vec!();
        let mut used_latest = vec!();
        for table_entity in all_tables.entities {
            let latest_entity_info = self.connector.describe_latest(&org_info.org_id, &Self::latest_compaction_work_item_key(&table_entity.entity_id)).await.map_err(from_modyne)?;
            match latest_entity_info {
                Some(latest_entity_info) => {
                    if latest_entity_info.entity_id != Self::NO_WORK_ITEM.to_owned() {
                        let leases = self.connector.valid_leases_compaction_work_item_lease(&org_info.org_id, &latest_entity_info.entity_id, None, Some(LEASE_LENGTH_MS)).await.map_err(from_modyne)?;
                        if leases.len() == 0 {
                            let work_item = self.connector.describe_compaction_work_item(&mut self.compaction_work_item_cache, &org_info.org_id, &latest_entity_info.entity_id).await.map_err(from_modyne)?;
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
            true => {
                tracing::info!("Compaction: returning {} items", work_items.len());
                Ok(work_items)
            },
            false => {
                tracing::info!("Compaction: returning 0 items");
                Ok(vec!())
            }
        }
    }

    pub async fn get_cleanup_work_items(&mut self, org_info: &OrgInfo) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        let all_tables = self.connector.fetch_entities(&org_info.org_id, &"powdrr_table".to_string(), None).await.map_err(from_modyne)?;
        let mut work_items = vec!();
        let mut transaction = TransactWrite::new();
        for table_entity in all_tables.entities {
            let available_infos = self.connector.oldest_available_cleanup_work_item_lease(&org_info.org_id, &table_entity.entity_id, None, Some(LEASE_LENGTH_MS)).await.map_err(from_modyne)?;
            for available_info in available_infos.iter() {
                let work_item = self.connector.describe_cleanup_work_item(&mut PowdrrNamedCleanupWorkItemCache::new(), &org_info.org_id, &available_info.name).await.map_err(from_modyne)?;
                assert!(work_item.is_some());
                work_items.push(work_item.unwrap());
                transaction = self.connector.claim_cleanup_work_item_lease(transaction, available_info);
            }
        }

        if work_items.len() > 0 {
            match self.connector.commit_conditional_transaction(transaction).await.map_err(from_modyne)? {
                true => {
                    tracing::info!("Cleanup: returning {} items", work_items.len());
                    Ok(work_items)
                },
                false => {
                    tracing::info!("Cleanup: returning 0 items");
                    Ok(vec!())
                }
            }
        } else {
            tracing::info!("Cleanup: returning 0 items");
            Ok(vec!())
        }
    }

    pub async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        let mut work_done = false;
        for (org_id, table_name) in self.update_cache.clone().iter() {
            work_done = work_done | self.update_standard_checkpoint(org_id, table_name).await?;
            work_done = work_done | self.update_extension_checkpoint(org_id, table_name).await?;
        }
        self.update_cache.clear();
        Ok(work_done)
    }

    async fn update_standard_checkpoint(&mut self, org_id: &String, table_name: &String) -> Result<bool, ServiceApiError> {
        // TODO: need a bulk fetcher

        let latest_speedboat_trackers = self.connector.oldest_available_speedboat_commit_checkpointed(org_id, table_name, None, None).await.map_err(from_modyne)?;
        if latest_speedboat_trackers.len() == 0 {
            return Ok(false);
        }

        let latest_checkpoint_info = self.connector.describe_latest(org_id, &Self::latest_checkpoint_key(table_name, &None)).await.map_err(from_modyne)?.unwrap();
        let latest_checkpoint = self.connector.describe_checkpoint(&mut self.checkpoints_cache, org_id, &latest_checkpoint_info.entity_id).await.map_err(from_modyne)?.unwrap();

        let mut latest_speedboats = vec!();
        for speedboat_tracker in latest_speedboat_trackers.iter() {
            latest_speedboats.push(self.connector.describe_speedboat_commit(&mut self.speedboat_cache, org_id, &speedboat_tracker.name).await.map_err(from_modyne)?.unwrap());
        }

        let (new_checkpoint, changed) = self.clone_and_apply(
            org_id,
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
            let num_files_threshold = self.mode.compaction_mode.threshold();
            let compact = speedboat_files.file_paths.len() as u64 >= num_files_threshold || speedboat_files.sizes.iter().sum::<u64>() > 30 * 1024 * 1024;
            tracing::info!("Compaction threshold: {} files, {} bytes, compact: {}", num_files_threshold, speedboat_files.sizes.iter().sum::<u64>(), compact);
            if compact {
                let latest_compaction = self.connector.describe_latest(org_id, &Self::latest_compaction_work_item_key(table_name)).await.map_err(from_modyne)?.unwrap();
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

    async fn update_extension_checkpoint(&mut self, org_id: &String, table_name: &String) -> Result<bool, ServiceApiError> {
        let latest_extension_trackers = self.connector.oldest_available_extension_commit_checkpointed(org_id, table_name, None, None).await.map_err(from_modyne)?;
        if latest_extension_trackers.len() == 0 {
            return Ok(false);
        }

        let mut extension_commits = vec!();
        for tracker in latest_extension_trackers.iter() {
            extension_commits.push(self.connector.describe_extension_commit(&mut self.extension_cache, org_id, &tracker.name).await.map_err(from_modyne)?.unwrap());
        }

        let waiting_checkpoints = self.connector.oldest_available_checkpoint_waiting_for_extension(org_id, table_name, None, None).await.map_err(from_modyne)?;
        let mut work_done = false;
        for checkpoint_tracker in waiting_checkpoints.iter() {
            // TODO: all commits per loop should be in a transaction
            work_done = true;

            let old_checkpoint = self.connector.describe_checkpoint(&mut self.checkpoints_cache, org_id, &checkpoint_tracker.name).await.map_err(from_modyne)?.unwrap();
            let (mut new_checkpoint, changed) = self.clone_and_apply(
                org_id,
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
                let latest_checkpoint = self.connector.describe_latest(org_id, &Self::latest_checkpoint_key(table_name, &Some("es".to_string()))).await.map_err(from_modyne)?.unwrap();
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

    fn org_info_key(access_key_id: &String, secret_access_key: &String) -> String {
        format!("{}:{}", access_key_id, secret_access_key)
    }

    pub async fn create_org(&mut self, settings: &OrgSettings) -> Result<(), ServiceApiError> {
        // Org settings are stored as-is.
        // An OrgInfo is created using the credentials as a key for fast lookups.
        let mut transaction = TransactWrite::new();
        transaction = self.connector.private_create_org_settings_core(transaction, &MANAGEMENT_ORG_ID.to_string(), &settings.org_id, settings);
        for creds in settings.creds.iter() {
            transaction = self.connector.cached_create_org_creds_core(transaction, &mut self.org_cache, &MANAGEMENT_ORG_ID.to_string(), &Self::org_info_key(&creds.access_key_id, &creds.secret_access_key), &settings.to_org_info());
        }
        let result = self.connector.commit_conditional_transaction(transaction).await.map_err(from_modyne)?;
        assert!(result);
        Ok(())
    }

    pub async fn lookup_org(&mut self, access_key_id: &String, secret_access_key: &String) -> Result<Option<OrgInfo>, ServiceApiError> {
        self.connector.describe_org_creds(&mut self.org_cache, &MANAGEMENT_ORG_ID.to_string(), &Self::org_info_key(access_key_id, secret_access_key)).await.map_err(from_modyne)
    }
}
