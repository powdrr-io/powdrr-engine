use crate::data_contract::{
    CleanupCommit, CleanupWorkItem, CompactionCommit, CompactionWorkItem, CreateIndexTemplateBody,
    CreateTable, ExtensionCommit, ExtensionWorkItem, IcebergCommit, OrgInfo, OrgSettings,
    SpeedboatCommit, SpeedboatCommitTableInfo, TableDescription, TableMetadataCheckpoint,
};
use crate::dynamodb::{
    DynamoDbConnector, EntityVersionInfo, PowdrrNamedCleanupWorkItemCache,
    PowdrrNamedCompactionCommitCache, PowdrrNamedCompactionWorkItemCache,
    PowdrrNamedExtensionCommitCache, PowdrrNamedExtensionWorkItemCache,
    PowdrrNamedIcebergCommitCache, PowdrrNamedOrgInfoCache, PowdrrNamedSpeedboatCommitCache,
    PowdrrNamedTableMetadataCheckpointCache, TableBody,
};
use crate::metadata_store::{
    CheckpointUpdateRequest, ClaimedCleanupWorkItem, ClaimedCompactionWorkItem,
    ClaimedExtensionWorkItem, MetadataClaimKind, MetadataStore, PublishedCheckpointRecord,
    PublishedCheckpointRole, PublishedCheckpointSelector,
};
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::state_provider::ServiceApiError;
use crate::test_api::{StateMode, TestProcessingMode};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::Client;
use modyne::TestTableExt;
use modyne::model::TransactWrite;
use powdrr_control_plane::ilm_policy::ILMPolicyDefinition;
use std::collections::{HashMap, HashSet};

const LEASE_LENGTH_MS: i64 = 60 * 1000; // 1 minute

fn from_modyne(e: modyne::Error) -> ServiceApiError {
    ServiceApiError::new(e.to_string())
}

fn create_table_request(
    name: String,
    tags: HashMap<String, String>,
    serving: Option<crate::data_contract::ServingTableConfig>,
    dynamodb: Option<crate::data_contract::DynamoDbTableConfig>,
    mongodb: Option<crate::data_contract::MongoDbTableConfig>,
    redis: Option<crate::data_contract::RedisTableConfig>,
) -> CreateTable {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "tags": tags,
        "serving": serving,
        "dynamodb": dynamodb,
        "mongodb": mongodb,
        "redis": redis,
    }))
    .expect("table metadata request should deserialize")
}

fn table_description_from_parts(
    name: String,
    tags: HashMap<String, String>,
    serving: Option<crate::data_contract::ServingTableConfig>,
    dynamodb: Option<crate::data_contract::DynamoDbTableConfig>,
    mongodb: Option<crate::data_contract::MongoDbTableConfig>,
    redis: Option<crate::data_contract::RedisTableConfig>,
) -> TableDescription {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "tags": tags,
        "serving": serving,
        "dynamodb": dynamodb,
        "mongodb": mongodb,
        "redis": redis,
    }))
    .expect("table description should deserialize")
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
            _ => panic!("Invalid state mode for testing"),
        };
        let address = address_option
            .as_ref()
            .unwrap_or(&"http://localhost:4566".to_owned())
            .clone();
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
            }
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
        }
    }

    pub fn seed(&self) -> Result<(), ServiceApiError> {
        Ok(())
    }

    fn latest_checkpoint_key(table_name: &String, extension: &Option<String>) -> String {
        match extension {
            Some(x) => format!("checkpoint#{}#{}", table_name, x),
            None => format!("checkpoint#{}", table_name),
        }
    }

    fn latest_active_checkpoint_key(table_name: &String, extension: &Option<String>) -> String {
        match extension {
            Some(x) => format!("published_checkpoint#active#{}#{}", table_name, x),
            None => format!("published_checkpoint#active#{}", table_name),
        }
    }

    fn latest_extension_work_item_key(table_name: &String, extension: &String) -> String {
        format!("extension_work_item#{}#{}", table_name, extension)
    }

    fn latest_compaction_work_item_key(table_name: &String) -> String {
        format!("compaction_work_item#{}", table_name)
    }

    fn checkpoint_publication_request_key(org_id: &String, table_name: &String) -> String {
        format!("checkpoint_publication_request#{}#{}", org_id, table_name)
    }

    fn parse_checkpoint_publication_request_key(key: &String) -> Option<(String, String)> {
        let raw = key.strip_prefix("checkpoint_publication_request#")?;
        let (org_id, table_name) = raw.split_once('#')?;
        Some((org_id.to_string(), table_name.to_string()))
    }

    const NO_WORK_ITEM: &'static str = "-1";

    async fn checkpoint_publication_requests(
        &mut self,
    ) -> Result<Vec<EntityVersionInfo>, ServiceApiError> {
        let entities = self
            .connector
            .fetch_entities(&MANAGEMENT_ORG_ID.to_string(), &"latest".to_string(), None)
            .await
            .map_err(from_modyne)?;

        let mut requests = vec![];
        for entity in entities.entities.iter() {
            if !entity
                .entity_id
                .starts_with("checkpoint_publication_request#")
            {
                continue;
            }

            let request = self
                .connector
                .describe_latest(&MANAGEMENT_ORG_ID.to_string(), &entity.entity_id)
                .await
                .map_err(from_modyne)?;
            if let Some(request) = request {
                if request.entity_id != Self::NO_WORK_ITEM {
                    requests.push(request);
                }
            }
        }

        Ok(requests)
    }

    async fn get_checkpoint_id_from_latest_key(
        &mut self,
        org_id: &String,
        key: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        Ok(self
            .connector
            .describe_latest(org_id, key)
            .await
            .map_err(from_modyne)?
            .map(|val| CheckpointDescriptor::from_full_name(&val.entity_id).full_checkpoint_id()))
    }

    async fn checkpoint_publication_request_exists(
        &mut self,
        org_id: &String,
        table_name: &String,
    ) -> Result<bool, ServiceApiError> {
        Ok(self
            .connector
            .describe_latest(
                &MANAGEMENT_ORG_ID.to_string(),
                &Self::checkpoint_publication_request_key(org_id, table_name),
            )
            .await
            .map_err(from_modyne)?
            .map(|entity| entity.entity_id != Self::NO_WORK_ITEM)
            .unwrap_or(false))
    }

    async fn get_active_checkpoint_id(
        &mut self,
        org_id: &String,
        table_name: &String,
        extension: &Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        let active_key = Self::latest_active_checkpoint_key(table_name, extension);
        match self
            .get_checkpoint_id_from_latest_key(org_id, &active_key)
            .await?
        {
            Some(checkpoint_id) => Ok(Some(checkpoint_id)),
            None => {
                if self
                    .checkpoint_publication_request_exists(org_id, table_name)
                    .await?
                {
                    Ok(None)
                } else {
                    self.get_checkpoint_id_from_latest_key(
                        org_id,
                        &Self::latest_checkpoint_key(table_name, extension),
                    )
                    .await
                }
            }
        }
    }

    async fn set_active_checkpoint_id(
        &mut self,
        org_id: &String,
        table_name: &String,
        extension: &Option<String>,
        checkpoint_id: &String,
    ) -> Result<bool, ServiceApiError> {
        let checkpoint_full_name =
            CheckpointDescriptor::new(table_name.clone(), checkpoint_id.clone()).full_name();
        let key = Self::latest_active_checkpoint_key(table_name, extension);
        match self
            .connector
            .describe_latest(org_id, &key)
            .await
            .map_err(from_modyne)?
        {
            Some(existing) => {
                if existing.entity_id == checkpoint_full_name {
                    return Ok(false);
                }
                let transaction = DynamoDbConnector::bump_version(
                    TransactWrite::new(),
                    &existing,
                    &checkpoint_full_name,
                );
                self.connector
                    .commit_conditional_transaction(transaction)
                    .await
                    .map_err(from_modyne)
            }
            None => self
                .connector
                .create_latest(
                    org_id,
                    &key,
                    &EntityVersionInfo::new(org_id, &key, &checkpoint_full_name),
                )
                .await
                .map_err(from_modyne),
        }
    }

    async fn checkpoint_publication_still_pending(
        &mut self,
        org_id: &String,
        table_name: &String,
    ) -> Result<bool, ServiceApiError> {
        if !self
            .connector
            .oldest_available_speedboat_commit_checkpointed(org_id, table_name, Some(1), None)
            .await
            .map_err(from_modyne)?
            .is_empty()
        {
            return Ok(true);
        }

        if !self
            .connector
            .oldest_available_checkpoint_waiting_for_extension(org_id, table_name, Some(1), None)
            .await
            .map_err(from_modyne)?
            .is_empty()
        {
            return Ok(true);
        }

        for extension in [None, Some("es".to_string())] {
            let committed = self
                .get_checkpoint_id_from_latest_key(
                    org_id,
                    &Self::latest_checkpoint_key(table_name, &extension),
                )
                .await?;
            let active = self
                .get_active_checkpoint_id(org_id, table_name, &extension)
                .await?;
            if committed != active {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub async fn add_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        metadata: &TableMetadataCheckpoint,
    ) -> Result<(), ServiceApiError> {
        self.create_table(
            org_info,
            &create_table_request(
                metadata.table_name.clone(),
                Default::default(),
                None,
                None,
                None,
                None,
            ),
        )
        .await?;
        if metadata.speedboat_metadata.is_some() {
            self.speedboat_commit(
                org_info,
                &SpeedboatCommit {
                    type_files: vec![SpeedboatCommitTableInfo {
                        commit_type: "commit".to_string(),
                        table_name: metadata.table_name.clone(),
                        segments: vec![],
                        files: metadata
                            .speedboat_metadata
                            .as_ref()
                            .unwrap()
                            .files
                            .file_paths
                            .clone(),
                        sizes: metadata
                            .speedboat_metadata
                            .as_ref()
                            .unwrap()
                            .files
                            .sizes
                            .clone(),
                        schema: Some(metadata.schema.clone()),
                    }],
                    compaction: None,
                },
            )
            .await?;
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
            )
            .await?;
        }
        if metadata.deletes_metadata.is_some() {
            todo!("Need to implement this now")
        }

        Ok(())
    }

    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        let org_id = MANAGEMENT_ORG_ID.to_string();
        let entity_type = "powdrr_table".to_string();
        let mut table_names = self
            .connector
            .fetch_entities(&org_id, &entity_type, None)
            .await
            .map_err(from_modyne)?
            .entities
            .into_iter()
            .map(|entity| entity.entity_id)
            .collect::<Vec<_>>();
        table_names.sort();
        Ok(table_names)
    }

    pub async fn create_table(
        &mut self,
        org_info: &OrgInfo,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        self.connector
            .create_table_helper(
                &org_info.org_id.to_string(),
                &create_table.name,
                &TableBody {
                    tags: create_table.tags.clone(),
                    serving: create_table.serving.clone(),
                    dynamodb: create_table.dynamodb.clone(),
                    mongodb: create_table.mongodb.clone(),
                    redis: create_table.redis.clone(),
                },
            )
            .await
            .map_err(from_modyne)
    }

    pub async fn upsert_table_metadata(
        &mut self,
        org_info: &OrgInfo,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        self.connector
            .upsert_table_helper(
                &org_info.org_id.to_string(),
                &create_table.name,
                &TableBody {
                    tags: create_table.tags.clone(),
                    serving: create_table.serving.clone(),
                    dynamodb: create_table.dynamodb.clone(),
                    mongodb: create_table.mongodb.clone(),
                    redis: create_table.redis.clone(),
                },
            )
            .await
            .map_err(from_modyne)
    }
    pub async fn describe_table(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError> {
        let result = self
            .connector
            .describe_powdrr_table(&org_info.org_id.to_string(), name)
            .await
            .map(|x| {
                x.map(|x| {
                    table_description_from_parts(
                        name.clone(),
                        x.tags.clone(),
                        x.serving.clone(),
                        x.dynamodb.clone(),
                        x.mongodb.clone(),
                        x.redis.clone(),
                    )
                })
            })
            .map_err(from_modyne)?;

        match result {
            Some(x) => Ok(Some(x)),
            None => {
                match self
                    .connector
                    .describe_alias(&org_info.org_id.to_string(), name)
                    .await
                    .map_err(from_modyne)?
                {
                    None => Ok(None),
                    Some(table_name) => self
                        .connector
                        .describe_powdrr_table(&org_info.org_id.to_string(), &table_name)
                        .await
                        .map(|x| {
                            x.map(|x| {
                                table_description_from_parts(
                                    table_name.clone(),
                                    x.tags.clone(),
                                    x.serving.clone(),
                                    x.dynamodb.clone(),
                                    x.mongodb.clone(),
                                    x.redis.clone(),
                                )
                            })
                        })
                        .map_err(from_modyne),
                }
            }
        }
    }

    pub async fn add_alias(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        self.connector
            .create_alias(&org_info.org_id.to_string(), alias, table_name)
            .await
            .map_err(from_modyne)
    }

    pub async fn remove_alias(
        &mut self,
        org_info: &OrgInfo,
        _table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        self.connector
            .delete_alias(&org_info.org_id.to_string(), alias)
            .await
            .map_err(from_modyne)
    }

    pub async fn create_table_template(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceApiError> {
        self.connector
            .create_table_template(&org_info.org_id.to_string(), name, template)
            .await
            .map_err(from_modyne)
    }

    pub async fn describe_table_template(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        self.connector
            .describe_table_template(&org_info.org_id.clone(), name)
            .await
            .map_err(from_modyne)
    }

    pub async fn create_pipeline(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceApiError> {
        self.connector
            .create_pipeline(&org_info.org_id.clone(), name, pipeline)
            .await
            .map_err(from_modyne)
    }

    pub async fn describe_pipeline(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        self.connector
            .describe_pipeline(&org_info.org_id.clone(), name)
            .await
            .map_err(from_modyne)
    }

    pub async fn create_lifetime_policy(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
        policy: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceApiError> {
        self.connector
            .create_lifetime_policy(&org_info.org_id.clone(), name, policy)
            .await
            .map_err(from_modyne)
    }

    pub async fn describe_lifetime_policy(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        self.connector
            .describe_lifetime_policy(&org_info.org_id.clone(), name)
            .await
            .map_err(from_modyne)
    }

    pub async fn speedboat_commit(
        &mut self,
        org_info: &OrgInfo,
        commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceApiError> {
        let tables: HashSet<String> = commit
            .type_files
            .iter()
            .map(|x| x.table_name.clone())
            .collect();
        assert_eq!(
            tables.len(),
            1,
            "Only really support single table commits right now"
        );
        let retval = self
            .connector
            .commit_speedboat(
                &org_info.org_id.clone(),
                &commit.type_files[0].table_name,
                commit,
            )
            .await
            .map_err(from_modyne)?;
        if retval {
            MetadataStore::queue_checkpoint_publication(
                self,
                &CheckpointUpdateRequest::new(
                    org_info.org_id.clone(),
                    commit.type_files[0].table_name.clone(),
                ),
            )
            .await?;
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
        let compactions = self
            .gather_compactions(org_id, speedboat_commits, iceberg_commits)
            .await
            .unwrap();
        metadata.clone_and_apply(
            speedboat_commits,
            iceberg_commits,
            extension_commits,
            &compactions,
        )
    }

    async fn gather_compactions(
        &mut self,
        org_id: &String,
        speedboat_commits: &Vec<SpeedboatCommit>,
        iceberg_commits: &Vec<IcebergCommit>,
    ) -> Result<HashMap<String, CompactionCommit>, ServiceApiError> {
        let mut compactions = vec![];
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
                self.connector
                    .describe_compaction(&mut self.compactions_cache, org_id, &compaction)
                    .await
                    .map_err(from_modyne)?
                    .unwrap(),
            );
        }

        Ok(compaction_commits)
    }

    pub async fn iceberg_commit(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        iceberg_commit: &IcebergCommit,
    ) -> Result<bool, ServiceApiError> {
        let retval = self
            .connector
            .commit_iceberg(&org_info.org_id.clone(), table_name, iceberg_commit)
            .await
            .map_err(from_modyne)?;
        if retval {
            MetadataStore::queue_checkpoint_publication(
                self,
                &CheckpointUpdateRequest::new(org_info.org_id.clone(), table_name.clone()),
            )
            .await?;
        }
        Ok(retval)
    }

    pub async fn extension_commit(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError> {
        let retval = self
            .connector
            .commit_extension_work_item_completed(&org_info.org_id, table_name, &commit)
            .await
            .map_err(from_modyne)?;
        if retval {
            MetadataStore::queue_checkpoint_publication(
                self,
                &CheckpointUpdateRequest::new(org_info.org_id.clone(), table_name.clone()),
            )
            .await?;
        }
        Ok(retval)
    }

    pub async fn compaction_commit(
        &mut self,
        org_info: &OrgInfo,
        _table_name: &String,
        commit: &CompactionCommit,
    ) -> Result<bool, ServiceApiError> {
        // NOTE: this just notes what the compactor is saying. We don't generate the new checkpoint
        // until we see an iceberg or speedboat commit with the new info.
        assert!(commit.compaction_id.len() > 0);
        self.connector
            .create_compaction(
                &mut self.compactions_cache,
                &org_info.org_id,
                &commit.compaction_id,
                commit,
            )
            .await
            .map_err(from_modyne)
    }

    pub async fn cleanup_commit(
        &mut self,
        org_info: &OrgInfo,
        commit: &CleanupCommit,
    ) -> Result<bool, ServiceApiError> {
        let mut transaction = TransactWrite::new();
        transaction = DynamoDbConnector::mark_done_cleanup_work_item_lease_inner(
            transaction,
            &org_info.org_id,
            &commit.table_name,
            &commit.id,
            None,
        );
        self.connector
            .commit_conditional_transaction(transaction)
            .await
            .map_err(from_modyne)
    }

    pub async fn get_latest_committed_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        let checkpoint_id = self
            .get_checkpoint_id_from_latest_key(
                &org_info.org_id,
                &Self::latest_checkpoint_key(table_name, &extensions),
            )
            .await?;
        if let Some(checkpoint_id) = checkpoint_id.as_ref() {
            tracing::info!(
                "Latest committed checkpoint for {}: {}",
                table_name,
                checkpoint_id
            );
        }
        Ok(checkpoint_id)
    }

    pub async fn get_published_active_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        self.get_active_checkpoint_id(&org_info.org_id, table_name, &extensions)
            .await
    }

    pub async fn get_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        tracing::info!("Getting checkpoint for {}", checkpoint.full_name());
        match self
            .connector
            .describe_checkpoint(
                &mut self.checkpoints_cache,
                &org_info.org_id,
                &checkpoint.full_name(),
            )
            .await
            .map_err(from_modyne)
        {
            Ok(val) => {
                tracing::info!(
                    "Got checkpoint for {}: {}",
                    checkpoint.full_name(),
                    val.is_some()
                );
                Ok(val)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_extension_work_items(
        &mut self,
        org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        let all_tables = self
            .connector
            .fetch_entities(&org_info.org_id, &"powdrr_table".to_string(), None)
            .await
            .map_err(from_modyne)?;
        let mut work_items = vec![];
        let mut used_latest = vec![];
        for table_entity in all_tables.entities {
            let latest_es_key =
                &Self::latest_extension_work_item_key(&table_entity.entity_id, extension_type);
            let latest_entity_info = self
                .connector
                .describe_latest(&org_info.org_id, latest_es_key)
                .await
                .map_err(from_modyne)?;
            match latest_entity_info {
                Some(latest_entity_info) => {
                    if latest_entity_info.entity_id != Self::NO_WORK_ITEM.to_owned() {
                        let lease = self
                            .connector
                            .valid_leases_extension_work_item_lease(
                                &org_info.org_id,
                                &latest_entity_info.entity_id,
                                None,
                                Some(LEASE_LENGTH_MS),
                            )
                            .await
                            .map_err(from_modyne)?;
                        if lease.len() == 0 {
                            let work_item = self
                                .connector
                                .describe_extension_work_item(
                                    &mut self.extension_work_item_cache,
                                    &org_info.org_id,
                                    &latest_entity_info.entity_id,
                                )
                                .await
                                .map_err(from_modyne)?;
                            assert!(work_item.is_some());
                            work_items.push(work_item.unwrap());
                            used_latest.push(latest_entity_info);
                        }
                    }
                }
                None => {
                    tracing::error!(
                        "Unable to find latest extension work item for table {}",
                        table_entity.entity_id
                    );
                }
            }
        }
        match self
            .connector
            .commit_extension_work_item_taken(&used_latest, &Self::NO_WORK_ITEM.to_string())
            .await
            .map_err(from_modyne)?
        {
            true => {
                tracing::info!("Extensions: returning {} items", work_items.len());
                Ok(work_items)
            }
            false => {
                tracing::info!("Extensions: returning 0 items");
                Ok(vec![])
            }
        }
    }

    pub async fn get_compaction_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        let all_tables = self
            .connector
            .fetch_entities(&org_info.org_id, &"powdrr_table".to_string(), None)
            .await
            .map_err(from_modyne)?;
        let mut work_items = vec![];
        let mut used_latest = vec![];
        for table_entity in all_tables.entities {
            let latest_entity_info = self
                .connector
                .describe_latest(
                    &org_info.org_id,
                    &Self::latest_compaction_work_item_key(&table_entity.entity_id),
                )
                .await
                .map_err(from_modyne)?;
            match latest_entity_info {
                Some(latest_entity_info) => {
                    if latest_entity_info.entity_id != Self::NO_WORK_ITEM.to_owned() {
                        let leases = self
                            .connector
                            .valid_leases_compaction_work_item_lease(
                                &org_info.org_id,
                                &latest_entity_info.entity_id,
                                None,
                                Some(LEASE_LENGTH_MS),
                            )
                            .await
                            .map_err(from_modyne)?;
                        if leases.len() == 0 {
                            let work_item = self
                                .connector
                                .describe_compaction_work_item(
                                    &mut self.compaction_work_item_cache,
                                    &org_info.org_id,
                                    &latest_entity_info.entity_id,
                                )
                                .await
                                .map_err(from_modyne)?;
                            assert!(work_item.is_some());
                            let compaction = work_item.unwrap();
                            work_items.push((table_entity.entity_id.clone(), compaction));
                            used_latest.push(latest_entity_info);
                        }
                    }
                }
                None => {
                    tracing::error!(
                        "Unable to find latest extension work item for table {}",
                        table_entity.entity_id
                    );
                }
            }
        }
        match self
            .connector
            .commit_compaction_work_item_taken(&used_latest, &Self::NO_WORK_ITEM.to_string())
            .await
            .map_err(from_modyne)?
        {
            true => {
                tracing::info!("Compaction: returning {} items", work_items.len());
                Ok(work_items)
            }
            false => {
                tracing::info!("Compaction: returning 0 items");
                Ok(vec![])
            }
        }
    }

    pub async fn get_cleanup_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        let all_tables = self
            .connector
            .fetch_entities(&org_info.org_id, &"powdrr_table".to_string(), None)
            .await
            .map_err(from_modyne)?;
        let mut work_items = vec![];
        let mut transaction = TransactWrite::new();
        for table_entity in all_tables.entities {
            let available_infos = self
                .connector
                .oldest_available_cleanup_work_item_lease(
                    &org_info.org_id,
                    &table_entity.entity_id,
                    None,
                    Some(LEASE_LENGTH_MS),
                )
                .await
                .map_err(from_modyne)?;
            for available_info in available_infos.iter() {
                let work_item = self
                    .connector
                    .describe_cleanup_work_item(
                        &mut PowdrrNamedCleanupWorkItemCache::new(),
                        &org_info.org_id,
                        &available_info.name,
                    )
                    .await
                    .map_err(from_modyne)?;
                assert!(work_item.is_some());
                work_items.push(work_item.unwrap());
                transaction = self
                    .connector
                    .claim_cleanup_work_item_lease(transaction, available_info);
            }
        }

        if work_items.len() > 0 {
            match self
                .connector
                .commit_conditional_transaction(transaction)
                .await
                .map_err(from_modyne)?
            {
                true => {
                    tracing::info!("Cleanup: returning {} items", work_items.len());
                    Ok(work_items)
                }
                false => {
                    tracing::info!("Cleanup: returning 0 items");
                    Ok(vec![])
                }
            }
        } else {
            tracing::info!("Cleanup: returning 0 items");
            Ok(vec![])
        }
    }

    pub async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        let mut work_done = false;
        for request in self.checkpoint_publication_requests().await?.iter() {
            let (org_id, table_name) =
                match Self::parse_checkpoint_publication_request_key(&request.key) {
                    Some(parsed) => parsed,
                    None => continue,
                };

            work_done = work_done
                | self
                    .update_standard_checkpoint(&org_id, &table_name)
                    .await?;
            if let Some(committed_checkpoint_id) = self
                .get_checkpoint_id_from_latest_key(
                    &org_id,
                    &Self::latest_checkpoint_key(&table_name, &None),
                )
                .await?
            {
                work_done = work_done
                    | self
                        .set_active_checkpoint_id(
                            &org_id,
                            &table_name,
                            &None,
                            &committed_checkpoint_id,
                        )
                        .await?;
            }
            work_done = work_done
                | self
                    .update_extension_checkpoint(&org_id, &table_name)
                    .await?;
            if let Some(committed_extension_checkpoint_id) = self
                .get_checkpoint_id_from_latest_key(
                    &org_id,
                    &Self::latest_checkpoint_key(&table_name, &Some("es".to_string())),
                )
                .await?
            {
                work_done = work_done
                    | self
                        .set_active_checkpoint_id(
                            &org_id,
                            &table_name,
                            &Some("es".to_string()),
                            &committed_extension_checkpoint_id,
                        )
                        .await?;
            }

            if !self
                .checkpoint_publication_still_pending(&org_id, &table_name)
                .await?
            {
                let transaction = DynamoDbConnector::bump_version(
                    TransactWrite::new(),
                    request,
                    &Self::NO_WORK_ITEM.to_string(),
                );
                let _ = self
                    .connector
                    .commit_conditional_transaction(transaction)
                    .await
                    .map_err(from_modyne)?;
            }
        }
        Ok(work_done)
    }

    async fn update_standard_checkpoint(
        &mut self,
        org_id: &String,
        table_name: &String,
    ) -> Result<bool, ServiceApiError> {
        // TODO: need a bulk fetcher

        let latest_speedboat_trackers = self
            .connector
            .oldest_available_speedboat_commit_checkpointed(org_id, table_name, None, None)
            .await
            .map_err(from_modyne)?;
        if latest_speedboat_trackers.len() == 0 {
            return Ok(false);
        }

        let latest_checkpoint_info = self
            .connector
            .describe_latest(org_id, &Self::latest_checkpoint_key(table_name, &None))
            .await
            .map_err(from_modyne)?
            .unwrap();
        let latest_checkpoint = self
            .connector
            .describe_checkpoint(
                &mut self.checkpoints_cache,
                org_id,
                &latest_checkpoint_info.entity_id,
            )
            .await
            .map_err(from_modyne)?
            .unwrap();

        let mut latest_speedboats = vec![];
        for speedboat_tracker in latest_speedboat_trackers.iter() {
            latest_speedboats.push(
                self.connector
                    .describe_speedboat_commit(
                        &mut self.speedboat_cache,
                        org_id,
                        &speedboat_tracker.name,
                    )
                    .await
                    .map_err(from_modyne)?
                    .unwrap(),
            );
        }

        let (new_checkpoint, changed) = self
            .clone_and_apply(
                org_id,
                &latest_checkpoint,
                &latest_speedboats,
                &vec![],
                &vec![],
            )
            .await;

        assert!(changed);

        let mut compaction_latest = None;
        let mut compaction_work_item = None;
        if new_checkpoint.speedboat_metadata.is_some() {
            let speedboat_files = &new_checkpoint.speedboat_metadata.as_ref().unwrap().files;
            let num_files_threshold = self.mode.compaction_mode.threshold();
            let compact = speedboat_files.file_paths.len() as u64 >= num_files_threshold
                || speedboat_files.sizes.iter().sum::<u64>() > 30 * 1024 * 1024;
            tracing::info!(
                "Compaction threshold: {} files, {} bytes, compact: {}",
                num_files_threshold,
                speedboat_files.sizes.iter().sum::<u64>(),
                compact
            );
            if compact {
                let latest_compaction = self
                    .connector
                    .describe_latest(org_id, &Self::latest_compaction_work_item_key(table_name))
                    .await
                    .map_err(from_modyne)?
                    .unwrap();
                if latest_compaction.entity_id == Self::NO_WORK_ITEM.to_owned() {
                    compaction_latest = Some(latest_compaction);
                    compaction_work_item = Some(CompactionWorkItem::from_checkpoint(
                        &new_checkpoint,
                        &vec![],
                    ));
                }
            }
        }

        match self
            .connector
            .commit_checkpoint(
                &latest_checkpoint_info,
                &latest_speedboat_trackers,
                &new_checkpoint,
                &compaction_latest,
                &compaction_work_item,
            )
            .await
            .map_err(from_modyne)
        {
            Ok(val) => {
                if !val {
                    tracing::info!("Contention detected, not committing checkpoint");
                }
            }
            Err(e) => {
                tracing::error!("Error committing checkpoint: {}", e);
            }
        }

        Ok(true)
    }

    async fn update_extension_checkpoint(
        &mut self,
        org_id: &String,
        table_name: &String,
    ) -> Result<bool, ServiceApiError> {
        let latest_extension_trackers = self
            .connector
            .oldest_available_extension_commit_checkpointed(org_id, table_name, None, None)
            .await
            .map_err(from_modyne)?;
        if latest_extension_trackers.len() == 0 {
            return Ok(false);
        }

        let mut extension_commits = vec![];
        for tracker in latest_extension_trackers.iter() {
            extension_commits.push(
                self.connector
                    .describe_extension_commit(&mut self.extension_cache, org_id, &tracker.name)
                    .await
                    .map_err(from_modyne)?
                    .unwrap(),
            );
        }

        let waiting_checkpoints = self
            .connector
            .oldest_available_checkpoint_waiting_for_extension(org_id, table_name, None, None)
            .await
            .map_err(from_modyne)?;
        let mut work_done = false;
        for checkpoint_tracker in waiting_checkpoints.iter() {
            // TODO: all commits per loop should be in a transaction
            work_done = true;

            let old_checkpoint = self
                .connector
                .describe_checkpoint(
                    &mut self.checkpoints_cache,
                    org_id,
                    &checkpoint_tracker.name,
                )
                .await
                .map_err(from_modyne)?
                .unwrap();
            let (mut new_checkpoint, changed) = self
                .clone_and_apply(
                    org_id,
                    &old_checkpoint,
                    &vec![],
                    &vec![],
                    &extension_commits,
                )
                .await;
            new_checkpoint.original_checkpoint_id =
                Some(match old_checkpoint.original_checkpoint_id {
                    Some(original_checkpoint_id) => original_checkpoint_id.clone(),
                    None => old_checkpoint.checkpoint_id.clone(),
                });

            if new_checkpoint.fully_covered_for_extension(&"es".to_string()) {
                let latest_checkpoint = self
                    .connector
                    .describe_latest(
                        org_id,
                        &Self::latest_checkpoint_key(table_name, &Some("es".to_string())),
                    )
                    .await
                    .map_err(from_modyne)?
                    .unwrap();
                let commit =
                    latest_checkpoint.entity_id < new_checkpoint.get_descriptor().full_name();
                //let commit = true;
                if commit {
                    let retval = self
                        .connector
                        .commit_checkpoint(
                            &latest_checkpoint,
                            &vec![],
                            &new_checkpoint,
                            &None,
                            &None,
                        )
                        .await
                        .map_err(from_modyne)?;
                    assert!(retval);
                }
            }

            if changed {
                self.connector
                    .mark_done_checkpoint_waiting_for_extension(checkpoint_tracker)
                    .await
                    .map_err(from_modyne)?;
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
        transaction = self.connector.private_create_org_settings_core(
            transaction,
            &MANAGEMENT_ORG_ID.to_string(),
            &settings.org_id,
            settings,
        );
        for creds in settings.creds.iter() {
            transaction = self.connector.cached_create_org_creds_core(
                transaction,
                &mut self.org_cache,
                &MANAGEMENT_ORG_ID.to_string(),
                &Self::org_info_key(&creds.access_key_id, &creds.secret_access_key),
                &settings.to_org_info(),
            );
        }
        let result = self
            .connector
            .commit_conditional_transaction(transaction)
            .await
            .map_err(from_modyne)?;
        assert!(result);
        Ok(())
    }

    pub async fn lookup_org(
        &mut self,
        access_key_id: &String,
        secret_access_key: &String,
    ) -> Result<Option<OrgInfo>, ServiceApiError> {
        self.connector
            .describe_org_creds(
                &mut self.org_cache,
                &MANAGEMENT_ORG_ID.to_string(),
                &Self::org_info_key(access_key_id, secret_access_key),
            )
            .await
            .map_err(from_modyne)
    }

    pub async fn lookup_secret_access_key(
        &mut self,
        access_key_id: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        let entities = self
            .connector
            .fetch_entities(
                &MANAGEMENT_ORG_ID.to_string(),
                &"org_settings".to_string(),
                None,
            )
            .await
            .map_err(from_modyne)?;
        let mut matched_secret = None;
        for entity in entities.entities {
            let Some(settings) = self
                .connector
                .describe_org_settings(&MANAGEMENT_ORG_ID.to_string(), &entity.entity_id)
                .await
                .map_err(from_modyne)?
            else {
                continue;
            };
            for creds in settings.creds.iter() {
                if &creds.access_key_id != access_key_id {
                    continue;
                }
                if matched_secret.is_some() {
                    return Err(ServiceApiError::new(format!(
                        "Multiple org credentials share access key {}",
                        access_key_id
                    )));
                }
                matched_secret = Some(creds.secret_access_key.clone());
            }
        }
        Ok(matched_secret)
    }
}

#[async_trait::async_trait]
impl MetadataStore for DynamoDBServiceImpl {
    async fn queue_checkpoint_publication(
        &mut self,
        request: &CheckpointUpdateRequest,
    ) -> Result<(), ServiceApiError> {
        let management_org_id = MANAGEMENT_ORG_ID.to_string();
        let key = Self::checkpoint_publication_request_key(&request.org_id, &request.table_name);

        match self
            .connector
            .describe_latest(&management_org_id, &key)
            .await
            .map_err(from_modyne)?
        {
            Some(existing) => {
                let transaction =
                    DynamoDbConnector::bump_version(TransactWrite::new(), &existing, &key);
                let _ = self
                    .connector
                    .commit_conditional_transaction(transaction)
                    .await
                    .map_err(from_modyne)?;
            }
            None => {
                let _ = self
                    .connector
                    .create_latest(
                        &management_org_id,
                        &key,
                        &EntityVersionInfo::new(&management_org_id, &key, &key),
                    )
                    .await
                    .map_err(from_modyne)?;
            }
        }
        Ok(())
    }

    async fn get_latest_committed_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        DynamoDBServiceImpl::get_latest_committed_checkpoint(self, org_info, table_name, extension)
            .await
    }

    async fn get_published_checkpoint_record(
        &mut self,
        org_info: &OrgInfo,
        selector: &PublishedCheckpointSelector,
    ) -> Result<Option<PublishedCheckpointRecord>, ServiceApiError> {
        let checkpoint_id = match selector.role {
            PublishedCheckpointRole::Active => {
                self.get_active_checkpoint_id(
                    &org_info.org_id,
                    &selector.table_name,
                    &selector.extension,
                )
                .await?
            }
            PublishedCheckpointRole::Target => {
                DynamoDBServiceImpl::get_latest_committed_checkpoint(
                    self,
                    org_info,
                    &selector.table_name,
                    selector.extension.clone(),
                )
                .await?
            }
        };

        Ok(
            checkpoint_id.map(|checkpoint_id| PublishedCheckpointRecord {
                selector: selector.clone(),
                checkpoint_id,
            }),
        )
    }

    async fn get_checkpoint_metadata(
        &mut self,
        org_info: &OrgInfo,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        DynamoDBServiceImpl::get_checkpoint(self, org_info, checkpoint).await
    }

    async fn claim_extension_work_items(
        &mut self,
        org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ClaimedExtensionWorkItem>, ServiceApiError> {
        Ok(
            DynamoDBServiceImpl::get_extension_work_items(self, org_info, extension_type)
                .await?
                .into_iter()
                .map(|work_item| ClaimedExtensionWorkItem {
                    claim: MetadataClaimKind::Leased,
                    work_item,
                })
                .collect(),
        )
    }

    async fn claim_compaction_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<ClaimedCompactionWorkItem>, ServiceApiError> {
        Ok(
            DynamoDBServiceImpl::get_compaction_work_items(self, org_info)
                .await?
                .into_iter()
                .map(|(table_name, work_item)| ClaimedCompactionWorkItem {
                    claim: MetadataClaimKind::Leased,
                    table_name,
                    work_item,
                })
                .collect(),
        )
    }

    async fn claim_cleanup_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<ClaimedCleanupWorkItem>, ServiceApiError> {
        Ok(DynamoDBServiceImpl::get_cleanup_work_items(self, org_info)
            .await?
            .into_iter()
            .map(|work_item| ClaimedCleanupWorkItem {
                claim: MetadataClaimKind::Leased,
                work_item,
            })
            .collect())
    }

    async fn advance_published_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        DynamoDBServiceImpl::update_all_checkpoints(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_contract::{ExtensionFile, FileSetPayload, IcebergMetadata, LicenseType};
    use crate::metadata_store::{MetadataStore, PublishedCheckpointSelector};
    use crate::schema_massager::PowdrrSchema;
    use std::collections::HashMap;

    fn fake_org_info() -> OrgInfo {
        OrgInfo {
            org_id: "fake_org_id".to_string(),
            license_type: LicenseType::Free,
        }
    }

    fn iceberg_metadata(file_path: &String, snapshot_id: &str) -> IcebergMetadata {
        let schema = PowdrrSchema::minimal();
        IcebergMetadata {
            table_schema: schema.clone(),
            snapshot_id: Some(snapshot_id.to_string()),
            files: FileSetPayload::single(file_path.clone(), 128, schema),
            partition_spec: vec![],
            sort_order: vec![],
            column_names: vec![],
            column_stats: vec![],
            access_artifacts: vec![],
            file_stats: vec![],
        }
    }

    #[tokio::test]
    async fn metadata_store_committed_and_published_frontiers_diverge_until_advanced() {
        let mut service_impl = DynamoDBServiceImpl::test(TestProcessingMode::default()).await;
        let org_info = fake_org_info();
        let table_name = "dynamodb_frontier_table".to_string();
        let file_path = "s3://warehouse/table/data-0001.parquet".to_string();

        service_impl
            .create_table(
                &org_info,
                &create_table_request(table_name.clone(), HashMap::new(), None, None, None, None),
            )
            .await
            .unwrap();

        service_impl
            .iceberg_commit(
                &org_info,
                &table_name,
                &IcebergCommit {
                    metadata: iceberg_metadata(&file_path, "1"),
                    deletes_table_info: None,
                    compactions: vec![],
                },
            )
            .await
            .unwrap();

        let committed_checkpoint = MetadataStore::get_latest_committed_checkpoint(
            &mut service_impl,
            &org_info,
            &table_name,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            MetadataStore::get_published_checkpoint_record(
                &mut service_impl,
                &org_info,
                &PublishedCheckpointSelector::active(table_name.clone(), None),
            )
            .await
            .unwrap(),
            None
        );
        let target_record = MetadataStore::get_published_checkpoint_record(
            &mut service_impl,
            &org_info,
            &PublishedCheckpointSelector::target(table_name.clone(), None),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(target_record.checkpoint_id, committed_checkpoint);

        assert!(
            MetadataStore::advance_published_checkpoints(&mut service_impl)
                .await
                .unwrap()
        );

        let published_record = MetadataStore::get_published_checkpoint_record(
            &mut service_impl,
            &org_info,
            &PublishedCheckpointSelector::active(table_name.clone(), None),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(published_record.checkpoint_id, committed_checkpoint);
    }

    #[tokio::test]
    async fn iceberg_extension_checkpoints_publish_after_extension_commit() {
        let mut service_impl = DynamoDBServiceImpl::test(TestProcessingMode::default()).await;
        let org_info = fake_org_info();
        let table_name = "iceberg_snapshot_table".to_string();
        let file_path = "s3://warehouse/table/data-0001.parquet".to_string();

        service_impl
            .create_table(
                &org_info,
                &create_table_request(table_name.clone(), HashMap::new(), None, None, None, None),
            )
            .await
            .unwrap();

        let initial_extension_checkpoint = service_impl
            .get_latest_committed_checkpoint(&org_info, &table_name, Some("es".to_string()))
            .await
            .unwrap()
            .unwrap();

        service_impl
            .iceberg_commit(
                &org_info,
                &table_name,
                &IcebergCommit {
                    metadata: iceberg_metadata(&file_path, "1"),
                    deletes_table_info: None,
                    compactions: vec![],
                },
            )
            .await
            .unwrap();
        let request_key =
            DynamoDBServiceImpl::checkpoint_publication_request_key(&org_info.org_id, &table_name);
        let mut request = service_impl
            .connector
            .describe_latest(&MANAGEMENT_ORG_ID.to_string(), &request_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.entity_id, request_key);

        assert!(!service_impl.update_all_checkpoints().await.unwrap());
        request = service_impl
            .connector
            .describe_latest(&MANAGEMENT_ORG_ID.to_string(), &request_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.entity_id, request_key);

        let work_items = service_impl
            .get_extension_work_items(&org_info, &"es".to_string())
            .await
            .unwrap();
        assert_eq!(work_items.len(), 1);
        let work_item = work_items.first().unwrap();
        assert_eq!(
            work_item.iceberg_files.file_paths.clone(),
            vec![file_path.clone()]
        );

        let extension_files = vec![ExtensionFile {
            suffix: "search_index".to_string(),
            location: "s3://warehouse/table/data-0001.search_index.parquet".to_string(),
        }];
        service_impl
            .extension_commit(
                &org_info,
                &table_name,
                &ExtensionCommit {
                    id: work_item.id.clone(),
                    extension: "es".to_string(),
                    files: HashMap::from([(file_path.clone(), extension_files.clone())]),
                },
            )
            .await
            .unwrap();
        request = service_impl
            .connector
            .describe_latest(&MANAGEMENT_ORG_ID.to_string(), &request_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.entity_id, request_key);

        assert!(service_impl.update_all_checkpoints().await.unwrap());
        request = service_impl
            .connector
            .describe_latest(&MANAGEMENT_ORG_ID.to_string(), &request_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.entity_id, DynamoDBServiceImpl::NO_WORK_ITEM);

        let latest_extension_checkpoint = service_impl
            .get_latest_committed_checkpoint(&org_info, &table_name, Some("es".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(latest_extension_checkpoint, initial_extension_checkpoint);

        let checkpoint = service_impl
            .get_checkpoint(
                &org_info,
                &CheckpointDescriptor::new(table_name.clone(), latest_extension_checkpoint),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(checkpoint.fully_covered_for_extension(&"es".to_string()));
        assert_eq!(
            checkpoint
                .iceberg_metadata
                .as_ref()
                .unwrap()
                .snapshot_id
                .as_deref(),
            Some("1")
        );
        assert_eq!(
            checkpoint
                .extension_metadata
                .get("es")
                .unwrap()
                .get(&file_path)
                .unwrap(),
            &extension_files
        );
    }
}
