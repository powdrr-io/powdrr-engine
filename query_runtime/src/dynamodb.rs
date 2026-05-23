use crate::data_contract::{
    CleanupWorkItem, CompactionCommit, CompactionWorkItem, CreateIndexTemplateBody,
    ExtensionCommit, ExtensionWorkItem, FileSetPayload, IcebergCommit, SpeedboatCommit,
    TableMetadataCheckpoint,
};
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::schema_massager::PowdrrSchema;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use idgenerator::IdInstance;
use modyne::expr::Filter;
use modyne::{
    Aggregate, Entity, EntityExt, Error, Item, ProjectionExt, QueryInput, QueryInputExt, Table,
    expr, keys, model::TransactWrite, projections, read_projection,
};
use powdrr_control_plane::ilm_policy::ILMPolicyDefinition;
use std::collections::HashMap;

pub struct DynamoDbConnector {
    table_name: std::sync::Arc<str>,
    client: aws_sdk_dynamodb::Client,
}

impl DynamoDbConnector {
    pub fn new(client: aws_sdk_dynamodb::Client) -> Self {
        Self::new_with_table(client, "Powdrr")
    }

    pub fn new_with_table(client: aws_sdk_dynamodb::Client, table_name: &str) -> Self {
        Self {
            table_name: std::sync::Arc::from(table_name),
            client,
        }
    }
}

impl Table for DynamoDbConnector {
    type PrimaryKey = keys::Primary;
    type IndexKeys = keys::Gsi1;

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn client(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }
}

#[derive(Debug, modyne_derive::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct PowdrrEntity {
    pub type_name: String,
    pub org_id: String,
    pub entity_id: String,
    pub tags: HashMap<String, String>,
}

pub struct PowdrrEntityInput<'a> {
    type_name: &'a String,
    org_id: &'a String,
    entity_id: String,
}

impl Entity for PowdrrEntity {
    type KeyInput<'a> = PowdrrEntityInput<'a>;
    type Table = DynamoDbConnector;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("ENTITY#{}_{}", input.org_id, input.type_name),
            range: format!("ID#{}", input.entity_id),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        Self::primary_key(PowdrrEntityInput {
            type_name: &self.type_name,
            org_id: &self.org_id,
            entity_id: self.entity_id.clone(),
        })
        .into()
    }
}

#[derive(Default)]
pub struct PowdrrEntities {
    pub entities: Vec<PowdrrEntity>,
}

struct PowdrrEntityQuery<'a> {
    entity_type: &'a String,
    org_id: &'a String,
}

impl<'a> QueryInput for PowdrrEntityQuery<'a> {
    type Index = keys::Primary;
    type Aggregate = PowdrrEntities;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        expr::KeyCondition::in_partition(format!("ENTITY#{}_{}", self.org_id, self.entity_type))
    }
}

projections! {
    pub enum PowdrrEntitiesProjection {
        PowdrrEntity,
    }
}

impl Aggregate for PowdrrEntities {
    type Projections = PowdrrEntitiesProjection;

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        match read_projection!(item)? {
            Self::Projections::PowdrrEntity(entity) => self.entities.push(entity),
        }

        Ok(())
    }
}

impl DynamoDbConnector {
    pub async fn fetch_entities(
        &self,
        org_id: &String,
        entity_type: &String,
        limit: Option<u32>,
    ) -> Result<PowdrrEntities, Error> {
        let query = PowdrrEntityQuery {
            entity_type,
            org_id,
        };

        let mut entities = PowdrrEntities::default();

        let result = query.query().set_limit(limit).execute(self).await?;

        entities.reduce(result.items.unwrap_or_default())?;
        Ok(entities)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    name: String,
    org_id: String,
}

pub struct OrgIdNameInput<'a> {
    name: &'a String,
    org_id: &'a String,
}

pub struct OrgIdEntityNameInput<'a> {
    entity_name: &'a String,
    org_id: &'a String,
    parent_entity: &'a String,
    name: &'a String,
}

const UNCLAIMED_TIME: i64 = 0; // The beginning of time

#[derive(Debug, modyne_derive::EntityDef, serde::Serialize, serde::Deserialize, Clone)]
pub struct PowdrrTracker {
    pub entity_name: String,
    pub org_id: String,
    pub parent_entity: String,
    pub name: String,
    pub version: i64,
    pub claimed_at: i64,
}

impl Entity for PowdrrTracker {
    type KeyInput<'a> = OrgIdEntityNameInput<'a>;
    type Table = DynamoDbConnector;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!(
            "{}_TRACKER#{}#{}",
            input.entity_name, input.org_id, input.parent_entity
        );
        keys::Primary {
            hash: common.clone(),
            range: input.name.clone(),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        Self::primary_key(OrgIdEntityNameInput {
            entity_name: &self.entity_name,
            org_id: &self.org_id,
            parent_entity: &self.parent_entity,
            name: &self.name,
        })
        .into()
    }
}

pub struct PowdrrTrackerQuery {
    entity_name: String,
    org_id: String,
    parent_entity: String,
}

impl QueryInput for PowdrrTrackerQuery {
    type Index = keys::Primary;
    type Aggregate = PowdrrTrackerQueryResults;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        let key = format!(
            "{}_TRACKER#{}#{}",
            self.entity_name, self.org_id, self.parent_entity
        );
        expr::KeyCondition::in_partition(key)
    }
}

#[derive(Default)]
pub struct PowdrrTrackerQueryResults {
    pub trackers: Vec<PowdrrTracker>,
}

projections! {
    pub enum PowdrrTrackerResultProjection {
        PowdrrTracker,
    }
}

impl Aggregate for PowdrrTrackerQueryResults {
    type Projections = PowdrrTrackerResultProjection;

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        match read_projection!(item)? {
            Self::Projections::PowdrrTracker(tracker) => self.trackers.push(tracker),
        }

        Ok(())
    }
}

macro_rules! powdrr_named_entity_core {
    ($entity_name:tt, $type_name:ident) => {
        paste::item! {
            #[derive(Debug, modyne_derive::EntityDef, serde::Serialize, serde::Deserialize, Clone)]
            pub struct [< PowdrrNamed $type_name >] {
                pub name: String,
                pub org_id: String,
                pub entity: $type_name
            }

            impl Entity for [< PowdrrNamed $type_name >] {
                type KeyInput<'a> = OrgIdNameInput<'a>;
                type Table = DynamoDbConnector;
                type IndexKeys = ();

                fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
                    let common = format!("{}#{}_{}", stringify!($entity_name), input.org_id, input.name);
                    keys::Primary {
                        hash: common.clone(),
                        range: common,
                    }
                }

                fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
                    Self::primary_key(OrgIdNameInput{
                        name: &self.name,
                        org_id: &self.org_id
                    }).into()
                }
            }

            #[allow(dead_code)]
            impl DynamoDbConnector {
                pub fn [< private_create_ $entity_name _core >](&self, transaction: TransactWrite, org_id: &String, name: &String, template: &$type_name) -> TransactWrite {
                    let entity = PowdrrEntity {
                        org_id: org_id.clone(),
                        type_name: stringify!($entity_name).to_string(),
                        entity_id: name.clone(),
                        tags: Default::default(),
                    };
                    let named_entity = [< PowdrrNamed $type_name >] {
                        name: name.clone(),
                        org_id: org_id.clone(),
                        entity: template.clone(),
                    };

                    transaction
                        .operation(entity.create())
                        .operation(named_entity.create())
                }


                async fn [< private_create_ $entity_name >](&self, org_id: &String, name: &String, template: &$type_name) -> Result<bool, Error> {
                    let transaction = self.[< private_create_ $entity_name _core >](TransactWrite::new(), org_id, name, template);
                    self.commit_conditional_transaction(transaction).await
                }

                async fn [< private_describe_ $entity_name >](&self, org_id: &String, name: &String) -> Result<Option<$type_name>, Error> {
                    let result = [< PowdrrNamed $type_name >]::get(OrgIdNameInput{ org_id: org_id, name: name }).execute(self).await?;
                    match result.item {
                        Some(item) => [< PowdrrNamed $type_name >]::from_item(item).map(|x|Some(x.entity)),
                        None => Ok(None),
                    }
                }

                fn [< private_delete_ $entity_name _core >](&self, transaction: TransactWrite, org_id: &String, name: &String) -> TransactWrite {
                    transaction.operation([< PowdrrNamed $type_name >]::delete(OrgIdNameInput{ org_id: org_id, name: name }))
                }

                async fn [< private_delete_ $entity_name >](&self, org_id: &String, name: &String) -> Result<bool, Error> {
                    let transaction = self.[< private_delete_ $entity_name _core >](TransactWrite::new(), org_id, name);
                    self.commit_conditional_transaction(transaction).await
                }
            }
        }
    };
}

macro_rules! powdrr_named_entity {
    ($entity_name:tt, $type_name:ident) => {
        powdrr_named_entity_core!($entity_name, $type_name);
        paste::item! {
            #[allow(dead_code)]
            impl DynamoDbConnector {
                pub async fn [< create_ $entity_name >](&self, org_id: &String, name: &String, template: &$type_name) -> Result<bool, Error> {
                    self.[< private_create_ $entity_name >](org_id, name, template).await
                }

                pub async fn [< describe_ $entity_name >](&self, org_id: &String, name: &String) -> Result<Option<$type_name>, Error> {
                    self.[< private_describe_ $entity_name >](org_id, name).await
                }

                pub async fn [< delete_ $entity_name >](&self, org_id: &String, name: &String) -> Result<bool, Error> {
                    self.[< private_delete_ $entity_name >](org_id, name).await
                }
            }
        }
    };
}

macro_rules! powdrr_named_cached_entity {
    ($entity_name:tt, $type_name:ident) => {
        powdrr_named_cached_entity_core!($entity_name, $type_name);
        paste::item! {
            #[allow(dead_code)]
            impl DynamoDbConnector {
                pub async fn [< create_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String, template: &$type_name) -> Result<bool, Error> {
                    self.[< cached_create_ $entity_name >](cache, org_id, name, template).await
                }

                pub async fn [< describe_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String) -> Result<Option<$type_name>, Error> {
                    self.[< cached_describe_ $entity_name >](cache, org_id, name).await
                }

                pub async fn [< delete_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String) -> Result<bool, Error> {
                    self.[< cached_delete_ $entity_name >](cache, org_id, name).await
                }
            }
        }
    }
}

macro_rules! powdrr_named_cached_entity_core {
    ($entity_name:tt, $type_name:ident) => {
        powdrr_named_entity_core!($entity_name, $type_name);
        paste::item! {
            // TODO: this should be an LRU cache
            pub struct [< PowdrrNamed $type_name Cache >] {
                cache: HashMap<CacheKey, $type_name>,
            }

            impl [< PowdrrNamed $type_name Cache >] {
                pub fn new() -> Self {
                    Self { cache: HashMap::new() }
                }
            }

            #[allow(dead_code)]
            impl DynamoDbConnector {
                pub fn [< cached_create_ $entity_name _core >](&self, transaction: TransactWrite, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String, template: &$type_name) -> TransactWrite {
                    let key = CacheKey{ org_id: org_id.clone(), name: name.clone() };
                    cache.cache.insert(key.clone(), template.clone());
                    self.[< private_create_ $entity_name _core >](transaction, org_id, name, template)
                }

                async fn [< cached_create_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String, template: &$type_name) -> Result<bool, Error> {
                    let transaction = self.[< cached_create_ $entity_name _core >](TransactWrite::new(), cache, org_id, name, template);
                    self.commit_conditional_transaction(transaction).await
                }

                async fn [< cached_describe_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String) -> Result<Option<$type_name>, Error> {
                    let key = CacheKey{ org_id: org_id.clone(), name: name.clone() };
                    match cache.cache.get(&key) {
                        Some(entity) => Ok(Some(entity.clone())),
                        None => {
                            let result = self.[< private_describe_ $entity_name >](org_id, name).await?;
                            match result {
                                Some(item) => {
                                    cache.cache.insert(key.clone(), item.clone());
                                    Ok(Some(item))
                                },
                                None => Ok(None)
                            }
                        }
                    }
                }

                async fn [< cached_delete_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String) -> Result<bool, Error> {
                    cache.cache.remove(&CacheKey{ org_id: org_id.clone(), name: name.clone() });
                    self.[< private_delete_ $entity_name >](org_id, name).await
                }
            }
        }
    }
}

macro_rules! powdrr_tracker {
    ($entity_name:tt) => {
        paste::item! {
            #[allow(dead_code)]
            impl DynamoDbConnector {
                pub fn [< create_ $entity_name _core >](transaction: TransactWrite, org_id: &String, parent_entity: &String, name: &String, claimed_at: Option<i64>) -> TransactWrite {
                    let tracker = PowdrrTracker {
                        entity_name: stringify!($entity_name).to_string(),
                        org_id: org_id.clone(),
                        parent_entity: parent_entity.clone(),
                        name: name.clone(),
                        version: 0,
                        claimed_at: claimed_at.unwrap_or(UNCLAIMED_TIME),
                    };
                    transaction.operation(tracker.create())
                }

                pub async fn [< create_ $entity_name >](&self, org_id: &String, parent_entity: &String, name: &String) -> Result<(), Error> {
                    Self::[< create_ $entity_name _core >](TransactWrite::new(), org_id, parent_entity, name, None)
                        .execute(self)
                        .await?;

                    Ok(())
                }

                pub async fn [< delete_ $entity_name >](&self, org_id: &String, parent_entity: &String, name: &String) -> Result<(), Error> {
                    PowdrrTracker::delete(OrgIdEntityNameInput{ entity_name: &stringify!($entity_name).to_string(), parent_entity: parent_entity, org_id: org_id, name: name }).execute(self).await?;
                    Ok(())
                }

                pub async fn [< oldest_available_ $entity_name >](&self, org_id: &String, parent_entity: &String, limit: Option<u32>, earliest_claimed_at_delta: Option<i64>) -> Result<Vec<PowdrrTracker>, Error> {
                    let query_input = PowdrrTrackerQuery { entity_name: stringify!($entity_name).to_string(), org_id: org_id.clone(), parent_entity: parent_entity.clone() };

                    let mut trackers = PowdrrTrackerQueryResults::default();

                    let result = match earliest_claimed_at_delta {
                        Some(delta) => {
                            query_input
                                .query()
                                .filter(Filter::new("version <> :negative_one AND claimed_at < :earliest_claimed_at")
                                    .value(":negative_one", -1)
                                    .value(":earliest_claimed_at", chrono::Utc::now().timestamp_millis() - delta)
                                )
                                .set_limit(limit)
                                .execute(self)
                                .await?
                        },
                        None => {
                            query_input
                                .query()
                                .filter(Filter::new("version <> :negative_one")
                                    .value(":negative_one", -1)
                                )
                                .set_limit(limit)
                                .execute(self)
                                .await?
                        }
                    };

                    trackers.reduce(result.items.unwrap_or_default())?;

                    Ok(trackers.trackers)
                }

                pub async fn [< valid_leases_ $entity_name >](&self, org_id: &String, parent_entity: &String, limit: Option<u32>, earliest_claimed_at_delta: Option<i64>) -> Result<Vec<PowdrrTracker>, Error> {
                    let query_input = PowdrrTrackerQuery { entity_name: stringify!($entity_name).to_string(), org_id: org_id.clone(), parent_entity: parent_entity.clone() };

                    let mut trackers = PowdrrTrackerQueryResults::default();

                    let result = match earliest_claimed_at_delta {
                        Some(delta) => {
                            query_input
                                .query()
                                .filter(Filter::new("version <> :negative_one AND claimed_at > :earliest_claimed_at")
                                    .value(":negative_one", -1)
                                    .value(":earliest_claimed_at", chrono::Utc::now().timestamp_millis() - delta)
                                )
                                .set_limit(limit)
                                .execute(self)
                                .await?
                        },
                        None => {
                            query_input
                                .query()
                                .filter(Filter::new("version <> :negative_one")
                                    .value(":negative_one", -1)
                                )
                                .set_limit(limit)
                                .execute(self)
                                .await?
                        }
                    };

                    trackers.reduce(result.items.unwrap_or_default())?;

                    Ok(trackers.trackers)
                }


                pub fn [< claim_ $entity_name >](&self, transaction: TransactWrite, tracker: &PowdrrTracker) -> TransactWrite {
                    let key = OrgIdEntityNameInput {
                        entity_name: &stringify!($entity_name).to_string(),
                        org_id: &tracker.org_id,
                        parent_entity: &tracker.parent_entity,
                        name: &tracker.name,
                    };

                    let expression = expr::Update::new("SET version = :expected_plus_one, claimed_at = :now")
                        .value(":now", chrono::Utc::now().timestamp_millis())
                        .value(":expected_plus_one", tracker.version + 1);
                    let condition = expr::Condition::new("version = :expected")
                        .value(":expected", tracker.version);

                    transaction.operation(PowdrrTracker::update(key).expression(expression).condition(condition))
                }

                pub async fn [< mark_done_ $entity_name >](&self, tracker: &PowdrrTracker) -> Result<(), Error> {
                    Self::[< mark_done_ $entity_name _core >](TransactWrite::new(), &tracker).execute(self).await?;
                    Ok(())
                }

                pub fn [< mark_done_ $entity_name _core >](transaction: TransactWrite, tracker: &PowdrrTracker) -> TransactWrite {
                    Self::[< mark_done_ $entity_name _inner >](transaction, &tracker.org_id, &tracker.parent_entity, &tracker.name, Some(tracker.version))
                }

                pub fn [< mark_done_ $entity_name _inner >](transaction: TransactWrite, org_id: &String, parent_entity: &String, name: &String, expected_version: Option<i64>) -> TransactWrite {
                    let key = OrgIdEntityNameInput {
                        entity_name: &stringify!($entity_name).to_string(),
                        org_id,
                        parent_entity,
                        name,
                    };
                    let expression = expr::Update::new("SET version = :negative_one")
                        .value(":negative_one", -1);

                    match expected_version {
                        Some(expected_version_local) => {
                            let condition = expr::Condition::new("version = :expected")
                                .value(":expected", expected_version_local);
                            transaction.operation(PowdrrTracker::update(key).expression(expression).condition(condition))
                        },
                        None => {
                            transaction.operation(PowdrrTracker::update(key).expression(expression))
                        }
                    }
                }
            }
        }
    };
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableBody {
    pub tags: HashMap<String, String>,
    #[serde(default)]
    pub serving: Option<crate::data_contract::ServingTableConfig>,
    #[serde(default)]
    pub dynamodb: Option<crate::data_contract::DynamoDbTableConfig>,
    #[serde(default)]
    pub mongodb: Option<crate::data_contract::MongoDbTableConfig>,
    #[serde(default)]
    pub redis: Option<crate::data_contract::RedisTableConfig>,
}

impl TableBody {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            tags: HashMap::new(),
            serving: None,
            dynamodb: None,
            mongodb: None,
            redis: None,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EntityVersionInfo {
    pub org_id: String,
    pub key: String,
    pub version: u64,
    pub entity_id: String,
}

impl EntityVersionInfo {
    pub(crate) fn new(org_id: &String, key: &String, entity_id: &String) -> Self {
        Self {
            org_id: org_id.clone(),
            key: key.clone(),
            version: 0,
            entity_id: entity_id.clone(),
        }
    }

    pub fn get_query(&self) -> OrgIdNameInput<'_> {
        OrgIdNameInput {
            name: &self.key,
            org_id: &self.org_id,
        }
    }
}

powdrr_named_entity!(alias, String);
powdrr_named_entity!(powdrr_table, TableBody);
powdrr_named_entity!(table_template, CreateIndexTemplateBody);
powdrr_named_entity!(pipeline, PipelineDefinition);
powdrr_named_entity!(lifetime_policy, ILMPolicyDefinition);
powdrr_named_entity!(latest, EntityVersionInfo);

// Note: only things where a given key can only ever have one value are cacheable
powdrr_named_cached_entity!(compaction, CompactionCommit);
powdrr_named_cached_entity!(checkpoint, TableMetadataCheckpoint);
powdrr_named_cached_entity!(speedboat_commit, SpeedboatCommit);
powdrr_named_cached_entity!(iceberg_commit, IcebergCommit);
powdrr_named_cached_entity!(extension_commit, ExtensionCommit);
powdrr_named_cached_entity!(compaction_work_item, CompactionWorkItem);
powdrr_named_cached_entity!(extension_work_item, ExtensionWorkItem);
powdrr_named_cached_entity!(cleanup_work_item, CleanupWorkItem);

powdrr_tracker!(extension_work_item_lease);
powdrr_tracker!(compaction_work_item_lease);
powdrr_tracker!(cleanup_work_item_lease);
powdrr_tracker!(speedboat_commit_checkpointed);
powdrr_tracker!(extension_commit_checkpointed);
powdrr_tracker!(checkpoint_waiting_for_extension);

impl DynamoDbConnector {
    fn latest_checkpoint_key(table_name: &String, extension: &Option<String>) -> String {
        match extension {
            Some(x) => format!("checkpoint#{}#{}", table_name, x),
            None => format!("checkpoint#{}", table_name),
        }
    }

    fn latest_extension_work_item_key(table_name: &String, extension: &String) -> String {
        format!("extension_work_item#{}#{}", table_name, extension)
    }

    fn latest_compaction_work_item_key(table_name: &String) -> String {
        format!("compaction_work_item#{}", table_name)
    }

    const NO_WORK_ITEM: &'static str = "-1";

    pub(crate) fn bump_version(
        transaction: TransactWrite,
        old: &EntityVersionInfo,
        new_entity_id: &String,
    ) -> TransactWrite {
        let expression =
            expr::Update::new("SET entity.version = :plus_one, entity.entity_id = :new_id")
                .value(":new_id", new_entity_id.clone())
                .value(":plus_one", old.version + 1);
        let condition = expr::Condition::new("entity.version = :old").value(":old", old.version);

        transaction.operation(
            PowdrrNamedEntityVersionInfo::update(old.get_query())
                .expression(expression)
                .condition(condition),
        )
    }

    pub fn create_latest_core(
        &self,
        transaction: TransactWrite,
        entity: &EntityVersionInfo,
    ) -> TransactWrite {
        self.private_create_latest_core(transaction, &entity.org_id, &entity.key, entity)
    }

    pub async fn create_table_helper(
        &mut self,
        org_id: &String,
        table_name: &String,
        table_body: &TableBody,
    ) -> Result<bool, Error> {
        let checkpoint = TableMetadataCheckpoint::new(
            table_name.clone(),
            IdInstance::next_id().to_string(),
            PowdrrSchema::minimal(),
        );

        tracing::info!(
            "Created checkpoint for table {}: {}",
            table_name,
            checkpoint.checkpoint_id
        );

        let mut transaction = TransactWrite::new();
        transaction =
            self.private_create_powdrr_table_core(transaction, org_id, table_name, table_body);
        transaction = self.private_create_checkpoint_core(
            transaction,
            org_id,
            &checkpoint.get_descriptor().full_name(),
            &checkpoint,
        );
        transaction = self.create_latest_core(
            transaction,
            &EntityVersionInfo::new(
                org_id,
                &Self::latest_checkpoint_key(table_name, &None),
                &checkpoint.get_descriptor().full_name(),
            ),
        );
        transaction = self.create_latest_core(
            transaction,
            &EntityVersionInfo::new(
                org_id,
                &Self::latest_checkpoint_key(table_name, &Some("es".to_string())),
                &checkpoint.get_descriptor().full_name(),
            ),
        );
        transaction = self.create_latest_core(
            transaction,
            &EntityVersionInfo::new(
                org_id,
                &Self::latest_extension_work_item_key(table_name, &"es".to_string()),
                &Self::NO_WORK_ITEM.to_owned(),
            ),
        );
        transaction = self.create_latest_core(
            transaction,
            &EntityVersionInfo::new(
                org_id,
                &Self::latest_compaction_work_item_key(table_name),
                &Self::NO_WORK_ITEM.to_owned(),
            ),
        );
        self.commit_conditional_transaction(transaction).await
    }

    pub async fn upsert_table_helper(
        &mut self,
        org_id: &String,
        table_name: &String,
        table_body: &TableBody,
    ) -> Result<bool, Error> {
        if self
            .describe_powdrr_table(org_id, table_name)
            .await?
            .is_none()
        {
            return self
                .create_table_helper(org_id, table_name, table_body)
                .await;
        }

        let expression = expr::Update::new("SET entity = :entity").value(":entity", table_body);
        let transaction = TransactWrite::new().operation(
            PowdrrNamedTableBody::update(OrgIdNameInput {
                org_id,
                name: table_name,
            })
            .expression(expression),
        );
        self.commit_conditional_transaction(transaction).await
    }

    pub async fn commit_checkpoint(
        &self,
        input_latest: &EntityVersionInfo,
        input_speedboat_trackers: &Vec<PowdrrTracker>,
        new_checkpoint: &TableMetadataCheckpoint,
        compaction_latest: &Option<EntityVersionInfo>,
        compaction_work_item: &Option<CompactionWorkItem>,
    ) -> Result<bool, Error> {
        let mut transaction = TransactWrite::new();

        transaction = Self::bump_version(
            transaction,
            input_latest,
            &new_checkpoint.get_descriptor().full_name(),
        );

        for input_tracker in input_speedboat_trackers.iter() {
            transaction = DynamoDbConnector::mark_done_speedboat_commit_checkpointed_core(
                transaction,
                input_tracker,
            );
        }
        let checkpoint_obj = PowdrrNamedTableMetadataCheckpoint {
            name: new_checkpoint.get_descriptor().full_name(),
            org_id: input_latest.org_id.clone(),
            entity: new_checkpoint.clone(),
        };
        transaction = transaction.operation(checkpoint_obj.create());

        if !new_checkpoint.fully_covered_for_extension(&"es".to_string()) {
            transaction = Self::create_checkpoint_waiting_for_extension_core(
                transaction,
                &input_latest.org_id,
                &new_checkpoint.table_name,
                &new_checkpoint.get_descriptor().full_name(),
                None,
            )
        }

        if compaction_work_item.is_some() {
            assert!(compaction_latest.is_some());
            let compaction_work_item_id = compaction_work_item.as_ref().unwrap().id.clone();
            transaction = self.private_create_compaction_work_item_core(
                transaction,
                &input_latest.org_id,
                &compaction_work_item_id,
                compaction_work_item.as_ref().unwrap(),
            );
            transaction = Self::bump_version(
                transaction,
                compaction_latest.as_ref().unwrap(),
                &compaction_work_item_id,
            );
        }

        self.commit_conditional_transaction(transaction).await
    }

    pub async fn commit_conditional_transaction(
        &self,
        transaction: TransactWrite,
    ) -> Result<bool, Error> {
        match transaction.execute(self).await {
            Ok(_) => Ok(true),
            Err(e) => {
                let service_error = e.as_service_error().unwrap();
                if service_error.is_transaction_canceled_exception() {
                    match service_error {
                        TransactWriteItemsError::TransactionCanceledException(inner) => {
                            if inner.cancellation_reasons.is_some() {
                                let cancellation_reasons =
                                    inner.cancellation_reasons.as_ref().unwrap();
                                let reasons = format!("{:?}", cancellation_reasons);
                                println!("Transaction canceled: {}", reasons);
                                //panic!("Transaction canceled: {}", reasons);
                            }
                        }
                        _ => (),
                    }
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub async fn commit_extension_work_item_taken(
        &self,
        old_entity_infos: &Vec<EntityVersionInfo>,
        new_entity_id: &String,
    ) -> Result<bool, Error> {
        if old_entity_infos.len() == 0 {
            return Ok(true);
        }

        let mut transaction = TransactWrite::new();

        for entity_info in old_entity_infos.iter() {
            transaction = Self::bump_version(transaction, entity_info, new_entity_id);
            transaction = Self::create_extension_work_item_lease_core(
                transaction,
                &entity_info.org_id,
                &entity_info.key,
                &entity_info.entity_id,
                Some(chrono::Utc::now().timestamp_millis()),
            );
        }

        self.commit_conditional_transaction(transaction).await
    }

    pub async fn commit_compaction_work_item_taken(
        &self,
        old_entity_infos: &Vec<EntityVersionInfo>,
        new_entity_id: &String,
    ) -> Result<bool, Error> {
        if old_entity_infos.len() == 0 {
            return Ok(true);
        }

        let mut transaction = TransactWrite::new();

        for entity_info in old_entity_infos.iter() {
            transaction = Self::bump_version(transaction, entity_info, new_entity_id);
            transaction = Self::create_compaction_work_item_lease_core(
                transaction,
                &entity_info.org_id,
                &entity_info.key,
                &entity_info.entity_id,
                Some(chrono::Utc::now().timestamp_millis()),
            );
        }

        self.commit_conditional_transaction(transaction).await
    }

    pub async fn commit_speedboat(
        &self,
        org_id: &String,
        table_name: &String,
        commit: &SpeedboatCommit,
    ) -> Result<bool, Error> {
        // 1. Save the commit itself
        // 2. Save a tracker for creating a new checkpoint based on this commit
        // 3. Create either a new ES work item or update the existing one
        // 4. Update the latest ES tracker to the new/updated one

        let mut transaction = TransactWrite::new();

        let speedboat_commit_id = &IdInstance::next_id().to_string();
        // Step 1
        transaction = self.private_create_speedboat_commit_core(
            transaction,
            org_id,
            speedboat_commit_id,
            commit,
        );
        // Step 2
        transaction = Self::create_speedboat_commit_checkpointed_core(
            transaction,
            org_id,
            table_name,
            speedboat_commit_id,
            None,
        );

        let latest_es_key = &Self::latest_extension_work_item_key(&table_name, &"es".to_string());
        let latest_es = self.describe_latest(org_id, latest_es_key).await?;
        assert!(latest_es.is_some());
        let new_es_id = IdInstance::next_id().to_string();
        let mut work_item = if latest_es.as_ref().unwrap().entity_id == Self::NO_WORK_ITEM {
            ExtensionWorkItem {
                id: "".to_string(),
                extension_type: "es".to_string(),
                table_name: table_name.to_string(),
                checkpoint_id: None,
                table_schema: PowdrrSchema::minimal(),
                speedboat_files: FileSetPayload::new(),
                iceberg_files: FileSetPayload::new(),
            }
        } else {
            self.describe_extension_work_item(
                &mut PowdrrNamedExtensionWorkItemCache::new(),
                org_id,
                &latest_es.as_ref().unwrap().entity_id,
            )
            .await?
            .unwrap()
        };
        work_item.id = new_es_id.clone();
        work_item.merge_speedboat(commit);
        // Step 3
        transaction = self.cached_create_extension_work_item_core(
            transaction,
            &mut PowdrrNamedExtensionWorkItemCache::new(),
            org_id,
            &new_es_id,
            &work_item,
        );
        // Step 4
        transaction = Self::bump_version(transaction, latest_es.as_ref().unwrap(), &new_es_id);
        // TODO: delete old id?

        self.commit_conditional_transaction(transaction).await
    }

    async fn gather_compactions(
        &self,
        org_id: &String,
        speedboat_commits: &Vec<SpeedboatCommit>,
        iceberg_commits: &Vec<IcebergCommit>,
    ) -> Result<HashMap<String, CompactionCommit>, Error> {
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
                self.describe_compaction(
                    &mut PowdrrNamedCompactionCommitCache::new(),
                    org_id,
                    &compaction,
                )
                .await?
                .unwrap(),
            );
        }

        Ok(compaction_commits)
    }

    pub async fn commit_iceberg(
        &self,
        org_id: &String,
        table_name: &String,
        commit: &IcebergCommit,
    ) -> Result<bool, Error> {
        // 1. Save the commit itself
        // 2. Save an updated checkpoint and bump the latest
        // 3. Create either a new ES work item or update the existing one
        // 4. Update the latest ES tracker to the new/updated one
        // 5. If the commit references compactions then:
        //     a. create the replacement checkpoint
        //     b. removed the deleted checkpoints
        //     c. free up the compaction work item lease
        //     d. create a cleanup work item

        let mut transaction = TransactWrite::new();

        let iceberg_commit_id = &IdInstance::next_id().to_string();
        // Step 1
        transaction =
            self.private_create_iceberg_commit_core(transaction, org_id, iceberg_commit_id, commit);

        let latest_info = self
            .describe_latest(org_id, &Self::latest_checkpoint_key(table_name, &None))
            .await?
            .unwrap();
        let latest_obj = self
            .describe_checkpoint(
                &mut PowdrrNamedTableMetadataCheckpointCache::new(),
                &org_id,
                &latest_info.entity_id,
            )
            .await?
            .unwrap();

        let compactions = self
            .gather_compactions(org_id, &vec![], &vec![commit.clone()])
            .await?;

        let (new_checkpoint, _) =
            latest_obj.clone_and_apply(&vec![], &vec![commit.clone()], &vec![], &compactions);

        // Step 2
        transaction = Self::bump_version(
            transaction,
            &latest_info,
            &new_checkpoint.get_descriptor().full_name(),
        );
        let checkpoint_obj = PowdrrNamedTableMetadataCheckpoint {
            name: new_checkpoint.get_descriptor().full_name(),
            org_id: org_id.clone(),
            entity: new_checkpoint.clone(),
        };
        transaction = transaction.operation(checkpoint_obj.create());
        if !new_checkpoint.fully_covered_for_extension(&"es".to_string()) {
            transaction = Self::create_checkpoint_waiting_for_extension_core(
                transaction,
                org_id,
                &new_checkpoint.table_name,
                &new_checkpoint.get_descriptor().full_name(),
                None,
            );
        }

        // Step 3
        let latest_es_key = &Self::latest_extension_work_item_key(&table_name, &"es".to_string());
        let latest_es = self.describe_latest(org_id, latest_es_key).await?;
        assert!(latest_es.is_some());
        let new_es_id = IdInstance::next_id().to_string();
        let mut work_item = if latest_es.as_ref().unwrap().entity_id == Self::NO_WORK_ITEM {
            ExtensionWorkItem {
                id: "".to_string(),
                extension_type: "es".to_string(),
                table_name: table_name.to_string(),
                checkpoint_id: None,
                table_schema: PowdrrSchema::minimal(),
                speedboat_files: FileSetPayload::new(),
                iceberg_files: FileSetPayload::new(),
            }
        } else {
            self.describe_extension_work_item(
                &mut PowdrrNamedExtensionWorkItemCache::new(),
                org_id,
                &latest_es.as_ref().unwrap().entity_id,
            )
            .await?
            .unwrap()
        };
        work_item.id = new_es_id.clone();
        work_item.merge_iceberg(commit);
        transaction = self.cached_create_extension_work_item_core(
            transaction,
            &mut PowdrrNamedExtensionWorkItemCache::new(),
            org_id,
            &new_es_id,
            &work_item,
        );

        // Step 4
        transaction = Self::bump_version(transaction, latest_es.as_ref().unwrap(), &new_es_id);
        // TODO: delete old id?

        // Step 5
        for (compaction_id, compaction_commit) in compactions.iter() {
            // Step 5a
            let checkpoint_descriptor = CheckpointDescriptor::new(
                table_name.clone(),
                compaction_commit.checkpoint_id_to_replace.clone(),
            );
            let mut cloned_checkpoint_to_replace = self
                .describe_checkpoint(
                    &mut PowdrrNamedTableMetadataCheckpointCache::new(),
                    &org_id,
                    &checkpoint_descriptor.full_name(),
                )
                .await?
                .unwrap()
                .clone();
            cloned_checkpoint_to_replace.checkpoint_id = IdInstance::next_id().to_string();
            cloned_checkpoint_to_replace
                .apply_compaction_for_replacement(compaction_commit, &commit.metadata);
            assert!(
                cloned_checkpoint_to_replace
                    .original_checkpoint_id
                    .is_none()
            );
            cloned_checkpoint_to_replace.original_checkpoint_id =
                Some(compaction_commit.checkpoint_id_to_replace.clone());

            let checkpoint_obj = PowdrrNamedTableMetadataCheckpoint {
                name: cloned_checkpoint_to_replace.get_descriptor().full_name(),
                org_id: org_id.clone(),
                entity: new_checkpoint.clone(),
            };
            transaction = transaction.operation(checkpoint_obj.create());

            if !new_checkpoint.fully_covered_for_extension(&"es".to_string()) {
                transaction = Self::create_checkpoint_waiting_for_extension_core(
                    transaction,
                    org_id,
                    &cloned_checkpoint_to_replace.table_name,
                    &cloned_checkpoint_to_replace.get_descriptor().full_name(),
                    None,
                )
            }

            // Step 5b
            // TODO: delete the listed checkpoints

            // Step 5c
            let key =
                Self::latest_compaction_work_item_key(&cloned_checkpoint_to_replace.table_name);
            transaction = Self::mark_done_compaction_work_item_lease_inner(
                transaction,
                org_id,
                &key,
                compaction_id,
                None,
            );

            // Step 5d
            let cleanup_work_item = CleanupWorkItem {
                id: IdInstance::next_id().to_string(),
                table_name: cloned_checkpoint_to_replace.table_name.clone(),
                files_to_delete: compaction_commit
                    .removed_speedboat_files
                    .iter()
                    .chain(compaction_commit.removed_delete_files.iter())
                    .map(|x| x.clone())
                    .collect(),
            };
            let cleanup_work_item_id = cleanup_work_item.id.clone();
            transaction = self.cached_create_cleanup_work_item_core(
                transaction,
                &mut PowdrrNamedCleanupWorkItemCache::new(),
                org_id,
                &cleanup_work_item_id,
                &cleanup_work_item,
            );
            transaction = Self::create_cleanup_work_item_lease_core(
                transaction,
                org_id,
                &cloned_checkpoint_to_replace.table_name,
                &cleanup_work_item_id,
                None,
            );
        }

        self.commit_conditional_transaction(transaction).await
    }

    pub async fn commit_extension_work_item_completed(
        &self,
        org_id: &String,
        table_name: &String,
        commit: &ExtensionCommit,
    ) -> Result<bool, Error> {
        let mut transaction = TransactWrite::new();

        let key = Self::latest_extension_work_item_key(table_name, &"es".to_string());
        transaction = Self::mark_done_extension_work_item_lease_inner(
            transaction,
            org_id,
            &key,
            &commit.id,
            Some(0),
        );
        transaction =
            self.private_create_extension_commit_core(transaction, org_id, &commit.id, &commit);
        transaction = Self::create_extension_commit_checkpointed_core(
            transaction,
            org_id,
            table_name,
            &commit.id,
            None,
        );

        self.commit_conditional_transaction(transaction).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_contract::SpeedboatCommitTableInfo;
    use crate::peers::CheckpointDescriptor;
    use crate::schema_massager::PowdrrSchema;
    use aws_config::{BehaviorVersion, Region};
    use aws_sdk_dynamodb::Client;
    use modyne::TestTableExt;

    async fn create_connector() -> DynamoDbConnector {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("us-east-1")) // Region doesn't matter for local, but required
            .endpoint_url("http://localhost:4566")
            .credentials_provider(aws_credential_types::Credentials::new(
                "test", "test", None, None, "static",
            ))
            .load()
            .await;

        let client = Client::new(&config);

        let connector = DynamoDbConnector::new(client);

        let _ = connector.delete_table().send().await;
        let _create_table = match connector.create_table().send().await {
            Ok(_) => (),
            Err(e) => {
                panic!("{:?}", e)
            }
        };
        connector
    }

    #[tokio::test]
    async fn test_basic_create_and_search() {
        let connector = create_connector().await;

        match connector
            .create_powdrr_table(
                &"dude".to_string(),
                &"fresh".to_string(),
                &TableBody {
                    tags: HashMap::from([("foo".to_string(), "bar".to_string())]),
                    serving: None,
                    dynamodb: None,
                    mongodb: None,
                    redis: None,
                },
            )
            .await
        {
            Ok(_) => (),
            Err(e) => {
                panic!("{:?}", e)
            }
        }

        let result = connector
            .describe_powdrr_table(&"dude".to_string(), &"fresh".to_string())
            .await
            .unwrap();
        match result {
            Some(table) => {
                assert_eq!(table.tags.get("foo").unwrap(), "bar");
            }
            None => {
                panic!("Table not found");
            }
        }

        match connector
            .create_latest(
                &"dude".to_string(),
                &"fake_table#es".to_string(),
                &EntityVersionInfo::new(
                    &"dude".to_string(),
                    &"fake_table#es".to_string(),
                    &"-1".to_string(),
                ),
            )
            .await
        {
            Ok(_) => (),
            Err(e) => {
                panic!("{:?}", e)
            }
        }

        let result = connector
            .describe_latest(&"dude".to_string(), &"fake_table#es".to_string())
            .await
            .unwrap();
        match result {
            Some(info) => {
                assert_eq!(info.entity_id, "-1");
                assert_eq!(info.version, 0);
            }
            None => {
                panic!("Not found");
            }
        }

        let checkpoint = TableMetadataCheckpoint {
            table_name: "".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "".to_string(),
            iceberg_metadata: None,
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: Default::default(),
            schema: PowdrrSchema::minimal(),
        };

        let mut cache = PowdrrNamedTableMetadataCheckpointCache::new();

        match connector
            .create_checkpoint(
                &mut cache,
                &"dude".to_string(),
                &"fresh".to_string(),
                &checkpoint,
            )
            .await
        {
            Ok(_) => (),
            Err(e) => {
                panic!("{:?}", e)
            }
        }

        let result = connector
            .describe_checkpoint(&mut cache, &"dude".to_string(), &"fresh".to_string())
            .await
            .unwrap();
        match result {
            Some(_table) => (),
            None => {
                panic!("Table not found");
            }
        }
    }

    #[tokio::test]
    async fn test_tracking_create_and_search() {
        let connector = create_connector().await;

        let mut cache = PowdrrNamedSpeedboatCommitCache::new();

        let commit = SpeedboatCommit {
            type_files: vec![SpeedboatCommitTableInfo {
                commit_type: "commit".to_string(),
                table_name: "fake_table".to_string(),
                segments: vec![],
                files: vec!["fake_file".to_string()],
                sizes: vec![100],
                schema: None,
            }],
            compaction: Some("fake_compaction".to_string()),
        };

        connector
            .create_speedboat_commit(
                &mut cache,
                &"fake_org".to_string(),
                &"fake_id".to_string(),
                &commit,
            )
            .await
            .unwrap();

        let found = connector
            .describe_speedboat_commit(&mut cache, &"fake_org".to_string(), &"fake_id".to_string())
            .await
            .unwrap();
        match found {
            Some(commit) => {
                assert_eq!(commit.type_files.len(), 1);
                assert_eq!(commit.type_files[0].commit_type, "commit");
                assert_eq!(commit.type_files[0].table_name, "fake_table");
                assert_eq!(commit.type_files[0].files.len(), 1);
                assert_eq!(commit.type_files[0].files[0], "fake_file");
            }
            None => {
                panic!("Commit not found");
            }
        };

        connector
            .create_speedboat_commit_checkpointed(
                &"fake_org".to_string(),
                &"fake_table".to_string(),
                &"fake_id".to_string(),
            )
            .await
            .unwrap();
        let trackers = connector
            .oldest_available_speedboat_commit_checkpointed(
                &"fake_org".to_string(),
                &"fake_table".to_string(),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(trackers.len(), 1);

        let _ = DynamoDbConnector::mark_done_speedboat_commit_checkpointed_core(
            TransactWrite::new(),
            &trackers[0],
        )
        .execute(&connector)
        .await
        .unwrap();

        let trackers_again = connector
            .oldest_available_speedboat_commit_checkpointed(
                &"fake_org".to_string(),
                &"fake_table".to_string(),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(trackers_again.len(), 0);

        match DynamoDbConnector::mark_done_speedboat_commit_checkpointed_core(
            TransactWrite::new(),
            &trackers[0],
        )
        .execute(&connector)
        .await
        {
            Ok(_) => panic!("Should have failed"),
            Err(_) => (),
        };
    }

    #[tokio::test]
    async fn test_update_checkpoint() {
        let connector = create_connector().await;

        let mut checkpoint_cache = PowdrrNamedTableMetadataCheckpointCache::new();
        let mut speedboat_cache = PowdrrNamedSpeedboatCommitCache::new();

        let first_checkpoint = TableMetadataCheckpoint {
            table_name: "fake_table".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "1".to_string(),
            iceberg_metadata: None,
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: Default::default(),
            schema: PowdrrSchema::minimal(),
        };

        let speedboat_commit = SpeedboatCommit {
            type_files: vec![SpeedboatCommitTableInfo {
                commit_type: "commit".to_string(),
                table_name: "fake_table".to_string(),
                segments: vec![],
                files: vec!["fake_file".to_string()],
                sizes: vec![100],
                schema: Some(PowdrrSchema::minimal()),
            }],
            compaction: None,
        };

        connector
            .create_latest(
                &"fake_org".to_string(),
                &first_checkpoint.table_name,
                &EntityVersionInfo::new(
                    &"fake_org".to_string(),
                    &first_checkpoint.table_name,
                    &first_checkpoint.checkpoint_id,
                ),
            )
            .await
            .unwrap();
        connector
            .create_checkpoint(
                &mut checkpoint_cache,
                &"fake_org".to_string(),
                &first_checkpoint.checkpoint_id,
                &first_checkpoint,
            )
            .await
            .unwrap();
        connector
            .create_speedboat_commit(
                &mut speedboat_cache,
                &"fake_org".to_string(),
                &"fake_id".to_string(),
                &speedboat_commit,
            )
            .await
            .unwrap();
        connector
            .create_speedboat_commit_checkpointed(
                &"fake_org".to_string(),
                &"fake_table".to_string(),
                &"fake_id".to_string(),
            )
            .await
            .unwrap();

        let latest_speedboat_trackers = connector
            .oldest_available_speedboat_commit_checkpointed(
                &"fake_org".to_string(),
                &"fake_table".to_string(),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(latest_speedboat_trackers.len(), 1);

        let latest_checkpoint_info = connector
            .describe_latest(&"fake_org".to_string(), &"fake_table".to_string())
            .await
            .unwrap()
            .unwrap();
        let latest_checkpoint = connector
            .describe_checkpoint(
                &mut checkpoint_cache,
                &"fake_org".to_string(),
                &latest_checkpoint_info.entity_id,
            )
            .await
            .unwrap()
            .unwrap();

        let mut latest_speedboats = vec![];
        for speedboat_tracker in latest_speedboat_trackers.iter() {
            latest_speedboats.push(
                connector
                    .describe_speedboat_commit(
                        &mut speedboat_cache,
                        &"fake_org".to_string(),
                        &speedboat_tracker.name,
                    )
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }

        let (new_checkpoint, changed) = latest_checkpoint.clone_and_apply(
            &latest_speedboats,
            &vec![],
            &vec![],
            &HashMap::new(),
        );
        assert!(changed);

        match connector
            .commit_checkpoint(
                &latest_checkpoint_info,
                &latest_speedboat_trackers,
                &new_checkpoint,
                &None,
                &None,
            )
            .await
        {
            Ok(val) => {
                assert!(val);
            }
            Err(_e) => {
                panic!("Test failed")
            }
        };

        let new_latest = connector
            .describe_latest(&"fake_org".to_string(), &"fake_table".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(new_latest.version, latest_checkpoint_info.version);
        assert_eq!(
            CheckpointDescriptor::from_full_name(&new_latest.entity_id).checkpoint_id,
            new_checkpoint.checkpoint_id
        );
    }
}
