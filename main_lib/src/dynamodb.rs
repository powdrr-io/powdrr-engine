use std::collections::HashMap;
use idgenerator::IdInstance;
use modyne::{expr, keys, model::TransactWrite, projections, read_projection, Aggregate, Entity, EntityExt, Error, Item, ProjectionExt, QueryInput, QueryInputExt, Table};
use modyne::expr::Filter;
use crate::data_contract::{CompactionCommit, CompactionWorkItem, CreateIndexTemplateBody, ExtensionCommit, ExtensionWorkItem, IcebergCommit, SpeedboatCommit, TableMetadataCheckpoint};
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::pipeline::PipelineDefinition;
use crate::schema_massager::PowdrrSchema;

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
        }).into()
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
    pub async fn fetch_entities(&self, org_id: &String, entity_type: &String, limit: Option<u32>) -> Result<PowdrrEntities, Error> {
        let query = PowdrrEntityQuery{ entity_type, org_id };

        let mut entities = PowdrrEntities::default();

        let result = query
            .query()
            .set_limit(limit)
            .execute(self)
            .await?;

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


#[derive(Debug, modyne_derive::EntityDef, serde::Serialize, serde::Deserialize, Clone)]
pub struct PowdrrTracker {
    pub entity_name: String,
    pub org_id: String,
    pub parent_entity: String,
    pub name: String,
    pub version: u64
}

impl Entity for PowdrrTracker {
    type KeyInput<'a> = OrgIdEntityNameInput<'a>;
    type Table = DynamoDbConnector;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("{}_TRACKER#{}#{}", input.entity_name, input.org_id, input.parent_entity);
        keys::Primary {
            hash: common.clone(),
            range: input.name.clone(),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        Self::primary_key(OrgIdEntityNameInput{
            entity_name: &self.entity_name,
            org_id: &self.org_id,
            parent_entity: &self.parent_entity,
            name: &self.name,
        }).into()
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
        let key = format!("{}_TRACKER#{}#{}", self.entity_name, self.org_id, self.parent_entity);
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

            impl DynamoDbConnector {
                fn [< private_create_ $entity_name _core >](&self, transaction: TransactWrite, org_id: &String, name: &String, template: &$type_name) -> TransactWrite {
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


                async fn [< private_create_ $entity_name >](&self, org_id: &String, name: &String, template: &$type_name) -> Result<(), Error> {
                    self.[< private_create_ $entity_name _core >](TransactWrite::new(), org_id, name, template)
                        .execute(self)
                        .await?;

                    Ok(())
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

                async fn [< private_delete_ $entity_name >](&self, org_id: &String, name: &String) -> Result<(), Error> {
                    self.[< private_delete_ $entity_name _core >](TransactWrite::new(), org_id, name)
                        .execute(self)
                        .await?;
                    Ok(())
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
                pub async fn [< create_ $entity_name >](&self, org_id: &String, name: &String, template: &$type_name) -> Result<(), Error> {
                    self.[< private_create_ $entity_name >](org_id, name, template).await
                }

                pub async fn [< describe_ $entity_name >](&self, org_id: &String, name: &String) -> Result<Option<$type_name>, Error> {
                    self.[< private_describe_ $entity_name >](org_id, name).await
                }

                pub async fn [< delete_ $entity_name >](&self, org_id: &String, name: &String) -> Result<(), Error> {
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
                pub async fn [< create_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String, template: &$type_name) -> Result<(), Error> {
                    self.[< cached_create_ $entity_name >](cache, org_id, name, template).await
                }

                pub async fn [< describe_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String) -> Result<Option<$type_name>, Error> {
                    self.[< cached_describe_ $entity_name >](cache, org_id, name).await
                }

                pub async fn [< delete_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String) -> Result<(), Error> {
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
                fn [< cached_create_ $entity_name _core >](&self, transaction: TransactWrite, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String, template: &$type_name) -> TransactWrite {
                    let key = CacheKey{ org_id: org_id.clone(), name: name.clone() };
                    cache.cache.insert(key.clone(), template.clone());
                    self.[< private_create_ $entity_name _core >](transaction, org_id, name, template)
                }

                async fn [< cached_create_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String, template: &$type_name) -> Result<(), Error> {
                    self.[< cached_create_ $entity_name _core >](TransactWrite::new(), cache, org_id, name, template).execute(self).await?;
                    Ok(())
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

                async fn [< cached_delete_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String) -> Result<(), Error> {
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
                pub fn [< create_ $entity_name _core >](transaction: TransactWrite, org_id: &String, parent_entity: &String, name: &String) -> TransactWrite {
                    let tracker = PowdrrTracker {
                        entity_name: stringify!($entity_name).to_string(),
                        org_id: org_id.clone(),
                        parent_entity: parent_entity.clone(),
                        name: name.clone(),
                        version: 0
                    };
                    transaction.operation(tracker.create())
                }

                pub async fn [< create_ $entity_name >](&self, org_id: &String, parent_entity: &String, name: &String) -> Result<(), Error> {
                    Self::[< create_ $entity_name _core >](TransactWrite::new(), org_id, parent_entity, name)
                        .execute(self)
                        .await?;

                    Ok(())
                }

                pub async fn [< delete_ $entity_name >](&self, org_id: &String, parent_entity: &String, name: &String) -> Result<(), Error> {
                    PowdrrTracker::delete(OrgIdEntityNameInput{ entity_name: &stringify!($entity_name).to_string(), parent_entity: parent_entity, org_id: org_id, name: name }).execute(self).await?;
                    Ok(())
                }

                pub async fn [< oldest_available_ $entity_name >](&self, org_id: &String, parent_entity: &String, limit: Option<u32>) -> Result<Vec<PowdrrTracker>, Error> {
                    let query_input = PowdrrTrackerQuery { entity_name: stringify!($entity_name).to_string(), org_id: org_id.clone(), parent_entity: parent_entity.clone() };

                    let mut trackers = PowdrrTrackerQueryResults::default();

                    let result = query_input
                        .query()
                        .filter(Filter::new("version <> :negative_one")
                            .value(":negative_one", -1)
                        )
                        .set_limit(limit)
                        .execute(self)
                        .await?;

                    trackers.reduce(result.items.unwrap_or_default())?;

                    Ok(trackers.trackers)
                }

                pub async fn [< mark_done_ $entity_name >](&self, tracker: &PowdrrTracker) -> Result<(), Error> {
                    Self::[< mark_done_ $entity_name _core >](TransactWrite::new(), &tracker).execute(self).await?;
                    Ok(())
                }

                fn [< mark_done_ $entity_name _core >](transaction: TransactWrite, tracker: &PowdrrTracker) -> TransactWrite {
                    Self::[< mark_done_ $entity_name _inner >](transaction, &tracker.org_id, &tracker.parent_entity, &tracker.name, Some(tracker.version))
                }

                fn [< mark_done_ $entity_name _inner >](transaction: TransactWrite, org_id: &String, parent_entity: &String, name: &String, expected: Option<u64>) -> TransactWrite {
                    let key = OrgIdEntityNameInput {
                        entity_name: &stringify!($entity_name).to_string(),
                        org_id,
                        parent_entity,
                        name,
                    };
                    let expression = expr::Update::new("SET version = :negative_one")
                        .value(":negative_one", -1);

                    match expected {
                        Some(expected_version) => {
                            let condition = expr::Condition::new("version = :expected")
                                .value(":expected", expected_version);
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
}

impl TableBody {
    pub(crate) fn new() -> Self {
        Self {
            tags: HashMap::new(),
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
            entity_id: entity_id.clone()
        }
    }

    pub fn get_query(&self) -> OrgIdNameInput {
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


powdrr_tracker!(speedboat_commit_checkpointed);
powdrr_tracker!(extension_commit_checkpointed);
powdrr_tracker!(checkpoint_waiting_for_extension);


impl DynamoDbConnector {
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

    fn bump_version(transaction: TransactWrite, old: &EntityVersionInfo, new_entity_id: &String) -> TransactWrite {
        let expression = expr::Update::new("SET entity.version = :plus_one, entity.entity_id = :new_id")
            .value(":new_id", new_entity_id.clone())
            .value(":plus_one", old.version + 1);
        let condition = expr::Condition::new("entity.version = :old")
            .value(":old", old.version);

        transaction.operation(PowdrrNamedEntityVersionInfo::update(old.get_query()).expression(expression).condition(condition))
    }

    pub fn create_latest_core(&self, transaction: TransactWrite, entity: &EntityVersionInfo) -> TransactWrite {
        self.private_create_latest_core(transaction, &entity.org_id, &entity.key, entity)
    }

    pub async fn create_table(&mut self, org_id: &String, table_name: &String, table_body: &TableBody) -> Result<(), Error> {
        let checkpoint = TableMetadataCheckpoint::new(
            table_name.clone(),
            IdInstance::next_id().to_string(),
            PowdrrSchema{ fields: vec![] }
        );

        tracing::info!("Created checkpoint for table {}: {}", table_name, checkpoint.checkpoint_id);

        let mut transaction = TransactWrite::new();
        transaction = self.private_create_powdrr_table_core(transaction, org_id, table_name, table_body);
        transaction = self.private_create_checkpoint_core(transaction, org_id, &checkpoint.get_descriptor().full_name(), &checkpoint);
        transaction = self.create_latest_core(transaction, &EntityVersionInfo::new(org_id, &Self::latest_checkpoint_key(table_name, &None), &checkpoint.get_descriptor().full_name()));
        transaction = self.create_latest_core(transaction, &EntityVersionInfo::new(org_id, &Self::latest_checkpoint_key(table_name, &Some("es".to_string())), &checkpoint.get_descriptor().full_name()));
        transaction = self.create_latest_core(transaction, &EntityVersionInfo::new(org_id, &Self::latest_extension_work_item_key(table_name, &"es".to_string()), &Self::NO_WORK_ITEM.to_owned()));
        transaction = self.create_latest_core(transaction, &EntityVersionInfo::new(org_id, &Self::latest_compaction_work_item_key(table_name), &Self::NO_WORK_ITEM.to_owned()));
        transaction.execute(self).await?;

        Ok(())
    }

    pub async fn commit_checkpoint(
        &self,
        input_latest: &EntityVersionInfo,
        input_speedboat_trackers: &Vec<PowdrrTracker>,
        new_checkpoint: &TableMetadataCheckpoint,
    ) -> Result<bool, Error> {
        let mut transaction = TransactWrite::new();

        transaction = Self::bump_version(
            transaction,
            input_latest,
            &new_checkpoint.get_descriptor().full_name()
        );

        for input_tracker in input_speedboat_trackers.iter() {
            transaction = DynamoDbConnector::mark_done_speedboat_commit_checkpointed_core(
                transaction,
                input_tracker
            );
        }
        let checkpoint_obj = PowdrrNamedTableMetadataCheckpoint {
            name: new_checkpoint.get_descriptor().full_name(),
            org_id: input_latest.org_id.clone(),
            entity: new_checkpoint.clone(),
        };
        transaction = transaction.operation(checkpoint_obj.create());

        if !new_checkpoint.fully_covered_for_extension(&"es".to_string()) {
            transaction = Self::create_checkpoint_waiting_for_extension_core(transaction, &input_latest.org_id, &new_checkpoint.table_name, &new_checkpoint.get_descriptor().full_name())
        }

        match transaction.execute(self).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.as_service_error().unwrap().is_transaction_canceled_exception() {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub async fn commit_work_item_taken(&self, old_entity_infos: &Vec<EntityVersionInfo>, new_entity_id: &String) -> Result<bool, Error> {
        if old_entity_infos.len() == 0 {
            return Ok(true);
        }

        let mut transaction = TransactWrite::new();

        for entity_info in old_entity_infos.iter() {
            transaction = Self::bump_version(
                transaction,
                entity_info,
                new_entity_id
            );
        }

        match transaction.execute(self).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.as_service_error().unwrap().is_transaction_canceled_exception() {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub async fn commit_work_item(&self, old: &EntityVersionInfo, new_entity_id: &String) -> Result<bool, Error> {
        let mut transaction = TransactWrite::new();

        transaction = Self::bump_version(
            transaction,
            old,
            new_entity_id
        );

        match transaction.execute(self).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.as_service_error().unwrap().is_transaction_canceled_exception() {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use aws_config::{BehaviorVersion, Region};
    use aws_sdk_dynamodb::Client;
    use crate::data_contract::SpeedboatCommitTableInfo;
    use crate::schema_massager::PowdrrSchema;
    use super::*;
    use modyne::TestTableExt;
    use crate::peers::CheckpointDescriptor;

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
            },
        };
        connector
    }

    #[tokio::test]
    async fn test_basic_create_and_search() {
        let connector = create_connector().await;

        match connector.create_powdrr_table(&"dude".to_string(), &"fresh".to_string(), &TableBody { tags: HashMap::from([("foo".to_string(), "bar".to_string())]) }).await {
            Ok(_) => (),
            Err(e) => {
                panic!("{:?}", e)
            },
        }

        let result = connector.describe_powdrr_table(&"dude".to_string(), &"fresh".to_string()).await.unwrap();
        match result {
            Some(table) => {
                assert_eq!(table.tags.get("foo").unwrap(), "bar");
            },
            None => {
                panic!("Table not found");
            },
        }

        match connector.create_latest(&"dude".to_string(), &"fake_table#es".to_string(), &EntityVersionInfo::new(&"dude".to_string(), &"fake_table#es".to_string(), &"-1".to_string())).await {
            Ok(_) => (),
            Err(e) => {
                panic!("{:?}", e)
            },
        }

        let result = connector.describe_latest(&"dude".to_string(), &"fake_table#es".to_string()).await.unwrap();
        match result {
            Some(info) => {
                assert_eq!(info.entity_id, "-1");
                assert_eq!(info.version, 0);
            },
            None => {
                panic!("Not found");
            },
        }

        let checkpoint = TableMetadataCheckpoint {
            table_name: "".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "".to_string(),
            iceberg_metadata: None,
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: Default::default(),
            schema: PowdrrSchema { fields: vec![] },
        };

        let mut cache = PowdrrNamedTableMetadataCheckpointCache::new();

        match connector.create_checkpoint(&mut cache, &"dude".to_string(), &"fresh".to_string(), &checkpoint).await {
            Ok(_) => (),
            Err(e) => {
                panic!("{:?}", e)
            },
        }

        let result = connector.describe_checkpoint(&mut cache, &"dude".to_string(), &"fresh".to_string()).await.unwrap();
        match result {
            Some(_table) => {
                ()
            },
            None => {
                panic!("Table not found");
            },
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
                files: vec!["fake_file".to_string()],
                sizes: vec![100],
                schema: None,
            }],
            compactions: vec!["fake_compaction".to_string()],
        };

        connector.create_speedboat_commit(&mut cache, &"fake_org".to_string(), &"fake_id".to_string(), &commit).await.unwrap();

        let found = connector.describe_speedboat_commit(&mut cache, &"fake_org".to_string(), &"fake_id".to_string()).await.unwrap();
        match found {
            Some(commit) => {
                assert_eq!(commit.type_files.len(), 1);
                assert_eq!(commit.type_files[0].commit_type, "commit");
                assert_eq!(commit.type_files[0].table_name, "fake_table");
                assert_eq!(commit.type_files[0].files.len(), 1);
                assert_eq!(commit.type_files[0].files[0], "fake_file");
            },
            None => {
                panic!("Commit not found");
            },
        };

        connector.create_speedboat_commit_checkpointed(&"fake_org".to_string(), &"fake_table".to_string(), &"fake_id".to_string()).await.unwrap();
        let trackers = connector.oldest_available_speedboat_commit_checkpointed(&"fake_org".to_string(), &"fake_table".to_string(), None).await.unwrap();
        assert_eq!(trackers.len(), 1);

        let _ =
            DynamoDbConnector::mark_done_speedboat_commit_checkpointed_core(TransactWrite::new(), &trackers[0])
            .execute(&connector)
            .await.unwrap();

        let trackers_again = connector.oldest_available_speedboat_commit_checkpointed(&"fake_org".to_string(), &"fake_table".to_string(), None).await.unwrap();
        assert_eq!(trackers_again.len(), 0);

        match DynamoDbConnector::mark_done_speedboat_commit_checkpointed_core(TransactWrite::new(), &trackers[0])
            .execute(&connector)
            .await {
            Ok(_) => panic!("Should have failed"),
            Err(_) => ()
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
            schema: PowdrrSchema { fields: vec![] },
        };

        let speedboat_commit = SpeedboatCommit {
            type_files: vec![SpeedboatCommitTableInfo {
                commit_type: "commit".to_string(),
                table_name: "fake_table".to_string(),
                files: vec!["fake_file".to_string()],
                sizes: vec![100],
                schema: Some(PowdrrSchema { fields: vec![] }),
            }],
            compactions: vec![],
        };

        connector.create_latest(&"fake_org".to_string(), &first_checkpoint.table_name, &EntityVersionInfo::new(&"fake_org".to_string(), &first_checkpoint.table_name, &first_checkpoint.checkpoint_id)).await.unwrap();
        connector.create_checkpoint(&mut checkpoint_cache, &"fake_org".to_string(), &first_checkpoint.checkpoint_id, &first_checkpoint).await.unwrap();
        connector.create_speedboat_commit(&mut speedboat_cache, &"fake_org".to_string(), &"fake_id".to_string(), &speedboat_commit).await.unwrap();
        connector.create_speedboat_commit_checkpointed(&"fake_org".to_string(), &"fake_table".to_string(), &"fake_id".to_string()).await.unwrap();

        let latest_speedboat_trackers = connector.oldest_available_speedboat_commit_checkpointed(&"fake_org".to_string(), &"fake_table".to_string(), None).await.unwrap();
        assert_eq!(latest_speedboat_trackers.len(), 1);

        let latest_checkpoint_info = connector.describe_latest(&"fake_org".to_string(), &"fake_table".to_string()).await.unwrap().unwrap();
        let latest_checkpoint = connector.describe_checkpoint(&mut checkpoint_cache, &"fake_org".to_string(), &latest_checkpoint_info.entity_id).await.unwrap().unwrap();

        let mut latest_speedboats = vec!();
        for speedboat_tracker in latest_speedboat_trackers.iter() {
            latest_speedboats.push(connector.describe_speedboat_commit(&mut speedboat_cache, &"fake_org".to_string(), &speedboat_tracker.name).await.unwrap().unwrap());
        }

        let (new_checkpoint, changed) = latest_checkpoint.clone_and_apply(
            &latest_speedboats,
            &vec!(),
            &vec!(),
            &HashMap::new()
        );
        assert!(changed);

        match connector.commit_checkpoint(
            &latest_checkpoint_info,
            &latest_speedboat_trackers,
            &new_checkpoint
        ).await {
            Ok(val) => {
                assert!(val);
            },
            Err(_e) => {
                panic!("Test failed")
            }
        };

        let new_latest = connector.describe_latest(&"fake_org".to_string(), &"fake_table".to_string()).await.unwrap().unwrap();
        assert_ne!(new_latest.version, latest_checkpoint_info.version);
        assert_eq!(CheckpointDescriptor::from_full_name(&new_latest.entity_id).checkpoint_id, new_checkpoint.checkpoint_id);
    }
}
