use std::collections::HashMap;
use modyne::{expr, keys, model::TransactWrite, projections, read_projection, Aggregate, Entity, EntityExt, Error, Item, ProjectionExt, QueryInput, QueryInputExt, Table};
use modyne::expr::Filter;
use modyne::model::ConditionalUpdate;
use powdrr_lib::data_contract::{CompactionCommit, CreateIndexTemplateBody, ExtensionCommit, IcebergCommit, SpeedboatCommit, TableMetadataCheckpoint};
use powdrr_lib::elastic_search_lifetime_policy::ILMPolicyDefinition;
use powdrr_lib::pipeline::PipelineDefinition;

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


impl DynamoDbConnector {
/*
    pub async fn upsert_address(
        &self,
        user_name: &UserNameRef,
        address_type: &str,
        input: Address,
    ) -> Result<(), Error> {
        let expression = expr::Update::new("SET #address.#address_type = :address")
            .name("#address", "address")
            .name("#address_type", address_type)
            .value(":address", input);

        Customer::update(user_name)
            .expression(expression)
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn get_customer_orders_page(
        &self,
        user_name: &UserNameRef,
        next: Option<Item>,
        limit: Option<u32>,
    ) -> Result<(CustomerOrders, Option<Item>), Error> {
        let query_input = CustomerOrdersQuery { user_name };

        let mut customer_orders = CustomerOrders::default();

        let result = query_input
            .query()
            .set_exclusive_start_key(next)
            .set_limit(limit)
            .execute(self)
            .await?;

        customer_orders.reduce(result.items.unwrap_or_default())?;

        Ok((customer_orders, result.last_evaluated_key))
    }

    pub async fn save_order(&self, order: Order, items: Vec<OrderItem>) -> Result<(), Error> {
        let mut builder = TransactWrite::new().operation(order.create());

        for item in items {
            builder = builder.operation(item.create());
        }

        let _result = builder.execute(self).await?;

        Ok(())
    }

    pub async fn update_order_status(
        &self,
        user_name: &UserNameRef,
        order_id: OrderId,
        status: OrderStatus,
    ) -> Result<(), Error> {
        let key = OrderKeyInput {
            user_name,
            order_id,
        };

        let expression = expr::Update::new("SET #status = :status")
            .name("#status", "status")
            .value(":status", status);

        Order::update(key)
            .expression(expression)
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn get_order(&self, order_id: OrderId) -> Result<OrderWithItems, Error> {
        let query_input = OrderWithItemsQuery { order_id };

        let mut order = OrderWithItems::default();
        let mut next = None;

        loop {
            let result = query_input
                .query()
                .set_exclusive_start_key(next)
                .execute(self)
                .await?;

            order.reduce(result.items.unwrap_or_default())?;

            let Some(last_evaluated_key) = result.last_evaluated_key else {
                break;
            };

            next = Some(last_evaluated_key);
        }

        Ok(order)
    }

 */
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


pub struct OrgIdNameInput<'a> {
    name: &'a String,
    org_id: &'a String,
}

pub struct OrgIdTableNameInput<'a> {
    name: &'a String,
    table_name: &'a String,
    org_id: &'a String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    name: String,
    org_id: String,
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
                fn [< private_create_ $entity_name _core >](&self, org_id: &String, name: &String, template: &$type_name) -> TransactWrite {
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

                    TransactWrite::new()
                        .operation(entity.create())
                        .operation(named_entity.create())
                }


                async fn [< private_create_ $entity_name >](&self, org_id: &String, name: &String, template: &$type_name) -> Result<(), Error> {
                    self.[< private_create_ $entity_name _core >](org_id, name, template)
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

                async fn [< private_delete_ $entity_name >](&self, org_id: &String, name: &String) -> Result<(), Error> {
                    [< PowdrrNamed $type_name >]::delete(OrgIdNameInput{ org_id: org_id, name: name }).execute(self).await?;
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
                async fn [< cached_create_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String, template: &$type_name) -> Result<(), Error> {
                    let key = CacheKey{ org_id: org_id.clone(), name: name.clone() };
                    cache.cache.insert(key.clone(), template.clone());
                    self.[< private_create_ $entity_name >](org_id, name, template).await
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


macro_rules! powdrr_named_cached_tracked_entity {
    ($entity_name:tt, $type_name:ident) => {
        powdrr_named_cached_entity_core!($entity_name, $type_name);
        paste::item! {
            #[derive(Debug, modyne_derive::EntityDef, serde::Serialize, serde::Deserialize, Clone)]
            pub struct [< PowdrrNamed $type_name Tracker >] {
                pub name: String,
                pub org_id: String,
                pub table_name: String,
                pub checkpointed: u8,
                pub indexed: u8,
                pub billed: u8,
            }

            impl Entity for [< PowdrrNamed $type_name Tracker >] {
                type KeyInput<'a> = OrgIdTableNameInput<'a>;
                type Table = DynamoDbConnector;
                type IndexKeys = ();

                fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
                    let common = format!("{}_TRACKER#{}#{}", stringify!($entity_name), input.org_id, input.table_name);
                    keys::Primary {
                        hash: common.clone(),
                        range: input.name.clone(),
                    }
                }

                fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
                    Self::primary_key(OrgIdTableNameInput{
                        name: &self.name,
                        table_name: &self.table_name,
                        org_id: &self.org_id
                    }).into()
                }
            }

            pub struct [< PowdrrNamed $type_name TrackerQuery >] {
                org_id: String,
                table_name: String,
            }

            impl QueryInput for [< PowdrrNamed $type_name TrackerQuery >] {
                type Index = keys::Primary;
                type Aggregate = SpeedboatCommitTrackers;

                fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
                    expr::KeyCondition::in_partition(format!("speedboat_commit_TRACKER#{}#{}", self.org_id, self.table_name))
                }
            }

            #[derive(Default)]
            pub struct [< PowdrrNamed $type_name TrackerResult >] {
                pub trackers: Vec<[< PowdrrNamed $type_name Tracker >]>,
            }

            projections! {
                pub enum [< PowdrrNamed $type_name TrackerResultProjection >] {
                    [< PowdrrNamed $type_name Tracker >],
                }
            }

            impl Aggregate for [< PowdrrNamed $type_name TrackerResult >] {
                type Projections = [< PowdrrNamed $type_name TrackerResultProjection >];

                fn merge(&mut self, item: Item) -> Result<(), Error> {
                    match read_projection!(item)? {
                        Self::Projections::[< PowdrrNamed $type_name Tracker >](tracker) => self.trackers.push(tracker),
                    }

                    Ok(())
                }
            }

            #[allow(dead_code)]
            impl DynamoDbConnector {
                pub async fn [< create_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, table_name: &String, name: &String, template: &$type_name) -> Result<(), Error> {
                    self.[< tracking_create_ $entity_name _core >](cache, org_id, table_name, name, template).await
                }

                async fn [< tracking_create_ $entity_name _core >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, table_name: &String, name: &String, template: &$type_name) -> Result<(), Error> {
                    let tracker = [< PowdrrNamed $type_name Tracker >] {
                        name: name.clone(),
                        org_id: org_id.clone(),
                        table_name: table_name.clone(),
                        checkpointed: 0,
                        indexed: 0,
                        billed: 0,
                    };
                    self.[< private_create_ $entity_name _core >](org_id, name, template)
                        .operation(tracker.create())
                        .execute(self)
                        .await?;

                    let key = CacheKey{ org_id: org_id.clone(), name: name.clone() };
                    cache.cache.insert(key.clone(), template.clone());

                    Ok(())
                }

                pub async fn [< describe_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String) -> Result<Option<$type_name>, Error> {
                    self.[< cached_describe_ $entity_name >](cache, org_id, name).await
                }

                pub async fn [< delete_ $entity_name >](&self, cache: &mut [< PowdrrNamed $type_name Cache >], org_id: &String, name: &String) -> Result<(), Error> {
                    self.[< cached_delete_ $entity_name >](cache, org_id, name).await
                    // TODO: delete tracker
                }

                pub async fn [< oldest_available_ $entity_name >](&self, org_id: &String, table_name: &String, limit: Option<u32>) -> Result<Vec<[< PowdrrNamed $type_name Tracker >]>, Error> {
                    let query_input = [< PowdrrNamed $type_name TrackerQuery >] { org_id: org_id.clone(), table_name: table_name.clone() };

                    let mut trackers = [< PowdrrNamed $type_name TrackerResult >]::default();

                    let result = query_input
                        .query()
                        .filter(Filter::new("checkpointed = :zero")
                            .value(":zero", 0)
                        )
                        .set_limit(limit)
                        .execute(self)
                        .await?;

                    trackers.reduce(result.items.unwrap_or_default())?;

                    Ok(trackers.trackers)
                }

                pub fn [< mark_checkpointed_ $entity_name >](org_id: &String, table_name: &String, name: &String) -> ConditionalUpdate {
                    let key = OrgIdTableNameInput {
                        name,
                        table_name,
                        org_id,
                    };
                    let expression = expr::Update::new("SET checkpointed = :one")
                        .value(":one", 1);
                    let condition = expr::Condition::new("checkpointed = :zero")
                        .value(":zero", 0);

                    PowdrrNamedSpeedboatCommitTracker::update(key).expression(expression).condition(condition)
                }
            }
        }
    };
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableBody {
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableCheckpointInfo {
    pub version: u64,
    pub checkpoint_id: String,
}


powdrr_named_entity!(alias, String);
powdrr_named_entity!(powdrr_table, TableBody);
powdrr_named_entity!(table_template, CreateIndexTemplateBody);
powdrr_named_entity!(pipeline, PipelineDefinition);
powdrr_named_entity!(lifetime_policy, ILMPolicyDefinition);
powdrr_named_entity!(latest, TableCheckpointInfo);

// Note: only things where a given key can only ever have one value are cacheable
powdrr_named_cached_entity!(compaction, CompactionCommit);
powdrr_named_cached_entity!(checkpoint, TableMetadataCheckpoint);

// Note: the primary value can never change. Only the tracker is updated
powdrr_named_cached_tracked_entity!(speedboat_commit, SpeedboatCommit);
powdrr_named_cached_tracked_entity!(iceberg_commit, IcebergCommit);
powdrr_named_cached_tracked_entity!(extension_commit, ExtensionCommit);


pub struct SpeedboatCommitsTrackerQuery {
    org_id: String,
    table_name: String,
}

#[derive(Default)]
pub struct SpeedboatCommitTrackers {
    pub trackers: Vec<PowdrrNamedSpeedboatCommitTracker>,
}

impl QueryInput for SpeedboatCommitsTrackerQuery {
    type Index = keys::Primary;
    type Aggregate = SpeedboatCommitTrackers;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        expr::KeyCondition::in_partition(format!("speedboat_commit_TRACKER#{}#{}", self.org_id, self.table_name))
    }
}

projections! {
    pub enum SpeedboatCommitTrackersEntities {
        PowdrrNamedSpeedboatCommitTracker,
    }
}

impl Aggregate for SpeedboatCommitTrackers {
    type Projections = SpeedboatCommitTrackersEntities;

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        match read_projection!(item)? {
            Self::Projections::PowdrrNamedSpeedboatCommitTracker(tracker) => self.trackers.push(tracker),
        }

        Ok(())
    }
}

impl DynamoDbConnector {
    pub async fn mark_checkpointed(
        &self,
        org_id: &String,
        table_name: &String,
        name: &String,
    ) -> Result<bool, Error> {
        match TransactWrite::new()
            .operation(DynamoDbConnector::mark_checkpointed_speedboat_commit(org_id, table_name, name))
            .execute(self)
            .await {
            Ok(_success) => {
                Ok(true)
            },
            Err(failure) => {
                if failure.as_service_error().unwrap().is_transaction_canceled_exception() {
                    Ok(false)
                } else {
                    Err(failure.into())
                }
            },
        }
    }

    pub async fn commit_checkpoint(
        &self,
        org_id: &String,
        input_latest: &TableCheckpointInfo,
        input_speedboat_trackers: &Vec<PowdrrNamedSpeedboatCommitTracker>,
        new_checkpoint: &TableMetadataCheckpoint,
    ) -> Result<bool, Error> {
        let mut transaction = TransactWrite::new();

        let key = OrgIdNameInput {
            name: &new_checkpoint.table_name,
            org_id,
        };
        let expression = expr::Update::new("SET entity.version = :plus_one, entity.checkpoint_id = :new_id")
            .value(":new_id", new_checkpoint.checkpoint_id.clone())
            .value(":plus_one", input_latest.version + 1);
        let condition = expr::Condition::new("entity.version = :old")
            .value(":old", input_latest.version);

        let operation = PowdrrNamedTableCheckpointInfo::update(key).expression(expression).condition(condition);
        transaction = transaction.operation(operation);

        for input_tracker in input_speedboat_trackers.iter() {
            transaction = transaction.operation(DynamoDbConnector::mark_checkpointed_speedboat_commit(
                &input_tracker.org_id,
                &input_tracker.table_name,
                &input_tracker.name,
            ));
        }

        let checkpoint_obj = PowdrrNamedTableMetadataCheckpoint {
            name: new_checkpoint.checkpoint_id.clone(),
            org_id: org_id.clone(),
            entity: new_checkpoint.clone(),
        };
        transaction = transaction.operation(checkpoint_obj.create());

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
    use powdrr_lib::data_contract::SpeedboatCommitTableInfo;
    use powdrr_lib::schema_massager::PowdrrSchema;
    use super::*;
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

        connector.create_speedboat_commit(&mut cache, &"fake_org".to_string(), &"fake_table".to_string(), &"fake_id".to_string(), &commit).await.unwrap();

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

        let trackers = connector.oldest_available_speedboat_commit(&"fake_org".to_string(), &"fake_table".to_string(), None).await.unwrap();
        assert_eq!(trackers.len(), 1);

        let success = connector.mark_checkpointed(&"fake_org".to_string(), &"fake_table".to_string(), &"fake_id".to_string()).await.unwrap();
        assert_eq!(success, true);

        let trackers = connector.oldest_available_speedboat_commit(&"fake_org".to_string(), &"fake_table".to_string(), None).await.unwrap();
        assert_eq!(trackers.len(), 0);

        let second_success = connector.mark_checkpointed(&"fake_org".to_string(), &"fake_table".to_string(), &"fake_id".to_string()).await.unwrap();
        assert_eq!(second_success, false);
    }

    #[tokio::test]
    async fn test_update_checkpoint() {
        let connector = create_connector().await;

        let mut checkpoint_cache = PowdrrNamedTableMetadataCheckpointCache::new();
        let mut speedboat_cache = PowdrrNamedSpeedboatCommitCache::new();

        let first_checkpoint = TableMetadataCheckpoint {
            table_name: "fake_table".to_string(),
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

        connector.create_latest(&"fake_org".to_string(), &first_checkpoint.table_name, &TableCheckpointInfo { version: 0, checkpoint_id: first_checkpoint.checkpoint_id.clone() }).await.unwrap();
        connector.create_checkpoint(&mut checkpoint_cache, &"fake_org".to_string(), &first_checkpoint.checkpoint_id, &first_checkpoint).await.unwrap();
        connector.create_speedboat_commit(&mut speedboat_cache, &"fake_org".to_string(), &"fake_table".to_string(), &"fake_id".to_string(), &speedboat_commit).await.unwrap();

        let latest_speedboat_trackers = connector.oldest_available_speedboat_commit(&"fake_org".to_string(), &"fake_table".to_string(), None).await.unwrap();
        assert_eq!(latest_speedboat_trackers.len(), 1);

        let latest_checkpoint_info = connector.describe_latest(&"fake_org".to_string(), &"fake_table".to_string()).await.unwrap().unwrap();
        let latest_checkpoint = connector.describe_checkpoint(&mut checkpoint_cache, &"fake_org".to_string(), &latest_checkpoint_info.checkpoint_id).await.unwrap().unwrap();

        let mut latest_speedboats = vec!();
        for speedboat_tracker in latest_speedboat_trackers.iter() {
            latest_speedboats.push(connector.describe_speedboat_commit(&mut speedboat_cache, &"fake_org".to_string(), &speedboat_tracker.name).await.unwrap().unwrap());
        }

        let new_checkpoint = latest_checkpoint.clone_and_apply(
            &latest_speedboats,
            &vec!(),
            &vec!(),
            &HashMap::new()
        );

        match connector.commit_checkpoint(
            &"fake_org".to_string(),
            &latest_checkpoint_info,
            &latest_speedboat_trackers,
            &new_checkpoint,
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
        assert_eq!(new_latest.checkpoint_id, new_checkpoint.checkpoint_id);
    }
}



/*
pub struct CustomerOrdersQuery<'a> {
    user_name: &'a UserNameRef,
}

impl QueryInput for CustomerOrdersQuery<'_> {
    type Index = keys::Primary;
    type Aggregate = CustomerOrders;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        expr::KeyCondition::in_partition(format!("CUSTOMER#{}", self.user_name))
    }
}

#[derive(Debug, modyne_derive::EntityDef, serde::Serialize, serde::Deserialize)]
struct CustomerEmail {
    user_name: UserName,
    email: UserEmail,
}

impl Entity for CustomerEmail {
    type KeyInput<'a> = &'a UserEmailRef;
    type Table = DynamoDbConnector;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("CUSTOMEREMAIL#{}", input);
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        Self::primary_key(&self.email).into()
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct OrderId(String);

impl OrderId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self("foo_bar_random_string".to_string())
    }
}

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, modyne_derive::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Order {
    pub user_name: UserName,
    pub order_id: OrderId,
    //#[serde(with = "time::serde::rfc3339")]
    //pub created_at: time::OffsetDateTime,
    pub number_of_items: u32,
    pub amount: f32,
    pub status: OrderStatus,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Accepted,
    Canceled,
    Shipped,
    Delivered,
}

pub struct OrderKeyInput<'a> {
    user_name: &'a UserNameRef,
    order_id: OrderId,
}

impl Entity for Order {
    type KeyInput<'a> = OrderKeyInput<'a>;
    type Table = DynamoDbConnector;
    type IndexKeys = keys::Gsi1;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("CUSTOMER#{}", input.user_name),
            range: format!("#ORDER#{}", input.order_id),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(OrderKeyInput {
                user_name: &self.user_name,
                order_id: self.order_id.clone(),
            }),
            indexes: keys::Gsi1 {
                hash: format!("ORDER#{}", self.order_id),
                range: format!("ORDER#{}", self.order_id),
            },
        }
    }
}

#[braid(serde)]
pub struct ItemId;

#[derive(Debug, modyne_derive::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct OrderItem {
    pub order_id: String,
    pub item_id: ItemId,
    pub description: String,
    pub price: f32,
}

#[derive(Debug)]
pub struct OrderItemKeyInput<'a> {
    order_id: String,
    item_id: &'a ItemIdRef,
}

impl Entity for OrderItem {
    type KeyInput<'a> = OrderItemKeyInput<'a>;
    type Table = DynamoDbConnector;
    type IndexKeys = keys::Gsi1;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("ORDER#{}", input.order_id),
            range: format!("ORDER#{}#ITEM#{}", input.order_id, input.item_id),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(OrderItemKeyInput {
                order_id: self.order_id.clone(),
                item_id: &self.item_id,
            }),
            indexes: keys::Gsi1 {
                hash: format!("ORDER#{}", self.order_id),
                range: format!("ITEM#{}", self.item_id),
            },
        }
    }
}

/// A projection of customer data that does not include address information.
#[derive(Debug, modyne_derive::Projection, serde::Serialize, serde::Deserialize)]
#[entity(Customer)]
pub struct CustomerHeader {
    pub user_name: UserName,
    pub name: String,
    pub email: UserEmail,
}

#[derive(Debug, Default)]
pub struct CustomerOrders {
    pub orders: Vec<Order>,
    pub customer: Option<CustomerHeader>,
}

pub struct CustomerOrdersQuery<'a> {
    user_name: &'a UserNameRef,
}

impl QueryInput for CustomerOrdersQuery<'_> {
    type Index = keys::Primary;
    type Aggregate = CustomerOrders;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        expr::KeyCondition::in_partition(format!("CUSTOMER#{}", self.user_name))
    }
}

projections! {
    pub enum CustomerOrdersEntities {
        Order,
        CustomerHeader,
    }
}

impl Aggregate for CustomerOrders {
    type Projections = CustomerOrdersEntities;

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        match read_projection!(item)? {
            Self::Projections::Order(order) => self.orders.push(order),
            Self::Projections::CustomerHeader(header) => self.customer = Some(header),
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct OrderWithItems {
    pub order: Option<Order>,
    pub items: Vec<OrderItem>,
}

pub struct OrderWithItemsQuery {
    pub order_id: OrderId,
}

impl QueryInput for OrderWithItemsQuery {
    type Index = keys::Gsi1;
    type Aggregate = OrderWithItems;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        expr::KeyCondition::in_partition(format!("ORDER#{}", self.order_id))
    }
}

projections! {
    pub enum OrderWithItemsEntities {
        Order,
        OrderItem,
    }
}

impl Aggregate for OrderWithItems {
    type Projections = OrderWithItemsEntities;

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        match read_projection!(item)? {
            Self::Projections::Order(order) => self.order = Some(order),
            Self::Projections::OrderItem(item) => self.items.push(item),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::AttributeValue;

    use super::*;

    #[test]
    fn verify_user_orders_entities_projection_expression() {
        assert_eq!(
            <CustomerOrdersEntities as modyne::ProjectionSet>::projection_expression(),
            Some(expr::StaticProjection {
                expression: "user_name,order_id,created_at,number_of_items,amount,#prj_000,#prj_001,email,entity_type",
                names: &[
                    ("#prj_000", "status"),
                    ("#prj_001", "name"),
                ],
            })
        );
    }

    #[test]
    fn verify_order_with_items_entities_projection_expression() {
        assert_eq!(
        <OrderWithItemsEntities as modyne::ProjectionSet>::projection_expression(),
        Some(expr::StaticProjection {
            expression: "user_name,order_id,created_at,number_of_items,amount,#prj_000,item_id,description,price,entity_type",
            names: &[
                ("#prj_000", "status"),
            ],
        }),
    );
    }

    #[test]
    fn verify_order_entity_full_item_serializes_as_expected() {
        let order_id = "1VrgXBQ0VCshuQUnh1HrDIHQNwY".parse().unwrap();
        let order = Order {
            user_name: UserName::from_static("alexdebrie"),
            order_id,
            //created_at: time::OffsetDateTime::from_unix_timestamp(1578016664).unwrap(),
            number_of_items: 7,
            status: OrderStatus::Shipped,
            amount: 67.43,
        };

        let item = order.into_item();

        assert_eq!(item["PK"].as_s().unwrap(), "CUSTOMER#alexdebrie");
        assert_eq!(
            item["SK"].as_s().unwrap(),
            "#ORDER#1VrgXBQ0VCshuQUnh1HrDIHQNwY"
        );
        assert_eq!(
            item["GSI1PK"].as_s().unwrap(),
            "ORDER#1VrgXBQ0VCshuQUnh1HrDIHQNwY"
        );
        assert_eq!(
            item["GSI1SK"].as_s().unwrap(),
            "ORDER#1VrgXBQ0VCshuQUnh1HrDIHQNwY"
        );
        assert_eq!(item["entity_type"].as_s().unwrap(), "order");
        assert_eq!(item["user_name"].as_s().unwrap(), "alexdebrie");
        assert_eq!(
            item["order_id"].as_s().unwrap(),
            "1VrgXBQ0VCshuQUnh1HrDIHQNwY"
        );
        assert_eq!(item["created_at"].as_s().unwrap(), "2020-01-03T01:57:44Z");
        assert_eq!(item["number_of_items"].as_n().unwrap(), "7");
        assert_eq!(item["status"].as_s().unwrap(), "SHIPPED");
        assert_eq!(item["amount"].as_n().unwrap(), "67.43");
        assert_eq!(item.len(), 11);
    }

    #[test]
    fn verify_customer_orders_entity_hydrates_as_expected() {
        #[allow(non_snake_case)]
        fn Str(s: &str) -> AttributeValue {
            AttributeValue::S(s.to_string())
        }

        #[allow(non_snake_case)]
        fn Num(s: &str) -> AttributeValue {
            AttributeValue::N(s.to_string())
        }

        let items = [
            [
                ("entity_type", Str("customer")),
                ("user_name", Str("alexdebrie")),
                ("name", Str("Alex DeBrie")),
                ("email", Str("alexdebrie1@gmail.com")),
            ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<Item>(),
            [
                ("entity_type", Str("order")),
                ("user_name", Str("alexdebrie")),
                ("order_id", Str("1VwVAvJk1GvBFfpTAjm0KG7Cg9d")),
                ("created_at", Str("2020-01-04T18:53:24Z")),
                ("number_of_items", Num("2")),
                ("status", Str("CANCELED")),
                ("amount", Num("12.43")),
            ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            [
                ("entity_type", Str("order")),
                ("user_name", Str("alexdebrie")),
                ("order_id", Str("1VrgXBQ0VCshuQUnh1HrDIHQNwY")),
                ("created_at", Str("2020-01-03T01:57:44Z")),
                ("number_of_items", Num("7")),
                ("status", Str("SHIPPED")),
                ("amount", Num("67.43")),
            ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        ];

        let mut customer_orders = CustomerOrders::default();

        for item in items {
            customer_orders.merge(item).unwrap();
        }

        assert!(customer_orders.customer.is_some());
        assert_eq!(customer_orders.orders.len(), 2);
    }
}
*/
