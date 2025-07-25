use std::{path::Path, sync::Arc};
use std::time::Duration;
use datafusion::{arrow::array::RecordBatch, error::DataFusionError, prelude::{DataFrame, NdJsonReadOptions, ParquetReadOptions, SessionContext}};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::HashMap;
use datafusion::config::ConfigOptions;
use datafusion::prelude::SessionConfig;
use idgenerator::IdInstance;
use liquid_cache_parquet::cache::policies::DiscardPolicy;
use liquid_cache_parquet::common::LiquidCacheMode;
use liquid_cache_parquet::LiquidCacheInProcessBuilder;
use lru_mem::{HeapSize, LruCache, TryInsertError};
use object_store::{aws::{AmazonS3, AmazonS3Builder}, ObjectStore};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};
use url::Url;

use crate::util::log_err;


const S3_ENDPOINT_VALUE: &str = "http://localhost:9000";
const S3_ACCESS_KEY_ID_VALUE: &str = "admin";
const S3_SECRET_ACCESS_KEY_VALUE: &str = "password";
const S3_REGION_VALUE: &str = "us-east-1";


fn create_store() -> Arc<AmazonS3> {
    let s3_file_system: object_store::aws::AmazonS3 = AmazonS3Builder::new()
        .with_access_key_id(S3_ACCESS_KEY_ID_VALUE)
        .with_secret_access_key(S3_SECRET_ACCESS_KEY_VALUE)
        .with_region(S3_REGION_VALUE)
        .with_endpoint(S3_ENDPOINT_VALUE)
        .with_bucket_name("warehouse")
        .with_allow_http(true)
        .build().unwrap();

    Arc::new(s3_file_system)
}

static S3_FILE_STORE: std::sync::LazyLock<Arc<AmazonS3>> = std::sync::LazyLock::new(|| create_store());


fn create_session() -> SessionContext {
    let options = ConfigOptions::default();
    // UNCOMMENT TO ENABLE 'SHOW TABLES'
    //options.set("datafusion.catalog.information_schema", "true").unwrap();

    let config = SessionConfig::from(options);

    let temp_dir = TempDir::new().unwrap();

    let (ctx, _) = match LiquidCacheInProcessBuilder::new()
        .with_max_cache_bytes(10 * 1024 * 1024 * 1024) // 1GB
        .with_cache_dir(temp_dir.path().to_path_buf())
        .with_cache_mode(LiquidCacheMode::Liquid {
            transcode_in_background: true,
        })
        .with_cache_strategy(Box::new(DiscardPolicy))
        .build(config) {
        Ok(ctx) => ctx,
        Err(e) => panic!("Failed to create session: {}", e),
    };

    //let ctx = SessionContext::new_with_config(config);

    let s3_url = Url::parse("s3://warehouse").unwrap();

    ctx.register_object_store(&s3_url, S3_FILE_STORE.clone());

    ctx
}


#[allow(dead_code)]
enum CacheTrackerActorMessage {
    Reserve {
        respond_to: oneshot::Sender<()>,
        top_level_name: String,
        related_names: Vec<String>,
        total_size: u64
    },
    Release {
        respond_to: oneshot::Sender<()>,
        top_level_name: String,
    },
    TableCreated {
        respond_to: oneshot::Sender<()>,
        table_name: String,
    },
    TableDropped {
        respond_to: oneshot::Sender<()>,
        table_name: String,
    },
    GetTables {
        respond_to: oneshot::Sender<Vec<String>>,
    }
}


struct HeapSizeTracker {
    size: u64,
}

impl HeapSize for HeapSizeTracker {
    fn heap_size(&self) -> usize {
        self.size as usize
    }
}


struct CacheTrackerActor {
    receiver: mpsc::Receiver<CacheTrackerActorMessage>,
    lru_cache: LruCache<String, HeapSizeTracker>,
    related: HashMap<String, Vec<String>>,
    reservations: HashMap<String, u64>,
    top_level_to_delete: Vec<String>,
    existing_tables: Vec<String>,
}

impl CacheTrackerActor {
    pub fn new(receiver: mpsc::Receiver<CacheTrackerActorMessage>) -> Self {
        Self {
            receiver,
            lru_cache: LruCache::new(2 * 1024 * 1024 * 1024),
            related: HashMap::new(),
            reservations: HashMap::new(),
            top_level_to_delete: vec!(),
            existing_tables: vec!(),
        }
    }

    fn increment_reservation(&mut self, name: &String) -> () {
        match self.reservations.get_mut(name) {
            Some(r) => {
                *r += 1;
            },
            None => {
                self.reservations.insert(name.clone(), 1);
            }
        }
    }

    fn decrement_reservation(&mut self, name: &String) -> bool {
        match self.reservations.get_mut(name) {
            Some(r) => {
                *r -= 1;
                *r == 0
            },
            None => panic!("Tried to decrement reservation for {} but it doesn't exist", name)
        }
    }

    async fn drop(&mut self, name: &String) -> () {
        let _ = DATA_FUSION_CONTEXT.sql(format!("DROP TABLE IF EXISTS {};", name).as_str()).await;
        // assert!(self.existing_tables.contains(&name));
        self.existing_tables.retain(|n| n != name);
        self.reservations.remove(name);
        assert!(self.existing_tables.len() >= self.reservations.len());
    }

    #[allow(unused_assignments)]
    async fn handle_message(&mut self, msg: CacheTrackerActorMessage) {
        match msg {
            CacheTrackerActorMessage::Reserve { respond_to, top_level_name, related_names, total_size } => {
                // Increment the reservation count on the top level.
                self.increment_reservation(&top_level_name);

                // Touch the top level file in the LRU to load it or keep it fresh.
                // This will also update the total size for this top level file in the LRU.
                // That can happen if extension files have been generated since this file was
                // first loaded.

                // TODO: This is an optimistic add impl which is probably totally misguided since
                // under normal operation the LRU is always full. This should be replaced
                // with something that assumes that removes are necessary.
                assert!(total_size > 0);
                loop {
                    let mut local_total_size = total_size;
                    match self.lru_cache.try_insert(top_level_name.clone(), HeapSizeTracker { size: local_total_size }) {
                        Err(err) => match err {
                            TryInsertError::EntryTooLarge { key: _, value: _, entry_size: _, max_size: _ } => panic!("Files with top level {} is too large to fit in the LRU", top_level_name),
                            TryInsertError::OccupiedEntry { key, value } => {
                                local_total_size = if local_total_size > value.size { local_total_size } else { value.size };
                                self.lru_cache.remove(&key);
                            },
                            TryInsertError::WouldEjectLru { key: _, value: _, entry_size: _, free_memory: _ } => {
                                match self.lru_cache.remove_lru() {
                                    Some((key, value)) => {
                                        assert!(value.size > 0);
                                        self.top_level_to_delete.push(key.clone());
                                    },
                                    None => panic!("LRU cache is empty")
                                }
                            }
                        },
                        Ok(_) => break
                    }
                }

                // Ensure the related files are tracked appropriately.
                match self.related.get_mut(&top_level_name) {
                    Some(existing_related_names) => {
                        // Add any new related files to the list.
                        // TODO: This is O(n^2) but we expect the number of related files to be small.
                        // If it becomes a problem, we can optimize this.
                        for related_name in related_names.iter() {
                            if !existing_related_names.contains(related_name) {
                                existing_related_names.push(related_name.clone());
                            }
                        }
                    },
                    None => {
                        self.related.insert(top_level_name.clone(), related_names.clone());
                    }
                };
                let _ = respond_to.send(());
            },
            CacheTrackerActorMessage::Release { respond_to, top_level_name} => {
                self.decrement_reservation(&top_level_name);

                let mut to_delete = vec!();
                for possible_delete in self.top_level_to_delete.iter_mut() {
                    let should_drop = self.reservations.get_mut(possible_delete).unwrap_or(&mut 0) == &0;
                    if should_drop {
                        to_delete.push(possible_delete.clone());
                    }
                }
                self.top_level_to_delete.retain(|name| !to_delete.contains(name));

                for top_level_name in to_delete {
                    self.drop(&top_level_name).await;
                    let related_names = self.related.get(&top_level_name)
                        .map(|names| names.clone())
                        .unwrap_or_default();
                    for related_name in related_names {
                        self.drop(&related_name).await;
                    }
                    self.related.remove(&top_level_name);
                }
                let _ = respond_to.send(());
            },
            CacheTrackerActorMessage::TableCreated { respond_to, table_name } => {
                if !self.existing_tables.contains(&table_name) {
                    self.existing_tables.push(table_name.clone());
                }
                let _ = respond_to.send(());
            },
            CacheTrackerActorMessage::TableDropped { respond_to, table_name } => {
                assert!(self.existing_tables.contains(&table_name));
                self.existing_tables.retain(|name| name != &table_name);
                let _ = respond_to.send(());
            },
            CacheTrackerActorMessage::GetTables { respond_to } => {
                let _ = respond_to.send(self.existing_tables.clone());
            }
        }
    }
}


#[derive(Clone)]
pub struct LRUCacheHandle {
    sender: mpsc::Sender<CacheTrackerActorMessage>,
}

async fn run_lru_cache_actor_message_pump(mut actor: CacheTrackerActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg).await;
    }
}

impl LRUCacheHandle {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let actor = CacheTrackerActor::new(receiver);
        tokio::spawn(run_lru_cache_actor_message_pump(actor));
        Self { sender }
    }

    async fn reserve(&self, top_level_name: &String, size: u64, related_names: Vec<String>) -> () {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::Reserve {
            respond_to: send,
            top_level_name: top_level_name.clone(),
            total_size: size,
            related_names
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn release(&self, top_level_name: &String) -> () {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::Release {
            respond_to: send,
            top_level_name: top_level_name.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn table_created(&self, table_name: &String) -> () {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::TableCreated {
            respond_to: send,
            table_name: table_name.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn table_dropped(&self, table_name: &String) -> () {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::TableDropped {
            respond_to: send,
            table_name: table_name.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    #[allow(dead_code)]
    async fn get_tables(&self) -> Vec<String> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::GetTables {
            respond_to: send,
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }
}


static DATA_FUSION_CONTEXT: std::sync::LazyLock<SessionContext> = std::sync::LazyLock::new(|| create_session());
static LRU_CACHE_HANDLE: std::sync::LazyLock<LRUCacheHandle> = std::sync::LazyLock::new(|| LRUCacheHandle::new());


pub(crate) async fn reserve(top_level_name: &String, total_size: u64, related_names: Vec<String>) -> () {
    assert!(total_size > 0);
    LRU_CACHE_HANDLE.reserve(top_level_name, total_size, related_names).await
}

pub(crate) async fn release(top_level_name: &String) -> () {
    LRU_CACHE_HANDLE.release(top_level_name).await
}

async fn load_parquet_file_as_table(file_path: &String, local_name: &String) -> Result<(), DataFusionError> {
    match DATA_FUSION_CONTEXT.table_exist(local_name) {
        Ok(exists) => match exists {
            true => return Ok(()),
            false => ()
        },
        Err(e) => return log_err(e),
    };    
    if file_path.starts_with("s3:") {
        let file_path_var = file_path;
        let local_name_var = local_name;
        let query_str = format!(r#"CREATE EXTERNAL TABLE {local_name_var}
        STORED AS PARQUET
        LOCATION '{file_path_var}';"#);
        loop {
            LRU_CACHE_HANDLE.table_created(local_name_var).await;
            match DATA_FUSION_CONTEXT.sql(&query_str).await {
                Err(e) => {
                    println!("Transient s3 error? {}", e);
                    let _ = DATA_FUSION_CONTEXT.sql(format!("DROP TABLE IF EXISTS {local_name_var};").as_str()).await;
                    LRU_CACHE_HANDLE.table_dropped(local_name_var).await;
                },
                _ => return Ok(())
            }
        }
    } else {
        let result = DATA_FUSION_CONTEXT.register_parquet(local_name, file_path, ParquetReadOptions::new()).await;
        match result {
            Err(e) => {
                if e.message().contains("already exists") {
                    Ok(())
                } else {
                    log_err(e)
                }
            },
            _ => {
                LRU_CACHE_HANDLE.table_created(local_name).await;
                Ok(())
            }
        }
    }
}

async fn load_json_file_as_table(file_path: &String, local_name: &String, schema: &Schema) -> Result<(), DataFusionError> {
    match DATA_FUSION_CONTEXT.table_exist(local_name) {
        Ok(exists) => match exists {
            true => return Ok(()),
            false => ()
        },
        Err(e) => return log_err(e),
    };
    LRU_CACHE_HANDLE.table_created(local_name).await;
    let reader_options = NdJsonReadOptions::default().schema(&schema);
    match DATA_FUSION_CONTEXT.register_json(local_name, file_path, reader_options).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.message().contains("already exists") {
                Ok(())
            } else {
                Err(e)
            }
        }
    }
}


pub(crate) fn path_to_table_name(file_path: &String) -> String {
    let safe_name = file_path
        .replace("/", "_")
        .replace(".", "_")
        .replace(":", "_")
        .replace("-", "_");
    format!("table_{}", safe_name)   
}

pub(crate) async fn load_file_as_table(new_local_name: &String, file_path: &String, parquet: bool, schema: Option<Schema>) -> Result<(), DataFusionError> {
    if parquet {
        match load_parquet_file_as_table(&file_path, &new_local_name).await {
            Err(e) => return log_err(e),
            Ok(_) => ()
        }
    } else {
        assert!(schema.is_some(), "You must provide a schema for a JSON file");
        match load_json_file_as_table(file_path, &new_local_name, &schema.unwrap()).await {
            Err(e) => return log_err(e),
            Ok(_) => ()
        }
    }
    Ok(())
}


#[allow(dead_code)]
pub(crate) async fn load_json_as_memtable(file_path: &String, local_name: &String, schema: &Schema) -> Result<(), DataFusionError> {
    let final_file_path = if file_path.starts_with("file://") {
        file_path.replace("file://", "")
    } else {
        file_path.clone()
    };

    let file_contents = match std::fs::read(final_file_path) {
        Ok(c) => c,
        Err(_) => panic!("Could not read file {}", file_path)
    };

    let json_reader = match arrow_json::ReaderBuilder::new(Arc::new(schema.clone())).build(file_contents.as_slice()) {
        Ok(d) => d,
        Err(_) => panic!("Private API returned result that does not match schema")
    };

    let record_batches: Vec<RecordBatch> = match json_reader.collect() {
        Ok(batches) => batches,
        Err(e) => return log_err(DataFusionError::ArrowError(e, None))
    };

    load_memtable_with_name(local_name, &record_batches).await
}

pub(crate) async fn load_memtable(records: &Vec<RecordBatch>) -> Result<String, DataFusionError> {
    let result_table_name = format!("table_{}", IdInstance::next_id().to_string());
    load_memtable_with_name(&result_table_name, records).await?;
    Ok(result_table_name)
}

pub(crate) async fn load_memtable_with_name(result_table_name: &String, records: &Vec<RecordBatch>) -> Result<(), DataFusionError> {
    if records.len() == 0 {
        panic!("Do not call this if you have no records");
    }
    let schema = records.get(0).unwrap().schema();
    let table = match datafusion::datasource::MemTable::try_new(schema, vec!(records.to_vec())) {
        Ok(t) => Arc::new(t),
        Err(e) => return log_err(e),
    };
    loop {
        match DATA_FUSION_CONTEXT.table_exist(result_table_name) {
            Ok(exists) => {
                if !exists {
                    LRU_CACHE_HANDLE.table_created(result_table_name).await;
                    match DATA_FUSION_CONTEXT.register_table(result_table_name, table) {
                        Ok(_) => return Ok(()),
                        Err(e) => return log_err(e)
                    }
                } else {
                    return Ok(())
                }
            },
            Err(e) => return log_err(e)
        }
    }    
}


const NUM_TRIES: u32 = 4;


pub(crate) async fn execute_sql(sql: &String) -> Result<DataFrame, DataFusionError> {
    assert!(!sql.to_lowercase().contains("create table"), "Use the create_table function instead");
    assert!(!sql.to_lowercase().contains("create external table"), "Use the create_table function instead");
    assert!(!sql.to_lowercase().contains("drop table"), "Use the drop function instead");
    for try_num in 1..=NUM_TRIES {
        match private_execute_sql(sql).await {
            Ok(df) => return Ok(df),
            Err(e) => {
                if try_num == NUM_TRIES {
                    return Err(e)
                } else {
                    match e {
                        // The metadata tracking means that in normal operation we'll never ask for an S3 object
                        // that we don't have a record of. Therefore most likely if there is an issue
                        // fetching an object it is some eventually consistency or rate limiting issue.
                        // We'll do some exponential backoff and hope that the issue resolves itself.
                        DataFusionError::ParquetError(_) => {
                            tokio::time::sleep(Duration::from_millis(3_u64.pow(try_num))).await;
                        }
                        DataFusionError::ObjectStore(_) => {
                            tokio::time::sleep(Duration::from_millis(3_u64.pow(try_num))).await;
                        }
                        _ => return Err(e)
                    }
                }
            }
        }
    }
    // Unreachable
    panic!("Should not reach this point");
}


pub(crate) async fn create_table(table_name: &String, sql: &String) -> Result<(), DataFusionError> {
    LRU_CACHE_HANDLE.table_created(table_name).await;
    match private_execute_sql(&format!("CREATE TABLE {} AS {}", table_name, sql)).await {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn private_execute_sql(sql: &String) -> Result<DataFrame, DataFusionError> {
    match DATA_FUSION_CONTEXT.sql(sql).await {
        Ok(d) => Ok(d),
        Err(e) => log_err(e)
    }
}


pub(crate) async fn exists(path: &String) -> bool {
    if path.starts_with("s3://") {
        let path_only = path[17..].to_string();
        match S3_FILE_STORE.as_ref().get(&object_store::path::Path::parse(path_only).unwrap()).await {
            Ok(_) => true,
            Err(_) => false
        }
    } else {
        Path::new(path).exists()
    }
}

pub(crate) async fn drop(table_name: &String) -> () {
    LRU_CACHE_HANDLE.table_dropped(table_name).await;
    match DATA_FUSION_CONTEXT.sql(format!("DROP TABLE IF EXISTS {};", table_name).as_str()).await {
        Ok(_) => (),
        Err(e) => panic!("Failed to drop table {}: {}", table_name, e)
    }
}

#[allow(dead_code)]
pub(crate) async fn get_tracked_tables() -> Vec<String> {
    LRU_CACHE_HANDLE.get_tables().await
}

#[allow(dead_code)]
pub(crate) async fn print_datafusion_tables() -> () {
    let table_df = match DATA_FUSION_CONTEXT.sql("show tables;").await {
        Ok(df) => df,
        Err(e) => {
            panic!("Failed to show tables: {}", e)
        }
    };

    table_df.show().await.unwrap();
}
