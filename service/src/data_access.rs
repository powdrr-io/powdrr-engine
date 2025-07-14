use std::{path::Path, sync::Arc};
use std::num::NonZero;
use datafusion::{arrow::array::RecordBatch, error::DataFusionError, prelude::{DataFrame, NdJsonReadOptions, ParquetReadOptions, SessionContext}};
use datafusion::common::HashMap;
use idgenerator::IdInstance;
use lru::LruCache;
use object_store::{aws::{AmazonS3, AmazonS3Builder}, ObjectStore};
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
        .with_bucket_name("icebergdata")
        .with_allow_http(true)
        .build().unwrap();

    Arc::new(s3_file_system)
}

static S3_FILE_STORE: std::sync::LazyLock<Arc<AmazonS3>> = std::sync::LazyLock::new(|| create_store());


fn create_session() -> SessionContext {
    let ctx = SessionContext::new();

    let s3_url = Url::parse("s3://icebergdata").unwrap();  

    ctx.register_object_store(&s3_url, S3_FILE_STORE.clone());

    ctx
}


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
}


struct CacheTrackerActor {
    receiver: mpsc::Receiver<CacheTrackerActorMessage>,
    lru_cache: LruCache<String, u64>,
    related: HashMap<String, Vec<String>>,
    reservations: HashMap<String, u64>,
    top_level_to_delete: Vec<String>,
}

impl CacheTrackerActor {
    pub fn new(receiver: mpsc::Receiver<CacheTrackerActorMessage>) -> Self {
        Self {
            receiver,
            lru_cache: LruCache::new(NonZero::new(10 ).unwrap()),
            related: HashMap::new(),
            reservations: HashMap::new(),
            top_level_to_delete: vec!()
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
        println!("Dropped table {} from cache", name);
        self.reservations.remove(name);
    }

    async fn handle_message(&mut self, msg: CacheTrackerActorMessage) {
        match msg {
            CacheTrackerActorMessage::Reserve { respond_to, top_level_name, related_names, total_size } => {
                // Increment the reservation count on the top level.
                self.increment_reservation(&top_level_name);

                // Touch the top level file in the LRU to load it or keep it fresh.
                // This will also update the total size for this top level file in the LRU.
                // That can happen if extension files have been generated since this file was
                // first loaded.
                match self.lru_cache.push(top_level_name.clone(), total_size) {
                    Some((existing_key, _)) => {
                        if existing_key != top_level_name {
                            // This means a different top level name has been pushed out of the LRU.
                            // We need to schedule it for deletion.
                            self.top_level_to_delete.push(existing_key);
                        }
                    },
                    None => ()
                };

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
                if self.decrement_reservation(&top_level_name) {
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
}


static DATA_FUSION_CONTEXT: std::sync::LazyLock<SessionContext> = std::sync::LazyLock::new(|| create_session());
static LRU_CACHE_HANDLE: std::sync::LazyLock<LRUCacheHandle> = std::sync::LazyLock::new(|| LRUCacheHandle::new());


pub(crate) async fn reserve(top_level_name: &String, total_size: u64, related_names: Vec<String>) -> () {
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
            match DATA_FUSION_CONTEXT.sql(&query_str).await {
                Err(e) => {
                    println!("Transient s3 error? {}", e);
                    let _ = DATA_FUSION_CONTEXT.sql(format!("DROP TABLE IF EXISTS {local_name_var};").as_str()).await;
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
            _ => Ok(())
        }
    }
}


async fn load_json_file_as_table(file_path: &String, local_name: &String) -> Result<(), DataFusionError> {
    match DATA_FUSION_CONTEXT.table_exist(local_name) {
        Ok(exists) => match exists {
            true => return Ok(()),
            false => ()
        },
        Err(e) => return log_err(e),
    };     
    match DATA_FUSION_CONTEXT.register_json(local_name, file_path, NdJsonReadOptions::default()).await {
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

pub(crate) async fn load_file_as_table(new_local_name: &String, file_path: &String, parquet: bool) -> Result<(), DataFusionError> {
    if parquet {
        match load_parquet_file_as_table(&file_path, &new_local_name).await {
            Err(e) => return log_err(e),
            Ok(_) => println!("Loaded parquet table {} from {}", new_local_name, file_path)
        }
    } else {
        match load_json_file_as_table(file_path, &new_local_name).await {
            Err(e) => return log_err(e),
            Ok(_) => println!("Loaded json table {} from {}", new_local_name, file_path)
        }
    }
    Ok(())
}

pub(crate) async fn load_memtable(records: &Vec<RecordBatch>) -> Result<String, DataFusionError> {
    if records.len() == 0 {
        panic!("Do not call this if you have no records");
    }
    let schema = records.get(0).unwrap().schema();
    let table = match datafusion::datasource::MemTable::try_new(schema, vec!(records.to_vec())) {
        Ok(t) => Arc::new(t),
        Err(e) => return log_err(e),
    };
    loop {
        let result_table_name = &format!("table_{}", IdInstance::next_id().to_string());
        match DATA_FUSION_CONTEXT.table_exist(result_table_name) {
            Ok(exists) => {
                if !exists {
                    match DATA_FUSION_CONTEXT.register_table(result_table_name, table) {
                        Ok(_) => return Ok(result_table_name.clone()),
                        Err(e) => return log_err(e)
                    }
                }
            },
            Err(e) => return log_err(e)
        }
    }    
}


pub(crate) async fn execute_sql(sql: &String) -> Result<DataFrame, DataFusionError> {
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