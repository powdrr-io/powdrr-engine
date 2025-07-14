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


#[derive(Debug, Clone)]
struct LRUCachePutResponse {
    exists: bool,
    to_remove: Vec<String>,
}


enum LRUCacheHandleActorMessage {
    Put {
        respond_to: oneshot::Sender<LRUCachePutResponse>,
        file_location: String,
        size: u64,
        upstream_file_location: Option<String>
    },
}


struct LRUCacheHandleActor {
    receiver: mpsc::Receiver<LRUCacheHandleActorMessage>,
    lru_cache: LruCache<String, u64>,
    related: HashMap<String, Vec<String>>
}

impl LRUCacheHandleActor {
    pub fn new(receiver: mpsc::Receiver<LRUCacheHandleActorMessage>) -> Self {
        Self {
            receiver,
            lru_cache: LruCache::new(NonZero::new(1000).unwrap()),
            related: HashMap::new(),
        }
    }

    fn handle_message(&mut self, msg: LRUCacheHandleActorMessage) {
        match msg {
            LRUCacheHandleActorMessage::Put { respond_to, file_location, size, upstream_file_location } => {
                match upstream_file_location {
                    Some(ufl) => {
                        if !self.related.contains_key(&ufl) {
                            self.related.insert(ufl.clone(), vec!());
                        }
                        self.related.get_mut(&ufl).unwrap().push(file_location.clone());
                    },
                    None => ()
                }
                let retval = match self.lru_cache.push(file_location.clone(), size) {
                    Some((existing_key, _)) => {
                        if existing_key == file_location {
                            LRUCachePutResponse{
                                exists: true,
                                to_remove: vec!()
                            }
                        } else {
                            let mut to_remove = vec!(existing_key.clone());
                            match self.related.get(&existing_key) {
                                Some(related_files) => {
                                    to_remove.extend(related_files.clone());
                                    self.related.remove(&existing_key);
                                },
                                None => ()
                            };
                            LRUCachePutResponse{
                                exists: false,
                                to_remove
                            }
                        }
                    },
                    None => {
                        LRUCachePutResponse{
                            exists: false,
                            to_remove: vec!()
                        }
                    }
                };
                let _ = respond_to.send(retval);
            },
        }
    }
}


#[derive(Clone)]
pub struct LRUCacheHandle {
    sender: mpsc::Sender<LRUCacheHandleActorMessage>,
}

async fn run_lru_cache_actor_message_pump(mut actor: LRUCacheHandleActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg);
    }
}

impl LRUCacheHandle {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let actor = LRUCacheHandleActor::new(receiver);
        tokio::spawn(run_lru_cache_actor_message_pump(actor));
        Self { sender }
    }

    async fn put(&self, file_location: &String, size: u64, upstream_file_location: Option<String>) -> LRUCachePutResponse {
        let (send, recv) = oneshot::channel();
        let msg = LRUCacheHandleActorMessage::Put {
            respond_to: send,
            file_location: file_location.clone(),
            size: size,
            upstream_file_location: upstream_file_location.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }
}


static DATA_FUSION_CONTEXT: std::sync::LazyLock<SessionContext> = std::sync::LazyLock::new(|| create_session());
static LRU_CACHE_HANDLE: std::sync::LazyLock<LRUCacheHandle> = std::sync::LazyLock::new(|| LRUCacheHandle::new());


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
                    let _ = DATA_FUSION_CONTEXT.sql("DROP TABLE IF EXISTS {local_name_var};").await;
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


fn path_to_table_name(file_path: &String) -> String {
    let safe_name = file_path
        .replace("/", "_")
        .replace(".", "_")
        .replace(":", "_")
        .replace("-", "_");
    format!("table_{}", safe_name)   
}

pub(crate) async fn load_file_as_table(file_path: &String, size: u64, upstream_table_name: Option<String>, parquet: bool) -> Result<String, DataFusionError> {
    let new_local_name = path_to_table_name(file_path);
    load_file_as_table_with_name(&new_local_name, file_path, size, upstream_table_name, parquet).await
}

pub(crate) async fn load_file_as_table_with_name(new_local_name: &String, file_path: &String, size: u64, upstream_table_name: Option<String>, parquet: bool) -> Result<String, DataFusionError> {
    let retval = LRU_CACHE_HANDLE.put(new_local_name, size, upstream_table_name).await;
    for to_remove_name in retval.to_remove.iter() {
        let _ = DATA_FUSION_CONTEXT.sql(&format!("DROP TABLE IF EXISTS {};", to_remove_name)).await;
        println!("Dropped table {} from cache", to_remove_name);
    }
    if !retval.exists {
        if parquet {
            match load_parquet_file_as_table(&file_path, &new_local_name).await {
                Err(e) => return log_err(e),
                _ => ()
            }
        } else {
            match load_json_file_as_table(file_path, &new_local_name).await {
                Err(e) => return log_err(e),
                _ => ()
            }
        }
    }
    Ok(new_local_name.clone())
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