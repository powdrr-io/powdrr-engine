use std::{future::Future, pin::Pin, time::Duration};
use futures::FutureExt;
use gotham::{handler::HandlerFuture, helpers::http::response::create_response, hyper::{body, Body, StatusCode}, mime, state::{FromState, State}};
use gotham::plain::test::AsyncTestServer;
use serde::{Deserialize, Serialize};

use crate::{compaction::perform_compaction, data_access, elastic_search_index::{self, create_index}, state_provider::{STATE_PROVIDER}};
use crate::data_contract::{CleanupCommit, CleanupWorkItem, TableMetadataCheckpoint};
use crate::prefetch::perform_prefetch;

#[derive(Serialize, Deserialize)]
pub(crate) struct TestCreateIndex {
    pub file_path: String,
    pub doc_id_field_name: String,
}


#[derive(Serialize, Deserialize, Clone)]
pub enum StateMode {
    Testing,
    Ephemeral,
    TestingDynamoDb(Option<String>),
    Leaderless {
        server_address: String,
        access_key: String,
        secret_key: String
    }
}

impl StateMode {
    pub fn is_testing(&self) -> bool {
        match self {
            StateMode::Testing | StateMode::TestingDynamoDb(_) => true,
            _ => false
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum StorageMode {
    S3 {
        rest_endpoint: Option<String>,
        s3_endpoint: Option<String>,
    }
}

impl StorageMode {
    pub fn default() -> Self {
        Self::S3 {
            rest_endpoint: None,
            s3_endpoint: None
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum CacheMode {
    Redis(Option<String>),
    Native,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum PeerMode {
    SelfOnly,
    Remote(Vec<String>)
}

impl PeerMode {
    pub fn to_peer_mode_type(&self) -> PeerModeType {
        match self {
            PeerMode::Remote(addresses) => PeerModeType::Remote(addresses.clone()),
            PeerMode::SelfOnly => PeerModeType::SelfOnly,
        }
    }
}

#[derive(Clone)]
pub enum PeerModeType {
    SelfOnly,
    Remote(Vec<String>),
    Testing(AsyncTestServer)
}

#[derive(Serialize, Deserialize, Clone)]
pub enum IndexingMode {
    Sync,
    Async,
    Disabled
}

impl IndexingMode {
    fn is_disabled(&self) -> bool {
        match self {
            IndexingMode::Disabled => true,
            _ => false
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum CompactionMode {
    Async(Option<u64>),
    External(String),
    Disabled
}

impl CompactionMode {
    const DEFAULT_NUM_FILES_THRESHOLD: u64 = 100;
    pub(crate) fn threshold(&self) -> u64 {
        match self {
            CompactionMode::Async(threshold) => threshold.unwrap_or(Self::DEFAULT_NUM_FILES_THRESHOLD),
            _ => Self::DEFAULT_NUM_FILES_THRESHOLD,
        }
    }
}

impl CompactionMode {
    fn is_disabled(&self) -> bool {
        match self {
            CompactionMode::Disabled => true,
            _ => false
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum PrefetchMode {
    Enabled,
    Disabled
}

impl PrefetchMode {
    fn is_disabled(&self) -> bool {
        match self {
            PrefetchMode::Disabled => true,
            _ => false
        }
    }
}


#[derive(Serialize, Deserialize, Clone)]
pub struct TestProcessingMode {
    pub state_mode: StateMode,
    pub storage_mode: StorageMode,
    pub cache_mode: CacheMode,
    pub peer_mode: PeerMode,
    pub indexing_mode: IndexingMode,
    pub compaction_mode: CompactionMode,
    pub prefetch_mode: PrefetchMode,
}

impl TestProcessingMode {
    pub fn default() -> Self {
        Self {
            state_mode: StateMode::TestingDynamoDb(None),
            storage_mode: StorageMode::default(),
            cache_mode: CacheMode::Redis(None),
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Sync,
            compaction_mode: CompactionMode::Async(None),
            prefetch_mode: PrefetchMode::Disabled,
        }
    }

    pub fn dynamo_testing(address: Option<String>) -> Self {
        Self {
            state_mode: StateMode::TestingDynamoDb(address),
            storage_mode: StorageMode::default(),
            cache_mode: CacheMode::Redis(None),
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Sync,
            compaction_mode: CompactionMode::Async(None),
            prefetch_mode: PrefetchMode::Disabled,
        }
    }
}


pub fn test_v1_create_index(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let invocation_obj: TestCreateIndex = match serde_json::from_str(&body_content) {
            Ok(io) => io,
            Err(_) => panic!("This should not happen"),
        };
        match elastic_search_index::create_index_parquet(&invocation_obj.file_path, &invocation_obj.doc_id_field_name).await {
            Err(_) => panic!("Let's just panic for now"),
            Ok(_) => ()
        }
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}


pub fn test_v1_add_checkpoint(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let invocation_obj: TableMetadataCheckpoint = match serde_json::from_str(&body_content) {
            Ok(io) => io,
            Err(_) => panic!("This should not happen"),
        };

        STATE_PROVIDER.add_checkpoint(&invocation_obj).await;
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}

pub fn test_v1_set_testing_mode(state: State) -> Pin<Box<HandlerFuture>> {
    async {
        STATE_PROVIDER.set_testing_mode(&TestProcessingMode::default()).await;
        data_access::drop_all_iceberg_tables(&"default".to_string()).await.unwrap();
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}


#[allow(warnings)]
pub(crate) async fn do_available_extension_work(extensions: &Vec<String>) -> bool {
    let mut work_done = false;
    // We keep track of this to see what all iceberg snapshots we should look through to
    // see what types of compactions have happened.
    let mut last_iceberg_snapshot_id: i64 = 0;

    tracing::info!("Checking for indexing work");
    let index_work = match STATE_PROVIDER.get_extension_work_items(&"es".to_string()).await {
        Ok(work) => work,
        Err(e) => {
            let error = format!("{}", e);
            panic!("oh no");
        },
    };

    if index_work.len() > 0 {
        tracing::info!("Doing indexing work");

        for work_item in index_work.iter() {
            work_done = true;
            match create_index(work_item).await {
                Ok(_) => (),
                Err(e) => {
                    let _error = format!("{}", e);
                    tracing::error!("Error occurred while indexing: {}", e);
                },
            }
        }
        tracing::info!("Done with indexing work");
    }

    work_done
}

pub(crate) async fn do_available_compaction_work(start_snapshot_id: i64) -> (i64, bool) {
    // We keep track of snapshot id to see what all iceberg snapshots we should look through to
    // see what types of compactions have happened.
    let mut last_iceberg_snapshot_id: i64 = start_snapshot_id;
    let mut work_done = false;

    let compact_work = match STATE_PROVIDER.get_compaction_work_items().await {
        Ok(work) => work,
        Err(_e) => {
            panic!("oh no")
        },
    };
    if compact_work.len() > 0 {
        tracing::info!("Doing compaction work");
        work_done = true;
        match perform_compaction(compact_work, last_iceberg_snapshot_id).await {
            Ok(id) => { last_iceberg_snapshot_id = id; },
            Err(e) => {
                tracing::error!("!!!!!!!!!!!!!!!!!!!!!!!!!  Error performing compaction: {:?}", e);
                // TODO: do something to trigger a retry of this compaction
            },
        }
        tracing::info!("Done with compaction work");
    }
    (last_iceberg_snapshot_id, work_done)
}

pub(crate) async fn do_next_prefetch() -> usize {
    let prefetch_work = match STATE_PROVIDER.get_next_prefetch_checkpoints(None).await {
        Ok(work) => work,
        Err(_) => panic!("oh no"),
    };
    if prefetch_work.len() > 0 {
        match perform_prefetch(&vec!(), &prefetch_work).await {
            Ok(_) => (),
            Err(e) => {
                tracing::error!("!!!!!!!!!!!!!!!!!!!!!!!!!  Error performing prefetch: {:?}", e);
                // TODO: do something? Track how many failed in a row?
            },
        }
    }
    prefetch_work.len()
}

pub(crate) async fn do_next_cleanup() -> usize {
    let cleanup_work = match STATE_PROVIDER.get_cleanup_work_items().await {
        Ok(work) => work,
        Err(_) => panic!("oh no"),
    };
    if cleanup_work.len() > 0 {
        for work_item in cleanup_work.iter() {
            perform_cleanup_work(work_item).await;
            match STATE_PROVIDER.cleanup_commit(&CleanupCommit{ id: work_item.id.clone(), table_name: work_item.table_name.clone() }).await {
                Ok(_) => (),
                Err(_) => panic!("oh no"),
            }
        }
    }
    cleanup_work.len()
}

pub(crate) async fn perform_cleanup_work(cleanup_work_item: &CleanupWorkItem) -> () {
    assert!(cleanup_work_item.files_to_delete.len() > 0);
    if cleanup_work_item.files_to_delete[0].starts_with("s3://") {
        data_access::delete_s3_files(&cleanup_work_item.files_to_delete).await;
    } else {
        for file_to_delete in cleanup_work_item.files_to_delete.iter() {
            if file_to_delete.ends_with(".json") || file_to_delete.ends_with(".arrow") {
                std::fs::remove_file(file_to_delete).unwrap();
            } else {
                match std::fs::remove_file(format!("{}.arrow", file_to_delete)) {
                    Ok(_) => (),
                    Err(e) => {
                        let _error = format!("{}", e);
                        tracing::error!("Error occurred while deleting file {}: {}", file_to_delete, e);
                    }
                }
            }
        }
    }
}



fn do_update_checkpoint_work_for_forever(wait_time_ms: u64) -> impl Future<Output = ()> {
    async move {
        let mut work_done;
        loop {
            match STATE_PROVIDER.update_all_checkpoints().await {
                Ok(checkpoint_work_done) => work_done = checkpoint_work_done,
                Err(e) => {
                    tracing::error!("Error updating checkpoints: {}", e);
                    work_done = false;
                }
            };
            if !work_done {
                tokio::time::sleep(Duration::from_millis(wait_time_ms)).await;
            }
        }
    }
}


fn do_extension_work_for_forever(extensions: Vec<String>, wait_time_ms: u64) -> impl Future<Output = ()> {
    async move {
        let mut work_done;
        loop {
            work_done = do_available_extension_work(&extensions).await;
            if !work_done {
                tokio::time::sleep(Duration::from_millis(wait_time_ms)).await;
            }
        }
    }
}


fn do_compaction_work_for_forever(wait_time_ms: u64) -> impl Future<Output = ()> {
    async move {
        let mut last_iceberg_snapshot_id: i64 = -1;
        let mut work_done;
        loop {
            (last_iceberg_snapshot_id, work_done) = do_available_compaction_work(last_iceberg_snapshot_id).await;
            if !work_done {
                tokio::time::sleep(Duration::from_millis(wait_time_ms)).await;
            }
        }
    }
}


fn do_prefetch_work_for_forever(wait_time_ms: u64) -> impl Future<Output = ()> {
    async move {
        loop {
            let num = do_next_prefetch().await;
            if num == 0 {
                tokio::time::sleep(Duration::from_millis(wait_time_ms)).await;
            }
        }
    }
}


fn do_cleanup_work_for_forever() -> impl Future<Output = ()> {
    async move {
        loop {
            do_next_cleanup().await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}


pub fn test_v1_set_testing_processing_mode(mut state: State) -> Pin<Box<HandlerFuture>> {
    async {
        let mode = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => {
                let body_content = String::from_utf8(vb.to_vec()).unwrap();
                match serde_json::from_str(&body_content) {
                    Ok(io) => io,
                    Err(_) => panic!("This should not happen"),
                }
            },
            Err(_) => TestProcessingMode::default()
        };

        STATE_PROVIDER.set_testing_mode(&mode).await;
        if mode.state_mode.is_testing() {
            data_access::drop_all_iceberg_tables(&"default".to_string()).await.unwrap();
        }
        tokio::spawn(do_update_checkpoint_work_for_forever(1000));
        if !mode.indexing_mode.is_disabled() {
            tokio::spawn(do_extension_work_for_forever(vec!("es".to_string()), 1000));
        }
        if !mode.compaction_mode.is_disabled() {
            tokio::spawn(do_compaction_work_for_forever(1000));
        }
        if !mode.prefetch_mode.is_disabled() {
            tokio::spawn(do_prefetch_work_for_forever(1000));
        }
        tokio::spawn(do_cleanup_work_for_forever());
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}


pub fn test_v1_process_work(mut state: State) -> Pin<Box<HandlerFuture>> {
    async {
        let mut work_done: bool;
        let mut snapshot_id = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => {
                let body_content = String::from_utf8(vb.to_vec()).unwrap();
                if body_content.len() == 0 {
                    0
                } else {
                    body_content.parse::<i64>().unwrap()
                }
            },
            Err(_) => {
                panic!("Oh no");
            },
        };
        loop {
            work_done = do_available_extension_work(&vec!("es".to_string())).await;
            let (new_snapshot_id, compaction_work_done) = do_available_compaction_work(snapshot_id).await;
            snapshot_id = new_snapshot_id;
            work_done = work_done | compaction_work_done;
            match STATE_PROVIDER.update_all_checkpoints().await {
                Ok(checkpoint_work_done) => work_done = work_done | checkpoint_work_done,
                Err(e) => {
                    tracing::error!("Error updating checkpoints: {}", e);
                }
            };
            let num_deletes = do_next_cleanup().await;
            work_done = work_done | (num_deletes > 0);
            if !work_done {
                break;
            }
        }

        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, snapshot_id.to_string());
        Ok((state, res))        
    }.boxed()
}
