pub use powdrr_control_plane::test_api::{
    CacheMode, CompactionMode, IndexingMode, PeerMode, PrefetchMode, StateMode, StorageMode,
    TestProcessingMode,
};

use std::{future::Future, time::Duration};

use crate::data_contract::{CleanupCommit, CleanupWorkItem};
use crate::prefetch::perform_prefetch;
use crate::{
    compaction::perform_compaction, data_access, elastic_search_index::create_index,
    state_provider::STATE_PROVIDER,
};

use gotham::plain::test::AsyncTestServer;

#[derive(Clone)]
pub enum PeerModeType {
    SelfOnly,
    Remote(Vec<String>),
    Testing(AsyncTestServer),
}

pub fn peer_mode_to_type(mode: &PeerMode) -> PeerModeType {
    match mode {
        PeerMode::Remote(addresses) => PeerModeType::Remote(addresses.clone()),
        PeerMode::SelfOnly => PeerModeType::SelfOnly,
    }
}

#[allow(warnings)]
pub async fn do_available_extension_work(extensions: &Vec<String>) -> bool {
    let mut work_done = false;
    let mut last_iceberg_snapshot_id: i64 = 0;

    tracing::info!("Checking for indexing work");
    let index_work = match STATE_PROVIDER
        .get_extension_work_items(&"es".to_string())
        .await
    {
        Ok(work) => work,
        Err(e) => {
            let _error = format!("{}", e);
            panic!("oh no");
        }
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
                }
            }
        }
        tracing::info!("Done with indexing work");
    }

    work_done
}

pub async fn do_available_compaction_work(start_snapshot_id: i64) -> (i64, bool) {
    let mut last_iceberg_snapshot_id: i64 = start_snapshot_id;
    let mut work_done = false;

    let compact_work = match STATE_PROVIDER.get_compaction_work_items().await {
        Ok(work) => work,
        Err(_e) => {
            panic!("oh no")
        }
    };
    if compact_work.len() > 0 {
        tracing::info!("Doing compaction work");
        work_done = true;
        match perform_compaction(compact_work, last_iceberg_snapshot_id).await {
            Ok(id) => {
                last_iceberg_snapshot_id = id;
            }
            Err(e) => {
                tracing::error!(
                    "!!!!!!!!!!!!!!!!!!!!!!!!!  Error performing compaction: {:?}",
                    e
                );
            }
        }
        tracing::info!("Done with compaction work");
    }
    (last_iceberg_snapshot_id, work_done)
}

pub async fn do_next_prefetch() -> usize {
    let prefetch_work = match STATE_PROVIDER.get_next_prefetch_checkpoints(None).await {
        Ok(work) => work,
        Err(_) => panic!("oh no"),
    };
    if prefetch_work.len() > 0 {
        match perform_prefetch(&vec![], &prefetch_work).await {
            Ok(_) => (),
            Err(e) => {
                tracing::error!(
                    "!!!!!!!!!!!!!!!!!!!!!!!!!  Error performing prefetch: {:?}",
                    e
                );
            }
        }
    }
    prefetch_work.len()
}

pub async fn do_next_cleanup() -> usize {
    let cleanup_work = match STATE_PROVIDER.get_cleanup_work_items().await {
        Ok(work) => work,
        Err(_) => panic!("oh no"),
    };
    if cleanup_work.len() > 0 {
        for work_item in cleanup_work.iter() {
            perform_cleanup_work(work_item).await;
            match STATE_PROVIDER
                .cleanup_commit(&CleanupCommit {
                    id: work_item.id.clone(),
                    table_name: work_item.table_name.clone(),
                })
                .await
            {
                Ok(_) => (),
                Err(_) => panic!("oh no"),
            }
        }
    }
    cleanup_work.len()
}

pub async fn perform_cleanup_work(cleanup_work_item: &CleanupWorkItem) {
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
                        tracing::error!(
                            "Error occurred while deleting file {}: {}",
                            file_to_delete,
                            e
                        );
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
            match STATE_PROVIDER.advance_published_checkpoints().await {
                Ok(checkpoint_work_done) => work_done = checkpoint_work_done,
                Err(e) => {
                    tracing::error!("Error advancing published checkpoints: {}", e);
                    work_done = false;
                }
            };
            if !work_done {
                tokio::time::sleep(Duration::from_millis(wait_time_ms)).await;
            }
        }
    }
}

fn do_extension_work_for_forever(
    extensions: Vec<String>,
    wait_time_ms: u64,
) -> impl Future<Output = ()> {
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
            (last_iceberg_snapshot_id, work_done) =
                do_available_compaction_work(last_iceberg_snapshot_id).await;
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

pub fn spawn_processing_mode_workers(mode: &TestProcessingMode) {
    tokio::spawn(do_update_checkpoint_work_for_forever(1000));
    if !mode.indexing_mode.is_disabled() {
        tokio::spawn(do_extension_work_for_forever(vec!["es".to_string()], 1000));
    }
    if !mode.compaction_mode.is_disabled() {
        tokio::spawn(do_compaction_work_for_forever(1000));
    }
    if !mode.prefetch_mode.is_disabled() {
        tokio::spawn(do_prefetch_work_for_forever(1000));
    }
    tokio::spawn(do_cleanup_work_for_forever());
}
