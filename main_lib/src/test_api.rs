use std::{future::Future, pin::Pin, time::Duration};

use futures::FutureExt;
use gotham::{handler::HandlerFuture, helpers::http::response::create_response, hyper::{body, Body, StatusCode}, mime, state::{FromState, State}};
use serde::{Deserialize, Serialize};

use crate::{compaction::perform_compaction, elastic_search_index::{self, create_index}, state_hosted_service::{TableMetadataCheckpoint, API_SERVICE_CLIENT}};


#[derive(Serialize, Deserialize)]
pub(crate) struct TestCreateIndex {
    pub file_path: String,
    pub doc_id_field_name: String,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct TestProcessingMode {
    pub sync_indexing: Option<bool>,
    pub compaction_leader: Option<String>
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

        API_SERVICE_CLIENT.add_checkpoint(&invocation_obj).await;
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}

pub fn test_v1_set_testing_mode(state: State) -> Pin<Box<HandlerFuture>> {
    async {
        API_SERVICE_CLIENT.set_testing_mode(&TestProcessingMode{ sync_indexing: None, compaction_leader: None }).await;
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}

#[allow(warnings)]
pub(crate) async fn do_all_available_extension_work(extensions: &Vec<String>) -> () {
    loop {
        let mut work_done = false;
        // We keep track of this to see what all iceberg snapshots we should look through to
        // see what types of compactions have happened.
        let mut last_iceberg_snapshot_id: i64 = 0;

        tracing::info!("Checking for indexing work");
        let index_work = match API_SERVICE_CLIENT.get_extension_work_items(&"es".to_string()).await {
            Ok(work) => work,
            Err(_) => panic!("oh no"),
        };
        tracing::info!("Doing indexing work");

        for work_item in index_work.iter() {
            work_done = true;
            match create_index(&work_item).await {
                Ok(_) => (),
                Err(_) => panic!("Need some real error handling some day"),
            }
        }
        tracing::info!("Done with indexing work");

        if !work_done {
            break;
        }            
    }    
}

pub(crate) async fn do_all_available_compaction_work(start_snapshot_id: i64) -> i64 {
    // We keep track of snapshot id to see what all iceberg snapshots we should look through to
    // see what types of compactions have happened.
    let mut last_iceberg_snapshot_id: i64 = start_snapshot_id;
    loop {
        let mut work_done = false;

        tracing::info!("Checking for compaction work");
        let compact_work = match API_SERVICE_CLIENT.get_compaction_work_items().await {
            Ok(work) => work,
            Err(_) => panic!("oh no"),
        };
        if compact_work.len() > 0 {
            tracing::info!("Doing compaction work");
            work_done = true;
            match perform_compaction(compact_work, last_iceberg_snapshot_id).await {
                Ok(id) => { last_iceberg_snapshot_id = id; },
                Err(e) => {
                    tracing::error!("Error performing compaction: {:?}", e);
                    // TODO: do something to trigger a retry of this compaction
                },
            }
            tracing::info!("Done with compaction work");
        }

        if !work_done {
            break;
        }
    }
    last_iceberg_snapshot_id
}


fn do_extension_work_for_forever(extensions: Vec<String>, wait_time_ms: u64) -> impl Future<Output = ()> {
    async move {
        loop {
            do_all_available_extension_work(&extensions).await;
            tokio::time::sleep(Duration::from_millis(wait_time_ms)).await;
        }
    }
}


fn do_compaction_work_for_forever(wait_time_ms: u64) -> impl Future<Output = ()> {
    async move {
        let mut last_iceberg_snapshot_id: i64 = -1;
        loop {
            last_iceberg_snapshot_id = do_all_available_compaction_work(last_iceberg_snapshot_id).await;
            tokio::time::sleep(Duration::from_millis(wait_time_ms)).await;
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
            Err(_) => TestProcessingMode{ sync_indexing: None, compaction_leader: None }
        };

        API_SERVICE_CLIENT.set_testing_mode(&mode).await;
        tokio::spawn(do_extension_work_for_forever(vec!("es".to_string()), 1000));
        tokio::spawn(do_compaction_work_for_forever(1000));
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}


pub fn test_v1_process_work(state: State) -> Pin<Box<HandlerFuture>> {
    async {
        do_all_available_extension_work(&vec!("es".to_string())).await;
        do_all_available_compaction_work(-1).await;
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}
