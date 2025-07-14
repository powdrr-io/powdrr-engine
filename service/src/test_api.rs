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
        API_SERVICE_CLIENT.set_testing_mode(false).await;
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}


#[allow(warnings)]
pub(crate) async fn do_all_available_work() -> () {
    loop {
        let mut work_done = false;
        // We keep track of this to see what all iceberg snapshots we should look through to
        // see what types of compactions have happened.
        let mut last_iceberg_snapshot_id: i64 = 0;
        let index_work = match API_SERVICE_CLIENT.get_extension_work_items(&"es".to_string()).await {
            Ok(work) => work,
            Err(_) => panic!("oh no"),
        };
        for table_metadata in index_work.iter() {
            work_done = true;
            match create_index(&table_metadata).await {
                Ok(_) => (),
                Err(_) => panic!("Need some real error handling some day"),
            }
        }

        let compact_work = match API_SERVICE_CLIENT.get_compaction_work_items().await {
            Ok(work) => work,
            Err(_) => panic!("oh no"),
        };
        if compact_work.len() > 0 {
            work_done = true;
            match perform_compaction(compact_work, last_iceberg_snapshot_id).await {
                Ok(id) => { last_iceberg_snapshot_id = id; },
                Err(_) => panic!("Need some real error handling some day"),
            }
        }

        if !work_done {
            break;
        }            
    }    
}


fn do_work_for_forever(wait_time_ms: u64) -> impl Future<Output = ()> {
    async move {
        loop {
            do_all_available_work().await;
            tokio::time::sleep(Duration::from_millis(wait_time_ms)).await;
        }
    }
}


pub fn test_v1_set_testing_processing_mode(state: State) -> Pin<Box<HandlerFuture>> {
    async {
        API_SERVICE_CLIENT.set_testing_mode(true).await;
        tokio::spawn(do_work_for_forever(1000));
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}


pub fn test_v1_process_work(state: State) -> Pin<Box<HandlerFuture>> {
    async {
        do_all_available_work().await;
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))        
    }.boxed()
}