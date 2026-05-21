use std::pin::Pin;

use futures::FutureExt;
use gotham::{
    handler::HandlerFuture,
    helpers::http::response::create_response,
    hyper::{Body, StatusCode, body},
    mime,
    prelude::FromState,
    state::State,
};
use serde::{Deserialize, Serialize};

use powdrr_query_lib::data_access;
use powdrr_query_lib::data_contract::TableMetadataCheckpoint;
use powdrr_query_runtime::elastic_search_index;
use powdrr_query_runtime::state_provider::STATE_PROVIDER;
use powdrr_query_runtime::test_api::{
    TestProcessingMode, do_available_compaction_work, do_available_extension_work, do_next_cleanup,
    spawn_processing_mode_workers,
};

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
        match elastic_search_index::create_index_parquet(
            &invocation_obj.file_path,
            &invocation_obj.doc_id_field_name,
        )
        .await
        {
            Err(_) => panic!("Let's just panic for now"),
            Ok(_) => (),
        }
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))
    }
    .boxed()
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
    }
    .boxed()
}

pub fn test_v1_set_testing_mode(state: State) -> Pin<Box<HandlerFuture>> {
    async {
        STATE_PROVIDER
            .set_testing_mode(&TestProcessingMode::default())
            .await;
        data_access::drop_all_iceberg_tables(&"default".to_string())
            .await
            .unwrap();
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))
    }
    .boxed()
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
            }
            Err(_) => TestProcessingMode::default(),
        };

        STATE_PROVIDER.set_testing_mode(&mode).await;
        if mode.state_mode.is_testing() {
            data_access::drop_all_iceberg_tables(&"default".to_string())
                .await
                .unwrap();
        }
        spawn_processing_mode_workers(&mode);
        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))
    }
    .boxed()
}

pub fn test_v1_process_work(mut state: State) -> Pin<Box<HandlerFuture>> {
    async {
        let mut work_done: bool;
        let mut snapshot_id = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => {
                let body_content = String::from_utf8(vb.to_vec()).unwrap();
                if body_content.is_empty() {
                    0
                } else {
                    body_content.parse::<i64>().unwrap()
                }
            }
            Err(_) => {
                panic!("Oh no");
            }
        };
        loop {
            work_done = do_available_extension_work(&vec!["es".to_string()]).await;
            let (new_snapshot_id, compaction_work_done) =
                do_available_compaction_work(snapshot_id).await;
            snapshot_id = new_snapshot_id;
            work_done = work_done | compaction_work_done;
            match STATE_PROVIDER.advance_published_checkpoints().await {
                Ok(checkpoint_work_done) => work_done = work_done | checkpoint_work_done,
                Err(e) => {
                    tracing::error!("Error advancing published checkpoints: {}", e);
                }
            };
            let num_deletes = do_next_cleanup().await;
            work_done = work_done | (num_deletes > 0);
            if !work_done {
                break;
            }
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::TEXT_PLAIN,
            snapshot_id.to_string(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn test_v1_advance_checkpoints(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        loop {
            match STATE_PROVIDER.advance_published_checkpoints().await {
                Ok(true) => continue,
                Ok(false) => break,
                Err(e) => {
                    tracing::error!("Error advancing published checkpoints: {}", e);
                    break;
                }
            };
        }

        let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "Ok");
        Ok((state, res))
    }
    .boxed()
}
