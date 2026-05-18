use crate::service_impl_provider::SERVICE_IMPL;
use futures_util::future::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{Body, StatusCode, body};
use gotham::mime;
use gotham::state::FromState;
use gotham::state::State;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft_memstore::{MemNodeId, TypeConfig};
use serde::Serialize;
use std::pin::Pin;

fn raft_result_response<T, E>(
    state: &State,
    result: &Result<T, E>,
) -> gotham::hyper::Response<gotham::hyper::Body>
where
    T: Serialize,
    E: Serialize,
{
    create_response(
        state,
        StatusCode::OK,
        mime::APPLICATION_JSON,
        serde_json::to_string(result).unwrap(),
    )
}

async fn parse_request<T>(state: &mut State) -> T
where
    T: for<'de> serde::Deserialize<'de>,
{
    let valid_body = match body::to_bytes(Body::take_from(state)).await {
        Ok(vb) => vb,
        Err(_) => panic!("Failed to read raft request body"),
    };
    let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
    serde_json::from_str(&body_content).unwrap()
}

pub fn append_entries(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let request: AppendEntriesRequest<TypeConfig> = parse_request(&mut state).await;
        let result: Result<AppendEntriesResponse<MemNodeId>, RaftError<MemNodeId>> =
            SERVICE_IMPL.raft_append_entries(request).await;
        let res = raft_result_response(&state, &result);
        Ok((state, res))
    }
    .boxed()
}

pub fn vote(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let request: VoteRequest<MemNodeId> = parse_request(&mut state).await;
        let result: Result<VoteResponse<MemNodeId>, RaftError<MemNodeId>> =
            SERVICE_IMPL.raft_vote(request).await;
        let res = raft_result_response(&state, &result);
        Ok((state, res))
    }
    .boxed()
}

pub fn install_snapshot(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let request: InstallSnapshotRequest<TypeConfig> = parse_request(&mut state).await;
        let result: Result<
            InstallSnapshotResponse<MemNodeId>,
            RaftError<MemNodeId, InstallSnapshotError>,
        > = SERVICE_IMPL.raft_install_snapshot(request).await;
        let res = raft_result_response(&state, &result);
        Ok((state, res))
    }
    .boxed()
}
