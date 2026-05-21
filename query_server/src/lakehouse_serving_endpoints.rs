use std::collections::HashMap;
use std::pin::Pin;

use futures::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{Body, body};
use gotham::mime;
use gotham::state::{FromState, State};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::elastic_search_http_types::NamePathExtractor;

use powdrr_query_lib::data_contract::{CreateTable, ServingTableConfig};
use powdrr_query_lib::serving_plan::ServingQueryClassification;
use powdrr_query_lib::serving_plan::ServingRequestPlan;
use powdrr_query_runtime::lakehouse_serving::{
    ServingCacheManagerRequestBody, ServingConfigResponse, execute_serving_cache_manager_request,
    execute_serving_layout_advice, execute_serving_query,
};
use powdrr_query_runtime::state_provider::STATE_PROVIDER;

pub fn get_serving_config(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state);
        match STATE_PROVIDER.describe_table(&path.name).await {
            Ok(Some(description)) => match description.serving {
                Some(serving) => {
                    let response = json_response(
                        &state,
                        StatusCode::OK,
                        &ServingConfigResponse {
                            acknowledged: true,
                            table: description.name,
                            serving,
                        },
                    );
                    Ok((state, response))
                }
                None => {
                    let response = json_response(
                        &state,
                        StatusCode::NOT_FOUND,
                        &json_error("No serving config declared for table"),
                    );
                    Ok((state, response))
                }
            },
            Ok(None) => {
                let response = json_response(
                    &state,
                    StatusCode::NOT_FOUND,
                    &json_error("Table not found"),
                );
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn put_serving_config(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let body = match parse_json_body::<ServingTableConfig>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        let (tags, dynamodb, mongodb, redis) = match STATE_PROVIDER.describe_table(&path).await {
            Ok(Some(description)) => (
                description.tags,
                description.dynamodb,
                description.mongodb,
                description.redis,
            ),
            Ok(None) => (HashMap::new(), None, None, None),
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                return Ok((state, response));
            }
        };

        let request = serde_json::from_value::<CreateTable>(serde_json::json!({
            "name": path.clone(),
            "tags": tags,
            "serving": body.clone(),
            "dynamodb": dynamodb,
            "mongodb": mongodb,
            "redis": redis,
        }))
        .expect("serving config table metadata should deserialize");

        match STATE_PROVIDER.upsert_table_metadata(&request).await {
            Ok(_) => {
                let response = json_response(
                    &state,
                    StatusCode::OK,
                    &ServingConfigResponse {
                        acknowledged: true,
                        table: path,
                        serving: body,
                    },
                );
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn serve_query(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let request = match parse_json_body::<ServingRequestPlan>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        match execute_serving_query(&path, request).await {
            Ok(response) => {
                let status = match response.classification {
                    ServingQueryClassification::FastPath => StatusCode::OK,
                    ServingQueryClassification::SlowPath => StatusCode::OK,
                    ServingQueryClassification::Rejected => StatusCode::UNPROCESSABLE_ENTITY,
                };
                let response = json_response(&state, status, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(&state, error.status, &json_error(&error.message));
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn manage_serving_cache(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let request = match parse_json_body::<ServingCacheManagerRequestBody>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        match execute_serving_cache_manager_request(&path, request).await {
            Ok(response) => {
                let response = json_response(&state, StatusCode::OK, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(&state, error.status, &json_error(&error.message));
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn get_serving_layout_advice(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        match execute_serving_layout_advice(&path).await {
            Ok(response) => {
                let response = json_response(&state, StatusCode::OK, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(&state, error.status, &json_error(&error.message));
                Ok((state, response))
            }
        }
    }
    .boxed()
}

async fn parse_json_body<T: for<'de> Deserialize<'de>>(state: &mut State) -> Result<T, String> {
    let valid_body = body::to_bytes(Body::take_from(state))
        .await
        .map_err(|error| error.to_string())?;
    serde_json::from_slice::<T>(&valid_body).map_err(|error| error.to_string())
}

fn json_response<T: Serialize>(
    state: &State,
    status: StatusCode,
    body: &T,
) -> gotham::hyper::Response<Body> {
    create_response(
        state,
        status,
        mime::APPLICATION_JSON,
        serde_json::to_string(body).unwrap(),
    )
}

fn json_error(message: &str) -> Value {
    serde_json::json!({ "error": message })
}
