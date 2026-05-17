use std::pin::Pin;

use futures_util::future::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{body, Body};
use gotham::mime;
use gotham::state::{FromState, State};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::elastic_search_endpoints::NamePathExtractor;
use crate::lakehouse_serving::{execute_serving_query, ServingQueryError, ServingQueryResponse};
use crate::serving_plan::ServingQueryClassification;
use crate::serving_protocol::{from_mongodb_find, MongoFindCommand};

const MONGO_NAMESPACE_PREFIX: &str = "powdrr";
const MONGO_BAD_VALUE_CODE: i32 = 2;
const MONGO_NAMESPACE_NOT_FOUND_CODE: i32 = 26;
const MONGO_INTERNAL_ERROR_CODE: i32 = 1;

#[derive(Serialize)]
struct MongoFindResponse {
    cursor: MongoCursorResponse,
    ok: f64,
}

#[derive(Serialize)]
struct MongoCursorResponse {
    id: i64,
    ns: String,
    #[serde(rename = "firstBatch")]
    first_batch: Vec<Value>,
}

#[derive(Serialize)]
struct MongoCommandErrorResponse {
    ok: f64,
    errmsg: String,
    code: i32,
    #[serde(rename = "codeName")]
    code_name: &'static str,
}

#[derive(Debug)]
struct MongoCommandError {
    status: StatusCode,
    code: i32,
    code_name: &'static str,
    message: String,
}

impl MongoCommandError {
    fn bad_value(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: MONGO_BAD_VALUE_CODE,
            code_name: "BadValue",
            message: message.into(),
        }
    }

    fn from_serving_error(error: ServingQueryError) -> Self {
        match error.status {
            StatusCode::NOT_FOUND => Self {
                status: StatusCode::NOT_FOUND,
                code: MONGO_NAMESPACE_NOT_FOUND_CODE,
                code_name: "NamespaceNotFound",
                message: error.message,
            },
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                Self::bad_value(error.message)
            }
            _ => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: MONGO_INTERNAL_ERROR_CODE,
                code_name: "InternalError",
                message: error.message,
            },
        }
    }

    fn from_query_response(response: &ServingQueryResponse) -> Self {
        let message = response
            .reason
            .clone()
            .unwrap_or_else(|| "Serving query could not be satisfied".to_string());

        match response.classification {
            ServingQueryClassification::FastPath => Self::bad_value(message),
            ServingQueryClassification::SlowPath => Self {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                code: MONGO_BAD_VALUE_CODE,
                code_name: "QueryPlanKilled",
                message,
            },
            ServingQueryClassification::Rejected => Self::bad_value(message),
        }
    }
}

pub fn mongodb_find(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let command = match parse_json_body::<MongoFindCommand>(&mut state).await {
            Ok(command) => command,
            Err(message) => {
                let response = json_error_response(&state, MongoCommandError::bad_value(message));
                return Ok((state, response));
            }
        };

        if command.find != path {
            let response = json_error_response(
                &state,
                MongoCommandError::bad_value(format!(
                    "Path table {} does not match Mongo find collection {}",
                    path, command.find
                )),
            );
            return Ok((state, response));
        }

        let request = match from_mongodb_find(&command) {
            Ok(request) => request,
            Err(error) => {
                let response =
                    json_error_response(&state, MongoCommandError::bad_value(error.to_string()));
                return Ok((state, response));
            }
        };

        let response = match execute_serving_query(&path, request).await {
            Ok(response) => response,
            Err(error) => {
                let response =
                    json_error_response(&state, MongoCommandError::from_serving_error(error));
                return Ok((state, response));
            }
        };

        if response.classification != ServingQueryClassification::FastPath {
            let response =
                json_error_response(&state, MongoCommandError::from_query_response(&response));
            return Ok((state, response));
        }

        let response = json_response(
            &state,
            StatusCode::OK,
            &MongoFindResponse {
                cursor: MongoCursorResponse {
                    id: 0,
                    ns: format!("{}.{}", MONGO_NAMESPACE_PREFIX, path),
                    first_batch: response.rows,
                },
                ok: 1.0,
            },
        );
        Ok((state, response))
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

fn json_error_response(state: &State, error: MongoCommandError) -> gotham::hyper::Response<Body> {
    json_response(
        state,
        error.status,
        &MongoCommandErrorResponse {
            ok: 0.0,
            errmsg: error.message,
            code: error.code,
            code_name: error.code_name,
        },
    )
}
