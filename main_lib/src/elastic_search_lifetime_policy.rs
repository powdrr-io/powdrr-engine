use std::collections::HashMap;
use std::pin::Pin;
use futures_util::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{body, Body};
use gotham::mime;
use gotham::prelude::FromState;
use gotham::state::State;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use crate::elastic_search_common::MIME_ES_JSON;
use crate::elastic_search_endpoints::NamePathExtractor;
use crate::elastic_search_responses::{ErrorDetails, SingleDocCreateFailedResult};
use crate::state_hosted_service::API_SERVICE_CLIENT;
use crate::util::log_service_err_response;

#[derive(Serialize, Deserialize, Clone)]
pub struct ILMPolicyDeleteAction {}

#[derive(Serialize, Deserialize, Clone)]
pub struct ILMPolicyDelete {
    pub min_age: String,
    pub actions: ILMPolicyActions,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ILMPolicyRolloverAction {
    pub max_size: Option<String>,
    pub max_age: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ILMPolicyActions {
    pub rollover: Option<ILMPolicyRolloverAction>,
    pub delete: Option<ILMPolicyDeleteAction>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ILMPolicyHot {
    pub actions: ILMPolicyActions,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ILMPolicyPhases {
    pub hot: Option<ILMPolicyHot>,
    pub delete: Option<ILMPolicyDelete>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ILMPolicyMeta {
    pub managed: bool,
    pub index_patterns: Option<Vec<String>>,
    pub version: Option<i64>,
    pub created_by: Option<String>,
    pub updated_by: Option<String>,
    pub description: Option<String>,
    pub generation: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ILMPolicyPolicy {
    pub _meta: Option<ILMPolicyMeta>,
    pub phases: ILMPolicyPhases,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ILMPolicyDefinition {
    pub policy: ILMPolicyPolicy,
}


pub fn es_get_ilm_policy(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_ilm_policy");
    // TODO: figure out what to do with ILM policy
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let table = path_extractor.name.to_string();

        match API_SERVICE_CLIENT.describe_lifetime_policy(&table).await {
            Ok(lp) => match lp {
                Some(_) => {
                    let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, "{}".to_string());
                    Ok((state, res))
                },
                None => {
                    let response = SingleDocCreateFailedResult {
                        error: ErrorDetails::single_cause(
                            &"resource_not_found_exception".to_string(),
                            &format!("Lifecycle policy not found: {table}"),
                            None,
                            None,
                            None,
                        ),
                        status: 404,
                    };
                    let res = create_response(&state, StatusCode::NOT_FOUND, mime::APPLICATION_JSON, serde_json::to_string(&response).unwrap());
                    Ok((state, res))
                }
            },
            Err(e) => return Ok(log_service_err_response(e, state))
        }

    }.boxed()
}


pub fn es_post_ilm_policy(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_post_ilm_policy");
    // TODO: figure out what to do with ILM policy
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let table = path_extractor.name.to_string();

        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();

        let policy: ILMPolicyDefinition = match serde_json::from_str(body_content.as_str()) {
            Ok(p) => p,
            Err(_) => panic!("Oh no"),
        };

        match API_SERVICE_CLIENT.create_lifetime_policy(&table, &policy).await {
            Ok(_) => (),
            Err(e) => return Ok(log_service_err_response(e, state))
        };

        let response = HashMap::from([("acknowledged", true)]);

        let res = create_response(&state, StatusCode::OK, MIME_ES_JSON.clone(), serde_json::to_string(&response).unwrap());
        Ok((state, res))
    }.boxed()
}

pub fn es_post_monitoring_bulk(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_post_monitoring_bulk");
    // TODO: figure out what this really means
    async {
        let response_str = r#"{
  "took": 0,
  "ignored": true,
  "errors": false
}"#;

        let res = create_response(&state, StatusCode::OK, MIME_ES_JSON.clone(), response_str);
        Ok((state, res))
    }.boxed()
}



#[cfg(test)]
mod tests {
    use std::fs;
    use super::*;

    #[test]
    fn test_parse_ilm_body() {
        let test_val = r#"{"policy":{"_meta":{"managed":true},"phases":{"hot":{"actions":{"rollover":{"max_age":"30d","max_primary_shard_size":"50gb"}}}}}}"#;

        let result = serde_json::from_str::<ILMPolicyDefinition>(test_val);
        match result {
            Ok(_) => (),
            Err(e) => {
                let error = format!("{}", e);
                let error_str = error.as_str();
                println!("{}", error_str);
                let _ = fs::write("/Users/gregory/code/powdrr-engine/main_lib/output.txt", error);
                panic!("nope");
            }
        }

        let test_val = r#"{"policy":{"phases":{"hot":{"actions":{"rollover":{"max_age":"1d","max_primary_shard_size":"50gb"}},"min_age":"0ms"},"delete":{"min_age":"1d","actions":{"delete":{}}}}}}"#;
        let result = serde_json::from_str::<ILMPolicyDefinition>(test_val);
        match result {
            Ok(_) => (),
            Err(e) => {
                let error = format!("{}", e);
                let error_str = error.as_str();
                println!("{}", error_str);
                let _ = fs::write("/Users/gregory/code/powdrr-engine/main_lib/output.txt", error);
                panic!("nope");
            }
        }

    }
}
