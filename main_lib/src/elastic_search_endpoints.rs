use std::{collections::HashMap, env, pin::Pin, sync::Arc};

use futures::FutureExt;
use gotham::{handler::HandlerFuture, helpers::http::response::create_response, hyper::{body, Body}, mime, prelude::StaticResponseExtender, state::{FromState, State, StateData}};
use gotham::helpers::http::response::create_empty_response;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{elastic_search_cluster_info, elastic_search_commands::LookupById, elastic_search_common::{execute_command, CommandContext}, elastic_search_ingest, elastic_search_parser, elastic_search_pipeline, state_provider::STATE_PROVIDER};
use crate::elastic_search_common::MIME_ES_JSON;
use crate::util::{log_service_err, log_service_err_response};

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct NamePathExtractor {
    pub(crate) name: String,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct NameIdPathExtractor {
    name: String,
    id: String,
}


#[derive(Serialize)]
struct ServerVersion {
    number: String,
    build_flavor: String,
    build_type: String,
    build_hash: String,
    build_date: String,
    build_snapshot: bool,
    lucene_version: String,
    minimum_wire_compatibility_version: String,
    minimum_index_compatibility_version: String,
}

#[derive(Serialize)]
struct ServerInfo {
    name: String,
    cluster_name: String,
    cluster_uuid: String,
    version: ServerVersion,
    tagline: String,
}

impl ServerInfo {
    fn new() -> Self {
        ServerInfo {
            name: env::var("node.name").unwrap_or("es01".into()), // TODO: pull this from env
            cluster_name: env::var("cluster.name").unwrap_or("docker-cluster".into()), // TODO: pull this from env
            cluster_uuid: uuid_b64::UuidB64::new().to_string(),
            version: ServerVersion { 
                number: "8.7.1".to_string(), 
                build_flavor: "default".to_string(), 
                build_type: "docker".to_string(), 
                build_hash: "f229ed3f893a515d590d0f39b05f68913e2d9b53".to_string(), // TODO: pull this from the docker image
                build_date: "2023-04-27T04:33:42.127815583Z".to_string(), // TODO: pull this from the docker image
                build_snapshot: false,
                lucene_version: "9.5.0".to_string(), 
                minimum_wire_compatibility_version: "7.17.0".to_string(), 
                minimum_index_compatibility_version: "7.0.0".to_string(),
            },
            tagline: "You Know, for Search".to_string(),
        }
    }
}

#[derive(Serialize)]
struct License {
    status: String,
    uid: String,
    #[serde(rename = "type")]
    _type: String,
    issue_date: String,
    issue_data_in_millis: u64,
    max_nodes: u64,
    max_resource_units: Option<u64>,
    issued_to: String,
    issuer: String,
    start_date_in_millis: i64,
}

impl License {
    fn new() -> HashMap<String, Self> {
        HashMap::from([("license".to_string(), License {
            status: "active".to_string(),
            uid: "98f6bcc7-ae8f-4f75-a9b7-e6e909416eaa".to_string(),
            _type: "basic".to_string(),
            issue_date: "2025-07-08T22:10:56.204Z".to_string(),
            issue_data_in_millis: 1752012656204,
            max_nodes: 1000,
            max_resource_units: None,
            issued_to: "docker-cluster".to_string(),
            issuer: "elasticsearch".to_string(),
            start_date_in_millis: -1,
        })])
    }

    fn xpack() -> String {
        include_str!("xpack_response.json").to_string()
    }
}

static SERVER_INFO: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| serde_json::to_string_pretty(&ServerInfo::new()).unwrap());


pub fn es_root(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_root"); 
    async {
        let server_info: String = SERVER_INFO.clone();
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, server_info);
        Ok((state, res))
    }.boxed()
}

pub fn es_nodes(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_nodes"); 
    async {
        // TODO
        let nodes_cfg = r#"{
  "nodes": {
    "M2BCY3K4RWCAIoe0ZNDj5w": {
      "ip": "host.docker.internal",
      "version": "8.7.1",
      "http": {
        "publish_address": "host.docker.internal:9200"
      }
    }
  }
}"#;
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, nodes_cfg);
        Ok((state, res))
    }.boxed()
}


pub fn es_license(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_license"); 
    async {
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(&License::new()).unwrap());
        Ok((state, res))
    }.boxed()    
}

pub fn es_xpack(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_xpack");
    async {
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, License::xpack());
        Ok((state, res))
    }.boxed()
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub(crate) struct QueryStringClusterSettings {
    include_defaults: Option<bool>,
    flat_settings: Option<bool>,
}


pub fn es_cluster_settings(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_cluster_settings"); 
    async {
        let query_string = QueryStringClusterSettings::take_from(&mut state);
        if !query_string.flat_settings.unwrap_or(false) {
            panic!("What does this mean?")
        }
        let res = if query_string.include_defaults.unwrap_or(false) {
            create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, elastic_search_cluster_info::CLUSTER_SETTINGS_WITH_DEFAULTS)
        } else {
            create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, elastic_search_cluster_info::CLUSTER_SETTINGS)
        };
        Ok((state, res))

    }.boxed()    
}

pub fn es_get_index(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index"); 
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);

        for table_name in path_extractor.name.to_string().split(",") {
            let table_desc = match STATE_PROVIDER.describe_table(&table_name.to_string()).await {
                Ok(td) => td,
                Err(e) => {
                    let res = log_service_err(e).generate_response(&state);
                    return Ok((state, res))
                }
            };
            if table_desc.is_none() {
                continue;
            }
            let response = table_desc.map_or_else(
                || "{}".to_string(), 
                |x|x.tags.get("_es_original").map_or_else(|| "{}".to_string(), |x|x.clone())
            );

            let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, response);
            return Ok((state, res))
        }
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, "{}");
        Ok((state, res))

    }.boxed()       
}

pub fn es_head_index(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);

        for table_name in path_extractor.name.to_string().split(",") {
            let table_desc = match STATE_PROVIDER.describe_table(&table_name.to_string()).await {
                Ok(td) => td,
                Err(e) => {
                    let res = log_service_err(e).generate_response(&state);
                    return Ok((state, res));
                }
            };
            let res = if table_desc.is_none() {
                create_empty_response(&state, StatusCode::NOT_FOUND)
            } else {
                let response = table_desc.map_or_else(
                    || "{}".to_string(),
                    |x| x.tags.get("_es_original").map_or_else(|| "{}".to_string(), |x| x.clone())
                );

                create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, response)
            };
            return Ok((state, res))
        }
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, "{}");
        Ok((state, res))

    }.boxed()
}


pub fn es_get_index_aliases(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index_aliases");
    async {
        let _path_extractor = NamePathExtractor::borrow_from(&state);
        // TODO: make this actually work
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, "{}");
        Ok((state, res))

    }.boxed()
}

pub fn es_get_index_settings(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index_aliases");
    async {
        let _path_extractor = NamePathExtractor::borrow_from(&state);
        // TODO: make this actually work
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, "{}");
        Ok((state, res))

    }.boxed()
}

pub fn es_get_index_template(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index_template"); 
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let index_name = path_extractor.name.to_string();

        let table_desc = match STATE_PROVIDER.describe_table_template(&index_name).await {
            Ok(td) => td,
            Err(e) => return Ok(log_service_err_response(e, state))
        };

        let response = table_desc.map_or_else(
            || "{}".to_string(), 
            |x|serde_json::to_string(&x).unwrap()
        );

        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, response);
        Ok((state, res))
    }.boxed()       
}


pub fn es_create_with_id(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_create_with_id"); 
    async {
        let path_extractor = NameIdPathExtractor::borrow_from(&state);
        let index_name = path_extractor.name.to_string();
        let doc_id = path_extractor.id.to_string();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        let create_single_result = elastic_search_ingest::create_single(&index_name, &doc_id, &body_content).await;
        match create_single_result {
            Ok(success) => {
                let res = success.generate_response(&state);
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(&state, StatusCode::ALREADY_REPORTED, mime::TEXT_PLAIN, e.message);
                Ok((state, res))
            }
        }
    }.boxed()
}

pub fn es_update_with_id(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_update_with_id"); 
    async {
        let path_extractor = NameIdPathExtractor::borrow_from(&state);
        let index_name = path_extractor.name.to_string();
        let doc_id = path_extractor.id.to_string();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        let create_single_result = elastic_search_ingest::upsert_single(&index_name, &doc_id, &body_content).await;
        match create_single_result {
            Ok(success_response) => {
                let res = success_response.generate_response(&state);
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(&state, StatusCode::ALREADY_REPORTED, mime::TEXT_PLAIN, e.message);
                Ok((state, res))
            }
        }
    }.boxed()
}

pub fn es_get_with_id(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_with_id"); 
    async {
        let path_extractor = NameIdPathExtractor::borrow_from(&state);
        let index_name = path_extractor.name.to_string();
        let doc_id = path_extractor.id.to_string();
        let table_desc = match STATE_PROVIDER.describe_table(&index_name).await {
            Ok(td) => td,
            Err(e) => return Ok(log_service_err_response(e, state))
        };
        match table_desc {
            Some(td) => {
                let command = LookupById::new(&td.name, &vec!(doc_id));
                let response = execute_command(CommandContext{}, Arc::new(command)).await;
                let res = response.generate_response(&state);
                Ok((state, res))
            },
            None => {
                panic!("Table not found");
            }
        }

    }.boxed()
}


pub fn es_delete_with_id(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_with_id"); 
    async {
        let path_extractor = NameIdPathExtractor::borrow_from(&state);
        let index_name = path_extractor.name.to_string();
        let doc_id = path_extractor.id.to_string();
        match elastic_search_ingest::delete(&index_name, &doc_id).await {
            Ok(r) => {
                let res = r.generate_response(&state);
                Ok((state, res))
            },
            Err(_) => panic!("Error time")
        }
    }.boxed()
}


pub fn es_create_pipeline(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_create_pipeline"); 
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let name = path_extractor.name.to_string();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        let create_pipeline_result = elastic_search_pipeline::create_pipeline(&name, &body_content).await;
        match create_pipeline_result {
            Ok(success) => {
                let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(&success).unwrap());
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(&state, StatusCode::ALREADY_REPORTED, mime::TEXT_PLAIN, e.message);
                Ok((state, res))
            }
        }
    }.boxed()
}

pub fn es_simulate_pipeline(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_create_pipeline"); 
    async {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        let simulate_pipeline_result = elastic_search_pipeline::simulate_pipeline(&None, &body_content).await;
        match simulate_pipeline_result {
            Ok(success) => {
                let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(&success).unwrap());
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(&state, StatusCode::ALREADY_REPORTED, mime::TEXT_PLAIN, e.message);
                Ok((state, res))
            }
        }
    }.boxed()
}

pub fn es_simulate_named_pipeline(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_create_pipeline"); 
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let name = path_extractor.name.to_string();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        let simulate_pipeline_result = elastic_search_pipeline::simulate_pipeline(&Some(name), &body_content).await;
        match simulate_pipeline_result {
            Ok(success) => {
                let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(&success).unwrap());
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(&state, StatusCode::ALREADY_REPORTED, mime::TEXT_PLAIN, e.message);
                Ok((state, res))
            }
        }
    }.boxed()
}


pub fn es_create_index(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_create_index"); 
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let table = path_extractor.name.to_string();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        let create_index_result = elastic_search_ingest::create_index(&table, &body_content).await;
        match create_index_result {
            Ok(success) => {
                let res = create_response(&state, StatusCode::OK, MIME_ES_JSON.clone(), serde_json::to_string(&success).unwrap());
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(&state, StatusCode::ALREADY_REPORTED, mime::TEXT_PLAIN, e.message);
                Ok((state, res))
            }
        }
    }.boxed()
}


pub fn es_create_index_template(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_create_index_template"); 
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let table = path_extractor.name.to_string();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        let create_index_result = elastic_search_ingest::create_index_template(&table, &body_content).await;
        match create_index_result {
            Ok(success) => {
                let res = create_response(&state, StatusCode::OK, MIME_ES_JSON.clone(), serde_json::to_string(&success).unwrap());
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(&state, StatusCode::ALREADY_REPORTED, mime::TEXT_PLAIN, e.message);
                Ok((state, res))
            }
        }
    }.boxed()
}


pub fn es_head_template(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_head_template");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let table = path_extractor.name.to_string();

        match STATE_PROVIDER.describe_table_template(&table).await {
            Ok(tt) => match tt {
                Some(_) => {
                    let res = create_empty_response(&state, StatusCode::OK);
                    Ok((state, res))
                },
                None => {
                    let res = create_empty_response(&state, StatusCode::NOT_FOUND);
                    Ok((state, res))
                }
            },
            Err(e) => return Ok(log_service_err_response(e, state))
        }
    }.boxed()
}

pub fn es_get_template(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_template");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let table = path_extractor.name.to_string();

        match STATE_PROVIDER.describe_table_template(&table).await {
            Ok(tt) => match tt {
                Some(t) => {
                    let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(&t).unwrap());
                    Ok((state, res))
                },
                None => {
                    let res = create_empty_response(&state, StatusCode::NOT_FOUND);
                    Ok((state, res))
                }
            },
            Err(e) => return Ok(log_service_err_response(e, state))
        }
    }.boxed()
}


#[derive(Deserialize, StateData, StaticResponseExtender)]
pub(crate) struct QueryStringAliases {
    #[allow(dead_code)]
    timeout: Option<String>
}

pub fn es_update_aliases(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_update_aliases"); 
    async {
        let _query_string = QueryStringAliases::take_from(&mut state);
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        let create_index_result = elastic_search_ingest::update_aliases(&body_content).await;
        match create_index_result {
            Ok(_) => {
                let response = HashMap::from([("acknowledged", true)]);
                let res = create_response(&state, StatusCode::OK, MIME_ES_JSON.clone(), serde_json::to_string(&response).unwrap());
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(&state, StatusCode::ALREADY_REPORTED, mime::TEXT_PLAIN, e.message);
                Ok((state, res))
            }
        }
    }.boxed()
}

#[derive(Deserialize, StateData, StaticResponseExtender, Clone)]
pub(crate) struct QueryStringSearch {
    #[allow(dead_code)]
    pub allow_partial_search_results: Option<bool>,
    #[allow(dead_code)]
    pub sort: Option<String>,
    pub rest_total_hits_as_int: Option<bool>,
}

impl QueryStringSearch {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        QueryStringSearch{ allow_partial_search_results: None, sort: None, rest_total_hits_as_int: None }
    }
}


/// Handler function for `POST` requests directed to `/_search`
pub fn es_search(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_search");
    async {
        let query_string = QueryStringSearch::take_from(&mut state);
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        let command = match elastic_search_parser::parse(None, &body_content, &query_string) {
            Ok(c) => c,
            Err(_) => {
                let res = create_response(&state, StatusCode::BAD_REQUEST, mime::TEXT_PLAIN, "Bad request".to_string());
                return Ok((state, res))
            }
        };
        let response = execute_command(CommandContext{}, Arc::new(command)).await;
        let res = response.generate_response(&state);
        Ok((state, res))
    }.boxed()
}

pub fn es_update_by_query(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_update_by_query");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let table = path_extractor.name.to_string();        
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let table_description = match STATE_PROVIDER.describe_table(&table).await {
            Ok(td) => match td {
                Some(td) => td,
                None => {
                    let res = create_response(&state, StatusCode::BAD_REQUEST, mime::TEXT_PLAIN, "Index does not exist".to_string());
                    return Ok((state, res))
                }
            },
            Err(e) => return Ok(log_service_err_response(e, state))
        };
        
        let command = match elastic_search_parser::parse_update_by_query(Some(table_description.name), &body_content) {
            Ok(c) => c,
            Err(_) => {
                let res = create_response(&state, StatusCode::BAD_REQUEST, mime::TEXT_PLAIN, "Bad request".to_string());
                return Ok((state, res))
            }
        };
        let response = execute_command(CommandContext{}, Arc::new(command)).await;
        let res = response.generate_response(&state);
        Ok((state, res))
    }.boxed()
}


/// Handler function for `POST` requests directed to `/:table/_search`
pub fn es_search_table(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_search_table");
    async {
        let path_extractor = NamePathExtractor::take_from(&mut state);
        let query_extractor = QueryStringSearch::take_from(&mut state);
        let table = path_extractor.name.to_string();
        let table_desc = match STATE_PROVIDER.describe_table(&table).await {
            Ok(td) => match td {
                Some(td) => td,
                None => {
                    let res = create_response(&state, StatusCode::BAD_REQUEST, mime::TEXT_PLAIN, "Bad request".to_string());
                    return Ok((state, res))
                },
            },
            Err(e) => return Ok(log_service_err_response(e, state))
        };
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => {
                let res = create_response(&state, StatusCode::BAD_REQUEST, mime::TEXT_PLAIN, "Bad request".to_string());
                return Ok((state, res))
            },
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let command = match elastic_search_parser::parse(Some(table_desc.name), &body_content, &query_extractor) {
            Ok(c) => c,
            Err(_e) => {
                let res = create_response(&state, StatusCode::BAD_REQUEST, mime::TEXT_PLAIN, "Bad request".to_string());
                return Ok((state, res))
            }
        };
        let response = execute_command(CommandContext{}, Arc::new(command)).await;
        let res = response.generate_response(&state);
        Ok((state, res))
    }.boxed()
}


/// Handler function for `POST` requests directed to `/:table/_pit`
pub fn es_index_pit(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_index_pit");
    async {
        let _path_extractor = NamePathExtractor::borrow_from(&state);
        // TODO: really generate this. just needs to be an encoded checkpoint id for this table
        let response_data = HashMap::from(
            [("succeeded", json!(true)),
             ("num_freed", json!(1))]
        );
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(&response_data).unwrap());
        Ok((state, res))
    }.boxed()
}

/// Handler function for `DELETE` requests directed to `/_pit`
pub fn es_delete_pit(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_delete_pit");
    async {
        let response_data = HashMap::from([("id", "t8jsAwEeLmtpYmFuYV90YXNrX21hbmFnZXJfOC43LjFfMDAxFkNScFZFdlZZUzNHTTBZdzVmOVY1VHcAFk0yQkNZM0s0UldDQUlvZTBaTkRqNXcAAAAAAAAAAAEWUkxXRUxKbWhUWkt3LXRTWHdhb3loQQABFkNScFZFdlZZUzNHTTBZdzVmOVY1VHcAAA==")]);
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(&response_data).unwrap());
        Ok((state, res))
    }.boxed()
}


/// Handler function for `POST` and 'PUT' requests directed to `/_bulk'
pub fn es_bulk_ingest(mut state: State) -> Pin<Box<HandlerFuture>> {
    //tracing::info!("es_bulk_ingest");
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();    
        //let ingest_result= elastic_search_ingest::ingest_and_commit(&body_content).await;
        let ingest_result = elastic_search_ingest::INGEST_HANDLE.send(&body_content).await;
        match ingest_result {
            Ok(success) => {
                let res = create_response(&state, StatusCode::OK, MIME_ES_JSON.clone(), serde_json::to_string(&success).unwrap());
                Ok((state, res))
            }
            Err(e) => {
                let _error = format!("{}", e.message);
                panic!("Oopsie");
            }
        }
    }.boxed()
}
