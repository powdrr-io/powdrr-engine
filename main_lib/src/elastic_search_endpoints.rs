use std::{collections::HashMap, env, pin::Pin, sync::Arc};

use futures::FutureExt;
use gotham::helpers::http::response::create_empty_response;
use gotham::{
    handler::HandlerFuture,
    helpers::http::response::create_response,
    hyper::{Body, body},
    mime,
    prelude::StaticResponseExtender,
    state::{FromState, State, StateData},
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::elastic_search_common::MIME_ES_JSON;
use crate::util::{log_service_err, log_service_err_response};
use crate::{
    data_contract::{AliasInfo, CreateIndexBody, TableDescription},
    elastic_search_cluster_info,
    elastic_search_commands::LookupById,
    elastic_search_common::{CommandContext, execute_command},
    elastic_search_ingest, elastic_search_parser, elastic_search_pipeline,
    elastic_search_responses::QueryResultShards,
    search_executor,
    state_provider::STATE_PROVIDER,
};

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct NamePathExtractor {
    pub(crate) name: String,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct NameIdPathExtractor {
    name: String,
    id: String,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct AliasPathExtractor {
    alias: String,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct NameAliasPathExtractor {
    name: String,
    alias: String,
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
        HashMap::from([(
            "license".to_string(),
            License {
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
            },
        )])
    }

    fn xpack() -> String {
        include_str!("xpack_response.json").to_string()
    }
}

static SERVER_INFO: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| serde_json::to_string_pretty(&ServerInfo::new()).unwrap());

pub fn es_root(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_root");
    async {
        let server_info: String = SERVER_INFO.clone();
        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, server_info);
        Ok((state, res))
    }
    .boxed()
}

pub fn es_root_head(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_root_head");
    async {
        let res = create_empty_response(&state, StatusCode::OK);
        Ok((state, res))
    }
    .boxed()
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
    }
    .boxed()
}

pub fn es_license(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_license");
    async {
        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            serde_json::to_string(&License::new()).unwrap(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_xpack(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_xpack");
    async {
        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            License::xpack(),
        );
        Ok((state, res))
    }
    .boxed()
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
            create_response(
                &state,
                StatusCode::OK,
                mime::APPLICATION_JSON,
                elastic_search_cluster_info::CLUSTER_SETTINGS_WITH_DEFAULTS,
            )
        } else {
            create_response(
                &state,
                StatusCode::OK,
                mime::APPLICATION_JSON,
                elastic_search_cluster_info::CLUSTER_SETTINGS,
            )
        };
        Ok((state, res))
    }
    .boxed()
}

fn empty_index_body() -> CreateIndexBody {
    CreateIndexBody {
        aliases: None,
        mappings: None,
        settings: None,
    }
}

fn parse_index_body(table_desc: &TableDescription) -> CreateIndexBody {
    table_desc
        .tags
        .get("_es_original")
        .and_then(|content| CreateIndexBody::parse(content).ok())
        .unwrap_or_else(empty_index_body)
}

fn alias_info_value(alias_info: &AliasInfo) -> Value {
    if alias_info.is_hidden {
        json!({ "is_hidden": true })
    } else {
        json!({})
    }
}

fn aliases_value(body: &CreateIndexBody) -> Value {
    let aliases = body.aliases.clone().unwrap_or_default();
    let alias_map = aliases
        .into_iter()
        .map(|(name, alias_info)| (name, alias_info_value(&alias_info)))
        .collect::<Map<String, Value>>();
    Value::Object(alias_map)
}

fn alias_names(body: &CreateIndexBody) -> Vec<String> {
    let mut aliases = body
        .aliases
        .clone()
        .unwrap_or_default()
        .into_keys()
        .collect::<Vec<_>>();
    aliases.sort();
    aliases
}

fn requested_parts(requested: &str) -> Vec<String> {
    requested
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn requests_all(parts: &[String]) -> bool {
    parts.is_empty() || parts.iter().any(|part| part == "*" || part == "_all")
}

fn matches_requested_value(value: &str, requested: &[String]) -> bool {
    requests_all(requested) || requested.iter().any(|requested_value| requested_value == value)
}

async fn all_table_descriptions() -> Result<Vec<TableDescription>, crate::state_provider::ServiceApiError>
{
    let mut table_descriptions = Vec::new();
    let mut table_names = STATE_PROVIDER.get_all_iceberg_tables().await?;
    table_names.sort();

    for table_name in table_names {
        if let Some(table_desc) = STATE_PROVIDER.describe_table(&table_name).await? {
            table_descriptions.push(table_desc);
        }
    }

    Ok(table_descriptions)
}

async fn requested_table_descriptions(
    requested_indices: &[String],
) -> Result<Vec<TableDescription>, crate::state_provider::ServiceApiError> {
    if requests_all(requested_indices) {
        return all_table_descriptions().await;
    }

    let mut table_descriptions = Vec::new();

    for table_name in requested_indices {
        if let Some(table_desc) = STATE_PROVIDER.describe_table(table_name).await? {
            table_descriptions.push(table_desc);
        }
    }

    Ok(table_descriptions)
}

fn filtered_aliases_value(body: &CreateIndexBody, requested_aliases: &[String]) -> Value {
    let aliases = body.aliases.clone().unwrap_or_default();
    let alias_map = aliases
        .into_iter()
        .filter(|(name, _)| matches_requested_value(name, requested_aliases))
        .map(|(name, alias_info)| (name, alias_info_value(&alias_info)))
        .collect::<Map<String, Value>>();
    Value::Object(alias_map)
}

async fn build_alias_response(
    requested_indices: &[String],
    requested_aliases: &[String],
) -> Result<Map<String, Value>, crate::state_provider::ServiceApiError> {
    let mut response = Map::new();
    let table_descriptions = requested_table_descriptions(requested_indices).await?;

    for table_desc in table_descriptions {
        let body = parse_index_body(&table_desc);
        let aliases = filtered_aliases_value(&body, requested_aliases);
        if aliases.as_object().is_some_and(|aliases| !aliases.is_empty()) {
            response.insert(table_desc.name.clone(), json!({ "aliases": aliases }));
        }
    }

    Ok(response)
}

fn normalize_settings_value(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| (key, normalize_settings_value(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(normalize_settings_value)
                .collect::<Vec<_>>(),
        ),
        Value::Number(number) => Value::String(number.to_string()),
        Value::Bool(boolean) => Value::String(boolean.to_string()),
        other => other,
    }
}

fn mappings_value(body: &CreateIndexBody) -> Value {
    body.mappings
        .clone()
        .map(|mappings| serde_json::to_value(mappings).unwrap())
        .unwrap_or_else(|| json!({}))
}

fn settings_value(body: &CreateIndexBody) -> Value {
    body.settings
        .clone()
        .map(|settings| normalize_settings_value(serde_json::to_value(settings).unwrap()))
        .unwrap_or_else(|| json!({}))
}

fn index_info_value(body: &CreateIndexBody) -> Value {
    json!({
        "aliases": aliases_value(body),
        "mappings": mappings_value(body),
        "settings": settings_value(body),
    })
}

fn count_body_as_search_body(body_content: &str) -> Result<String, String> {
    let mut parsed_body = if body_content.trim().is_empty() {
        Value::Object(Map::new())
    } else {
        serde_json::from_str::<Value>(body_content).map_err(|e| e.to_string())?
    };

    match parsed_body.as_object_mut() {
        Some(object) => {
            object.insert("size".to_string(), json!(0));
            Ok(parsed_body.to_string())
        }
        None => Err("count body must be a JSON object".to_string()),
    }
}

fn normalize_search_body(body_content: &str) -> String {
    if body_content.trim().is_empty() {
        "{}".to_string()
    } else {
        body_content.to_string()
    }
}

#[derive(Serialize)]
struct CountResponse {
    count: u64,
    _shards: QueryResultShards,
}

pub fn es_get_index(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let mut response = Map::new();

        for table_name in path_extractor.name.to_string().split(",") {
            let table_desc = match STATE_PROVIDER.describe_table(&table_name.to_string()).await {
                Ok(td) => td,
                Err(e) => {
                    let res = log_service_err(e).generate_response(&state);
                    return Ok((state, res));
                }
            };

            if let Some(table_desc) = table_desc {
                let body = parse_index_body(&table_desc);
                response.insert(table_desc.name.clone(), index_info_value(&body));
            }
        }

        if response.is_empty() {
            let res = create_empty_response(&state, StatusCode::NOT_FOUND);
            return Ok((state, res));
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            Value::Object(response).to_string(),
        );
        Ok((state, res))
    }
    .boxed()
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
                create_empty_response(&state, StatusCode::OK)
            };
            return Ok((state, res));
        }
        let res = create_empty_response(&state, StatusCode::NOT_FOUND);
        Ok((state, res))
    }
    .boxed()
}

pub fn es_get_index_aliases(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index_aliases");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let requested_indices = requested_parts(&path_extractor.name);
        let response = match build_alias_response(&requested_indices, &[]).await {
            Ok(response) => response,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        if response.is_empty() {
            let res = create_empty_response(&state, StatusCode::NOT_FOUND);
            return Ok((state, res));
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            Value::Object(response).to_string(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_get_aliases(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_aliases");
    async {
        let response = match build_alias_response(&[], &[]).await {
            Ok(response) => response,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        if response.is_empty() {
            let res = create_empty_response(&state, StatusCode::NOT_FOUND);
            return Ok((state, res));
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            Value::Object(response).to_string(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_get_named_aliases(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_named_aliases");
    async {
        let path_extractor = AliasPathExtractor::borrow_from(&state);
        let requested_aliases = requested_parts(&path_extractor.alias);
        let response = match build_alias_response(&[], &requested_aliases).await {
            Ok(response) => response,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        if response.is_empty() {
            let res = create_empty_response(&state, StatusCode::NOT_FOUND);
            return Ok((state, res));
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            Value::Object(response).to_string(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_get_index_named_aliases(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index_named_aliases");
    async {
        let path_extractor = NameAliasPathExtractor::borrow_from(&state);
        let requested_indices = requested_parts(&path_extractor.name);
        let requested_aliases = requested_parts(&path_extractor.alias);
        let response = match build_alias_response(&requested_indices, &requested_aliases).await {
            Ok(response) => response,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        if response.is_empty() {
            let res = create_empty_response(&state, StatusCode::NOT_FOUND);
            return Ok((state, res));
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            Value::Object(response).to_string(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_head_index_alias(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_head_index_alias");
    async {
        let path_extractor = NameAliasPathExtractor::borrow_from(&state);
        let requested_indices = requested_parts(&path_extractor.name);
        let requested_aliases = requested_parts(&path_extractor.alias);
        let response = match build_alias_response(&requested_indices, &requested_aliases).await {
            Ok(response) => response,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        let res = if response.is_empty() {
            create_empty_response(&state, StatusCode::NOT_FOUND)
        } else {
            create_empty_response(&state, StatusCode::OK)
        };
        Ok((state, res))
    }
    .boxed()
}

pub fn es_get_index_settings(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index_aliases");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let mut response = Map::new();

        for table_name in path_extractor.name.to_string().split(",") {
            let table_desc = match STATE_PROVIDER.describe_table(&table_name.to_string()).await {
                Ok(td) => td,
                Err(e) => {
                    let res = log_service_err(e).generate_response(&state);
                    return Ok((state, res));
                }
            };

            if let Some(table_desc) = table_desc {
                let body = parse_index_body(&table_desc);
                response.insert(
                    table_desc.name.clone(),
                    json!({ "settings": settings_value(&body) }),
                );
            }
        }

        if response.is_empty() {
            let res = create_empty_response(&state, StatusCode::NOT_FOUND);
            return Ok((state, res));
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            Value::Object(response).to_string(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_get_index_mapping(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index_mapping");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let mut response = Map::new();

        for table_name in path_extractor.name.to_string().split(",") {
            let table_desc = match STATE_PROVIDER.describe_table(&table_name.to_string()).await {
                Ok(td) => td,
                Err(e) => {
                    let res = log_service_err(e).generate_response(&state);
                    return Ok((state, res));
                }
            };

            if let Some(table_desc) = table_desc {
                let body = parse_index_body(&table_desc);
                response.insert(
                    table_desc.name.clone(),
                    json!({ "mappings": mappings_value(&body) }),
                );
            }
        }

        if response.is_empty() {
            let res = create_empty_response(&state, StatusCode::NOT_FOUND);
            return Ok((state, res));
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            Value::Object(response).to_string(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_get_index_template(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_index_template");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let index_name = path_extractor.name.to_string();

        let table_desc = match STATE_PROVIDER.describe_table_template(&index_name).await {
            Ok(td) => td,
            Err(e) => return Ok(log_service_err_response(e, state)),
        };

        let response =
            table_desc.map_or_else(|| "{}".to_string(), |x| serde_json::to_string(&x).unwrap());

        let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, response);
        Ok((state, res))
    }
    .boxed()
}

pub fn es_resolve_index(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_resolve_index");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let requested_names = requested_parts(&path_extractor.name);
        let table_descriptions = match all_table_descriptions().await {
            Ok(table_descriptions) => table_descriptions,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        let mut indices = Vec::new();
        let mut aliases_to_indices: HashMap<String, Vec<String>> = HashMap::new();

        for table_desc in table_descriptions {
            let body = parse_index_body(&table_desc);
            let aliases = alias_names(&body);
            let table_name = table_desc.name.clone();

            if matches_requested_value(&table_name, &requested_names) {
                indices.push(json!({
                    "name": table_name,
                    "aliases": aliases,
                    "attributes": ["open"],
                }));
            }

            for alias in alias_names(&body) {
                if matches_requested_value(&alias, &requested_names) {
                    aliases_to_indices
                        .entry(alias)
                        .or_default()
                        .push(table_desc.name.clone());
                }
            }
        }

        let mut aliases = aliases_to_indices
            .into_iter()
            .map(|(name, mut indices)| {
                indices.sort();
                json!({
                    "name": name,
                    "indices": indices,
                })
            })
            .collect::<Vec<_>>();

        indices.sort_by(|left, right| {
            left["name"]
                .as_str()
                .unwrap_or_default()
                .cmp(right["name"].as_str().unwrap_or_default())
        });
        aliases.sort_by(|left, right| {
            left["name"]
                .as_str()
                .unwrap_or_default()
                .cmp(right["name"].as_str().unwrap_or_default())
        });

        if indices.is_empty() && aliases.is_empty() {
            let res = create_empty_response(&state, StatusCode::NOT_FOUND);
            return Ok((state, res));
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            json!({
                "indices": indices,
                "aliases": aliases,
                "data_streams": [],
            })
            .to_string(),
        );
        Ok((state, res))
    }
    .boxed()
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
        let create_single_result =
            elastic_search_ingest::create_single(&index_name, &doc_id, &body_content).await;
        match create_single_result {
            Ok(success) => {
                let res = success.generate_response(&state);
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(
                    &state,
                    StatusCode::ALREADY_REPORTED,
                    mime::TEXT_PLAIN,
                    e.message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
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
        let create_single_result =
            elastic_search_ingest::upsert_single(&index_name, &doc_id, &body_content).await;
        match create_single_result {
            Ok(success_response) => {
                let res = success_response.generate_response(&state);
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(
                    &state,
                    StatusCode::ALREADY_REPORTED,
                    mime::TEXT_PLAIN,
                    e.message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
}

pub fn es_get_with_id(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_with_id");
    async {
        let path_extractor = NameIdPathExtractor::borrow_from(&state);
        let index_name = path_extractor.name.to_string();
        let doc_id = path_extractor.id.to_string();
        let table_desc = match STATE_PROVIDER.describe_table(&index_name).await {
            Ok(td) => td,
            Err(e) => return Ok(log_service_err_response(e, state)),
        };
        match table_desc {
            Some(td) => {
                let command = LookupById::new(&td.name, &vec![doc_id]);
                let response = execute_command(CommandContext {}, Arc::new(command)).await;
                let res = response.generate_response(&state);
                Ok((state, res))
            }
            None => {
                panic!("Table not found");
            }
        }
    }
    .boxed()
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
            }
            Err(_) => panic!("Error time"),
        }
    }
    .boxed()
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
        let create_pipeline_result =
            elastic_search_pipeline::create_pipeline(&name, &body_content).await;
        match create_pipeline_result {
            Ok(success) => {
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    serde_json::to_string(&success).unwrap(),
                );
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(
                    &state,
                    StatusCode::ALREADY_REPORTED,
                    mime::TEXT_PLAIN,
                    e.message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
}

pub fn es_simulate_pipeline(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_create_pipeline");
    async {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let simulate_pipeline_result =
            elastic_search_pipeline::simulate_pipeline(&None, &body_content).await;
        match simulate_pipeline_result {
            Ok(success) => {
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    serde_json::to_string(&success).unwrap(),
                );
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(
                    &state,
                    StatusCode::ALREADY_REPORTED,
                    mime::TEXT_PLAIN,
                    e.message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
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
        let simulate_pipeline_result =
            elastic_search_pipeline::simulate_pipeline(&Some(name), &body_content).await;
        match simulate_pipeline_result {
            Ok(success) => {
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    serde_json::to_string(&success).unwrap(),
                );
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(
                    &state,
                    StatusCode::ALREADY_REPORTED,
                    mime::TEXT_PLAIN,
                    e.message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
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
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    MIME_ES_JSON.clone(),
                    serde_json::to_string(&success).unwrap(),
                );
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(
                    &state,
                    StatusCode::ALREADY_REPORTED,
                    mime::TEXT_PLAIN,
                    e.message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
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
        let create_index_result =
            elastic_search_ingest::create_index_template(&table, &body_content).await;
        match create_index_result {
            Ok(success) => {
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    MIME_ES_JSON.clone(),
                    serde_json::to_string(&success).unwrap(),
                );
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(
                    &state,
                    StatusCode::ALREADY_REPORTED,
                    mime::TEXT_PLAIN,
                    e.message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
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
                }
                None => {
                    let res = create_empty_response(&state, StatusCode::NOT_FOUND);
                    Ok((state, res))
                }
            },
            Err(e) => return Ok(log_service_err_response(e, state)),
        }
    }
    .boxed()
}

pub fn es_get_template(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_get_template");
    async {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let table = path_extractor.name.to_string();

        match STATE_PROVIDER.describe_table_template(&table).await {
            Ok(tt) => match tt {
                Some(t) => {
                    let res = create_response(
                        &state,
                        StatusCode::OK,
                        mime::APPLICATION_JSON,
                        serde_json::to_string(&t).unwrap(),
                    );
                    Ok((state, res))
                }
                None => {
                    let res = create_empty_response(&state, StatusCode::NOT_FOUND);
                    Ok((state, res))
                }
            },
            Err(e) => return Ok(log_service_err_response(e, state)),
        }
    }
    .boxed()
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub(crate) struct QueryStringAliases {
    #[allow(dead_code)]
    timeout: Option<String>,
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
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    MIME_ES_JSON.clone(),
                    serde_json::to_string(&response).unwrap(),
                );
                Ok((state, res))
            }
            Err(e) => {
                let res = create_response(
                    &state,
                    StatusCode::ALREADY_REPORTED,
                    mime::TEXT_PLAIN,
                    e.message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
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
        QueryStringSearch {
            allow_partial_search_results: None,
            sort: None,
            rest_total_hits_as_int: None,
        }
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
        let body_content = normalize_search_body(&String::from_utf8(valid_body.to_vec()).unwrap());
        let command = match elastic_search_parser::parse(None, &body_content, &query_string) {
            Ok(c) => c,
            Err(_) => {
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    "Bad request".to_string(),
                );
                return Ok((state, res));
            }
        };
        let response =
            search_executor::execute_search_command(CommandContext {}, Arc::new(command)).await;
        let res = response.generate_response(&state);
        Ok((state, res))
    }
    .boxed()
}

pub fn es_count(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_count");
    async {
        let query_string = QueryStringSearch::take_from(&mut state);
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let search_body = match count_body_as_search_body(&body_content) {
            Ok(body) => body,
            Err(_) => {
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    "Bad request".to_string(),
                );
                return Ok((state, res));
            }
        };

        let command = match elastic_search_parser::parse(None, &search_body, &query_string) {
            Ok(c) => c,
            Err(_) => {
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    "Bad request".to_string(),
                );
                return Ok((state, res));
            }
        };

        let count_result = match search_executor::execute_count_command(Arc::new(command)).await {
            Ok(result) => result,
            Err(response) => {
                let res = response.generate_response(&state);
                return Ok((state, res));
            }
        };

        let count_response = CountResponse {
            count: count_result.total_hits,
            _shards: QueryResultShards {
                total: count_result.num_shards,
                successful: count_result.num_shards,
                skipped: 0,
                failed: 0,
            },
        };
        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            serde_json::to_string(&count_response).unwrap(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_count_table(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_count_table");
    async {
        let path_extractor = NamePathExtractor::take_from(&mut state);
        let query_string = QueryStringSearch::take_from(&mut state);
        let table = path_extractor.name.to_string();
        let table_desc = match STATE_PROVIDER.describe_table(&table).await {
            Ok(td) => match td {
                Some(td) => td,
                None => {
                    let res = create_response(
                        &state,
                        StatusCode::BAD_REQUEST,
                        mime::TEXT_PLAIN,
                        "Bad request".to_string(),
                    );
                    return Ok((state, res));
                }
            },
            Err(e) => return Ok(log_service_err_response(e, state)),
        };
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let search_body = match count_body_as_search_body(&body_content) {
            Ok(body) => body,
            Err(_) => {
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    "Bad request".to_string(),
                );
                return Ok((state, res));
            }
        };
        let command = match elastic_search_parser::parse(
            Some(table_desc.name),
            &search_body,
            &query_string,
        ) {
            Ok(c) => c,
            Err(_) => {
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    "Bad request".to_string(),
                );
                return Ok((state, res));
            }
        };
        let count_result = match search_executor::execute_count_command(Arc::new(command)).await {
            Ok(result) => result,
            Err(response) => {
                let res = response.generate_response(&state);
                return Ok((state, res));
            }
        };

        let count_response = CountResponse {
            count: count_result.total_hits,
            _shards: QueryResultShards {
                total: count_result.num_shards,
                successful: count_result.num_shards,
                skipped: 0,
                failed: 0,
            },
        };
        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            serde_json::to_string(&count_response).unwrap(),
        );
        Ok((state, res))
    }
    .boxed()
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
                    let res = create_response(
                        &state,
                        StatusCode::BAD_REQUEST,
                        mime::TEXT_PLAIN,
                        "Index does not exist".to_string(),
                    );
                    return Ok((state, res));
                }
            },
            Err(e) => return Ok(log_service_err_response(e, state)),
        };

        let command = match elastic_search_parser::parse_update_by_query(
            Some(table_description.name),
            &body_content,
        ) {
            Ok(c) => c,
            Err(_) => {
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    "Bad request".to_string(),
                );
                return Ok((state, res));
            }
        };
        let response = execute_command(CommandContext {}, Arc::new(command)).await;
        let res = response.generate_response(&state);
        Ok((state, res))
    }
    .boxed()
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
                    let res = create_response(
                        &state,
                        StatusCode::BAD_REQUEST,
                        mime::TEXT_PLAIN,
                        "Bad request".to_string(),
                    );
                    return Ok((state, res));
                }
            },
            Err(e) => return Ok(log_service_err_response(e, state)),
        };
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => {
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    "Bad request".to_string(),
                );
                return Ok((state, res));
            }
        };
        let body_content = normalize_search_body(&String::from_utf8(valid_body.to_vec()).unwrap());
        let command = match elastic_search_parser::parse(
            Some(table_desc.name),
            &body_content,
            &query_extractor,
        ) {
            Ok(c) => c,
            Err(_e) => {
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    "Bad request".to_string(),
                );
                return Ok((state, res));
            }
        };
        let response =
            search_executor::execute_search_command(CommandContext {}, Arc::new(command)).await;
        let res = response.generate_response(&state);
        Ok((state, res))
    }
    .boxed()
}

/// Handler function for `POST` requests directed to `/:table/_pit`
pub fn es_index_pit(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_index_pit");
    async {
        let _path_extractor = NamePathExtractor::borrow_from(&state);
        // TODO: really generate this. just needs to be an encoded checkpoint id for this table
        let response_data = HashMap::from([("succeeded", json!(true)), ("num_freed", json!(1))]);
        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            serde_json::to_string(&response_data).unwrap(),
        );
        Ok((state, res))
    }
    .boxed()
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
        let ingest_result = elastic_search_ingest::INGEST_HANDLE
            .send(&body_content)
            .await;
        match ingest_result {
            Ok(success) => {
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    MIME_ES_JSON.clone(),
                    serde_json::to_string(&success).unwrap(),
                );
                Ok((state, res))
            }
            Err(e) => {
                let _error = format!("{}", e.message);
                panic!("Oopsie");
            }
        }
    }
    .boxed()
}
