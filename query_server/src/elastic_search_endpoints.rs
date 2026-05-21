use std::{collections::HashMap, env, pin::Pin, sync::Arc};

use datafusion::arrow::datatypes::{DataType, Field};
use futures::FutureExt;
use gotham::helpers::http::response::create_empty_response;
use gotham::{
    handler::HandlerFuture,
    helpers::http::response::create_response,
    hyper::{Body, body},
    mime,
    state::{FromState, State},
};
use http::StatusCode;
use idgenerator::IdInstance;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::elastic_search_cluster_info;
use crate::elastic_search_http_types::{
    QueryStringAliases, QueryStringClusterHealth, QueryStringClusterSettings, QueryStringFieldCaps,
    QueryStringSearchExtractor,
};
use powdrr_query_lib::data_access;
use powdrr_query_lib::data_contract::{AliasInfo, CreateIndexBody, PropertyInfo, TableDescription};
use powdrr_query_lib::elastic_search_api_types::QueryStringSearch;
use powdrr_query_lib::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema};
use powdrr_query_runtime::elastic_search_common::{
    CommandContext, ElasticSearchResponse, MIME_ES_JSON, execute_command,
};
use powdrr_query_runtime::elastic_search_ingest;
use powdrr_query_runtime::elastic_search_parser;
use powdrr_query_runtime::elastic_search_pipeline;
use powdrr_query_runtime::elastic_search_responses::{
    ErrorDetails, QueryResultHit, QueryResultShards, QueryResults, SingleDocCreateFailedResult,
};
use powdrr_query_runtime::peers::CheckpointDescriptor;
use powdrr_query_runtime::search_executor;
use powdrr_query_runtime::search_runtime::df_to_serde_value;
use powdrr_query_runtime::state_provider::{STATE_PROVIDER, ServiceApiError};
use powdrr_query_runtime::util::{log_service_err, log_service_err_response};

pub use crate::elastic_search_http_types::{
    AliasPathExtractor, NameAliasPathExtractor, NameIdPathExtractor, NamePathExtractor,
};

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

#[derive(Deserialize)]
#[serde(untagged)]
enum FieldCapsFieldsInput {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Deserialize, Default)]
struct FieldCapsBody {
    fields: Option<FieldCapsFieldsInput>,
}

#[derive(Serialize)]
struct ClusterHealthResponse {
    cluster_name: String,
    status: String,
    timed_out: bool,
    number_of_nodes: u32,
    number_of_data_nodes: u32,
    active_primary_shards: u32,
    active_shards: u32,
    relocating_shards: u32,
    initializing_shards: u32,
    unassigned_shards: u32,
    delayed_unassigned_shards: u32,
    number_of_pending_tasks: u32,
    number_of_in_flight_fetch: u32,
    task_max_waiting_in_queue_millis: u32,
    active_shards_percent_as_number: f64,
}

#[derive(Serialize, Clone)]
struct FieldCapsTypeResponse {
    #[serde(rename = "type")]
    type_name: String,
    searchable: bool,
    aggregatable: bool,
    metadata_field: bool,
    indices: Vec<String>,
}

pub fn es_cluster_settings(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_cluster_settings");
    async {
        let query_string = QueryStringClusterSettings::take_from(&mut state);
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

fn elasticsearch_error_response(
    state: &State,
    status: StatusCode,
    error_type: &str,
    reason: &str,
) -> gotham::hyper::Response<Body> {
    let response = SingleDocCreateFailedResult {
        error: ErrorDetails::single_cause(
            &error_type.to_string(),
            &reason.to_string(),
            None,
            None,
            None,
        ),
        status: status.as_u16() as u32,
    };
    create_response(
        state,
        status,
        mime::APPLICATION_JSON,
        serde_json::to_string(&response).unwrap(),
    )
}

fn unsupported_api_response(state: &State, reason: &str) -> gotham::hyper::Response<Body> {
    elasticsearch_error_response(
        state,
        StatusCode::NOT_IMPLEMENTED,
        "unsupported_operation_exception",
        reason,
    )
}

pub fn es_unsupported_search_scroll(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_unsupported_search_scroll");
    async {
        let res = unsupported_api_response(
            &state,
            "The scroll API is not supported. Use point in time plus search_after instead.",
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_unsupported_search_template(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_unsupported_search_template");
    async {
        let res = unsupported_api_response(&state, "The search template API is not supported.");
        Ok((state, res))
    }
    .boxed()
}

pub fn es_unsupported_async_search(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_unsupported_async_search");
    async {
        let res = unsupported_api_response(&state, "The async search API is not supported.");
        Ok((state, res))
    }
    .boxed()
}

pub fn es_unsupported_cat_indices(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_unsupported_cat_indices");
    async {
        let res = unsupported_api_response(&state, "The cat indices API is not supported.");
        Ok((state, res))
    }
    .boxed()
}

pub fn es_unsupported_cat_aliases(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_unsupported_cat_aliases");
    async {
        let res = unsupported_api_response(&state, "The cat aliases API is not supported.");
        Ok((state, res))
    }
    .boxed()
}

pub fn es_unsupported_get_pipeline(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_unsupported_get_pipeline");
    async {
        let res = unsupported_api_response(
            &state,
            "Reading ingest pipeline definitions is not supported.",
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_unsupported_create_pipeline(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_unsupported_create_pipeline");
    async {
        let res = unsupported_api_response(
            &state,
            "Persisted ingest pipelines are not supported. Use POST /_ingest/pipeline/_simulate for inline pipeline execution.",
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_unsupported_get_pipeline_simulate(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_unsupported_get_pipeline_simulate");
    async {
        let res = unsupported_api_response(
            &state,
            "GET pipeline simulation is not supported. Use POST /_ingest/pipeline/_simulate or POST /_ingest/pipeline/{name}/_simulate.",
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_unsupported_named_pipeline_simulate(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_unsupported_named_pipeline_simulate");
    async {
        let res = unsupported_api_response(
            &state,
            "Named pipeline simulation is not supported because persisted ingest pipelines are not supported. Use POST /_ingest/pipeline/_simulate.",
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_cluster_health(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_cluster_health");
    async {
        let _query_string = QueryStringClusterHealth::take_from(&mut state);
        let response = ClusterHealthResponse {
            cluster_name: env::var("cluster.name").unwrap_or("docker-cluster".into()),
            status: "green".to_string(),
            timed_out: false,
            number_of_nodes: 1,
            number_of_data_nodes: 1,
            active_primary_shards: 1,
            active_shards: 1,
            relocating_shards: 0,
            initializing_shards: 0,
            unassigned_shards: 0,
            delayed_unassigned_shards: 0,
            number_of_pending_tasks: 0,
            number_of_in_flight_fetch: 0,
            task_max_waiting_in_queue_millis: 0,
            active_shards_percent_as_number: 100.0,
        };
        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            serde_json::to_string(&response).unwrap(),
        );
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

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let value_chars = value.chars().collect::<Vec<_>>();
    let mut pattern_index = 0usize;
    let mut value_index = 0usize;
    let mut last_star_index = None;
    let mut last_match_index = 0usize;

    while value_index < value_chars.len() {
        if pattern_index < pattern_chars.len()
            && (pattern_chars[pattern_index] == '?'
                || pattern_chars[pattern_index] == value_chars[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern_chars.len() && pattern_chars[pattern_index] == '*' {
            last_star_index = Some(pattern_index);
            pattern_index += 1;
            last_match_index = value_index;
        } else if let Some(star_index) = last_star_index {
            pattern_index = star_index + 1;
            last_match_index += 1;
            value_index = last_match_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern_chars.len() && pattern_chars[pattern_index] == '*' {
        pattern_index += 1;
    }

    pattern_index == pattern_chars.len()
}

fn matches_requested_value(value: &str, requested: &[String]) -> bool {
    requests_all(requested)
        || requested
            .iter()
            .any(|requested_value| wildcard_matches(requested_value, value))
}

async fn all_table_descriptions() -> Result<Vec<TableDescription>, ServiceApiError> {
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
) -> Result<Vec<TableDescription>, ServiceApiError> {
    let all_descriptions = all_table_descriptions().await?;
    let resolved_names =
        resolved_target_names_from_descriptions(requested_indices, &all_descriptions);
    Ok(all_descriptions
        .into_iter()
        .filter(|table_desc| resolved_names.contains(&table_desc.name))
        .collect())
}

fn matching_target_names(
    requested_value: &str,
    table_descriptions: &[TableDescription],
) -> Vec<String> {
    let mut matches = table_descriptions
        .iter()
        .filter(|table_desc| wildcard_matches(requested_value, &table_desc.name))
        .map(|table_desc| table_desc.name.clone())
        .collect::<Vec<_>>();

    for table_desc in table_descriptions {
        let body = parse_index_body(table_desc);
        if alias_names(&body)
            .iter()
            .any(|alias_name| wildcard_matches(requested_value, alias_name))
        {
            matches.push(table_desc.name.clone());
        }
    }

    matches.sort();
    matches.dedup();
    matches
}

fn resolved_target_names_from_descriptions(
    requested_indices: &[String],
    table_descriptions: &[TableDescription],
) -> Vec<String> {
    if requests_all(requested_indices) {
        let mut all_names = table_descriptions
            .iter()
            .map(|table_desc| table_desc.name.clone())
            .collect::<Vec<_>>();
        all_names.sort();
        all_names.dedup();
        return all_names;
    }

    let mut resolved = requested_indices
        .iter()
        .flat_map(|requested_value| matching_target_names(requested_value, table_descriptions))
        .collect::<Vec<_>>();
    resolved.sort();
    resolved.dedup();
    resolved
}

async fn resolve_read_target_names(
    requested_indices: &[String],
    query_string: &QueryStringSearch,
) -> Result<Vec<String>, (StatusCode, String)> {
    let table_descriptions = all_table_descriptions()
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.message))?;
    let mut resolved = Vec::new();
    let mut unresolved = Vec::new();

    if requests_all(requested_indices) {
        resolved = resolved_target_names_from_descriptions(requested_indices, &table_descriptions);
    } else {
        for requested_value in requested_indices {
            let matches = matching_target_names(requested_value, &table_descriptions);
            if matches.is_empty() {
                unresolved.push(requested_value.clone());
            } else {
                resolved.extend(matches);
            }
        }
        resolved.sort();
        resolved.dedup();
    }

    let ignore_unavailable = query_string.ignore_unavailable.unwrap_or(false);
    let allow_no_indices = query_string.allow_no_indices.unwrap_or(false);

    if !unresolved.is_empty() && !ignore_unavailable && !allow_no_indices {
        return Err((StatusCode::NOT_FOUND, "Index does not exist".to_string()));
    }

    if resolved.is_empty()
        && !requests_all(requested_indices)
        && !allow_no_indices
        && !ignore_unavailable
    {
        return Err((StatusCode::NOT_FOUND, "Index does not exist".to_string()));
    }

    Ok(resolved)
}

fn invalid_request_response(
    state: &State,
    status: StatusCode,
    message: &str,
) -> gotham::hyper::Response<Body> {
    create_response(state, status, mime::TEXT_PLAIN, message.to_string())
}

async fn resolve_single_read_target_name(
    target: Option<&str>,
    query_string: &QueryStringSearch,
) -> Result<Option<String>, (StatusCode, String)> {
    let Some(target) = target.map(str::trim).filter(|target| !target.is_empty()) else {
        return Ok(None);
    };

    let requested = requested_parts(target);
    let resolved = resolve_read_target_names(&requested, query_string).await?;
    if resolved.len() != 1 {
        return Ok(None);
    }

    Ok(resolved.into_iter().next())
}

async fn resolve_document_target_name(
    target: &str,
) -> Result<Option<String>, (StatusCode, String)> {
    let requested = requested_parts(target);
    if requested.is_empty() {
        return Ok(None);
    }

    let table_descriptions = all_table_descriptions()
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.message))?;
    let resolved = resolved_target_names_from_descriptions(&requested, &table_descriptions);
    match resolved.as_slice() {
        [] => Ok(None),
        [target] => Ok(Some(target.clone())),
        _ => Err((
            StatusCode::BAD_REQUEST,
            "Index expression must resolve to exactly one index".to_string(),
        )),
    }
}

async fn execute_search_response(
    target: Option<String>,
    body_content: &str,
    query_string: &QueryStringSearch,
) -> Result<ElasticSearchResponse, (StatusCode, String)> {
    let body_content = normalize_search_body(body_content);
    let command = match elastic_search_parser::parse(target, &body_content, query_string) {
        Ok(command) => command,
        Err(_) => {
            return Err((StatusCode::BAD_REQUEST, "Bad request".to_string()));
        }
    };

    Ok(search_executor::execute_search_command(CommandContext {}, Arc::new(command)).await)
}

async fn execute_search_response_for_target_expr(
    target_expr: Option<&str>,
    body_content: &str,
    query_string: &QueryStringSearch,
) -> Result<ElasticSearchResponse, (StatusCode, String)> {
    let Some(target_expr) = target_expr else {
        return execute_search_response(None, body_content, query_string).await;
    };

    let requested = requested_parts(target_expr);
    let resolved = resolve_read_target_names(&requested, query_string).await?;
    match resolved.as_slice() {
        [] => {
            let total_hits_complex = !query_string.rest_total_hits_as_int.unwrap_or(false);
            Ok(QueryResults::empty(0, 0, None, total_hits_complex).to_response())
        }
        [target] => execute_search_response(Some(target.clone()), body_content, query_string).await,
        _ => {
            let normalized_body = normalize_search_body(body_content);
            let commands = resolved
                .into_iter()
                .map(|target| {
                    elastic_search_parser::parse(Some(target), &normalized_body, query_string)
                        .map(Arc::new)
                        .map_err(|_| (StatusCode::BAD_REQUEST, "Bad request".to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(
                search_executor::execute_multi_target_search_commands(CommandContext {}, commands)
                    .await,
            )
        }
    }
}

async fn lookup_document_value(index_name: &str, doc_id: &str) -> Result<Value, String> {
    let table_desc = STATE_PROVIDER
        .describe_table(&index_name.to_string())
        .await
        .map_err(|e| e.message)?;

    let Some(table_desc) = table_desc else {
        return Ok(json!({
            "_index": index_name,
            "_id": doc_id,
            "found": false,
        }));
    };

    let checkpoint_id = STATE_PROVIDER
        .get_published_active_servable_checkpoint(&table_desc.name)
        .await
        .map_err(|e| e.message)?;
    let Some(checkpoint_id) = checkpoint_id else {
        return Ok(json!({
            "_index": table_desc.name,
            "_id": doc_id,
            "found": false,
        }));
    };

    let checkpoint = STATE_PROVIDER
        .get_checkpoint(CheckpointDescriptor::new(
            table_desc.name.clone(),
            checkpoint_id,
        ))
        .await
        .map_err(|e| e.message)?;
    let Some(checkpoint) = checkpoint else {
        return Ok(json!({
            "_index": table_desc.name,
            "_id": doc_id,
            "found": false,
        }));
    };

    let checkpoint_schema = checkpoint.schema.clone();
    let mut local_tables: Vec<(String, PowdrrSchema)> = Vec::new();
    let mut delete_local_tables = Vec::new();

    if let Some(iceberg_metadata) = checkpoint.iceberg_metadata {
        for file_descriptor in iceberg_metadata.files.as_file_tuples() {
            let local_name = format!("mget_iceberg_{}", IdInstance::next_id());
            data_access::load_file_as_table(
                &local_name,
                &file_descriptor.file_path,
                true,
                Some(file_descriptor.schema.to_arrow_schema()),
            )
            .await
            .map_err(|e| e.message().to_string())?;
            local_tables.push((local_name, file_descriptor.schema));
        }
    }

    if let Some(speedboat_metadata) = checkpoint.speedboat_metadata {
        for file_descriptor in speedboat_metadata.files.as_file_tuples() {
            let local_name = format!("mget_speedboat_{}", IdInstance::next_id());
            data_access::load_file_as_table(
                &local_name,
                &file_descriptor.file_path,
                false,
                Some(file_descriptor.schema.to_arrow_schema()),
            )
            .await
            .map_err(|e| e.message().to_string())?;
            local_tables.push((local_name, file_descriptor.schema));
        }
    }

    if let Some(deletes_metadata) = checkpoint.deletes_metadata {
        let delete_schema = PowdrrSchema::from(&vec![PowdrrField {
            name: "_id_seq_no".to_string(),
            data_type: PowdrrDataType::String,
        }]);

        for delete_file_path in deletes_metadata.files {
            let local_name = format!("mget_delete_{}", IdInstance::next_id());
            data_access::load_file_as_table(
                &local_name,
                &delete_file_path,
                false,
                Some(delete_schema.to_arrow_schema()),
            )
            .await
            .map_err(|e| e.message().to_string())?;
            delete_local_tables.push(local_name);
        }
    }

    if local_tables.is_empty() {
        return Ok(json!({
            "_index": table_desc.name,
            "_id": doc_id,
            "found": false,
        }));
    }

    let escaped_doc_id = doc_id.replace('\'', "''");
    let union_sql = local_tables
        .iter()
        .map(|(table_name, file_schema)| {
            build_document_lookup_select(table_name, &checkpoint_schema, file_schema)
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let deletes_table_name = create_document_lookup_deletes_table(&delete_local_tables).await?;
    let lookup_sql = format!(
        "SELECT docs.* FROM ({union_sql}) AS docs \
         LEFT JOIN {deletes_table_name} dt ON dt._id_seq_no = docs._id_seq_no \
         WHERE docs._id = '{escaped_doc_id}' AND dt._id_seq_no IS NULL \
         ORDER BY docs._seq_no DESC, docs._version DESC LIMIT 1"
    );
    let lookup_df = data_access::execute_sql(&lookup_sql)
        .await
        .map_err(|e| e.message().to_string())?;
    let serde_result = df_to_serde_value(&lookup_df)
        .await
        .map_err(|e| e.message.clone())?;

    for (table_name, _) in &local_tables {
        data_access::drop(table_name).await;
    }
    for table_name in &delete_local_tables {
        data_access::drop(table_name).await;
    }
    data_access::drop(&deletes_table_name).await;

    let Some(value) = serde_result.values.first() else {
        return Ok(json!({
            "_index": table_desc.name,
            "_id": doc_id,
            "found": false,
        }));
    };

    serde_json::to_value(QueryResultHit::from_record(
        &Some(table_desc.name.clone()),
        value,
        Some(true),
    ))
    .map_err(|e| e.to_string())
}

fn build_document_lookup_select(
    table_name: &str,
    checkpoint_schema: &PowdrrSchema,
    file_schema: &PowdrrSchema,
) -> String {
    let checkpoint_arrow_schema = checkpoint_schema.to_arrow_schema();
    let file_arrow_schema = file_schema.to_arrow_schema();
    let select_fields = checkpoint_arrow_schema
        .fields
        .iter()
        .map(|field| lookup_select_field_sql(field.as_ref(), &file_arrow_schema))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {select_fields} FROM {table_name}")
}

fn lookup_select_field_sql(
    field: &Field,
    file_arrow_schema: &datafusion::arrow::datatypes::Schema,
) -> String {
    let escaped_name = escape_lookup_identifier(field.name());
    if file_arrow_schema.field_with_name(field.name()).is_ok() {
        format!("\"{escaped_name}\"")
    } else {
        format!(
            "CAST(NULL AS {}) AS \"{escaped_name}\"",
            lookup_sql_type(field.data_type())
        )
    }
}

fn escape_lookup_identifier(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}

fn lookup_sql_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "DOUBLE",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "BIGINT",
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "STRING",
        _ => "STRING",
    }
}

async fn create_document_lookup_deletes_table(local_names: &[String]) -> Result<String, String> {
    let table_name = format!("mget_all_deletes_{}", IdInstance::next_id());
    let ddl_stmt = if local_names.is_empty() {
        "select null as _id_seq_no".to_string()
    } else {
        let union_selects = local_names
            .iter()
            .map(|table_name| format!("select * from {table_name}"))
            .collect::<Vec<_>>()
            .join(" union all ");
        format!("select * from ({union_selects})")
    };

    data_access::create_table(&table_name, &ddl_stmt)
        .await
        .map_err(|e| e.message().to_string())?;
    Ok(table_name)
}

fn parse_msearch_lines(body_content: &str) -> Result<Vec<(String, String)>, ()> {
    let lines = body_content
        .split_terminator('\n')
        .map(|line| line.trim_end_matches('\r').to_string())
        .collect::<Vec<_>>();

    if lines.len() % 2 != 0 {
        return Err(());
    }

    Ok(lines
        .chunks(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect())
}

fn msearch_error_value(status: StatusCode, reason: String) -> Value {
    serde_json::to_value(MultiSearchErrorItem {
        error: MultiSearchErrorBody {
            type_name: "illegal_argument_exception".to_string(),
            reason,
        },
        status: status.as_u16(),
    })
    .unwrap()
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
) -> Result<Map<String, Value>, ServiceApiError> {
    let mut response = Map::new();
    let table_descriptions = requested_table_descriptions(requested_indices).await?;

    for table_desc in table_descriptions {
        let body = parse_index_body(&table_desc);
        let aliases = filtered_aliases_value(&body, requested_aliases);
        if aliases
            .as_object()
            .is_some_and(|aliases| !aliases.is_empty())
        {
            response.insert(table_desc.name.clone(), json!({ "aliases": aliases }));
        }
    }

    Ok(response)
}

fn normalize_field_caps_fields(
    query_string: &QueryStringFieldCaps,
    body: &FieldCapsBody,
) -> Vec<String> {
    if let Some(fields) = &query_string.fields {
        return requested_parts(fields);
    }
    match &body.fields {
        Some(FieldCapsFieldsInput::Single(field)) => requested_parts(field),
        Some(FieldCapsFieldsInput::Multiple(fields)) => fields
            .iter()
            .map(String::as_str)
            .flat_map(requested_parts)
            .collect(),
        None => Vec::new(),
    }
}

fn field_is_requested(field_name: &str, requested_fields: &[String]) -> bool {
    requests_all(requested_fields)
        || requested_fields
            .iter()
            .any(|requested| requested == field_name)
}

fn field_caps_for_type(type_name: &str) -> (bool, bool) {
    match type_name {
        "text" => (true, false),
        "keyword" | "long" | "integer" | "short" | "byte" | "double" | "float" | "half_float"
        | "scaled_float" | "unsigned_long" | "date" | "boolean" | "ip" => (true, true),
        _ => (false, false),
    }
}

fn add_field_caps_entry(
    field_caps: &mut HashMap<String, HashMap<String, FieldCapsTypeResponse>>,
    field_name: &str,
    type_name: &str,
    table_name: &str,
) {
    let (searchable, aggregatable) = field_caps_for_type(type_name);
    let entry = field_caps
        .entry(field_name.to_string())
        .or_default()
        .entry(type_name.to_string())
        .or_insert_with(|| FieldCapsTypeResponse {
            type_name: type_name.to_string(),
            searchable,
            aggregatable,
            metadata_field: false,
            indices: Vec::new(),
        });
    if !entry.indices.contains(&table_name.to_string()) {
        entry.indices.push(table_name.to_string());
        entry.indices.sort();
    }
}

fn collect_field_caps_for_properties(
    prefix: Option<&str>,
    properties: &HashMap<String, PropertyInfo>,
    requested_fields: &[String],
    table_name: &str,
    field_caps: &mut HashMap<String, HashMap<String, FieldCapsTypeResponse>>,
) {
    for (name, property) in properties {
        let field_name = prefix
            .map(|prefix| format!("{prefix}.{name}"))
            .unwrap_or_else(|| name.clone());

        if let Some(type_name) = property.type_name.as_deref() {
            if field_is_requested(&field_name, requested_fields) {
                add_field_caps_entry(field_caps, &field_name, type_name, table_name);
            }
        }

        if let Some(subfields) = &property.fields {
            collect_field_caps_for_properties(
                Some(&field_name),
                subfields,
                requested_fields,
                table_name,
                field_caps,
            );
        }
        if let Some(nested_properties) = &property.properties {
            collect_field_caps_for_properties(
                Some(&field_name),
                nested_properties,
                requested_fields,
                table_name,
                field_caps,
            );
        }
    }
}

async fn field_caps_response(
    requested_indices: &[String],
    requested_fields: &[String],
) -> Result<Value, ServiceApiError> {
    let mut table_descriptions = requested_table_descriptions(requested_indices).await?;
    table_descriptions.sort_by(|left, right| left.name.cmp(&right.name));
    let mut indices = Vec::new();
    let mut field_caps = HashMap::<String, HashMap<String, FieldCapsTypeResponse>>::new();

    for table_desc in table_descriptions {
        let table_name = table_desc.name.clone();
        let body = parse_index_body(&table_desc);
        indices.push(table_name.clone());

        if let Some(mappings) = body.mappings {
            collect_field_caps_for_properties(
                None,
                &mappings.properties,
                requested_fields,
                &table_name,
                &mut field_caps,
            );
        }
    }

    Ok(json!({
        "indices": indices,
        "fields": field_caps,
    }))
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
        let requested_indices = requested_parts(&path_extractor.name);
        let table_descriptions = match requested_table_descriptions(&requested_indices).await {
            Ok(table_descriptions) => table_descriptions,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        for table_desc in table_descriptions {
            let body = parse_index_body(&table_desc);
            response.insert(table_desc.name.clone(), index_info_value(&body));
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
        let requested_indices = requested_parts(&path_extractor.name);
        let table_descriptions = match requested_table_descriptions(&requested_indices).await {
            Ok(table_descriptions) => table_descriptions,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };
        let res = if table_descriptions.is_empty() {
            create_empty_response(&state, StatusCode::NOT_FOUND)
        } else {
            create_empty_response(&state, StatusCode::OK)
        };
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
        let requested_indices = requested_parts(&path_extractor.name);
        let table_descriptions = match requested_table_descriptions(&requested_indices).await {
            Ok(table_descriptions) => table_descriptions,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        for table_desc in table_descriptions {
            let body = parse_index_body(&table_desc);
            response.insert(
                table_desc.name.clone(),
                json!({ "settings": settings_value(&body) }),
            );
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
        let requested_indices = requested_parts(&path_extractor.name);
        let table_descriptions = match requested_table_descriptions(&requested_indices).await {
            Ok(table_descriptions) => table_descriptions,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        for table_desc in table_descriptions {
            let body = parse_index_body(&table_desc);
            response.insert(
                table_desc.name.clone(),
                json!({ "mappings": mappings_value(&body) }),
            );
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

pub fn es_field_caps(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_field_caps");
    async {
        let query_string = QueryStringFieldCaps::take_from(&mut state);
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let parsed_body = if body_content.trim().is_empty() {
            FieldCapsBody::default()
        } else {
            match serde_json::from_str::<FieldCapsBody>(&body_content) {
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
            }
        };
        let requested_fields = normalize_field_caps_fields(&query_string, &parsed_body);
        let response = match field_caps_response(&[], &requested_fields).await {
            Ok(response) => response,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            response.to_string(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_field_caps_index(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_field_caps_index");
    async {
        let path_extractor = NamePathExtractor::take_from(&mut state);
        let query_string = QueryStringFieldCaps::take_from(&mut state);
        let requested_indices = requested_parts(&path_extractor.name);
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let parsed_body = if body_content.trim().is_empty() {
            FieldCapsBody::default()
        } else {
            match serde_json::from_str::<FieldCapsBody>(&body_content) {
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
            }
        };
        let requested_fields = normalize_field_caps_fields(&query_string, &parsed_body);
        let response = match field_caps_response(&requested_indices, &requested_fields).await {
            Ok(response) => response,
            Err(e) => {
                let res = log_service_err(e).generate_response(&state);
                return Ok((state, res));
            }
        };

        let res = create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            response.to_string(),
        );
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

pub fn es_index_auto_id(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_index_auto_id");
    async move {
        let path_extractor = NamePathExtractor::borrow_from(&state);
        let index_name = path_extractor.name.to_string();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let index_result =
            elastic_search_ingest::index_single(&index_name, None, &body_content).await;
        match index_result {
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

pub fn es_index_with_id(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_index_with_id");
    async move {
        let path_extractor = NameIdPathExtractor::borrow_from(&state);
        let index_name = path_extractor.name.to_string();
        let doc_id = path_extractor.id.to_string();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let index_result =
            elastic_search_ingest::index_single(&index_name, Some(&doc_id), &body_content).await;
        match index_result {
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
        let requested_index = path_extractor.name.to_string();
        let doc_id = path_extractor.id.to_string();
        let index_name = match resolve_single_read_target_name(
            Some(&requested_index),
            &QueryStringSearch::new(),
        )
        .await
        {
            Ok(Some(index_name)) => index_name,
            Ok(None) => {
                let res = create_empty_response(&state, StatusCode::NOT_FOUND);
                return Ok((state, res));
            }
            Err((status, message)) => {
                let res = invalid_request_response(&state, status, &message);
                return Ok((state, res));
            }
        };
        let table_desc = match STATE_PROVIDER.describe_table(&index_name).await {
            Ok(td) => td,
            Err(e) => return Ok(log_service_err_response(e, state)),
        };
        match table_desc {
            Some(_) => match lookup_document_value(&index_name, &doc_id).await {
                Ok(doc) => {
                    let found = doc
                        .get("found")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(true);
                    let status = if found {
                        StatusCode::OK
                    } else {
                        StatusCode::NOT_FOUND
                    };
                    let res = create_response(
                        &state,
                        status,
                        MIME_ES_JSON.clone(),
                        serde_json::to_string(&doc).unwrap(),
                    );
                    Ok((state, res))
                }
                Err(message) => {
                    let res =
                        invalid_request_response(&state, StatusCode::SERVICE_UNAVAILABLE, &message);
                    Ok((state, res))
                }
            },
            None => {
                let res = create_empty_response(&state, StatusCode::NOT_FOUND);
                Ok((state, res))
            }
        }
    }
    .boxed()
}

pub fn es_head_with_id(state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_head_with_id");
    async {
        let path_extractor = NameIdPathExtractor::borrow_from(&state);
        let requested_index = path_extractor.name.to_string();
        let doc_id = path_extractor.id.to_string();
        let index_name = match resolve_single_read_target_name(
            Some(&requested_index),
            &QueryStringSearch::new(),
        )
        .await
        {
            Ok(Some(index_name)) => index_name,
            Ok(None) => {
                let res = create_empty_response(&state, StatusCode::NOT_FOUND);
                return Ok((state, res));
            }
            Err((status, message)) => {
                let res = invalid_request_response(&state, status, &message);
                return Ok((state, res));
            }
        };

        match lookup_document_value(&index_name, &doc_id).await {
            Ok(doc) => {
                let found = doc
                    .get("found")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(true);
                let status = if found {
                    StatusCode::OK
                } else {
                    StatusCode::NOT_FOUND
                };
                let res = create_empty_response(&state, status);
                Ok((state, res))
            }
            Err(message) => {
                let res =
                    invalid_request_response(&state, StatusCode::SERVICE_UNAVAILABLE, &message);
                Ok((state, res))
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[derive(Deserialize)]
#[serde(untagged)]
enum MultiTargetInput {
    Single(String),
    Multiple(Vec<String>),
}

impl MultiTargetInput {
    fn parts(&self) -> Vec<String> {
        match self {
            MultiTargetInput::Single(value) => requested_parts(value),
            MultiTargetInput::Multiple(values) => values
                .iter()
                .map(String::as_str)
                .flat_map(requested_parts)
                .collect(),
        }
    }
}

#[derive(Default, Deserialize)]
struct MultiSearchHeader {
    index: Option<MultiTargetInput>,
}

#[derive(Deserialize)]
struct MultiGetDocRequest {
    #[serde(rename = "_index")]
    index: Option<String>,
    #[serde(rename = "_id")]
    id: String,
}

#[derive(Default, Deserialize)]
struct MultiGetRequest {
    docs: Option<Vec<MultiGetDocRequest>>,
    ids: Option<Vec<String>>,
}

#[derive(Serialize)]
struct MultiSearchErrorBody {
    #[serde(rename = "type")]
    type_name: String,
    reason: String,
}

#[derive(Serialize)]
struct MultiSearchErrorItem {
    error: MultiSearchErrorBody,
    status: u16,
}

#[derive(Serialize)]
struct MultiSearchResponse {
    responses: Vec<Value>,
}

#[derive(Serialize)]
struct MultiGetResponse {
    docs: Vec<Value>,
}

/// Handler function for `POST` requests directed to `/_search`
pub fn es_search(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_search");
    async {
        let query_string: QueryStringSearch =
            QueryStringSearchExtractor::take_from(&mut state).into_query_string_search();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let response =
            match execute_search_response_for_target_expr(Some("*"), &body_content, &query_string)
                .await
            {
                Ok(response) => response,
                Err((status, message)) => {
                    let res = invalid_request_response(&state, status, &message);
                    return Ok((state, res));
                }
            };
        let res = response.generate_response(&state);
        Ok((state, res))
    }
    .boxed()
}

pub fn es_count(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_count");
    async {
        let query_string: QueryStringSearch =
            QueryStringSearchExtractor::take_from(&mut state).into_query_string_search();
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

        let resolved_targets = match resolve_read_target_names(&[], &query_string).await {
            Ok(targets) => targets,
            Err((status, message)) => {
                let res = invalid_request_response(&state, status, &message);
                return Ok((state, res));
            }
        };

        let mut total_hits = 0u64;
        let mut total_shards = 0u32;
        for target in resolved_targets {
            let command =
                match elastic_search_parser::parse(Some(target), &search_body, &query_string) {
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

            let count_result = match search_executor::execute_count_command(Arc::new(command)).await
            {
                Ok(result) => result,
                Err(response) => {
                    let res = response.generate_response(&state);
                    return Ok((state, res));
                }
            };
            total_hits += count_result.total_hits;
            total_shards += count_result.num_shards;
        }

        let count_response = CountResponse {
            count: total_hits,
            _shards: QueryResultShards {
                total: total_shards,
                successful: total_shards,
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
        let query_string: QueryStringSearch =
            QueryStringSearchExtractor::take_from(&mut state).into_query_string_search();
        let requested_indices = requested_parts(&path_extractor.name);
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
        let resolved_targets =
            match resolve_read_target_names(&requested_indices, &query_string).await {
                Ok(targets) => targets,
                Err((status, message)) => {
                    let res = invalid_request_response(&state, status, &message);
                    return Ok((state, res));
                }
            };

        let mut total_hits = 0u64;
        let mut total_shards = 0u32;
        for target in resolved_targets {
            let command =
                match elastic_search_parser::parse(Some(target), &search_body, &query_string) {
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
            let count_result = match search_executor::execute_count_command(Arc::new(command)).await
            {
                Ok(result) => result,
                Err(response) => {
                    let res = response.generate_response(&state);
                    return Ok((state, res));
                }
            };
            total_hits += count_result.total_hits;
            total_shards += count_result.num_shards;
        }

        let count_response = CountResponse {
            count: total_hits,
            _shards: QueryResultShards {
                total: total_shards,
                successful: total_shards,
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
        let query_extractor: QueryStringSearch =
            QueryStringSearchExtractor::take_from(&mut state).into_query_string_search();
        let table = path_extractor.name.to_string();
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
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let response = match execute_search_response_for_target_expr(
            Some(&table),
            &body_content,
            &query_extractor,
        )
        .await
        {
            Ok(response) => response,
            Err((status, message)) => {
                let res = invalid_request_response(&state, status, &message);
                return Ok((state, res));
            }
        };
        let res = response.generate_response(&state);
        Ok((state, res))
    }
    .boxed()
}

pub fn es_msearch(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_msearch");
    async {
        let query_string: QueryStringSearch =
            QueryStringSearchExtractor::take_from(&mut state).into_query_string_search();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let requests = match parse_msearch_lines(&body_content) {
            Ok(requests) => requests,
            Err(_) => {
                let res = invalid_request_response(&state, StatusCode::BAD_REQUEST, "Bad request");
                return Ok((state, res));
            }
        };

        let mut responses = Vec::with_capacity(requests.len());
        for (header_line, body_line) in requests {
            let header = if header_line.trim().is_empty() {
                MultiSearchHeader::default()
            } else {
                match serde_json::from_str::<MultiSearchHeader>(&header_line) {
                    Ok(header) => header,
                    Err(_) => {
                        responses.push(msearch_error_value(
                            StatusCode::BAD_REQUEST,
                            "Bad request".to_string(),
                        ));
                        continue;
                    }
                }
            };

            let target_expr = header.index.as_ref().and_then(|index| {
                let parts = index.parts();
                if parts.is_empty() {
                    None
                } else {
                    Some(parts.join(","))
                }
            });

            match execute_search_response_for_target_expr(
                target_expr.as_deref(),
                &body_line,
                &query_string,
            )
            .await
            {
                Ok(response) => match serde_json::from_str::<Value>(&response.body) {
                    Ok(value) => responses.push(value),
                    Err(_) => responses.push(msearch_error_value(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "An error occurred".to_string(),
                    )),
                },
                Err((status, message)) => responses.push(msearch_error_value(status, message)),
            }
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            MIME_ES_JSON.clone(),
            serde_json::to_string(&MultiSearchResponse { responses }).unwrap(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_msearch_table(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_msearch_table");
    async {
        let path_extractor = NamePathExtractor::take_from(&mut state);
        let table = path_extractor.name.to_string();
        let query_string: QueryStringSearch =
            QueryStringSearchExtractor::take_from(&mut state).into_query_string_search();
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let requests = match parse_msearch_lines(&body_content) {
            Ok(requests) => requests,
            Err(_) => {
                let res = invalid_request_response(&state, StatusCode::BAD_REQUEST, "Bad request");
                return Ok((state, res));
            }
        };

        let mut responses = Vec::with_capacity(requests.len());
        for (header_line, body_line) in requests {
            let header = if header_line.trim().is_empty() {
                MultiSearchHeader::default()
            } else {
                match serde_json::from_str::<MultiSearchHeader>(&header_line) {
                    Ok(header) => header,
                    Err(_) => {
                        responses.push(msearch_error_value(
                            StatusCode::BAD_REQUEST,
                            "Bad request".to_string(),
                        ));
                        continue;
                    }
                }
            };

            let target_expr = header
                .index
                .as_ref()
                .map(|index| index.parts().join(","))
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| table.clone());

            match execute_search_response_for_target_expr(
                Some(&target_expr),
                &body_line,
                &query_string,
            )
            .await
            {
                Ok(response) => match serde_json::from_str::<Value>(&response.body) {
                    Ok(value) => responses.push(value),
                    Err(_) => responses.push(msearch_error_value(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "An error occurred".to_string(),
                    )),
                },
                Err((status, message)) => responses.push(msearch_error_value(status, message)),
            }
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            MIME_ES_JSON.clone(),
            serde_json::to_string(&MultiSearchResponse { responses }).unwrap(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_mget(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_mget");
    async {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let request = match serde_json::from_str::<MultiGetRequest>(&body_content) {
            Ok(request) => request,
            Err(_) => {
                let res = invalid_request_response(&state, StatusCode::BAD_REQUEST, "Bad request");
                return Ok((state, res));
            }
        };

        let mut docs = Vec::new();
        if let Some(request_docs) = request.docs {
            for request_doc in request_docs {
                let Some(index_name) = request_doc.index.as_deref() else {
                    let res =
                        invalid_request_response(&state, StatusCode::BAD_REQUEST, "Bad request");
                    return Ok((state, res));
                };
                let resolved_target = match resolve_document_target_name(index_name).await {
                    Ok(Some(target)) => target,
                    Ok(None) => {
                        docs.push(json!({
                            "_index": index_name,
                            "_id": request_doc.id,
                            "found": false,
                        }));
                        continue;
                    }
                    Err((status, message)) => {
                        let res = invalid_request_response(&state, status, &message);
                        return Ok((state, res));
                    }
                };

                match lookup_document_value(&resolved_target, &request_doc.id).await {
                    Ok(doc) => docs.push(doc),
                    Err(message) => {
                        let res = invalid_request_response(
                            &state,
                            StatusCode::SERVICE_UNAVAILABLE,
                            &message,
                        );
                        return Ok((state, res));
                    }
                }
            }
        } else {
            let res = invalid_request_response(&state, StatusCode::BAD_REQUEST, "Bad request");
            return Ok((state, res));
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            MIME_ES_JSON.clone(),
            serde_json::to_string(&MultiGetResponse { docs }).unwrap(),
        );
        Ok((state, res))
    }
    .boxed()
}

pub fn es_mget_table(mut state: State) -> Pin<Box<HandlerFuture>> {
    tracing::info!("es_mget_table");
    async {
        let path_extractor = NamePathExtractor::take_from(&mut state);
        let target_expr = path_extractor.name;
        let target =
            match resolve_single_read_target_name(Some(&target_expr), &QueryStringSearch::new())
                .await
            {
                Ok(Some(target)) => target,
                Ok(None) => {
                    let res = invalid_request_response(
                        &state,
                        StatusCode::BAD_REQUEST,
                        "Index expression must resolve to exactly one index",
                    );
                    return Ok((state, res));
                }
                Err((status, message)) => {
                    let res = invalid_request_response(&state, status, &message);
                    return Ok((state, res));
                }
            };

        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let request = match serde_json::from_str::<MultiGetRequest>(&body_content) {
            Ok(request) => request,
            Err(_) => {
                let res = invalid_request_response(&state, StatusCode::BAD_REQUEST, "Bad request");
                return Ok((state, res));
            }
        };

        let docs_to_lookup = if let Some(request_docs) = request.docs {
            request_docs
                .into_iter()
                .map(|request_doc| MultiGetDocRequest {
                    index: Some(request_doc.index.unwrap_or_else(|| target.clone())),
                    id: request_doc.id,
                })
                .collect::<Vec<_>>()
        } else if let Some(ids) = request.ids {
            ids.into_iter()
                .map(|id| MultiGetDocRequest {
                    index: Some(target.clone()),
                    id,
                })
                .collect::<Vec<_>>()
        } else {
            let res = invalid_request_response(&state, StatusCode::BAD_REQUEST, "Bad request");
            return Ok((state, res));
        };

        let mut docs = Vec::with_capacity(docs_to_lookup.len());
        for request_doc in docs_to_lookup {
            let index_name = request_doc.index.unwrap_or_else(|| target.clone());
            let resolved_target = match resolve_document_target_name(&index_name).await {
                Ok(Some(target)) => target,
                Ok(None) => {
                    docs.push(json!({
                        "_index": index_name,
                        "_id": request_doc.id,
                        "found": false,
                    }));
                    continue;
                }
                Err((status, message)) => {
                    let res = invalid_request_response(&state, status, &message);
                    return Ok((state, res));
                }
            };
            match lookup_document_value(&resolved_target, &request_doc.id).await {
                Ok(doc) => docs.push(doc),
                Err(message) => {
                    let res =
                        invalid_request_response(&state, StatusCode::SERVICE_UNAVAILABLE, &message);
                    return Ok((state, res));
                }
            }
        }

        let res = create_response(
            &state,
            StatusCode::OK,
            MIME_ES_JSON.clone(),
            serde_json::to_string(&MultiGetResponse { docs }).unwrap(),
        );
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
        let response_data = HashMap::from([(
            "id",
            json!("t8jsAwEeLmtpYmFuYV90YXNrX21hbmFnZXJfOC43LjFfMDAxFkNScFZFdlZZUzNHTTBZdzVmOVY1VHcAFk0yQkNZM0s0UldDQUlvZTBaTkRqNXcAAAAAAAAAAAEWUkxXRUxKbWhUWkt3LXRTWHdhb3loQQABFkNScFZFdlZZUzNHTTBZdzVmOVY1VHcAAA=="),
        )]);
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
                let res =
                    create_response(&state, StatusCode::BAD_REQUEST, mime::TEXT_PLAIN, e.message);
                Ok((state, res))
            }
        }
    }
    .boxed()
}
