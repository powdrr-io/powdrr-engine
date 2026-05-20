use std::{
    collections::{BTreeSet, HashMap},
    pin::Pin,
    sync::{
        atomic::{AtomicI64, Ordering},
        LazyLock, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::future::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{body, Body};
use gotham::mime;
use gotham::prelude::StaticResponseExtender;
use gotham::state::{FromState, State, StateData};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::data_contract::{
    CreateTable, MongoDbTableConfig, TableDescription, TableMetadataCheckpoint,
};
use crate::elastic_search_endpoints::NamePathExtractor;
use crate::lakehouse_serving::{execute_serving_query, ServingQueryError, ServingQueryResponse};
use crate::peers::CheckpointDescriptor;
use crate::schema_massager::{PowdrrDataType, PowdrrSchema};
use crate::serving_plan::ServingQueryClassification;
use crate::serving_protocol::{from_mongodb_find, MongoFindCommand, MongoProtocolError};
use crate::state_provider::{ServiceApiError, STATE_PROVIDER};

const MONGO_BAD_VALUE_CODE: i32 = 2;
const MONGO_CURSOR_NOT_FOUND_CODE: i32 = 43;
const MONGO_NAMESPACE_NOT_FOUND_CODE: i32 = 26;
const MONGO_INTERNAL_ERROR_CODE: i32 = 1;
const MONGO_CURSOR_TIMEOUT_MS: i64 = 10 * 60 * 1000;
static MONGO_CURSOR_IDS: AtomicI64 = AtomicI64::new(1);
static MONGO_CURSORS: LazyLock<Mutex<HashMap<i64, MongoCursorState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static MONGO_TEST_CURSOR_NOW_MS: LazyLock<Mutex<Option<i64>>> = LazyLock::new(|| Mutex::new(None));

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct MongoDatabasePathExtractor {
    pub database: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MongoDbConfigResponse {
    pub acknowledged: bool,
    pub table: String,
    pub mongodb: MongoDbTableConfig,
}

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

#[derive(Clone, Debug)]
struct MongoCursorState {
    ns: String,
    database: String,
    collection: String,
    remaining_rows: Vec<Value>,
    no_cursor_timeout: bool,
    expires_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Default)]
struct MongoCollectionMetadataStats {
    storage_size_bytes: u64,
    document_count: Option<u64>,
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
pub(crate) struct MongoCommandError {
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

    fn namespace_not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: MONGO_NAMESPACE_NOT_FOUND_CODE,
            code_name: "NamespaceNotFound",
            message: message.into(),
        }
    }

    fn cursor_not_found(cursor_id: i64) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: MONGO_CURSOR_NOT_FOUND_CODE,
            code_name: "CursorNotFound",
            message: format!("Mongo cursor {} was not found or expired", cursor_id),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: MONGO_INTERNAL_ERROR_CODE,
            code_name: "InternalError",
            message: message.into(),
        }
    }

    fn from_protocol_error(error: MongoProtocolError) -> Self {
        Self::bad_value(error.to_string())
    }

    fn from_serving_error(error: ServingQueryError) -> Self {
        match error.status {
            StatusCode::NOT_FOUND => Self::namespace_not_found(error.message),
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                Self::bad_value(error.message)
            }
            _ => Self::internal(error.message),
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

pub fn get_mongodb_config(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let result = async {
            let description = load_table_description(&path).await?;
            let config = description.mongodb.clone().ok_or_else(|| {
                MongoCommandError::namespace_not_found(format!(
                    "No Mongo config declared for table {}",
                    path
                ))
            })?;
            Ok::<_, MongoCommandError>(MongoDbConfigResponse {
                acknowledged: true,
                table: path,
                mongodb: config,
            })
        }
        .await;

        match result {
            Ok(response) => {
                let response = json_response(&state, StatusCode::OK, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_error_response(&state, error);
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn put_mongodb_config(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let body = match parse_json_body::<MongoDbTableConfig>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response = json_error_response(&state, MongoCommandError::bad_value(message));
                return Ok((state, response));
            }
        };

        let result = async {
            let existing = STATE_PROVIDER
                .describe_table(&path)
                .await
                .map_err(service_error)?;
            let tags = existing
                .as_ref()
                .map(|description| description.tags.clone())
                .unwrap_or_default();
            let serving = existing
                .as_ref()
                .and_then(|description| description.serving.clone());
            let dynamodb = existing
                .as_ref()
                .and_then(|description| description.dynamodb.clone());

            let schema = load_table_schema(&path).await?;
            validate_mongodb_config(&schema, &body)?;
            validate_mongodb_namespace_uniqueness(&path, &body).await?;

            let request = CreateTable {
                name: path.clone(),
                tags,
                serving,
                dynamodb,
                mongodb: Some(body.clone()),
            };

            STATE_PROVIDER
                .upsert_table_metadata(&request)
                .await
                .map_err(service_error)?;

            Ok::<_, MongoCommandError>(MongoDbConfigResponse {
                acknowledged: true,
                table: path,
                mongodb: body,
            })
        }
        .await;

        match result {
            Ok(response) => {
                let response = json_response(&state, StatusCode::OK, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_error_response(&state, error);
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn mongodb_command(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let database = MongoDatabasePathExtractor::borrow_from(&state)
            .database
            .clone();
        let payload = match parse_json_body::<Value>(&mut state).await {
            Ok(payload) => payload,
            Err(message) => {
                let response = json_error_response(&state, MongoCommandError::bad_value(message));
                return Ok((state, response));
            }
        };

        let result = execute_mongodb_command(&database, payload).await;
        match result {
            Ok(response) => {
                let response = json_response(&state, StatusCode::OK, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_error_response(&state, error);
                Ok((state, response))
            }
        }
    }
    .boxed()
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

        let config = match load_enabled_mongodb_config(&path).await {
            Ok(config) => config,
            Err(error) => {
                let response = json_error_response(&state, error);
                return Ok((state, response));
            }
        };

        if command.find != config.collection {
            let response = json_error_response(
                &state,
                MongoCommandError::bad_value(format!(
                    "Path table {} is exposed as Mongo collection {} but request targeted {}",
                    path, config.collection, command.find
                )),
            );
            return Ok((state, response));
        }

        match execute_mongodb_find_for_table(&path, &command, &config).await {
            Ok(response) => {
                let response = json_response(&state, StatusCode::OK, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_error_response(&state, error);
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub(crate) async fn execute_mongodb_command(
    database: &str,
    payload: Value,
) -> Result<Value, MongoCommandError> {
    let command = payload
        .as_object()
        .ok_or_else(|| MongoCommandError::bad_value("Mongo command body must be a document"))?;
    validate_command_database(command, database)?;

    if command.contains_key("hello")
        || command.contains_key("isMaster")
        || command.contains_key("ismaster")
    {
        return Ok(build_mongodb_hello_response());
    }
    if command.contains_key("ping") {
        return Ok(json!({ "ok": 1.0 }));
    }
    if command.contains_key("endSessions") {
        return Ok(json!({ "ok": 1.0 }));
    }
    if command.contains_key("buildInfo") {
        return Ok(build_mongodb_build_info_response());
    }
    if command.contains_key("listCollections") {
        return list_mongodb_collections(database, command).await;
    }
    if command.contains_key("listDatabases") {
        return list_mongodb_databases().await;
    }
    if command.contains_key("listIndexes") {
        return execute_mongodb_list_indexes(database, command).await;
    }
    if command.contains_key("collStats") {
        return execute_mongodb_coll_stats(database, command).await;
    }
    if command.contains_key("dbStats") {
        return execute_mongodb_db_stats(database, command).await;
    }
    if command.contains_key("getMore") {
        return execute_mongodb_get_more(database, command);
    }
    if command.contains_key("killCursors") {
        return execute_mongodb_kill_cursors(database, command);
    }
    if command.contains_key("find") {
        let find_command: MongoFindCommand = serde_json::from_value(payload)
            .map_err(|error| MongoCommandError::bad_value(error.to_string()))?;
        let binding = load_mongodb_collection_binding(database, &find_command.find).await?;
        let response =
            execute_mongodb_find_for_table(&binding.table_name, &find_command, &binding.config)
                .await?;
        return serde_json::to_value(response).map_err(|error| {
            MongoCommandError::internal(format!("Failed to encode Mongo find response: {}", error))
        });
    }

    Err(MongoCommandError::bad_value(
        "Unsupported Mongo command. Supported commands: hello, ping, endSessions, buildInfo, listCollections, listDatabases, listIndexes, collStats, dbStats, find, getMore, killCursors",
    ))
}

pub(crate) async fn execute_mongodb_command_value(database: &str, payload: Value) -> Value {
    match execute_mongodb_command(database, payload).await {
        Ok(response) => response,
        Err(error) => command_error_value(error),
    }
}

async fn execute_mongodb_find_for_table(
    table_name: &str,
    command: &MongoFindCommand,
    config: &MongoDbTableConfig,
) -> Result<MongoFindResponse, MongoCommandError> {
    let rewritten_command = rewrite_mongodb_find_command(command, config)?;
    let request =
        from_mongodb_find(&rewritten_command).map_err(MongoCommandError::from_protocol_error)?;
    let response = execute_serving_query(table_name, request)
        .await
        .map_err(MongoCommandError::from_serving_error)?;

    if response.classification != ServingQueryClassification::FastPath {
        return Err(MongoCommandError::from_query_response(&response));
    }

    let rows = response
        .rows
        .into_iter()
        .map(|row| shape_mongodb_row(row, command, config))
        .collect::<Result<Vec<_>, _>>()?;

    build_mongodb_find_response(
        format!("{}.{}", config.database, config.collection),
        command,
        config,
        rows,
    )
}

fn build_mongodb_hello_response() -> Value {
    json!({
        "isWritablePrimary": true,
        "ismaster": true,
        "secondary": false,
        "helloOk": true,
        "maxBsonObjectSize": 16 * 1024 * 1024,
        "maxMessageSizeBytes": 48 * 1000 * 1000,
        "maxWriteBatchSize": 100000,
        "minWireVersion": 0,
        "maxWireVersion": 21,
        "logicalSessionTimeoutMinutes": 30,
        "connectionId": 1,
        "readOnly": true,
        "ok": 1.0
    })
}

fn build_mongodb_build_info_response() -> Value {
    json!({
        "version": "0.0.1-powdrr",
        "gitVersion": "powdrr-mongo-http-bridge",
        "modules": [],
        "allocator": "system",
        "javascriptEngine": "none",
        "maxBsonObjectSize": 16 * 1024 * 1024,
        "bits": 64,
        "debug": true,
        "ok": 1.0
    })
}

async fn list_mongodb_collections(
    database: &str,
    command: &Map<String, Value>,
) -> Result<Value, MongoCommandError> {
    let bindings = list_mongodb_bindings().await?;
    let name_only = optional_bool(command, "nameOnly")?.unwrap_or(false);
    let filter_name = list_collections_filter_name(command)?;
    let mut collections = bindings
        .into_iter()
        .filter(|binding| {
            binding.config.enabled
                && binding.config.database == database
                && filter_name
                    .as_ref()
                    .map(|name| binding.config.collection == *name)
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    collections.sort_by(|left, right| left.config.collection.cmp(&right.config.collection));

    Ok(json!({
        "cursor": {
            "id": 0,
            "ns": format!("{}.$cmd.listCollections", database),
            "firstBatch": collections
                .into_iter()
                .map(|binding| {
                    if name_only {
                        json!({
                            "name": binding.config.collection,
                            "type": "collection"
                        })
                    } else {
                        json!({
                            "name": binding.config.collection,
                            "type": "collection",
                            "options": {},
                            "info": {
                                "readOnly": true
                            },
                            "idIndex": {
                                "name": "_id_",
                                "key": { "_id": 1 }
                            }
                        })
                    }
                })
                .collect::<Vec<_>>()
        },
        "ok": 1.0
    }))
}

async fn list_mongodb_databases() -> Result<Value, MongoCommandError> {
    let bindings = list_mongodb_bindings().await?;
    let mut databases = BTreeSet::new();
    for binding in bindings
        .into_iter()
        .filter(|binding| binding.config.enabled)
    {
        databases.insert(binding.config.database);
    }

    let total_size = databases.len() as i64;
    let databases = databases
        .into_iter()
        .map(|name| {
            json!({
                "name": name,
                "sizeOnDisk": 1,
                "empty": false
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "databases": databases,
        "totalSize": total_size,
        "ok": 1.0
    }))
}

async fn execute_mongodb_list_indexes(
    database: &str,
    command: &Map<String, Value>,
) -> Result<Value, MongoCommandError> {
    let collection = required_string(command, "listIndexes")?;
    let binding = load_mongodb_collection_binding(database, collection).await?;
    let namespace = format!("{}.{}", binding.config.database, binding.config.collection);

    Ok(json!({
        "cursor": {
            "id": 0,
            "ns": format!("{}.$cmd.listIndexes.{}", database, collection),
            "firstBatch": [
                {
                    "v": 2,
                    "key": { "_id": 1 },
                    "name": "_id_",
                    "ns": namespace
                }
            ]
        },
        "ok": 1.0
    }))
}

async fn execute_mongodb_coll_stats(
    database: &str,
    command: &Map<String, Value>,
) -> Result<Value, MongoCommandError> {
    let collection = required_string(command, "collStats")?;
    let scale_factor = command_scale_factor(command)?;
    let binding = load_mongodb_collection_binding(database, collection).await?;
    let metadata_stats = load_mongodb_collection_metadata_stats(&binding.table_name).await?;
    let namespace = format!("{}.{}", binding.config.database, binding.config.collection);
    let scaled_storage_size = scale_size(metadata_stats.storage_size_bytes, scale_factor);
    let avg_obj_size = metadata_stats.document_count.map(|document_count| {
        if document_count == 0 {
            0.0
        } else {
            metadata_stats.storage_size_bytes as f64 / document_count as f64
        }
    });

    Ok(json!({
        "ns": namespace,
        "count": metadata_stats.document_count,
        "size": scaled_storage_size,
        "storageSize": scaled_storage_size,
        "avgObjSize": avg_obj_size,
        "nindexes": 1,
        "totalIndexSize": 0,
        "indexSizes": { "_id_": 0 },
        "scaleFactor": scale_factor,
        "ok": 1.0
    }))
}

async fn execute_mongodb_db_stats(
    database: &str,
    command: &Map<String, Value>,
) -> Result<Value, MongoCommandError> {
    let scale_factor = command_scale_factor(command)?;
    let bindings = list_mongodb_bindings().await?;
    let enabled_bindings = bindings
        .into_iter()
        .filter(|binding| binding.config.enabled && binding.config.database == database)
        .collect::<Vec<_>>();

    let mut total_storage_size_bytes = 0u64;
    let mut total_document_count = Some(0u64);
    for binding in enabled_bindings.iter() {
        let metadata_stats = load_mongodb_collection_metadata_stats(&binding.table_name).await?;
        total_storage_size_bytes =
            total_storage_size_bytes.saturating_add(metadata_stats.storage_size_bytes);
        total_document_count = match (total_document_count, metadata_stats.document_count) {
            (Some(current_total), Some(document_count)) => {
                Some(current_total.saturating_add(document_count))
            }
            _ => None,
        };
    }

    let scaled_storage_size = scale_size(total_storage_size_bytes, scale_factor);
    let avg_obj_size = total_document_count.map(|document_count| {
        if document_count == 0 {
            0.0
        } else {
            total_storage_size_bytes as f64 / document_count as f64
        }
    });

    Ok(json!({
        "db": database,
        "collections": enabled_bindings.len(),
        "views": 0,
        "objects": total_document_count,
        "avgObjSize": avg_obj_size,
        "dataSize": scaled_storage_size,
        "storageSize": scaled_storage_size,
        "indexes": enabled_bindings.len(),
        "indexSize": 0,
        "scaleFactor": scale_factor,
        "ok": 1.0
    }))
}

fn build_mongodb_find_response(
    namespace: String,
    command: &MongoFindCommand,
    config: &MongoDbTableConfig,
    rows: Vec<Value>,
) -> Result<MongoFindResponse, MongoCommandError> {
    let batch_size = command
        .batch_size
        .map(validate_positive_batch_size)
        .transpose()?;
    let single_batch = command.single_batch.unwrap_or(false);
    let no_cursor_timeout = command.no_cursor_timeout.unwrap_or(false);

    match batch_size {
        Some(batch_size) if batch_size < rows.len() => {
            let mut rows = rows;
            let remaining_rows = rows.split_off(batch_size);
            let cursor_id = if single_batch {
                0
            } else {
                register_mongo_cursor(MongoCursorState {
                    ns: namespace.clone(),
                    database: config.database.clone(),
                    collection: config.collection.clone(),
                    remaining_rows,
                    no_cursor_timeout,
                    expires_at_ms: next_cursor_expiry_ms(
                        no_cursor_timeout,
                        current_cursor_time_ms(),
                    ),
                })?
            };
            Ok(MongoFindResponse {
                cursor: MongoCursorResponse {
                    id: cursor_id,
                    ns: namespace,
                    first_batch: rows,
                },
                ok: 1.0,
            })
        }
        Some(batch_size) => Ok(MongoFindResponse {
            cursor: MongoCursorResponse {
                id: 0,
                ns: namespace,
                first_batch: rows.into_iter().take(batch_size).collect(),
            },
            ok: 1.0,
        }),
        None => Ok(MongoFindResponse {
            cursor: MongoCursorResponse {
                id: 0,
                ns: namespace,
                first_batch: rows,
            },
            ok: 1.0,
        }),
    }
}

fn register_mongo_cursor(cursor: MongoCursorState) -> Result<i64, MongoCommandError> {
    let cursor_id = MONGO_CURSOR_IDS.fetch_add(1, Ordering::Relaxed);
    let mut cursors = MONGO_CURSORS
        .lock()
        .map_err(|_| MongoCommandError::internal("Mongo cursor registry lock is poisoned"))?;
    prune_expired_mongo_cursors(&mut cursors, current_cursor_time_ms());
    cursors.insert(cursor_id, cursor);
    Ok(cursor_id)
}

fn execute_mongodb_get_more(
    database: &str,
    command: &Map<String, Value>,
) -> Result<Value, MongoCommandError> {
    let cursor_id = required_i64(command, "getMore")?;
    let collection = required_string(command, "collection")?;
    let batch_size = command
        .get("batchSize")
        .map(value_as_i64)
        .transpose()?
        .map(validate_positive_batch_size)
        .transpose()?;
    let now_ms = current_cursor_time_ms();
    let mut cursors = MONGO_CURSORS
        .lock()
        .map_err(|_| MongoCommandError::internal("Mongo cursor registry lock is poisoned"))?;
    prune_expired_mongo_cursors(&mut cursors, now_ms);
    let mut cursor = cursors
        .remove(&cursor_id)
        .ok_or_else(|| MongoCommandError::cursor_not_found(cursor_id))?;

    if cursor.database != database || cursor.collection != collection {
        let actual_database = cursor.database.clone();
        let actual_collection = cursor.collection.clone();
        cursors.insert(cursor_id, cursor);
        return Err(MongoCommandError::bad_value(format!(
            "Mongo cursor {} belongs to {}.{}, not {}.{}",
            cursor_id, actual_database, actual_collection, database, collection
        )));
    }

    let batch_size = batch_size.unwrap_or(cursor.remaining_rows.len());
    let next_batch_len = batch_size.min(cursor.remaining_rows.len());
    let next_batch = cursor
        .remaining_rows
        .drain(..next_batch_len)
        .collect::<Vec<_>>();
    let ns = cursor.ns.clone();
    let response_cursor_id = if cursor.remaining_rows.is_empty() {
        0
    } else {
        cursor.expires_at_ms = next_cursor_expiry_ms(cursor.no_cursor_timeout, now_ms);
        cursors.insert(cursor_id, cursor);
        cursor_id
    };

    Ok(json!({
        "cursor": {
            "id": response_cursor_id,
            "ns": ns,
            "nextBatch": next_batch
        },
        "ok": 1.0
    }))
}

fn execute_mongodb_kill_cursors(
    database: &str,
    command: &Map<String, Value>,
) -> Result<Value, MongoCommandError> {
    let collection = required_string(command, "killCursors")?;
    let cursor_ids = command
        .get("cursors")
        .ok_or_else(|| {
            MongoCommandError::bad_value("Mongo killCursors command requires a cursors array")
        })?
        .as_array()
        .ok_or_else(|| {
            MongoCommandError::bad_value("Mongo killCursors command field cursors must be an array")
        })?
        .iter()
        .map(value_as_i64)
        .collect::<Result<Vec<_>, _>>()?;

    let mut killed = Vec::new();
    let mut not_found = Vec::new();
    let mut cursors = MONGO_CURSORS
        .lock()
        .map_err(|_| MongoCommandError::internal("Mongo cursor registry lock is poisoned"))?;
    prune_expired_mongo_cursors(&mut cursors, current_cursor_time_ms());
    for cursor_id in cursor_ids {
        match cursors.remove(&cursor_id) {
            Some(cursor) if cursor.database == database && cursor.collection == collection => {
                killed.push(cursor_id);
            }
            Some(cursor) => {
                cursors.insert(cursor_id, cursor);
                not_found.push(cursor_id);
            }
            None => not_found.push(cursor_id),
        }
    }

    Ok(json!({
        "cursorsKilled": killed,
        "cursorsNotFound": not_found,
        "cursorsAlive": [],
        "cursorsUnknown": [],
        "ok": 1.0
    }))
}

fn prune_expired_mongo_cursors(cursors: &mut HashMap<i64, MongoCursorState>, now_ms: i64) {
    cursors.retain(|_, cursor| !mongo_cursor_expired(cursor, now_ms));
}

fn mongo_cursor_expired(cursor: &MongoCursorState, now_ms: i64) -> bool {
    cursor
        .expires_at_ms
        .map(|expires_at_ms| now_ms >= expires_at_ms)
        .unwrap_or(false)
}

fn next_cursor_expiry_ms(no_cursor_timeout: bool, now_ms: i64) -> Option<i64> {
    if no_cursor_timeout {
        None
    } else {
        Some(now_ms + MONGO_CURSOR_TIMEOUT_MS)
    }
}

fn current_cursor_time_ms() -> i64 {
    #[cfg(test)]
    {
        if let Some(now_ms) = MONGO_TEST_CURSOR_NOW_MS
            .lock()
            .expect("Mongo cursor test clock lock poisoned")
            .as_ref()
            .copied()
        {
            return now_ms;
        }
    }

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as i64
}

fn validate_positive_batch_size(batch_size: i64) -> Result<usize, MongoCommandError> {
    if batch_size <= 0 {
        return Err(MongoCommandError::bad_value(
            "Mongo batchSize must be a positive integer",
        ));
    }
    usize::try_from(batch_size)
        .map_err(|_| MongoCommandError::bad_value("Mongo batchSize is too large"))
}

fn list_collections_filter_name(
    command: &Map<String, Value>,
) -> Result<Option<String>, MongoCommandError> {
    let Some(filter) = command.get("filter") else {
        return Ok(None);
    };
    let filter = filter.as_object().ok_or_else(|| {
        MongoCommandError::bad_value("Mongo listCollections filter must be a document")
    })?;
    match filter.get("name") {
        None => Ok(None),
        Some(name) => name
            .as_str()
            .map(|name| Some(name.to_string()))
            .ok_or_else(|| {
                MongoCommandError::bad_value(
                    "Mongo listCollections filter field name must be a string",
                )
            }),
    }
}

fn optional_bool(
    command: &Map<String, Value>,
    field: &str,
) -> Result<Option<bool>, MongoCommandError> {
    match command.get(field) {
        None => Ok(None),
        Some(value) => value.as_bool().map(Some).ok_or_else(|| {
            MongoCommandError::bad_value(format!("Mongo command field {} must be a boolean", field))
        }),
    }
}

fn command_scale_factor(command: &Map<String, Value>) -> Result<u64, MongoCommandError> {
    match command.get("scale") {
        None => Ok(1),
        Some(value) => {
            let scale_factor = value_as_i64(value)?;
            if scale_factor <= 0 {
                return Err(MongoCommandError::bad_value(
                    "Mongo command field scale must be a positive integer",
                ));
            }
            u64::try_from(scale_factor)
                .map_err(|_| MongoCommandError::bad_value("Mongo command field scale is too large"))
        }
    }
}

fn scale_size(size: u64, scale_factor: u64) -> u64 {
    if scale_factor <= 1 {
        size
    } else {
        size / scale_factor
    }
}

fn required_string<'a>(
    command: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, MongoCommandError> {
    command
        .get(field)
        .ok_or_else(|| MongoCommandError::bad_value(format!("Mongo command requires {}", field)))?
        .as_str()
        .ok_or_else(|| {
            MongoCommandError::bad_value(format!("Mongo command field {} must be a string", field))
        })
}

fn required_i64(command: &Map<String, Value>, field: &str) -> Result<i64, MongoCommandError> {
    let value = command
        .get(field)
        .ok_or_else(|| MongoCommandError::bad_value(format!("Mongo command requires {}", field)))?;
    value_as_i64(value)
}

fn value_as_i64(value: &Value) -> Result<i64, MongoCommandError> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .ok_or_else(|| MongoCommandError::bad_value("Mongo command integer field was invalid"))
}

fn rewrite_mongodb_find_command(
    command: &MongoFindCommand,
    config: &MongoDbTableConfig,
) -> Result<MongoFindCommand, MongoCommandError> {
    Ok(MongoFindCommand {
        find: command.find.clone(),
        filter: rewrite_mongodb_filter_document(&command.filter, &config.id.field)?,
        projection: rewrite_mongodb_projection(command.projection.as_ref(), &config.id.field)?,
        sort: rewrite_mongodb_sort(command.sort.as_ref(), &config.id.field)?,
        limit: command.limit,
        skip: command.skip,
        batch_size: command.batch_size,
        single_batch: command.single_batch,
        no_cursor_timeout: command.no_cursor_timeout,
    })
}

fn rewrite_mongodb_filter_document(
    filter: &Value,
    id_field: &str,
) -> Result<Value, MongoCommandError> {
    let filter_map = filter
        .as_object()
        .ok_or_else(|| MongoCommandError::bad_value("Mongo filter must be a document"))?;
    let mut rewritten = Map::new();
    for (field, value) in filter_map.iter() {
        match field.as_str() {
            "$and" => {
                let clauses = value.as_array().ok_or_else(|| {
                    MongoCommandError::bad_value("$and must be an array of documents")
                })?;
                rewritten.insert(
                    "$and".to_string(),
                    Value::Array(
                        clauses
                            .iter()
                            .map(|clause| rewrite_mongodb_filter_document(clause, id_field))
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                );
            }
            operator if operator.starts_with('$') => {
                rewritten.insert(operator.to_string(), value.clone());
            }
            _ => {
                rewritten.insert(rewrite_mongodb_field_name(field, id_field), value.clone());
            }
        }
    }
    Ok(Value::Object(rewritten))
}

fn rewrite_mongodb_projection(
    projection: Option<&Value>,
    id_field: &str,
) -> Result<Option<Value>, MongoCommandError> {
    let Some(projection) = projection else {
        return Ok(None);
    };
    let projection_map = projection
        .as_object()
        .ok_or_else(|| MongoCommandError::bad_value("Mongo projection must be a document"))?;

    let include_mongo_id = should_include_mongo_id(Some(projection))?;
    let mut rewritten = Map::new();
    for (field, value) in projection_map.iter() {
        if field == "_id" && mongodb_projection_mode(value) == Some(false) {
            continue;
        }
        rewritten.insert(rewrite_mongodb_field_name(field, id_field), value.clone());
    }
    if include_mongo_id {
        rewritten
            .entry(id_field.to_string())
            .or_insert_with(|| json!(1));
    }
    Ok(Some(Value::Object(rewritten)))
}

fn rewrite_mongodb_sort(
    sort: Option<&Value>,
    id_field: &str,
) -> Result<Option<Value>, MongoCommandError> {
    let Some(sort) = sort else {
        return Ok(None);
    };
    let sort_map = sort
        .as_object()
        .ok_or_else(|| MongoCommandError::bad_value("Mongo sort must be a document"))?;
    let mut rewritten = Map::new();
    for (field, value) in sort_map.iter() {
        rewritten.insert(rewrite_mongodb_field_name(field, id_field), value.clone());
    }
    Ok(Some(Value::Object(rewritten)))
}

fn shape_mongodb_row(
    row: Value,
    command: &MongoFindCommand,
    config: &MongoDbTableConfig,
) -> Result<Value, MongoCommandError> {
    let mut document = row.as_object().cloned().ok_or_else(|| {
        MongoCommandError::internal("Serving query returned a non-document row for Mongo")
    })?;
    let id_field = config.id.field.as_str();
    let include_mongo_id = should_include_mongo_id(command.projection.as_ref())?;
    let keep_source_id_field =
        projection_explicitly_includes_field(command.projection.as_ref(), id_field)?;

    if include_mongo_id {
        let id_value = document.get(id_field).cloned().ok_or_else(|| {
            MongoCommandError::internal(format!(
                "Mongo _id field backing column {} was not present in the serving result",
                id_field
            ))
        })?;
        document.insert("_id".to_string(), id_value);
    }

    if id_field != "_id" && !keep_source_id_field {
        document.remove(id_field);
    }

    Ok(Value::Object(document))
}

fn rewrite_mongodb_field_name(field: &str, id_field: &str) -> String {
    if field == "_id" {
        id_field.to_string()
    } else {
        field.to_string()
    }
}

fn should_include_mongo_id(projection: Option<&Value>) -> Result<bool, MongoCommandError> {
    let Some(projection) = projection else {
        return Ok(true);
    };
    let projection_map = projection
        .as_object()
        .ok_or_else(|| MongoCommandError::bad_value("Mongo projection must be a document"))?;
    match projection_map.get("_id") {
        Some(value) => mongodb_projection_mode(value).ok_or_else(|| {
            MongoCommandError::bad_value("Mongo projection field _id must be 0/1 or false/true")
        }),
        None => Ok(true),
    }
}

fn projection_explicitly_includes_field(
    projection: Option<&Value>,
    field_name: &str,
) -> Result<bool, MongoCommandError> {
    let Some(projection) = projection else {
        return Ok(false);
    };
    let projection_map = projection
        .as_object()
        .ok_or_else(|| MongoCommandError::bad_value("Mongo projection must be a document"))?;
    Ok(projection_map
        .get(field_name)
        .and_then(mongodb_projection_mode)
        .unwrap_or(false))
}

fn mongodb_projection_mode(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(boolean) => Some(*boolean),
        Value::Number(number) => match (number.as_i64(), number.as_u64()) {
            (Some(0), _) => Some(false),
            (Some(1), _) => Some(true),
            (_, Some(0)) => Some(false),
            (_, Some(1)) => Some(true),
            _ => None,
        },
        _ => None,
    }
}

async fn load_enabled_mongodb_config(
    table_name: &str,
) -> Result<MongoDbTableConfig, MongoCommandError> {
    let description = load_table_description(table_name).await?;
    match description.mongodb {
        Some(config) if config.enabled => Ok(config),
        Some(_) => Err(MongoCommandError::namespace_not_found(format!(
            "Mongo collection for table {} is disabled",
            table_name
        ))),
        None => Err(MongoCommandError::namespace_not_found(format!(
            "Mongo collection is not configured for table {}",
            table_name
        ))),
    }
}

#[derive(Clone, Debug)]
struct MongoTableBinding {
    table_name: String,
    config: MongoDbTableConfig,
}

async fn list_mongodb_bindings() -> Result<Vec<MongoTableBinding>, MongoCommandError> {
    let mut table_names = STATE_PROVIDER
        .get_all_iceberg_tables()
        .await
        .map_err(service_error)?;
    table_names.sort();

    let mut bindings = Vec::new();
    for table_name in table_names {
        let Some(description) = STATE_PROVIDER
            .describe_table(&table_name)
            .await
            .map_err(service_error)?
        else {
            continue;
        };
        let Some(config) = description.mongodb else {
            continue;
        };
        bindings.push(MongoTableBinding { table_name, config });
    }
    Ok(bindings)
}

async fn load_mongodb_collection_binding(
    database: &str,
    collection: &str,
) -> Result<MongoTableBinding, MongoCommandError> {
    let matches = list_mongodb_bindings()
        .await?
        .into_iter()
        .filter(|binding| {
            binding.config.enabled
                && binding.config.database == database
                && binding.config.collection == collection
        })
        .collect::<Vec<_>>();

    match matches.len() {
        0 => Err(MongoCommandError::namespace_not_found(format!(
            "Mongo collection {}.{} is not configured",
            database, collection
        ))),
        1 => Ok(matches.into_iter().next().unwrap()),
        _ => Err(MongoCommandError::internal(format!(
            "Multiple Powdrr tables are exposed as Mongo collection {}.{}",
            database, collection
        ))),
    }
}

async fn validate_mongodb_namespace_uniqueness(
    table_name: &str,
    config: &MongoDbTableConfig,
) -> Result<(), MongoCommandError> {
    if !config.enabled {
        return Ok(());
    }

    let duplicate = list_mongodb_bindings().await?.into_iter().find(|binding| {
        binding.table_name != table_name
            && binding.config.enabled
            && binding.config.database == config.database
            && binding.config.collection == config.collection
    });

    if let Some(binding) = duplicate {
        return Err(MongoCommandError::bad_value(format!(
            "Mongo collection {}.{} is already exposed by table {}",
            config.database, config.collection, binding.table_name
        )));
    }

    Ok(())
}

async fn load_mongodb_collection_metadata_stats(
    table_name: &str,
) -> Result<MongoCollectionMetadataStats, MongoCommandError> {
    let Some(checkpoint) = load_latest_table_checkpoint(table_name).await? else {
        return Ok(MongoCollectionMetadataStats::default());
    };
    let Some(iceberg_metadata) = checkpoint.iceberg_metadata.as_ref() else {
        return Ok(MongoCollectionMetadataStats::default());
    };

    let storage_size_bytes = iceberg_metadata.files.sizes.iter().copied().sum::<u64>();
    let document_count = if iceberg_metadata.file_stats.is_empty() {
        None
    } else {
        iceberg_metadata
            .file_stats
            .iter()
            .try_fold(0u64, |running_total, file_stats| {
                file_stats
                    .record_count
                    .map(|record_count| running_total.saturating_add(record_count))
            })
    };

    Ok(MongoCollectionMetadataStats {
        storage_size_bytes,
        document_count,
    })
}

async fn load_table_description(table_name: &str) -> Result<TableDescription, MongoCommandError> {
    STATE_PROVIDER
        .describe_table(&table_name.to_string())
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            MongoCommandError::namespace_not_found(format!("Table {} was not found", table_name))
        })
}

async fn load_latest_table_checkpoint(
    table_name: &str,
) -> Result<Option<TableMetadataCheckpoint>, MongoCommandError> {
    let checkpoint_id = STATE_PROVIDER
        .get_published_active_servable_checkpoint(&table_name.to_string())
        .await
        .map_err(service_error)?;
    let Some(checkpoint_id) = checkpoint_id else {
        return Ok(None);
    };

    STATE_PROVIDER
        .get_checkpoint(CheckpointDescriptor::new(
            table_name.to_string(),
            checkpoint_id,
        ))
        .await
        .map_err(service_error)
}

fn validate_command_database(
    command: &Map<String, Value>,
    database: &str,
) -> Result<(), MongoCommandError> {
    let Some(request_database) = command.get("$db") else {
        return Ok(());
    };
    match request_database.as_str() {
        Some(request_database) if request_database == database => Ok(()),
        Some(request_database) => Err(MongoCommandError::bad_value(format!(
            "Mongo command path database {} did not match $db {}",
            database, request_database
        ))),
        None => Err(MongoCommandError::bad_value(
            "Mongo command field $db must be a string",
        )),
    }
}

#[cfg(test)]
pub(crate) fn reset_mongodb_cursor_state_for_tests() {
    MONGO_CURSOR_IDS.store(1, Ordering::Relaxed);
    MONGO_CURSORS
        .lock()
        .expect("Mongo cursor registry lock poisoned")
        .clear();
    *MONGO_TEST_CURSOR_NOW_MS
        .lock()
        .expect("Mongo cursor test clock lock poisoned") = None;
}

#[cfg(test)]
pub(crate) fn set_mongodb_cursor_time_for_tests(now_ms: Option<i64>) {
    *MONGO_TEST_CURSOR_NOW_MS
        .lock()
        .expect("Mongo cursor test clock lock poisoned") = now_ms;
}

#[cfg(test)]
pub(crate) fn mongodb_cursor_timeout_ms_for_tests() -> i64 {
    MONGO_CURSOR_TIMEOUT_MS
}

async fn load_table_schema(table_name: &str) -> Result<PowdrrSchema, MongoCommandError> {
    let checkpoint_id = STATE_PROVIDER
        .get_published_active_servable_checkpoint(&table_name.to_string())
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            MongoCommandError::namespace_not_found(format!(
                "No checkpoint was available for table {}",
                table_name
            ))
        })?;
    let checkpoint = STATE_PROVIDER
        .get_checkpoint(CheckpointDescriptor::new(
            table_name.to_string(),
            checkpoint_id,
        ))
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            MongoCommandError::namespace_not_found(format!(
                "Checkpoint metadata was not found for table {}",
                table_name
            ))
        })?;
    schema_from_checkpoint(&checkpoint)
}

fn schema_from_checkpoint(
    checkpoint: &TableMetadataCheckpoint,
) -> Result<PowdrrSchema, MongoCommandError> {
    if let Some(metadata) = checkpoint.iceberg_metadata.as_ref() {
        return Ok(metadata.table_schema.clone());
    }
    if let Some(metadata) = checkpoint.speedboat_metadata.as_ref() {
        let mut schemas = metadata.files.schemas.iter();
        if let Some(first_schema) = schemas.next() {
            let mut merged = first_schema.clone();
            for schema in schemas {
                merged.merge_from(schema);
            }
            return Ok(merged);
        }
    }
    if !checkpoint.schema.fields().is_empty() {
        return Ok(checkpoint.schema.clone());
    }
    Err(MongoCommandError::internal(
        "Checkpoint did not contain a usable schema",
    ))
}

fn validate_mongodb_config(
    schema: &PowdrrSchema,
    config: &MongoDbTableConfig,
) -> Result<(), MongoCommandError> {
    if config.database.trim().is_empty() {
        return Err(MongoCommandError::bad_value(
            "Mongo database must be a non-empty string",
        ));
    }
    if config.collection.trim().is_empty() {
        return Err(MongoCommandError::bad_value(
            "Mongo collection must be a non-empty string",
        ));
    }
    if config.id.field.trim().is_empty() {
        return Err(MongoCommandError::bad_value(
            "Mongo _id mapping field must be a non-empty string",
        ));
    }
    mongodb_id_type(schema, &config.id.field)?;
    Ok(())
}

fn mongodb_id_type(schema: &PowdrrSchema, field_name: &str) -> Result<(), MongoCommandError> {
    let schema_map = schema.to_map();
    let field = schema_map.get(field_name).ok_or_else(|| {
        MongoCommandError::bad_value(format!("Unknown Mongo _id field {}", field_name))
    })?;
    match field.data_type {
        PowdrrDataType::String
        | PowdrrDataType::Integer
        | PowdrrDataType::Float
        | PowdrrDataType::Boolean => Ok(()),
        _ => Err(MongoCommandError::bad_value(format!(
            "Field {} is not a valid Mongo _id backing field type",
            field_name
        ))),
    }
}

fn service_error(error: ServiceApiError) -> MongoCommandError {
    MongoCommandError::internal(error.to_string())
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

fn command_error_value(error: MongoCommandError) -> Value {
    serde_json::to_value(command_error_response(&error)).unwrap()
}

fn command_error_response(error: &MongoCommandError) -> MongoCommandErrorResponse {
    MongoCommandErrorResponse {
        ok: 0.0,
        errmsg: error.message.clone(),
        code: error.code,
        code_name: error.code_name,
    }
}

fn json_error_response(state: &State, error: MongoCommandError) -> gotham::hyper::Response<Body> {
    json_response(state, error.status, &command_error_response(&error))
}
