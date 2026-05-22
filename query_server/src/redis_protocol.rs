use std::pin::Pin;

use futures_util::future::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{Body, body};
use gotham::mime;
use gotham::state::{FromState, State};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::elastic_search_http_types::NamePathExtractor;
use crate::exact_lookup::{
    ActiveCheckpointLookupError, execute_active_checkpoint_exact_lookup_batch_rows,
    load_active_checkpoint as load_shared_active_checkpoint,
};
use powdrr_query_lib::data_contract::{
    CreateTable, RedisTableConfig, TableDescription, TableMetadataCheckpoint,
};
use powdrr_query_lib::schema_massager::PowdrrSchema;
use powdrr_query_lib::serving_plan::{
    ServingPredicate, ServingQueryClassification, ServingRequestPlan,
};
use powdrr_query_runtime::lakehouse_serving::{ServingQueryError, execute_serving_query};
use powdrr_query_runtime::state_provider::{STATE_PROVIDER, ServiceApiError};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RedisConfigResponse {
    pub acknowledged: bool,
    pub table: String,
    pub redis: RedisTableConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RespValue {
    SimpleString(String),
    BulkString(Vec<u8>),
    NullBulkString,
    Integer(i64),
    Array(Vec<RespValue>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RedisCommandResult {
    pub response: RespValue,
    pub close_connection: bool,
}

#[derive(Debug)]
pub(crate) struct RedisCommandError {
    status: StatusCode,
    prefix: &'static str,
    message: String,
}

#[derive(Clone, Debug)]
struct RedisTableBinding {
    table_name: String,
    config: RedisTableConfig,
}

impl RedisCommandError {
    fn validation(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            prefix: "ERR",
            message: message.into(),
        }
    }

    fn namespace_not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            prefix: "ERR",
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            prefix: "ERR",
            message: message.into(),
        }
    }

    fn unsupported(command: &str) -> Self {
        Self::validation(format!("unsupported Redis command {}", command))
    }

    pub(crate) fn resp_message(&self) -> String {
        format!("{} {}", self.prefix, self.message)
    }
}

pub fn get_redis_config(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let result = async {
            let description = load_table_description(&path).await?;
            let config = description.redis.clone().ok_or_else(|| {
                RedisCommandError::namespace_not_found(format!(
                    "No Redis config declared for table {}",
                    path
                ))
            })?;
            Ok::<_, RedisCommandError>(RedisConfigResponse {
                acknowledged: true,
                table: path,
                redis: config,
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

pub fn put_redis_config(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let body = match parse_json_body::<RedisTableConfig>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response = json_error_response(&state, RedisCommandError::validation(message));
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
            let mongodb = existing
                .as_ref()
                .and_then(|description| description.mongodb.clone());

            let schema = load_table_schema(&path).await?;
            validate_redis_config(&schema, &body)?;
            validate_redis_database_uniqueness(&path, &body).await?;

            let request = CreateTable {
                name: path.clone(),
                tags,
                serving,
                dynamodb,
                mongodb,
                redis: Some(body.clone()),
            };

            STATE_PROVIDER
                .upsert_table_metadata(&request)
                .await
                .map_err(service_error)?;

            Ok::<_, RedisCommandError>(RedisConfigResponse {
                acknowledged: true,
                table: path,
                redis: body,
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

pub(crate) async fn execute_redis_command(
    selected_db: &mut u32,
    args: Vec<String>,
) -> Result<RedisCommandResult, RedisCommandError> {
    let command = args
        .first()
        .ok_or_else(|| RedisCommandError::validation("empty Redis command"))?
        .to_ascii_uppercase();
    let rest = &args[1..];

    match command.as_str() {
        "PING" => execute_ping(rest),
        "ECHO" => execute_echo(rest),
        "QUIT" => execute_quit(rest),
        "COMMAND" => execute_command_command(rest),
        "CLIENT" => execute_client_command(rest),
        "HELLO" => execute_hello(rest, *selected_db),
        "READONLY" => Ok(ok_result()),
        "SELECT" => execute_select(selected_db, rest).await,
        "GET" => execute_get(*selected_db, rest).await,
        "MGET" => execute_mget(*selected_db, rest).await,
        "EXISTS" => execute_exists(*selected_db, rest).await,
        _ => Err(RedisCommandError::unsupported(&command)),
    }
}

fn execute_ping(args: &[String]) -> Result<RedisCommandResult, RedisCommandError> {
    match args {
        [] => Ok(simple_result("PONG")),
        [message] => Ok(RedisCommandResult {
            response: RespValue::BulkString(message.clone().into_bytes()),
            close_connection: false,
        }),
        _ => Err(RedisCommandError::validation(
            "wrong number of arguments for 'ping' command",
        )),
    }
}

fn execute_echo(args: &[String]) -> Result<RedisCommandResult, RedisCommandError> {
    match args {
        [message] => Ok(RedisCommandResult {
            response: RespValue::BulkString(message.clone().into_bytes()),
            close_connection: false,
        }),
        _ => Err(RedisCommandError::validation(
            "wrong number of arguments for 'echo' command",
        )),
    }
}

fn execute_quit(args: &[String]) -> Result<RedisCommandResult, RedisCommandError> {
    if !args.is_empty() {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'quit' command",
        ));
    }
    Ok(RedisCommandResult {
        response: RespValue::SimpleString("OK".to_string()),
        close_connection: true,
    })
}

fn execute_command_command(args: &[String]) -> Result<RedisCommandResult, RedisCommandError> {
    if args.is_empty() {
        return Ok(RedisCommandResult {
            response: RespValue::Array(vec![]),
            close_connection: false,
        });
    }

    match args[0].to_ascii_uppercase().as_str() {
        "COUNT" => Ok(RedisCommandResult {
            response: RespValue::Integer(0),
            close_connection: false,
        }),
        "DOCS" | "INFO" | "GETKEYS" | "GETKEYSANDFLAGS" | "LIST" => Ok(RedisCommandResult {
            response: RespValue::Array(vec![]),
            close_connection: false,
        }),
        _ => Ok(RedisCommandResult {
            response: RespValue::Array(vec![]),
            close_connection: false,
        }),
    }
}

fn execute_client_command(args: &[String]) -> Result<RedisCommandResult, RedisCommandError> {
    let Some(subcommand) = args.first() else {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'client' command",
        ));
    };

    match subcommand.to_ascii_uppercase().as_str() {
        "SETINFO" => {
            if args.len() != 3 {
                return Err(RedisCommandError::validation(
                    "wrong number of arguments for 'client setinfo' command",
                ));
            }
            Ok(ok_result())
        }
        "SETNAME" => {
            if args.len() != 2 {
                return Err(RedisCommandError::validation(
                    "wrong number of arguments for 'client setname' command",
                ));
            }
            Ok(ok_result())
        }
        "GETNAME" => Ok(RedisCommandResult {
            response: RespValue::NullBulkString,
            close_connection: false,
        }),
        "ID" => Ok(RedisCommandResult {
            response: RespValue::Integer(1),
            close_connection: false,
        }),
        _ => Err(RedisCommandError::validation(format!(
            "unsupported CLIENT subcommand {}",
            subcommand
        ))),
    }
}

fn execute_hello(
    args: &[String],
    selected_db: u32,
) -> Result<RedisCommandResult, RedisCommandError> {
    if args.len() > 1 {
        return Err(RedisCommandError::validation(
            "HELLO only supports an optional proto argument",
        ));
    }

    if let Some(proto) = args.first() {
        let parsed = proto.parse::<u32>().map_err(|_| {
            RedisCommandError::validation("HELLO protover must be a positive integer")
        })?;
        if parsed != 2 && parsed != 3 {
            return Err(RedisCommandError::validation("unsupported HELLO protover"));
        }
    }

    Ok(RedisCommandResult {
        response: RespValue::Array(vec![
            bulk("server"),
            bulk("powdrr"),
            bulk("version"),
            bulk("0.0.1"),
            bulk("proto"),
            RespValue::Integer(2),
            bulk("mode"),
            bulk("standalone"),
            bulk("role"),
            bulk("master"),
            bulk("db"),
            RespValue::Integer(selected_db as i64),
            bulk("modules"),
            RespValue::Array(vec![]),
        ]),
        close_connection: false,
    })
}

async fn execute_select(
    selected_db: &mut u32,
    args: &[String],
) -> Result<RedisCommandResult, RedisCommandError> {
    let [database] = args else {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'select' command",
        ));
    };

    let database = parse_database(database)?;
    load_redis_database_binding(database).await?;
    *selected_db = database;
    Ok(ok_result())
}

async fn execute_get(
    selected_db: u32,
    args: &[String],
) -> Result<RedisCommandResult, RedisCommandError> {
    let [key] = args else {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'get' command",
        ));
    };

    let binding = load_redis_database_binding(selected_db).await?;
    let keys = vec![key.to_string()];
    let values = fetch_redis_values(&binding, &keys).await?;
    let value = values.into_iter().next().unwrap_or(None);
    Ok(RedisCommandResult {
        response: value
            .map(RespValue::BulkString)
            .unwrap_or(RespValue::NullBulkString),
        close_connection: false,
    })
}

async fn execute_mget(
    selected_db: u32,
    args: &[String],
) -> Result<RedisCommandResult, RedisCommandError> {
    if args.is_empty() {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'mget' command",
        ));
    }

    let binding = load_redis_database_binding(selected_db).await?;
    let values = fetch_redis_values(&binding, args).await?;
    let values = values
        .into_iter()
        .map(|value| {
            value
                .map(RespValue::BulkString)
                .unwrap_or(RespValue::NullBulkString)
        })
        .collect();

    Ok(RedisCommandResult {
        response: RespValue::Array(values),
        close_connection: false,
    })
}

async fn execute_exists(
    selected_db: u32,
    args: &[String],
) -> Result<RedisCommandResult, RedisCommandError> {
    if args.is_empty() {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'exists' command",
        ));
    }

    let binding = load_redis_database_binding(selected_db).await?;
    let count = fetch_redis_values(&binding, args)
        .await?
        .into_iter()
        .filter(|value| value.is_some())
        .count() as i64;

    Ok(RedisCommandResult {
        response: RespValue::Integer(count),
        close_connection: false,
    })
}

fn redis_lookup_request(binding: &RedisTableBinding, key: &str) -> ServingRequestPlan {
    ServingRequestPlan {
        select: Some(vec![binding.config.value_field.clone()]),
        filters: vec![ServingPredicate {
            field: binding.config.key_field.clone(),
            eq: Some(json!(key)),
            in_values: None,
            gt: None,
            gte: None,
            lt: None,
            lte: None,
        }],
        aggregate: None,
        order_by: vec![],
        limit: Some(2),
        allow_slow_path: false,
        explain: false,
    }
}

async fn fetch_redis_values(
    binding: &RedisTableBinding,
    keys: &[String],
) -> Result<Vec<Option<Vec<u8>>>, RedisCommandError> {
    let requests = keys
        .iter()
        .map(|key| redis_lookup_request(binding, key))
        .collect::<Vec<_>>();

    if let Some(row_sets) =
        execute_fast_path_point_lookup_batch_rows(&binding.table_name, &requests).await?
    {
        let mut values = Vec::with_capacity(row_sets.len());
        for (key, rows) in keys.iter().zip(row_sets.into_iter()) {
            values.push(redis_value_from_rows(
                &binding.table_name,
                &binding.config.value_field,
                key,
                rows,
            )?);
        }
        return Ok(values);
    }

    let mut values = Vec::with_capacity(requests.len());
    for (key, request) in keys.iter().zip(requests.into_iter()) {
        let response = execute_serving_query(&binding.table_name, request)
            .await
            .map_err(convert_serving_error)?;
        if response.classification != ServingQueryClassification::FastPath {
            return Err(RedisCommandError::validation(
                response.reason.unwrap_or_else(|| {
                    "Query did not qualify for the serving fast path".to_string()
                }),
            ));
        }
        values.push(redis_value_from_rows(
            &binding.table_name,
            &binding.config.value_field,
            key,
            response.rows,
        )?);
    }
    Ok(values)
}

fn redis_value_from_rows(
    table_name: &str,
    value_field: &str,
    key: &str,
    rows: Vec<Value>,
) -> Result<Option<Vec<u8>>, RedisCommandError> {
    if rows.len() > 1 {
        return Err(RedisCommandError::internal(format!(
            "Redis key {} matched multiple rows in table {}",
            key, table_name
        )));
    }
    let row = match rows.into_iter().next() {
        Some(row) => row,
        None => return Ok(None),
    };
    let value = row.get(value_field).cloned().unwrap_or(Value::Null);
    Ok(redis_value_bytes(&value))
}

async fn execute_fast_path_point_lookup_batch_rows(
    table_name: &str,
    requests: &[ServingRequestPlan],
) -> Result<Option<Vec<Vec<Value>>>, RedisCommandError> {
    execute_active_checkpoint_exact_lookup_batch_rows(table_name, requests)
        .await
        .map_err(convert_active_checkpoint_lookup_error)
}

fn redis_value_bytes(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone().into_bytes()),
        _ => Some(serde_json::to_vec(value).unwrap()),
    }
}

fn parse_database(database: &str) -> Result<u32, RedisCommandError> {
    database
        .parse::<u32>()
        .map_err(|_| RedisCommandError::validation("database index must be a non-negative integer"))
}

async fn list_redis_bindings() -> Result<Vec<RedisTableBinding>, RedisCommandError> {
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
        let Some(config) = description.redis else {
            continue;
        };
        bindings.push(RedisTableBinding { table_name, config });
    }
    Ok(bindings)
}

async fn load_redis_database_binding(
    database: u32,
) -> Result<RedisTableBinding, RedisCommandError> {
    let matches = list_redis_bindings()
        .await?
        .into_iter()
        .filter(|binding| binding.config.enabled && binding.config.database == database)
        .collect::<Vec<_>>();

    match matches.len() {
        0 => Err(RedisCommandError::namespace_not_found(format!(
            "Redis database {} is not configured",
            database
        ))),
        1 => Ok(matches.into_iter().next().unwrap()),
        _ => Err(RedisCommandError::internal(format!(
            "Multiple Powdrr tables are exposed as Redis database {}",
            database
        ))),
    }
}

async fn validate_redis_database_uniqueness(
    table_name: &str,
    config: &RedisTableConfig,
) -> Result<(), RedisCommandError> {
    if !config.enabled {
        return Ok(());
    }

    let duplicate = list_redis_bindings().await?.into_iter().find(|binding| {
        binding.table_name != table_name
            && binding.config.enabled
            && binding.config.database == config.database
    });
    if let Some(binding) = duplicate {
        return Err(RedisCommandError::validation(format!(
            "Redis database {} is already mapped to table {}",
            config.database, binding.table_name
        )));
    }
    Ok(())
}

async fn load_table_description(table_name: &str) -> Result<TableDescription, RedisCommandError> {
    STATE_PROVIDER
        .describe_table(&table_name.to_string())
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            RedisCommandError::namespace_not_found(format!("Table {} was not found", table_name))
        })
}

async fn load_table_schema(table_name: &str) -> Result<PowdrrSchema, RedisCommandError> {
    let checkpoint = load_active_checkpoint(table_name).await?;
    schema_from_checkpoint(&checkpoint)
}

async fn load_active_checkpoint(
    table_name: &str,
) -> Result<TableMetadataCheckpoint, RedisCommandError> {
    load_shared_active_checkpoint(table_name)
        .await
        .map_err(convert_active_checkpoint_lookup_error)
}

fn schema_from_checkpoint(
    checkpoint: &TableMetadataCheckpoint,
) -> Result<PowdrrSchema, RedisCommandError> {
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
    Err(RedisCommandError::internal(
        "Checkpoint did not contain a usable schema",
    ))
}

fn validate_redis_config(
    schema: &PowdrrSchema,
    config: &RedisTableConfig,
) -> Result<(), RedisCommandError> {
    if config.key_field.trim().is_empty() {
        return Err(RedisCommandError::validation(
            "Redis key field must be a non-empty string",
        ));
    }
    if config.value_field.trim().is_empty() {
        return Err(RedisCommandError::validation(
            "Redis value field must be a non-empty string",
        ));
    }

    let schema_map = schema.to_map();
    if !schema_map.contains_key(&config.key_field) {
        return Err(RedisCommandError::validation(format!(
            "Unknown Redis key field {}",
            config.key_field
        )));
    }
    if !schema_map.contains_key(&config.value_field) {
        return Err(RedisCommandError::validation(format!(
            "Unknown Redis value field {}",
            config.value_field
        )));
    }
    Ok(())
}

fn convert_serving_error(error: ServingQueryError) -> RedisCommandError {
    match error.status {
        StatusCode::NOT_FOUND => RedisCommandError::namespace_not_found(error.message),
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            RedisCommandError::validation(error.message)
        }
        _ => RedisCommandError::internal(error.message),
    }
}

fn convert_active_checkpoint_lookup_error(error: ActiveCheckpointLookupError) -> RedisCommandError {
    match error {
        ActiveCheckpointLookupError::NotFound(message) => {
            RedisCommandError::namespace_not_found(message)
        }
        ActiveCheckpointLookupError::Internal(message) => RedisCommandError::internal(message),
    }
}

fn service_error(error: ServiceApiError) -> RedisCommandError {
    RedisCommandError::internal(error.to_string())
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

fn json_error_response(state: &State, error: RedisCommandError) -> gotham::hyper::Response<Body> {
    json_response(
        state,
        error.status,
        &json!({
            "error": error.message,
        }),
    )
}

fn ok_result() -> RedisCommandResult {
    simple_result("OK")
}

fn simple_result(value: &str) -> RedisCommandResult {
    RedisCommandResult {
        response: RespValue::SimpleString(value.to_string()),
        close_connection: false,
    }
}

fn bulk(value: &str) -> RespValue {
    RespValue::BulkString(value.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_returns_resp2_friendly_metadata() {
        let result = execute_hello(&[], 7).unwrap();
        assert!(!result.close_connection);
        assert_eq!(
            result.response,
            RespValue::Array(vec![
                bulk("server"),
                bulk("powdrr"),
                bulk("version"),
                bulk("0.0.1"),
                bulk("proto"),
                RespValue::Integer(2),
                bulk("mode"),
                bulk("standalone"),
                bulk("role"),
                bulk("master"),
                bulk("db"),
                RespValue::Integer(7),
                bulk("modules"),
                RespValue::Array(vec![]),
            ])
        );
    }

    #[test]
    fn redis_value_bytes_stringifies_non_strings() {
        assert_eq!(redis_value_bytes(&json!("hello")), Some(b"hello".to_vec()));
        assert_eq!(redis_value_bytes(&json!(42)), Some(b"42".to_vec()));
        assert_eq!(
            redis_value_bytes(&json!({"ok": true})),
            Some(br#"{"ok":true}"#.to_vec())
        );
        assert_eq!(redis_value_bytes(&Value::Null), None);
    }
}
