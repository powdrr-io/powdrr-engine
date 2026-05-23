use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{LazyLock, Mutex},
};

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
    ActiveCheckpointLookupError,
    execute_active_checkpoint_exact_lookup_batch_projected_field_bytes,
    execute_active_checkpoint_exact_lookup_batch_rows,
    load_active_checkpoint as load_shared_active_checkpoint,
};
use powdrr_control_plane::data_contract::{
    CreateTable, RedisTableConfig, TableDescription, TableMetadataCheckpoint,
};
use powdrr_query_lib::schema_massager::PowdrrSchema;
use powdrr_query_lib::serving_plan::{
    ServingPredicate, ServingQueryClassification, ServingRequestPlan,
};
use powdrr_query_runtime::lakehouse_serving::{
    ProjectedFieldBytesRow, ServingQueryError, execute_serving_query,
};
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

#[derive(Clone, Debug)]
struct ResolvedRedisDatabaseBinding {
    binding: RedisTableBinding,
    active_checkpoint_id: String,
    known_fields: HashSet<String>,
    all_fields: Vec<String>,
}

static REDIS_DATABASE_BINDING_CACHE: LazyLock<Mutex<HashMap<u32, ResolvedRedisDatabaseBinding>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

type RedisProjectedFieldRow = ProjectedFieldBytesRow;

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

    fn read_only() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            prefix: "READONLY",
            message: "This Powdrr Redis frontend is running in read-only mode".to_string(),
        }
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
        if STATE_PROVIDER.is_read_only().await {
            let response = json_error_response(&state, RedisCommandError::read_only());
            return Ok((state, response));
        }
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
            let support = existing
                .as_ref()
                .and_then(|description| description.support.clone());
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
                support,
                dynamodb,
                mongodb,
                redis: Some(body.clone()),
            };

            STATE_PROVIDER
                .upsert_table_metadata(&request)
                .await
                .map_err(service_error)?;
            clear_redis_database_binding_cache();

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

    if STATE_PROVIDER.is_read_only().await && is_known_redis_write_command(&command) {
        return Err(RedisCommandError::read_only());
    }

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
        "HGET" => execute_hget(*selected_db, rest).await,
        "HMGET" => execute_hmget(*selected_db, rest).await,
        "HGETALL" => execute_hgetall(*selected_db, rest).await,
        "HEXISTS" => execute_hexists(*selected_db, rest).await,
        "EXISTS" => execute_exists(*selected_db, rest).await,
        _ => Err(RedisCommandError::unsupported(&command)),
    }
}

fn is_known_redis_write_command(command: &str) -> bool {
    matches!(
        command,
        "APPEND"
            | "DECR"
            | "DECRBY"
            | "DEL"
            | "EXPIRE"
            | "FLUSHALL"
            | "FLUSHDB"
            | "GETSET"
            | "HDEL"
            | "HINCRBY"
            | "HMSET"
            | "HSET"
            | "INCR"
            | "INCRBY"
            | "LPOP"
            | "LPUSH"
            | "MSET"
            | "PERSIST"
            | "PSETEX"
            | "RPOP"
            | "RPUSH"
            | "SADD"
            | "SET"
            | "SETEX"
            | "SETNX"
            | "UNLINK"
            | "XADD"
            | "ZADD"
    )
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
    let value_field = configured_redis_value_field(&binding)?;
    let keys = vec![key.to_string()];
    let values = fetch_redis_values(&binding, &keys, value_field).await?;
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
    let value_field = configured_redis_value_field(&binding)?;
    let values = fetch_redis_values(&binding, args, value_field).await?;
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

async fn execute_hget(
    selected_db: u32,
    args: &[String],
) -> Result<RedisCommandResult, RedisCommandError> {
    let [key, field] = args else {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'hget' command",
        ));
    };

    let binding = load_redis_database_binding(selected_db).await?;
    let select = select_known_fields(&binding.known_fields, &[field.to_string()]);
    if let Some(rows) = fetch_redis_rows_field_bytes(&binding, &[key.to_string()], &select).await? {
        let row = redis_projected_row_from_rows(
            &binding.binding.table_name,
            key,
            rows.into_iter().next().unwrap_or_default(),
        )?;
        return Ok(RedisCommandResult {
            response: redis_projected_field_bytes(row.as_ref(), field)
                .map(RespValue::BulkString)
                .unwrap_or(RespValue::NullBulkString),
            close_connection: false,
        });
    }
    let row = fetch_redis_row(&binding, key, &select).await?;
    Ok(RedisCommandResult {
        response: redis_field_bytes(row.as_ref(), field)
            .map(RespValue::BulkString)
            .unwrap_or(RespValue::NullBulkString),
        close_connection: false,
    })
}

async fn execute_hmget(
    selected_db: u32,
    args: &[String],
) -> Result<RedisCommandResult, RedisCommandError> {
    let Some((key, fields)) = args.split_first() else {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'hmget' command",
        ));
    };
    if fields.is_empty() {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'hmget' command",
        ));
    }

    let binding = load_redis_database_binding(selected_db).await?;
    let select = select_known_fields(&binding.known_fields, fields);
    if let Some(rows) = fetch_redis_rows_field_bytes(&binding, &[key.to_string()], &select).await? {
        let row = redis_projected_row_from_rows(
            &binding.binding.table_name,
            key,
            rows.into_iter().next().unwrap_or_default(),
        )?;
        let values = fields
            .iter()
            .map(|field| {
                redis_projected_field_bytes(row.as_ref(), field)
                    .map(RespValue::BulkString)
                    .unwrap_or(RespValue::NullBulkString)
            })
            .collect();

        return Ok(RedisCommandResult {
            response: RespValue::Array(values),
            close_connection: false,
        });
    }
    let row = fetch_redis_row(&binding, key, &select).await?;
    let values = fields
        .iter()
        .map(|field| {
            redis_field_bytes(row.as_ref(), field)
                .map(RespValue::BulkString)
                .unwrap_or(RespValue::NullBulkString)
        })
        .collect();

    Ok(RedisCommandResult {
        response: RespValue::Array(values),
        close_connection: false,
    })
}

async fn execute_hgetall(
    selected_db: u32,
    args: &[String],
) -> Result<RedisCommandResult, RedisCommandError> {
    let [key] = args else {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'hgetall' command",
        ));
    };

    let binding = load_redis_database_binding(selected_db).await?;
    let fields = binding.all_fields.clone();
    if let Some(rows) = fetch_redis_rows_field_bytes(&binding, &[key.to_string()], &fields).await? {
        let row = redis_projected_row_from_rows(
            &binding.binding.table_name,
            key,
            rows.into_iter().next().unwrap_or_default(),
        )?;
        let Some(row) = row.as_ref() else {
            return Ok(RedisCommandResult {
                response: RespValue::Array(vec![]),
                close_connection: false,
            });
        };

        let mut response = Vec::with_capacity(fields.len() * 2);
        for field in fields {
            response.push(RespValue::BulkString(field.clone().into_bytes()));
            response.push(
                redis_projected_field_bytes(Some(row), &field)
                    .map(RespValue::BulkString)
                    .unwrap_or(RespValue::NullBulkString),
            );
        }

        return Ok(RedisCommandResult {
            response: RespValue::Array(response),
            close_connection: false,
        });
    }
    let row = fetch_redis_row(&binding, key, &fields).await?;

    let Some(row) = row.as_ref() else {
        return Ok(RedisCommandResult {
            response: RespValue::Array(vec![]),
            close_connection: false,
        });
    };

    let mut response = Vec::with_capacity(fields.len() * 2);
    for field in fields {
        response.push(RespValue::BulkString(field.clone().into_bytes()));
        response.push(
            redis_field_bytes(Some(row), &field)
                .map(RespValue::BulkString)
                .unwrap_or(RespValue::NullBulkString),
        );
    }

    Ok(RedisCommandResult {
        response: RespValue::Array(response),
        close_connection: false,
    })
}

async fn execute_hexists(
    selected_db: u32,
    args: &[String],
) -> Result<RedisCommandResult, RedisCommandError> {
    let [key, field] = args else {
        return Err(RedisCommandError::validation(
            "wrong number of arguments for 'hexists' command",
        ));
    };

    let binding = load_redis_database_binding(selected_db).await?;
    let select = select_known_fields(&binding.known_fields, &[field.to_string()]);
    if let Some(rows) = fetch_redis_rows_field_bytes(&binding, &[key.to_string()], &select).await? {
        let row = redis_projected_row_from_rows(
            &binding.binding.table_name,
            key,
            rows.into_iter().next().unwrap_or_default(),
        )?;
        let exists = redis_projected_field_bytes(row.as_ref(), field).is_some();
        return Ok(RedisCommandResult {
            response: RespValue::Integer(if exists { 1 } else { 0 }),
            close_connection: false,
        });
    }
    let row = fetch_redis_row(&binding, key, &select).await?;
    let exists = redis_field_bytes(row.as_ref(), field).is_some();

    Ok(RedisCommandResult {
        response: RespValue::Integer(if exists { 1 } else { 0 }),
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
    let value_field = configured_redis_value_field(&binding)?;
    let count = fetch_redis_values(&binding, args, value_field)
        .await?
        .into_iter()
        .filter(|value| value.is_some())
        .count() as i64;

    Ok(RedisCommandResult {
        response: RespValue::Integer(count),
        close_connection: false,
    })
}

fn redis_lookup_request(key_field: &str, key: &str, select: Vec<String>) -> ServingRequestPlan {
    ServingRequestPlan {
        select: Some(select),
        filters: vec![ServingPredicate {
            field: key_field.to_string(),
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
    binding: &ResolvedRedisDatabaseBinding,
    keys: &[String],
    value_field: &str,
) -> Result<Vec<Option<Vec<u8>>>, RedisCommandError> {
    let select = vec![value_field.to_string()];
    if let Some(rows) = fetch_redis_rows_field_bytes(binding, keys, &select).await? {
        return keys
            .iter()
            .zip(rows.into_iter())
            .map(|rows| {
                let (key, rows) = rows;
                redis_projected_row_from_rows(&binding.binding.table_name, key, rows)
                    .map(|row| redis_projected_field_bytes(row.as_ref(), value_field))
            })
            .collect();
    }
    let rows = fetch_redis_rows(binding, keys, &select).await?;
    keys.iter()
        .zip(rows.into_iter())
        .map(|rows| {
            let (key, rows) = rows;
            redis_row_from_rows(&binding.binding.table_name, key, rows)
                .map(|row| redis_field_bytes(row.as_ref(), value_field))
        })
        .collect()
}

async fn fetch_redis_rows_field_bytes(
    binding: &ResolvedRedisDatabaseBinding,
    keys: &[String],
    select: &[String],
) -> Result<Option<Vec<Vec<RedisProjectedFieldRow>>>, RedisCommandError> {
    if select.is_empty() {
        return Ok(Some(
            std::iter::repeat_with(Vec::new).take(keys.len()).collect(),
        ));
    }

    let (lookup_keys, key_positions) = dedupe_lookup_keys(keys);
    let requests = lookup_keys
        .iter()
        .map(|key| redis_lookup_request(&binding.binding.config.key_field, key, select.to_vec()))
        .collect::<Vec<_>>();

    Ok(execute_fast_path_point_lookup_batch_projected_field_bytes(
        &binding.binding.table_name,
        &requests,
    )
    .await?
    .map(|row_sets| expand_lookup_row_sets(row_sets, &key_positions)))
}

async fn fetch_redis_row(
    binding: &ResolvedRedisDatabaseBinding,
    key: &str,
    select: &[String],
) -> Result<Option<Value>, RedisCommandError> {
    if select.is_empty() {
        return Ok(None);
    }
    let rows = fetch_redis_rows(binding, &[key.to_string()], select).await?;
    redis_row_from_rows(
        &binding.binding.table_name,
        key,
        rows.into_iter().next().unwrap_or_default(),
    )
}

async fn fetch_redis_rows(
    binding: &ResolvedRedisDatabaseBinding,
    keys: &[String],
    select: &[String],
) -> Result<Vec<Vec<Value>>, RedisCommandError> {
    let (lookup_keys, key_positions) = dedupe_lookup_keys(keys);
    let requests = lookup_keys
        .iter()
        .map(|key| redis_lookup_request(&binding.binding.config.key_field, key, select.to_vec()))
        .collect::<Vec<_>>();

    if let Some(row_sets) =
        execute_fast_path_point_lookup_batch_rows(&binding.binding.table_name, &requests).await?
    {
        return Ok(expand_lookup_row_sets(row_sets, &key_positions));
    }

    let mut values = Vec::with_capacity(requests.len());
    for request in requests {
        let response = execute_serving_query(&binding.binding.table_name, request)
            .await
            .map_err(convert_serving_error)?;
        if response.classification != ServingQueryClassification::FastPath {
            return Err(RedisCommandError::validation(
                response.reason.unwrap_or_else(|| {
                    "Query did not qualify for the serving fast path".to_string()
                }),
            ));
        }
        values.push(response.rows);
    }
    Ok(expand_lookup_row_sets(values, &key_positions))
}

fn redis_row_from_rows(
    table_name: &str,
    key: &str,
    rows: Vec<Value>,
) -> Result<Option<Value>, RedisCommandError> {
    if rows.len() > 1 {
        return Err(RedisCommandError::internal(format!(
            "Redis key {} matched multiple rows in table {}",
            key, table_name
        )));
    }
    Ok(rows.into_iter().next())
}

fn redis_field_bytes(row: Option<&Value>, field: &str) -> Option<Vec<u8>> {
    let row = row?;
    let value = row.get(field)?;
    redis_value_bytes(value)
}

fn redis_projected_row_from_rows(
    table_name: &str,
    key: &str,
    rows: Vec<RedisProjectedFieldRow>,
) -> Result<Option<RedisProjectedFieldRow>, RedisCommandError> {
    if rows.len() > 1 {
        return Err(RedisCommandError::internal(format!(
            "Redis key {} matched multiple rows in table {}",
            key, table_name
        )));
    }
    Ok(rows.into_iter().next())
}

fn redis_projected_field_bytes(
    row: Option<&RedisProjectedFieldRow>,
    field: &str,
) -> Option<Vec<u8>> {
    row.and_then(|row| row.get(field)).cloned().flatten()
}

async fn execute_fast_path_point_lookup_batch_rows(
    table_name: &str,
    requests: &[ServingRequestPlan],
) -> Result<Option<Vec<Vec<Value>>>, RedisCommandError> {
    execute_active_checkpoint_exact_lookup_batch_rows(table_name, requests)
        .await
        .map_err(convert_active_checkpoint_lookup_error)
}

async fn execute_fast_path_point_lookup_batch_projected_field_bytes(
    table_name: &str,
    requests: &[ServingRequestPlan],
) -> Result<Option<Vec<Vec<RedisProjectedFieldRow>>>, RedisCommandError> {
    execute_active_checkpoint_exact_lookup_batch_projected_field_bytes(table_name, requests)
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

fn configured_redis_value_field<'a>(
    binding: &'a ResolvedRedisDatabaseBinding,
) -> Result<&'a str, RedisCommandError> {
    binding.binding.config.value_field.as_deref().ok_or_else(|| {
        RedisCommandError::validation(
            "Redis value_field must be configured to use GET, MGET, or EXISTS; use HGET, HMGET, HGETALL, or HEXISTS for multi-column row access",
        )
    })
}

fn unique_fields(fields: &[String]) -> Vec<String> {
    let mut unique = Vec::with_capacity(fields.len());
    for field in fields {
        if !unique.iter().any(|existing| existing == field) {
            unique.push(field.clone());
        }
    }
    unique
}

fn dedupe_lookup_keys(keys: &[String]) -> (Vec<String>, Vec<usize>) {
    let mut positions_by_key = HashMap::new();
    let mut unique_keys = Vec::new();
    let mut key_positions = Vec::with_capacity(keys.len());
    for key in keys {
        let index = if let Some(index) = positions_by_key.get(key) {
            *index
        } else {
            let index = unique_keys.len();
            unique_keys.push(key.clone());
            positions_by_key.insert(key.clone(), index);
            index
        };
        key_positions.push(index);
    }
    (unique_keys, key_positions)
}

fn expand_lookup_row_sets<T: Clone>(row_sets: Vec<Vec<T>>, key_positions: &[usize]) -> Vec<Vec<T>> {
    key_positions
        .iter()
        .map(|index| row_sets.get(*index).cloned().unwrap_or_default())
        .collect()
}

fn select_known_fields(known_fields: &HashSet<String>, fields: &[String]) -> Vec<String> {
    let unique = unique_fields(fields);
    unique
        .into_iter()
        .filter(|field| known_fields.contains(field))
        .collect()
}

fn clear_redis_database_binding_cache() {
    REDIS_DATABASE_BINDING_CACHE.lock().unwrap().clear();
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
) -> Result<ResolvedRedisDatabaseBinding, RedisCommandError> {
    let cached = {
        REDIS_DATABASE_BINDING_CACHE
            .lock()
            .unwrap()
            .get(&database)
            .cloned()
    };
    if let Some(cached) = cached {
        if let Ok(checkpoint) = load_active_checkpoint(&cached.binding.table_name).await {
            if checkpoint.checkpoint_id == cached.active_checkpoint_id {
                return Ok(cached);
            }
            let resolved = resolved_redis_database_binding_from_checkpoint(
                cached.binding.clone(),
                &checkpoint,
            )?;
            REDIS_DATABASE_BINDING_CACHE
                .lock()
                .unwrap()
                .insert(database, resolved.clone());
            return Ok(resolved);
        }
    }

    refresh_redis_database_binding(database).await
}

async fn refresh_redis_database_binding(
    database: u32,
) -> Result<ResolvedRedisDatabaseBinding, RedisCommandError> {
    let matches = list_redis_bindings()
        .await?
        .into_iter()
        .filter(|binding| binding.config.enabled && binding.config.database == database)
        .collect::<Vec<_>>();

    let binding = match matches.len() {
        0 => {
            return Err(RedisCommandError::namespace_not_found(format!(
                "Redis database {} is not configured",
                database
            )));
        }
        1 => matches.into_iter().next().unwrap(),
        _ => {
            return Err(RedisCommandError::internal(format!(
                "Multiple Powdrr tables are exposed as Redis database {}",
                database
            )));
        }
    };

    let checkpoint = load_active_checkpoint(&binding.table_name).await?;
    let resolved = resolved_redis_database_binding_from_checkpoint(binding, &checkpoint)?;
    REDIS_DATABASE_BINDING_CACHE
        .lock()
        .unwrap()
        .insert(database, resolved.clone());
    Ok(resolved)
}

fn resolved_redis_database_binding_from_checkpoint(
    binding: RedisTableBinding,
    checkpoint: &TableMetadataCheckpoint,
) -> Result<ResolvedRedisDatabaseBinding, RedisCommandError> {
    let schema = schema_from_checkpoint(checkpoint)?;
    let all_fields = schema
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let known_fields = all_fields.iter().cloned().collect::<HashSet<_>>();
    Ok(ResolvedRedisDatabaseBinding {
        binding,
        active_checkpoint_id: checkpoint.checkpoint_id.clone(),
        known_fields,
        all_fields,
    })
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
        .map(|checkpoint| checkpoint.as_ref().clone())
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
    let schema_map = schema.to_map();
    if !schema_map.contains_key(&config.key_field) {
        return Err(RedisCommandError::validation(format!(
            "Unknown Redis key field {}",
            config.key_field
        )));
    }
    if let Some(value_field) = config.value_field.as_ref() {
        if value_field.trim().is_empty() {
            return Err(RedisCommandError::validation(
                "Redis value field must be a non-empty string when provided",
            ));
        }
        if !schema_map.contains_key(value_field) {
            return Err(RedisCommandError::validation(format!(
                "Unknown Redis value field {}",
                value_field
            )));
        }
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
