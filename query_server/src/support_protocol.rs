use std::collections::{HashMap, HashSet};
use std::pin::Pin;

use futures_util::future::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{body, Body};
use gotham::mime;
use gotham::state::{FromState, State};
use http::StatusCode;
use serde::Serialize;

use crate::dynamodb_protocol::merge_dynamodb_serving_patterns;
use crate::elastic_search_http_types::NamePathExtractor;
use crate::exact_lookup::{load_active_checkpoint, ActiveCheckpointLookupError};
use powdrr_control_plane::data_contract::{
    CreateTable, DynamoDbTableConfig, RedisTableConfig, ServingPattern, ServingTableConfig,
    SupportDynamoDbTableConfig, SupportKeySchemaConfig, SupportRedisTableConfig,
    SupportTableConfig, TableDescription, TableMetadataCheckpoint,
};
use powdrr_query_lib::schema_massager::{PowdrrDataType, PowdrrSchema};
use powdrr_query_runtime::state_provider::{ServiceApiError, STATE_PROVIDER};

const SUPPORT_ES_PATTERN_PREFIX: &str = "_support_es_";
const SUPPORT_REDIS_PATTERN_PREFIX: &str = "_support_redis_";
const DYNAMODB_CONFIG_PATTERN_PREFIX: &str = "_dynamodb_";

#[derive(Serialize, Clone, Debug)]
pub struct SupportConfigResponse {
    pub acknowledged: bool,
    pub table: String,
    pub support: SupportTableConfig,
    #[serde(default)]
    pub serving: Option<ServingTableConfig>,
    #[serde(default)]
    pub dynamodb: Option<DynamoDbTableConfig>,
    #[serde(default)]
    pub redis: Option<RedisTableConfig>,
}

#[derive(Debug)]
struct SupportConfigError {
    status: StatusCode,
    message: String,
}

impl SupportConfigError {
    fn validation(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }
}

pub fn get_support_config(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let result = async {
            let description = load_table_description(&path).await?;
            let support = description.support.clone().ok_or_else(|| {
                SupportConfigError::not_found(format!(
                    "No support config declared for table {}",
                    path
                ))
            })?;

            Ok::<_, SupportConfigError>(SupportConfigResponse {
                acknowledged: true,
                table: description.name,
                support,
                serving: description.serving,
                dynamodb: description.dynamodb,
                redis: description.redis,
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

pub fn put_support_config(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        if STATE_PROVIDER.is_read_only().await {
            let response = json_error_response(
                &state,
                SupportConfigError::validation(
                    "Support config writes are disabled in Powdrr read-only mode",
                ),
            );
            return Ok((state, response));
        }
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let body = match parse_json_body::<SupportTableConfig>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response = json_error_response(&state, SupportConfigError::validation(message));
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
            let mongodb = existing
                .as_ref()
                .and_then(|description| description.mongodb.clone());

            let schema = load_table_schema(&path).await?;
            validate_support_config(&schema, &body)?;

            let dynamodb = body
                .dynamodb
                .as_ref()
                .map(|config| derive_support_dynamodb_config(&body.key_schema, config));
            if let Some(config) = dynamodb.as_ref() {
                validate_derived_dynamodb_config(&schema, config)?;
            }

            let redis = body
                .redis
                .as_ref()
                .map(|config| derive_support_redis_config(&body.key_schema, config));
            if let Some(config) = redis.as_ref() {
                validate_derived_redis_config(&schema, config)?;
                validate_redis_database_uniqueness(&path, config).await?;
            }

            let serving = derive_support_serving_config(
                existing
                    .as_ref()
                    .and_then(|description| description.serving.clone()),
                &body.key_schema,
                body.elasticsearch.is_some(),
                dynamodb.as_ref(),
                redis.as_ref(),
            );

            let request = CreateTable {
                name: path.clone(),
                tags,
                serving: Some(serving.clone()),
                support: Some(body.clone()),
                dynamodb: dynamodb.clone(),
                mongodb,
                redis: redis.clone(),
            };

            STATE_PROVIDER
                .upsert_table_metadata(&request)
                .await
                .map_err(service_error)?;

            Ok::<_, SupportConfigError>(SupportConfigResponse {
                acknowledged: true,
                table: path,
                support: body,
                serving: Some(serving),
                dynamodb,
                redis,
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

fn derive_support_serving_config(
    existing: Option<ServingTableConfig>,
    key_schema: &SupportKeySchemaConfig,
    expose_elasticsearch: bool,
    dynamodb: Option<&DynamoDbTableConfig>,
    redis: Option<&RedisTableConfig>,
) -> ServingTableConfig {
    let mut patterns = existing.unwrap_or_default();
    patterns.patterns.retain(|pattern| {
        !pattern.name.starts_with(SUPPORT_ES_PATTERN_PREFIX)
            && !pattern.name.starts_with(SUPPORT_REDIS_PATTERN_PREFIX)
            && !pattern.name.starts_with(DYNAMODB_CONFIG_PATTERN_PREFIX)
    });

    if expose_elasticsearch {
        patterns
            .patterns
            .extend(derived_serving_patterns_for_key_schema(
                SUPPORT_ES_PATTERN_PREFIX,
                key_schema,
                true,
                true,
            ));
    }

    if let Some(redis) = redis {
        let _ = redis;
        patterns.patterns.push(ServingPattern {
            name: format!("{SUPPORT_REDIS_PATTERN_PREFIX}get"),
            eq_fields: vec![key_schema.primary_key.clone()],
            range_field: None,
            order_field: None,
            descending: false,
            max_limit: Some(1),
            projection: None,
            aggregate: None,
        });
    }

    if let Some(dynamodb) = dynamodb {
        patterns = merge_dynamodb_serving_patterns(Some(patterns), dynamodb);
    }

    patterns
}

fn derived_serving_patterns_for_key_schema(
    prefix: &str,
    key_schema: &SupportKeySchemaConfig,
    include_get_item_pattern: bool,
    include_exact_query_pattern: bool,
) -> Vec<ServingPattern> {
    let mut patterns = vec![];
    if include_get_item_pattern {
        patterns.push(ServingPattern {
            name: format!("{prefix}get_item"),
            eq_fields: match key_schema.range_key.as_ref() {
                Some(range_key) => vec![key_schema.primary_key.clone(), range_key.clone()],
                None => vec![key_schema.primary_key.clone()],
            },
            range_field: None,
            order_field: None,
            descending: false,
            max_limit: Some(1),
            projection: None,
            aggregate: None,
        });
    }
    if include_exact_query_pattern {
        if let Some(range_key) = key_schema.range_key.as_ref() {
            patterns.push(ServingPattern {
                name: format!("{prefix}exact_query"),
                eq_fields: vec![key_schema.primary_key.clone(), range_key.clone()],
                range_field: None,
                order_field: None,
                descending: false,
                max_limit: None,
                projection: None,
                aggregate: None,
            });
        }
    }
    if let Some(range_key) = key_schema.range_key.as_ref() {
        for descending in [false, true] {
            patterns.push(ServingPattern {
                name: format!(
                    "{prefix}partition_query_{}",
                    if descending { "desc" } else { "asc" }
                ),
                eq_fields: vec![key_schema.primary_key.clone()],
                range_field: None,
                order_field: Some(range_key.clone()),
                descending,
                max_limit: None,
                projection: None,
                aggregate: None,
            });
            patterns.push(ServingPattern {
                name: format!(
                    "{prefix}range_query_{}",
                    if descending { "desc" } else { "asc" }
                ),
                eq_fields: vec![key_schema.primary_key.clone()],
                range_field: Some(range_key.clone()),
                order_field: Some(range_key.clone()),
                descending,
                max_limit: None,
                projection: None,
                aggregate: None,
            });
        }
    }

    patterns
}

fn derive_support_dynamodb_config(
    key_schema: &SupportKeySchemaConfig,
    config: &SupportDynamoDbTableConfig,
) -> DynamoDbTableConfig {
    DynamoDbTableConfig {
        partition_key: key_schema.primary_key.clone(),
        sort_key: key_schema.range_key.clone(),
        local_secondary_indexes: config.local_secondary_indexes.clone(),
        global_secondary_indexes: config.global_secondary_indexes.clone(),
    }
}

fn derive_support_redis_config(
    key_schema: &SupportKeySchemaConfig,
    config: &SupportRedisTableConfig,
) -> RedisTableConfig {
    RedisTableConfig {
        enabled: true,
        database: config.database,
        key_field: key_schema.primary_key.clone(),
        value_field: Some(config.value_field.clone()),
    }
}

async fn load_table_description(table_name: &str) -> Result<TableDescription, SupportConfigError> {
    STATE_PROVIDER
        .describe_table(&table_name.to_string())
        .await
        .map_err(service_error)?
        .ok_or_else(|| SupportConfigError::not_found(format!("Table {} was not found", table_name)))
}

async fn load_table_schema(table_name: &str) -> Result<PowdrrSchema, SupportConfigError> {
    let checkpoint = load_active_checkpoint(table_name)
        .await
        .map_err(convert_active_checkpoint_lookup_error)?;
    schema_from_checkpoint(&checkpoint)
}

fn schema_from_checkpoint(
    checkpoint: &TableMetadataCheckpoint,
) -> Result<PowdrrSchema, SupportConfigError> {
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
    Err(SupportConfigError::internal(
        "Checkpoint did not contain a usable schema",
    ))
}

fn validate_support_config(
    schema: &PowdrrSchema,
    config: &SupportTableConfig,
) -> Result<(), SupportConfigError> {
    if config.key_schema.primary_key.trim().is_empty() {
        return Err(SupportConfigError::validation(
            "Support primary_key must be a non-empty string",
        ));
    }
    if let Some(range_key) = config.key_schema.range_key.as_ref() {
        if range_key.trim().is_empty() {
            return Err(SupportConfigError::validation(
                "Support range_key must be a non-empty string when set",
            ));
        }
        if range_key == &config.key_schema.primary_key {
            return Err(SupportConfigError::validation(
                "Support primary_key and range_key must be different fields",
            ));
        }
    }
    if config.elasticsearch.is_none() && config.dynamodb.is_none() && config.redis.is_none() {
        return Err(SupportConfigError::validation(
            "Support config must expose at least one surface",
        ));
    }

    let schema_map = schema.to_map();
    validate_schema_field_exists(&schema_map, &config.key_schema.primary_key, "primary_key")?;
    if let Some(range_key) = config.key_schema.range_key.as_ref() {
        validate_schema_field_exists(&schema_map, range_key, "range_key")?;
    }

    if let Some(redis) = config.redis.as_ref() {
        if config.key_schema.range_key.is_some() {
            return Err(SupportConfigError::validation(
                "Redis support config does not support range_key yet",
            ));
        }
        if redis.value_field.trim().is_empty() {
            return Err(SupportConfigError::validation(
                "Support Redis value_field must be a non-empty string",
            ));
        }
        validate_schema_field_exists(&schema_map, &redis.value_field, "redis.value_field")?;
    }

    if let Some(dynamodb) = config.dynamodb.as_ref() {
        validate_support_index_names(dynamodb)?;
    }

    Ok(())
}

fn validate_support_index_names(
    config: &SupportDynamoDbTableConfig,
) -> Result<(), SupportConfigError> {
    let mut seen = HashSet::new();
    for index in config.local_secondary_indexes.iter() {
        if !seen.insert(index.name.clone()) {
            return Err(SupportConfigError::validation(format!(
                "Duplicate local secondary index name {}",
                index.name
            )));
        }
    }
    for index in config.global_secondary_indexes.iter() {
        if !seen.insert(index.name.clone()) {
            return Err(SupportConfigError::validation(format!(
                "Duplicate global secondary index name {}",
                index.name
            )));
        }
    }
    Ok(())
}

fn validate_schema_field_exists(
    schema_map: &HashMap<String, powdrr_query_lib::schema_massager::PowdrrField>,
    field_name: &str,
    label: &str,
) -> Result<(), SupportConfigError> {
    if !schema_map.contains_key(field_name) {
        return Err(SupportConfigError::validation(format!(
            "Unknown support {} field {}",
            label, field_name
        )));
    }
    Ok(())
}

fn validate_derived_dynamodb_config(
    schema: &PowdrrSchema,
    config: &DynamoDbTableConfig,
) -> Result<(), SupportConfigError> {
    let schema_map = schema.to_map();
    validate_dynamodb_key_type(&schema_map, &config.partition_key, "partition_key")?;
    if let Some(sort_key) = config.sort_key.as_ref() {
        validate_dynamodb_key_type(&schema_map, sort_key, "sort_key")?;
    }
    for index in config.local_secondary_indexes.iter() {
        validate_dynamodb_key_type(
            &schema_map,
            &index.sort_key,
            &format!("local secondary index {}", index.name),
        )?;
    }
    for index in config.global_secondary_indexes.iter() {
        validate_dynamodb_key_type(
            &schema_map,
            &index.partition_key,
            &format!("global secondary index {}", index.name),
        )?;
        if let Some(sort_key) = index.sort_key.as_ref() {
            validate_dynamodb_key_type(
                &schema_map,
                sort_key,
                &format!("global secondary index {}", index.name),
            )?;
        }
    }
    Ok(())
}

fn validate_dynamodb_key_type(
    schema_map: &HashMap<String, powdrr_query_lib::schema_massager::PowdrrField>,
    field_name: &str,
    label: &str,
) -> Result<(), SupportConfigError> {
    let field = schema_map.get(field_name).ok_or_else(|| {
        SupportConfigError::validation(format!("Unknown support {} field {}", label, field_name))
    })?;
    match field.data_type {
        PowdrrDataType::String | PowdrrDataType::Integer | PowdrrDataType::Float => Ok(()),
        _ => Err(SupportConfigError::validation(format!(
            "Field {} is not a valid DynamoDB key type",
            field_name
        ))),
    }
}

fn validate_derived_redis_config(
    schema: &PowdrrSchema,
    config: &RedisTableConfig,
) -> Result<(), SupportConfigError> {
    let schema_map = schema.to_map();
    if !schema_map.contains_key(&config.key_field) {
        return Err(SupportConfigError::validation(format!(
            "Unknown Redis key field {}",
            config.key_field
        )));
    }
    let value_field = config.value_field.as_ref().ok_or_else(|| {
        SupportConfigError::validation(
            "Derived Redis config is missing value_field for unified support mapping",
        )
    })?;
    if !schema_map.contains_key(value_field) {
        return Err(SupportConfigError::validation(format!(
            "Unknown Redis value field {}",
            value_field
        )));
    }
    Ok(())
}

async fn validate_redis_database_uniqueness(
    table_name: &str,
    config: &RedisTableConfig,
) -> Result<(), SupportConfigError> {
    let all_tables = STATE_PROVIDER
        .get_all_iceberg_tables()
        .await
        .map_err(service_error)?;

    for table in all_tables {
        if table == table_name {
            continue;
        }
        let Some(description) = STATE_PROVIDER
            .describe_table(&table)
            .await
            .map_err(service_error)?
        else {
            continue;
        };

        if description
            .redis
            .as_ref()
            .map(|redis| redis.enabled && redis.database == config.database)
            .unwrap_or(false)
        {
            return Err(SupportConfigError::validation(format!(
                "Redis database {} is already mapped to table {}",
                config.database, description.name
            )));
        }
    }

    Ok(())
}

async fn parse_json_body<T: serde::de::DeserializeOwned>(state: &mut State) -> Result<T, String> {
    let valid_body = match body::to_bytes(Body::take_from(state)).await {
        Ok(vb) => vb,
        Err(error) => {
            return Err(format!("Failed to read request body: {}", error));
        }
    };
    serde_json::from_slice::<T>(&valid_body).map_err(|error| error.to_string())
}

fn json_response(
    state: &State,
    status: StatusCode,
    payload: &impl Serialize,
) -> gotham::hyper::Response<Body> {
    create_response(
        state,
        status,
        mime::APPLICATION_JSON,
        serde_json::to_string(payload).unwrap(),
    )
}

fn json_error_response(state: &State, error: SupportConfigError) -> gotham::hyper::Response<Body> {
    json_response(
        state,
        error.status,
        &serde_json::json!({ "error": error.message }),
    )
}

fn convert_active_checkpoint_lookup_error(
    error: ActiveCheckpointLookupError,
) -> SupportConfigError {
    match error {
        ActiveCheckpointLookupError::NotFound(message) => SupportConfigError::not_found(message),
        ActiveCheckpointLookupError::Internal(message) => SupportConfigError::internal(message),
    }
}

fn service_error(error: ServiceApiError) -> SupportConfigError {
    SupportConfigError::internal(error.to_string())
}
