use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{Body, body};
use gotham::mime;
use gotham::state::{FromState, State};
use hmac::{Hmac, Mac};
use http::{HeaderMap, StatusCode};
use idgenerator::IdInstance;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::elastic_search_http_types::NamePathExtractor;
use futures_util::future::FutureExt;
use powdrr_query_lib::data_access::{self, execute_sql_async, load_files_as_table};
use powdrr_query_lib::data_contract::{
    CreateTable, DynamoDbGlobalSecondaryIndexConfig, DynamoDbLocalSecondaryIndexConfig,
    DynamoDbTableConfig, FileDescriptor, FileSetPayload, IcebergMetadata, ServingPattern,
    ServingTableConfig, TableDescription, TableMetadataCheckpoint,
};
use powdrr_query_lib::lakehouse_serving::{
    ServingQueryError, ServingQueryResponse, execute_serving_query,
};
use powdrr_query_lib::peers::CheckpointDescriptor;
use powdrr_query_lib::schema_massager::{PowdrrDataType, PowdrrSchema, extract_powdrr_schema};
use powdrr_query_lib::search_runtime::batches_to_serde_value;
use powdrr_query_lib::serving_plan::{
    ServingPredicate, ServingQueryClassification, ServingRequestPlan, ServingSort,
};
use powdrr_query_lib::state_provider::{STATE_PROVIDER, ServiceApiError};

const DYNAMODB_TARGET_PREFIX: &str = "DynamoDB_20120810.";
const DYNAMODB_CONFIG_PATTERN_PREFIX: &str = "_dynamodb_";
const DEFAULT_LIST_TABLES_LIMIT: usize = 100;
const DEFAULT_QUERY_LIMIT: usize = 100;
const DYNAMODB_BINARY_MARKER: &str = "$binary";
const DYNAMODB_STRING_SET_MARKER: &str = "$string_set";
const DYNAMODB_NUMBER_SET_MARKER: &str = "$number_set";
const DYNAMODB_BINARY_SET_MARKER: &str = "$binary_set";
const SIGV4_ALLOWED_CLOCK_SKEW_MINUTES: i64 = 15;
const DYNAMODB_WRITE_LOCAL_DIR: &str = "powdrr-dynamodb-writes";

#[derive(Clone, Debug)]
pub(crate) struct DynamoDbRequestMeta {
    pub _access_key_id: Option<String>,
}

#[derive(Debug)]
struct DynamoDbError {
    status: StatusCode,
    type_name: &'static str,
    message: String,
}

impl DynamoDbError {
    fn new(status: StatusCode, type_name: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            type_name,
            message: message.into(),
        }
    }

    fn validation(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "ValidationException", message)
    }

    fn resource_not_found(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            message,
        )
    }

    fn auth(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "UnrecognizedClientException",
            message,
        )
    }

    fn conditional_check_failed(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "ConditionalCheckFailedException",
            message,
        )
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            message,
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DynamoDbConfigResponse {
    pub acknowledged: bool,
    pub table: String,
    pub dynamodb: DynamoDbTableConfig,
}

#[derive(Serialize)]
struct ListTablesResponse {
    #[serde(rename = "TableNames")]
    table_names: Vec<String>,
    #[serde(
        rename = "LastEvaluatedTableName",
        skip_serializing_if = "Option::is_none"
    )]
    last_evaluated_table_name: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct ListTablesRequest {
    exclusive_start_table_name: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct DescribeTableRequest {
    table_name: String,
}

#[derive(Serialize)]
struct DescribeTableResponse {
    #[serde(rename = "Table")]
    table: DynamoDbTableDescription,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct DynamoDbTableDescription {
    attribute_definitions: Vec<AttributeDefinition>,
    key_schema: Vec<KeySchemaElement>,
    table_name: String,
    table_status: &'static str,
    table_arn: String,
    billing_mode_summary: BillingModeSummary,
    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    global_secondary_indexes: Option<Vec<GlobalSecondaryIndexDescription>>,
    #[serde(
        rename = "LocalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    local_secondary_indexes: Option<Vec<LocalSecondaryIndexDescription>>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct AttributeDefinition {
    attribute_name: String,
    attribute_type: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct KeySchemaElement {
    attribute_name: String,
    key_type: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct BillingModeSummary {
    billing_mode: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GlobalSecondaryIndexDescription {
    index_name: String,
    index_status: &'static str,
    key_schema: Vec<KeySchemaElement>,
    projection: SecondaryIndexProjection,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct LocalSecondaryIndexDescription {
    index_name: String,
    key_schema: Vec<KeySchemaElement>,
    projection: SecondaryIndexProjection,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct SecondaryIndexProjection {
    projection_type: &'static str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct GetItemRequest {
    table_name: String,
    key: Map<String, Value>,
    #[serde(default)]
    projection_expression: Option<String>,
    #[serde(default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(default)]
    consistent_read: Option<bool>,
    #[serde(default)]
    return_consumed_capacity: Option<String>,
}

#[derive(Serialize)]
struct GetItemResponse {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    item: Option<Map<String, Value>>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    consumed_capacity: Option<ConsumedCapacity>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct PutItemRequest {
    table_name: String,
    item: Map<String, Value>,
    #[serde(default)]
    condition_expression: Option<String>,
    #[serde(default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(default)]
    expression_attribute_values: Option<HashMap<String, Value>>,
    #[serde(default)]
    return_values: Option<String>,
}

#[derive(Serialize)]
struct PutItemResponse {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    attributes: Option<Map<String, Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct DeleteItemRequest {
    table_name: String,
    key: Map<String, Value>,
    #[serde(default)]
    condition_expression: Option<String>,
    #[serde(default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(default)]
    expression_attribute_values: Option<HashMap<String, Value>>,
    #[serde(default)]
    return_values: Option<String>,
}

#[derive(Serialize)]
struct DeleteItemResponse {
    #[serde(rename = "Attributes", skip_serializing_if = "Option::is_none")]
    attributes: Option<Map<String, Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct BatchGetItemRequest {
    request_items: HashMap<String, KeysAndAttributes>,
    #[serde(default)]
    return_consumed_capacity: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct KeysAndAttributes {
    keys: Vec<Map<String, Value>>,
    #[serde(default)]
    projection_expression: Option<String>,
    #[serde(default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(default)]
    consistent_read: Option<bool>,
}

#[derive(Serialize)]
struct BatchGetItemResponse {
    #[serde(rename = "Responses")]
    responses: HashMap<String, Vec<Map<String, Value>>>,
    #[serde(rename = "UnprocessedKeys")]
    unprocessed_keys: Map<String, Value>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    consumed_capacity: Option<Vec<ConsumedCapacity>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct QueryRequest {
    table_name: String,
    key_condition_expression: String,
    #[serde(default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(default)]
    expression_attribute_values: Option<HashMap<String, Value>>,
    #[serde(default)]
    projection_expression: Option<String>,
    #[serde(default)]
    scan_index_forward: Option<bool>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    exclusive_start_key: Option<Map<String, Value>>,
    #[serde(default)]
    index_name: Option<String>,
    #[serde(default)]
    filter_expression: Option<String>,
    #[serde(default)]
    consistent_read: Option<bool>,
    #[serde(default)]
    select: Option<String>,
    #[serde(default)]
    return_consumed_capacity: Option<String>,
}

#[derive(Serialize)]
struct QueryResponse {
    #[serde(rename = "Items")]
    items: Vec<Map<String, Value>>,
    #[serde(rename = "Count")]
    count: usize,
    #[serde(rename = "ScannedCount")]
    scanned_count: usize,
    #[serde(rename = "LastEvaluatedKey", skip_serializing_if = "Option::is_none")]
    last_evaluated_key: Option<Map<String, Value>>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    consumed_capacity: Option<ConsumedCapacity>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct ScanRequest {
    table_name: String,
    #[serde(default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    #[serde(default)]
    expression_attribute_values: Option<HashMap<String, Value>>,
    #[serde(default)]
    projection_expression: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    exclusive_start_key: Option<Map<String, Value>>,
    #[serde(default)]
    index_name: Option<String>,
    #[serde(default)]
    filter_expression: Option<String>,
    #[serde(default)]
    consistent_read: Option<bool>,
    #[serde(default)]
    select: Option<String>,
    #[serde(default)]
    return_consumed_capacity: Option<String>,
}

#[derive(Serialize)]
struct ScanResponse {
    #[serde(rename = "Items")]
    items: Vec<Map<String, Value>>,
    #[serde(rename = "Count")]
    count: usize,
    #[serde(rename = "ScannedCount")]
    scanned_count: usize,
    #[serde(rename = "LastEvaluatedKey", skip_serializing_if = "Option::is_none")]
    last_evaluated_key: Option<Map<String, Value>>,
    #[serde(rename = "ConsumedCapacity", skip_serializing_if = "Option::is_none")]
    consumed_capacity: Option<ConsumedCapacity>,
}

struct DynamoDbTableContext {
    description: TableDescription,
    config: DynamoDbTableConfig,
    schema: PowdrrSchema,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
struct ConsumedCapacity {
    table_name: String,
    capacity_units: f64,
    read_capacity_units: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<CapacityBreakdown>,
    #[serde(
        rename = "LocalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    local_secondary_indexes: Option<HashMap<String, CapacityBreakdown>>,
    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    global_secondary_indexes: Option<HashMap<String, CapacityBreakdown>>,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
struct CapacityBreakdown {
    capacity_units: f64,
    read_capacity_units: f64,
}

#[derive(Clone, Debug)]
struct ParsedSigV4Authorization {
    access_key_id: String,
    credential_date: String,
    region: String,
    service: String,
    signed_headers: Vec<String>,
    signature: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DynamoDbKeySchemaConfig {
    partition_key: String,
    sort_key: Option<String>,
}

#[derive(Clone, Debug)]
struct ParsedFilterExpression {
    expression: Option<FilterNode>,
    filter_fields: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SelectMode {
    AllAttributes,
    AllProjectedAttributes,
    SpecificAttributes,
    Count,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReturnConsumedCapacityMode {
    None,
    Total,
    Indexes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteReturnValues {
    None,
    AllOld,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DynamoDbIndexKind {
    Table,
    LocalSecondaryIndex,
    GlobalSecondaryIndex,
}

#[derive(Clone, Debug)]
struct QueryTarget {
    key_schema: DynamoDbKeySchemaConfig,
    unique_lookup: bool,
    index_kind: DynamoDbIndexKind,
    index_name: Option<String>,
}

#[derive(Clone, Debug)]
enum FilterNode {
    Predicate(FilterPredicate),
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Not(Box<FilterNode>),
}

#[derive(Clone, Debug)]
struct FilterPredicate {
    operand: FilterOperand,
    kind: FilterPredicateKind,
}

#[derive(Clone, Debug)]
enum FilterOperand {
    Field(String),
    Size(String),
}

#[derive(Clone, Debug)]
enum FilterPredicateKind {
    Eq(Value),
    In(Vec<Value>),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
    Between { start: Value, end: Value },
    BeginsWith(String),
    Contains(Value),
    AttributeExists,
    AttributeNotExists,
    AttributeType(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FilterToken {
    Identifier(String),
    ValueToken(String),
    LParen,
    RParen,
    Comma,
    Eq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Not,
    Between,
    In,
}

pub fn get_dynamodb_config(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state);
        let result = async {
            let description = load_table_description(&path.name).await?;
            let config = description.dynamodb.clone().ok_or_else(|| {
                DynamoDbError::resource_not_found("No DynamoDB config declared for table")
            })?;
            Ok::<_, DynamoDbError>(DynamoDbConfigResponse {
                acknowledged: true,
                table: description.name,
                dynamodb: config,
            })
        }
        .await;

        match result {
            Ok(response) => {
                let response = json_response(
                    &state,
                    StatusCode::OK,
                    &serde_json::to_value(response).unwrap(),
                );
                Ok((state, response))
            }
            Err(error) => {
                let response = dynamodb_error_response(&state, error);
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn put_dynamodb_config(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let body = match parse_json_body::<DynamoDbTableConfig>(&mut state).await {
            Ok(body) => body,
            Err(error) => {
                let response = dynamodb_error_response(&state, error);
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

            let schema = load_table_schema(&path).await?;
            validate_dynamodb_config(&schema, &body)?;

            let existing_serving = existing
                .as_ref()
                .and_then(|description| description.serving.clone());
            let existing_mongodb = existing
                .as_ref()
                .and_then(|description| description.mongodb.clone());
            let request = CreateTable {
                name: path.clone(),
                tags,
                serving: Some(merge_dynamodb_serving_patterns(existing_serving, &body)),
                dynamodb: Some(body.clone()),
                mongodb: existing_mongodb,
            };

            STATE_PROVIDER
                .upsert_table_metadata(&request)
                .await
                .map_err(service_error)?;

            Ok::<_, DynamoDbError>(DynamoDbConfigResponse {
                acknowledged: true,
                table: path,
                dynamodb: body,
            })
        }
        .await;

        match result {
            Ok(response) => {
                let response = json_response(
                    &state,
                    StatusCode::OK,
                    &serde_json::to_value(response).unwrap(),
                );
                Ok((state, response))
            }
            Err(error) => {
                let response = dynamodb_error_response(&state, error);
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn dynamodb_api(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let headers = HeaderMap::borrow_from(&state).clone();
        let body_bytes = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(bytes) => bytes,
            Err(error) => {
                let response = dynamodb_error_response(
                    &state,
                    DynamoDbError::validation(format!("Failed to read request body: {}", error)),
                );
                return Ok((state, response));
            }
        };

        let result = async {
            let _meta = authenticate_request(&headers, &body_bytes).await?;
            let target = parse_target(&headers)?;
            let payload = if body_bytes.is_empty() {
                Value::Object(Map::new())
            } else {
                serde_json::from_slice::<Value>(&body_bytes).map_err(|error| {
                    DynamoDbError::validation(format!("Request body was not valid JSON: {}", error))
                })?
            };

            let response_body = match target.as_str() {
                "ListTables" => handle_list_tables(payload).await?,
                "DescribeTable" => handle_describe_table(payload).await?,
                "GetItem" => handle_get_item(payload).await?,
                "PutItem" => handle_put_item(payload).await?,
                "DeleteItem" => handle_delete_item(payload).await?,
                "BatchGetItem" => handle_batch_get_item(payload).await?,
                "Query" => handle_query(payload).await?,
                "Scan" => handle_scan(payload).await?,
                _ => {
                    return Err(DynamoDbError::validation(format!(
                        "Unsupported x-amz-target {}",
                        target
                    )));
                }
            };

            Ok::<_, DynamoDbError>(response_body)
        }
        .await;

        match result {
            Ok(value) => {
                let response = json_response(&state, StatusCode::OK, &value);
                Ok((state, response))
            }
            Err(error) => {
                let response = dynamodb_error_response(&state, error);
                Ok((state, response))
            }
        }
    }
    .boxed()
}

async fn handle_list_tables(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<ListTablesRequest>(payload).map_err(|error| {
        DynamoDbError::validation(format!("Invalid ListTables request: {}", error))
    })?;
    let mut table_names = vec![];
    for table_name in STATE_PROVIDER
        .get_all_iceberg_tables()
        .await
        .map_err(service_error)?
    {
        let Some(description) = STATE_PROVIDER
            .describe_table(&table_name)
            .await
            .map_err(service_error)?
        else {
            continue;
        };
        if description.dynamodb.is_some() {
            table_names.push(description.name);
        }
    }
    table_names.sort();

    let start_index = match request.exclusive_start_table_name.as_ref() {
        Some(start_name) => table_names
            .iter()
            .position(|name| name > start_name)
            .unwrap_or(table_names.len()),
        None => 0,
    };
    let limit = request
        .limit
        .unwrap_or(DEFAULT_LIST_TABLES_LIMIT)
        .clamp(1, 100);
    let selected = table_names
        .iter()
        .skip(start_index)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    let last_evaluated_table_name = if start_index + selected.len() < table_names.len() {
        selected.last().cloned()
    } else {
        None
    };

    Ok(serde_json::to_value(ListTablesResponse {
        table_names: selected,
        last_evaluated_table_name,
    })
    .unwrap())
}

async fn handle_describe_table(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<DescribeTableRequest>(payload).map_err(|error| {
        DynamoDbError::validation(format!("Invalid DescribeTable request: {}", error))
    })?;
    let context = load_dynamodb_table_context(&request.table_name).await?;
    validate_dynamodb_config(&context.schema, &context.config)?;

    let primary_key_schema = primary_key_schema(&context.config);
    let mut attribute_definitions = vec![];
    append_attribute_definition(
        &mut attribute_definitions,
        &context.schema,
        &primary_key_schema.partition_key,
    )?;
    if let Some(sort_key) = primary_key_schema.sort_key.as_ref() {
        append_attribute_definition(&mut attribute_definitions, &context.schema, sort_key)?;
    }
    let global_secondary_indexes = if context.config.global_secondary_indexes.is_empty() {
        None
    } else {
        Some(
            context
                .config
                .global_secondary_indexes
                .iter()
                .map(|index| {
                    append_attribute_definition(
                        &mut attribute_definitions,
                        &context.schema,
                        &index.partition_key,
                    )?;
                    if let Some(sort_key) = index.sort_key.as_ref() {
                        append_attribute_definition(
                            &mut attribute_definitions,
                            &context.schema,
                            sort_key,
                        )?;
                    }
                    Ok::<_, DynamoDbError>(GlobalSecondaryIndexDescription {
                        index_name: index.name.clone(),
                        index_status: "ACTIVE",
                        key_schema: key_schema_elements(&secondary_index_key_schema(index)),
                        projection: SecondaryIndexProjection {
                            projection_type: "ALL",
                        },
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    };
    let local_secondary_indexes = if context.config.local_secondary_indexes.is_empty() {
        None
    } else {
        Some(
            context
                .config
                .local_secondary_indexes
                .iter()
                .map(|index| {
                    append_attribute_definition(
                        &mut attribute_definitions,
                        &context.schema,
                        &index.sort_key,
                    )?;
                    Ok::<_, DynamoDbError>(LocalSecondaryIndexDescription {
                        index_name: index.name.clone(),
                        key_schema: key_schema_elements(&local_secondary_index_key_schema(
                            &context.config,
                            index,
                        )),
                        projection: SecondaryIndexProjection {
                            projection_type: "ALL",
                        },
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    };

    Ok(serde_json::to_value(DescribeTableResponse {
        table: DynamoDbTableDescription {
            attribute_definitions,
            key_schema: key_schema_elements(&primary_key_schema),
            table_name: context.description.name.clone(),
            table_status: "ACTIVE",
            table_arn: format!(
                "arn:aws:dynamodb:local:000000000000:table/{}",
                context.description.name
            ),
            billing_mode_summary: BillingModeSummary {
                billing_mode: "PAY_PER_REQUEST",
            },
            global_secondary_indexes,
            local_secondary_indexes,
        },
    })
    .unwrap())
}

async fn handle_get_item(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<GetItemRequest>(payload).map_err(|error| {
        DynamoDbError::validation(format!("Invalid GetItem request: {}", error))
    })?;
    let context = load_dynamodb_table_context(&request.table_name).await?;
    validate_consistent_read(request.consistent_read, DynamoDbIndexKind::Table)?;
    let consumed_capacity_mode =
        parse_return_consumed_capacity(request.return_consumed_capacity.as_ref())?;
    let key_schema = primary_key_schema(&context.config);
    let key = parse_key_map(&request.key, &key_schema)?;
    let projection = parse_projection_expression(
        request.projection_expression.as_ref(),
        request.expression_attribute_names.as_ref(),
    )?;
    let response = execute_fast_path_query(
        &request.table_name,
        ServingRequestPlan {
            select: projection,
            filters: key_to_predicates(&key_schema, &key),
            aggregate: None,
            order_by: vec![],
            limit: Some(1),
            allow_slow_path: false,
            explain: false,
        },
    )
    .await?;

    let item = response
        .rows
        .into_iter()
        .next()
        .map(|row| json_row_to_dynamodb_item(&row))
        .transpose()?;

    Ok(serde_json::to_value(GetItemResponse {
        item,
        consumed_capacity: consumed_capacity_for_read(
            consumed_capacity_mode,
            &request.table_name,
            DynamoDbIndexKind::Table,
            None,
            estimate_read_capacity_units(1, request.consistent_read.unwrap_or(false)),
        ),
    })
    .unwrap())
}

async fn handle_put_item(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<PutItemRequest>(payload).map_err(|error| {
        DynamoDbError::validation(format!("Invalid PutItem request: {}", error))
    })?;
    let context = load_dynamodb_table_context(&request.table_name).await?;
    let key_schema = primary_key_schema(&context.config);
    let item = dynamodb_item_to_json_row(&request.item)?;
    validate_put_item_fields(&context.schema, &item)?;
    let item_key = parse_item_key_map(&request.item, &key_schema)?;
    let return_values = parse_write_return_values(request.return_values.as_deref(), "PutItem")?;
    let condition_expression = parse_condition_expression(
        request.condition_expression.as_ref(),
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
    )?;

    let (checkpoint, rows) = load_mutable_table_rows(&request.table_name).await?;
    let (existing_item, mut retained_rows) = split_rows_by_key(rows, &key_schema, &item_key)?;
    if !condition_matches(existing_item.as_ref(), &condition_expression)? {
        return Err(DynamoDbError::conditional_check_failed(
            "The conditional request failed",
        ));
    }

    retained_rows.push(item);
    publish_mutated_checkpoint(&checkpoint, retained_rows).await?;

    Ok(serde_json::to_value(PutItemResponse {
        attributes: match return_values {
            WriteReturnValues::None => None,
            WriteReturnValues::AllOld => existing_item
                .as_ref()
                .map(json_row_to_dynamodb_item)
                .transpose()?,
        },
    })
    .unwrap())
}

async fn handle_delete_item(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<DeleteItemRequest>(payload).map_err(|error| {
        DynamoDbError::validation(format!("Invalid DeleteItem request: {}", error))
    })?;
    let context = load_dynamodb_table_context(&request.table_name).await?;
    let key_schema = primary_key_schema(&context.config);
    let key = parse_key_map(&request.key, &key_schema)?;
    let return_values = parse_write_return_values(request.return_values.as_deref(), "DeleteItem")?;
    let condition_expression = parse_condition_expression(
        request.condition_expression.as_ref(),
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
    )?;

    let (checkpoint, rows) = load_mutable_table_rows(&request.table_name).await?;
    let (existing_item, retained_rows) = split_rows_by_key(rows, &key_schema, &key)?;
    if !condition_matches(existing_item.as_ref(), &condition_expression)? {
        return Err(DynamoDbError::conditional_check_failed(
            "The conditional request failed",
        ));
    }
    if existing_item.is_some() {
        publish_mutated_checkpoint(&checkpoint, retained_rows).await?;
    }

    Ok(serde_json::to_value(DeleteItemResponse {
        attributes: match return_values {
            WriteReturnValues::None => None,
            WriteReturnValues::AllOld => existing_item
                .as_ref()
                .map(json_row_to_dynamodb_item)
                .transpose()?,
        },
    })
    .unwrap())
}

async fn handle_batch_get_item(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<BatchGetItemRequest>(payload).map_err(|error| {
        DynamoDbError::validation(format!("Invalid BatchGetItem request: {}", error))
    })?;
    let consumed_capacity_mode =
        parse_return_consumed_capacity(request.return_consumed_capacity.as_ref())?;
    let mut responses = HashMap::new();
    let mut consumed_capacity = vec![];
    for (table_name, keys_and_attributes) in request.request_items.iter() {
        let context = load_dynamodb_table_context(table_name).await?;
        validate_consistent_read(
            keys_and_attributes.consistent_read,
            DynamoDbIndexKind::Table,
        )?;
        let key_schema = primary_key_schema(&context.config);
        let projection = parse_projection_expression(
            keys_and_attributes.projection_expression.as_ref(),
            keys_and_attributes.expression_attribute_names.as_ref(),
        )?;
        let mut items = vec![];
        for key in keys_and_attributes.keys.iter() {
            let parsed_key = parse_key_map(key, &key_schema)?;
            let response = execute_fast_path_query(
                table_name,
                ServingRequestPlan {
                    select: projection.clone(),
                    filters: key_to_predicates(&key_schema, &parsed_key),
                    aggregate: None,
                    order_by: vec![],
                    limit: Some(1),
                    allow_slow_path: false,
                    explain: false,
                },
            )
            .await?;
            if let Some(row) = response.rows.into_iter().next() {
                items.push(json_row_to_dynamodb_item(&row)?);
            }
        }
        responses.insert(table_name.clone(), items);
        if let Some(entry) = consumed_capacity_for_read(
            consumed_capacity_mode,
            table_name,
            DynamoDbIndexKind::Table,
            None,
            estimate_read_capacity_units(
                keys_and_attributes.keys.len(),
                keys_and_attributes.consistent_read.unwrap_or(false),
            ),
        ) {
            consumed_capacity.push(entry);
        }
    }
    consumed_capacity.sort_by(|left, right| left.table_name.cmp(&right.table_name));

    Ok(serde_json::to_value(BatchGetItemResponse {
        responses,
        unprocessed_keys: Map::new(),
        consumed_capacity: if consumed_capacity.is_empty() {
            None
        } else {
            Some(consumed_capacity)
        },
    })
    .unwrap())
}

async fn handle_query(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<QueryRequest>(payload)
        .map_err(|error| DynamoDbError::validation(format!("Invalid Query request: {}", error)))?;
    let context = load_dynamodb_table_context(&request.table_name).await?;
    let table_key_schema = primary_key_schema(&context.config);
    let query_target = query_target(&context.config, request.index_name.as_deref())?;
    validate_consistent_read(request.consistent_read, query_target.index_kind)?;
    let consumed_capacity_mode =
        parse_return_consumed_capacity(request.return_consumed_capacity.as_ref())?;
    let requested_projection = parse_projection_expression(
        request.projection_expression.as_ref(),
        request.expression_attribute_names.as_ref(),
    )?;
    let select_mode = parse_select_mode(
        request.select.as_ref(),
        requested_projection.as_ref(),
        query_target.index_name.as_deref(),
    )?;
    let filter_expression = parse_filter_expression(
        request.filter_expression.as_ref(),
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
    )?;
    let parsed_query = parse_key_condition_expression(
        &request.key_condition_expression,
        &query_target.key_schema,
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
    )?;
    let sort_is_exact_eq = parsed_query
        .sort_filter
        .as_ref()
        .map(|filter| filter.eq.is_some())
        .unwrap_or(false);

    let ascending = request.scan_index_forward.unwrap_or(true);
    let page_limit = request.limit.unwrap_or(DEFAULT_QUERY_LIMIT).clamp(1, 1000);
    let mut key_filters = vec![ServingPredicate {
        field: query_target.key_schema.partition_key.clone(),
        eq: Some(parsed_query.partition_value),
        in_values: None,
        gt: None,
        gte: None,
        lt: None,
        lte: None,
    }];
    if let Some(sort_filter) = parsed_query.sort_filter {
        key_filters.push(sort_filter);
    }
    if let Some(exclusive_start_key) = request.exclusive_start_key.as_ref() {
        apply_exclusive_start_key(
            &mut key_filters,
            &query_target.key_schema,
            &table_key_schema,
            ascending,
            exclusive_start_key,
        )?;
    }

    let order_by = if sort_is_exact_eq {
        vec![]
    } else {
        query_target
            .key_schema
            .sort_key
            .as_ref()
            .map(|sort_key| {
                vec![ServingSort {
                    field: sort_key.clone(),
                    descending: !ascending,
                }]
            })
            .unwrap_or_default()
    };
    let effective_limit = query_fetch_limit(
        page_limit,
        sort_is_exact_eq,
        query_target.key_schema.sort_key.is_none(),
        query_target.unique_lookup,
    );
    let query_projection = query_select_fields(
        query_requested_projection(requested_projection.as_ref(), select_mode),
        &filter_expression,
        &query_target.key_schema,
        &context.schema,
    );

    let response = execute_fast_path_query(
        &request.table_name,
        ServingRequestPlan {
            select: query_projection,
            filters: key_filters,
            aggregate: None,
            order_by,
            limit: Some(effective_limit),
            allow_slow_path: false,
            explain: false,
        },
    )
    .await?;

    let mut evaluated_rows = response.rows;
    let last_evaluated_key = if evaluated_rows.len() > page_limit {
        let key = row_to_key(
            &evaluated_rows[page_limit - 1],
            &query_target.key_schema,
            &table_key_schema,
        )?;
        evaluated_rows.truncate(page_limit);
        Some(key)
    } else if evaluated_rows.len() == page_limit && !sort_is_exact_eq && !query_target.unique_lookup
    {
        Some(row_to_key(
            evaluated_rows
                .last()
                .ok_or_else(|| DynamoDbError::internal("Expected a row at page boundary"))?,
            &query_target.key_schema,
            &table_key_schema,
        )?)
    } else {
        None
    };
    let scanned_count = evaluated_rows.len();
    let filtered_rows = apply_filter_expression(evaluated_rows, &filter_expression)?;
    let count = filtered_rows.len();
    let projected_rows = project_rows(
        filtered_rows,
        query_requested_projection(requested_projection.as_ref(), select_mode),
    )?;
    let rows = if select_mode == SelectMode::Count {
        vec![]
    } else {
        projected_rows
            .into_iter()
            .map(|row| json_row_to_dynamodb_item(&row))
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(serde_json::to_value(QueryResponse {
        items: rows,
        count,
        scanned_count,
        last_evaluated_key,
        consumed_capacity: consumed_capacity_for_read(
            consumed_capacity_mode,
            &request.table_name,
            query_target.index_kind,
            query_target.index_name.as_deref(),
            estimate_read_capacity_units(scanned_count, request.consistent_read.unwrap_or(false)),
        ),
    })
    .unwrap())
}

async fn handle_scan(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<ScanRequest>(payload)
        .map_err(|error| DynamoDbError::validation(format!("Invalid Scan request: {}", error)))?;
    let context = load_dynamodb_table_context(&request.table_name).await?;
    let table_key_schema = primary_key_schema(&context.config);
    let query_target = query_target(&context.config, request.index_name.as_deref())?;
    validate_consistent_read(request.consistent_read, query_target.index_kind)?;
    let consumed_capacity_mode =
        parse_return_consumed_capacity(request.return_consumed_capacity.as_ref())?;
    let requested_projection = parse_projection_expression(
        request.projection_expression.as_ref(),
        request.expression_attribute_names.as_ref(),
    )?;
    let select_mode = parse_select_mode(
        request.select.as_ref(),
        requested_projection.as_ref(),
        query_target.index_name.as_deref(),
    )?;
    let filter_expression = parse_filter_expression(
        request.filter_expression.as_ref(),
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
    )?;
    let order_fields = ordered_key_fields(&query_target.key_schema, &table_key_schema);
    let effective_limit = request.limit.unwrap_or(DEFAULT_QUERY_LIMIT).clamp(1, 1000);
    let files = load_table_files(&request.table_name).await?;
    let scan_projection = scan_select_fields(
        query_requested_projection(requested_projection.as_ref(), select_mode),
        &filter_expression,
        &order_fields,
        &context.schema,
    );
    let exclusive_start_key = request
        .exclusive_start_key
        .as_ref()
        .map(|key| parse_exclusive_start_key_map(key, &query_target.key_schema, &table_key_schema))
        .transpose()?;
    let evaluated_rows = execute_scan_query(
        &files,
        &scan_projection,
        exclusive_start_key.as_ref(),
        &order_fields,
        effective_limit.saturating_add(1),
    )
    .await?;
    let mut evaluated_rows = evaluated_rows;
    let last_evaluated_key = if evaluated_rows.len() > effective_limit {
        let key = row_to_key(
            &evaluated_rows[effective_limit - 1],
            &query_target.key_schema,
            &table_key_schema,
        )?;
        evaluated_rows.truncate(effective_limit);
        Some(key)
    } else {
        None
    };
    let scanned_count = evaluated_rows.len();
    let filtered_rows = apply_filter_expression(evaluated_rows, &filter_expression)?;
    let count = filtered_rows.len();
    let projected_rows = project_rows(
        filtered_rows,
        query_requested_projection(requested_projection.as_ref(), select_mode),
    )?;
    let items = if select_mode == SelectMode::Count {
        vec![]
    } else {
        projected_rows
            .into_iter()
            .map(|row| json_row_to_dynamodb_item(&row))
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(serde_json::to_value(ScanResponse {
        items,
        count,
        scanned_count,
        last_evaluated_key,
        consumed_capacity: consumed_capacity_for_read(
            consumed_capacity_mode,
            &request.table_name,
            query_target.index_kind,
            query_target.index_name.as_deref(),
            estimate_read_capacity_units(scanned_count, request.consistent_read.unwrap_or(false)),
        ),
    })
    .unwrap())
}

fn service_error(error: ServiceApiError) -> DynamoDbError {
    DynamoDbError::internal(error.to_string())
}

fn validate_consistent_read(
    consistent_read: Option<bool>,
    index_kind: DynamoDbIndexKind,
) -> Result<(), DynamoDbError> {
    if consistent_read.unwrap_or(false) && index_kind == DynamoDbIndexKind::GlobalSecondaryIndex {
        return Err(DynamoDbError::validation(
            "Consistent reads are not supported on global secondary indexes",
        ));
    }
    Ok(())
}

async fn execute_fast_path_query(
    table_name: &str,
    request: ServingRequestPlan,
) -> Result<ServingQueryResponse, DynamoDbError> {
    let response = execute_serving_query(table_name, request)
        .await
        .map_err(convert_serving_error)?;
    if response.classification != ServingQueryClassification::FastPath {
        return Err(DynamoDbError::validation(response.reason.unwrap_or_else(
            || "Query did not qualify for the serving fast path".to_string(),
        )));
    }
    Ok(response)
}

fn convert_serving_error(error: ServingQueryError) -> DynamoDbError {
    let type_name = match error.status {
        StatusCode::NOT_FOUND => "ResourceNotFoundException",
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => "ValidationException",
        _ => "InternalServerError",
    };
    DynamoDbError::new(error.status, type_name, error.message)
}

async fn load_dynamodb_table_context(
    table_name: &str,
) -> Result<DynamoDbTableContext, DynamoDbError> {
    let description = load_table_description(table_name).await?;
    let config = description
        .dynamodb
        .clone()
        .ok_or_else(|| DynamoDbError::resource_not_found("Table is not exposed as DynamoDB"))?;
    let schema = load_table_schema(table_name).await?;
    Ok(DynamoDbTableContext {
        description,
        config,
        schema,
    })
}

async fn load_table_description(table_name: &str) -> Result<TableDescription, DynamoDbError> {
    STATE_PROVIDER
        .describe_table(&table_name.to_string())
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            DynamoDbError::resource_not_found(format!("Table {} was not found", table_name))
        })
}

async fn load_table_schema(table_name: &str) -> Result<PowdrrSchema, DynamoDbError> {
    let checkpoint = load_active_checkpoint(table_name).await?;
    schema_from_checkpoint(&checkpoint)
}

async fn load_latest_base_checkpoint(
    table_name: &str,
) -> Result<TableMetadataCheckpoint, DynamoDbError> {
    let checkpoint_id = STATE_PROVIDER
        .get_latest_checkpoint(&table_name.to_string(), None)
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            DynamoDbError::resource_not_found(format!(
                "No checkpoint was available for table {}",
                table_name
            ))
        })?;
    STATE_PROVIDER
        .get_checkpoint(CheckpointDescriptor::new(
            table_name.to_string(),
            checkpoint_id,
        ))
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            DynamoDbError::resource_not_found(format!(
                "Checkpoint metadata was not found for table {}",
                table_name
            ))
        })
}

async fn load_active_checkpoint(
    table_name: &str,
) -> Result<TableMetadataCheckpoint, DynamoDbError> {
    let checkpoint_id = STATE_PROVIDER
        .get_published_active_servable_checkpoint(&table_name.to_string())
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            DynamoDbError::resource_not_found(format!(
                "No checkpoint was available for table {}",
                table_name
            ))
        })?;
    STATE_PROVIDER
        .get_checkpoint(CheckpointDescriptor::new(
            table_name.to_string(),
            checkpoint_id,
        ))
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            DynamoDbError::resource_not_found(format!(
                "Checkpoint metadata was not found for table {}",
                table_name
            ))
        })
}

fn checkpoint_file_descriptors(
    checkpoint: &TableMetadataCheckpoint,
    operation: &str,
) -> Result<Vec<FileDescriptor>, DynamoDbError> {
    let iceberg_metadata = checkpoint.iceberg_metadata.as_ref().ok_or_else(|| {
        DynamoDbError::validation(format!(
            "DynamoDB {} currently requires Iceberg-backed storage",
            operation
        ))
    })?;
    if checkpoint
        .deletes_metadata
        .as_ref()
        .map(|metadata| !metadata.files.is_empty())
        .unwrap_or(false)
    {
        return Err(DynamoDbError::validation(format!(
            "Delete-aware DynamoDB {} is not implemented yet",
            operation
        )));
    }
    Ok(iceberg_metadata.files.as_file_tuples())
}

async fn load_table_files(table_name: &str) -> Result<Vec<FileDescriptor>, DynamoDbError> {
    let checkpoint = load_active_checkpoint(table_name).await?;
    checkpoint_file_descriptors(&checkpoint, "reads")
}

fn schema_from_checkpoint(
    checkpoint: &TableMetadataCheckpoint,
) -> Result<PowdrrSchema, DynamoDbError> {
    if let Some(metadata) = checkpoint.iceberg_metadata.as_ref() {
        return Ok(metadata.table_schema.clone());
    }
    if !checkpoint.schema.fields().is_empty() {
        return Ok(checkpoint.schema.clone());
    }
    Err(DynamoDbError::internal(
        "Checkpoint did not contain a usable schema",
    ))
}

async fn load_mutable_table_rows(
    table_name: &str,
) -> Result<(TableMetadataCheckpoint, Vec<Value>), DynamoDbError> {
    let checkpoint = load_latest_base_checkpoint(table_name).await?;
    let files = checkpoint_file_descriptors(&checkpoint, "writes")?;
    let mut rows = vec![];
    for file_group in group_files_by_schema(&files).into_iter() {
        let mut group_rows = execute_scan_file_group(file_group, "SELECT * FROM {table}").await?;
        rows.append(&mut group_rows);
    }
    Ok((checkpoint, rows))
}

async fn publish_mutated_checkpoint(
    previous_checkpoint: &TableMetadataCheckpoint,
    mut rows: Vec<Value>,
) -> Result<(), DynamoDbError> {
    let fallback_schema = schema_from_checkpoint(previous_checkpoint)?;
    let previous_iceberg_metadata =
        previous_checkpoint
            .iceberg_metadata
            .as_ref()
            .ok_or_else(|| {
                DynamoDbError::validation(
                    "DynamoDB writes currently require Iceberg-backed storage",
                )
            })?;
    let schema = merged_row_schema(&rows, &fallback_schema);
    coerce_rows_to_schema(&mut rows, &schema)?;
    let checkpoint_id = IdInstance::next_id().to_string();
    let output_path = mutation_output_file_path(previous_checkpoint, &checkpoint_id)?;
    let file_size = write_rows_to_parquet(&output_path, &rows, &schema).await?;
    let checkpoint = TableMetadataCheckpoint {
        table_name: previous_checkpoint.table_name.clone(),
        original_checkpoint_id: Some(previous_checkpoint.checkpoint_id.clone()),
        checkpoint_id: checkpoint_id.clone(),
        iceberg_metadata: Some(IcebergMetadata {
            table_schema: schema.clone(),
            snapshot_id: Some(checkpoint_id),
            files: FileSetPayload {
                file_paths: vec![output_path],
                schemas: vec![schema.clone()],
                file_schemas: vec![0],
                sizes: vec![file_size],
            },
            partition_spec: previous_iceberg_metadata.partition_spec.clone(),
            sort_order: previous_iceberg_metadata.sort_order.clone(),
            column_names: schema
                .fields()
                .iter()
                .map(|field| field.name.clone())
                .collect(),
            column_stats: vec![],
            access_artifacts: previous_iceberg_metadata.access_artifacts.clone(),
            file_stats: vec![],
        }),
        speedboat_metadata: None,
        deletes_metadata: None,
        extension_metadata: HashMap::new(),
        schema,
    };
    STATE_PROVIDER.add_checkpoint(&checkpoint).await;
    Ok(())
}

fn merged_row_schema(rows: &[Value], fallback_schema: &PowdrrSchema) -> PowdrrSchema {
    if rows.is_empty() {
        return fallback_schema.clone();
    }
    PowdrrSchema::merge_all(
        rows.iter()
            .map(extract_powdrr_schema)
            .collect::<Vec<PowdrrSchema>>(),
    )
}

fn coerce_rows_to_schema(rows: &mut [Value], schema: &PowdrrSchema) -> Result<(), DynamoDbError> {
    for row in rows.iter_mut() {
        if !row.is_object() {
            return Err(DynamoDbError::internal(
                "DynamoDB mutation rows must be JSON objects",
            ));
        }
        schema.coerce_value(row);
    }
    Ok(())
}

fn mutation_output_file_path(
    checkpoint: &TableMetadataCheckpoint,
    checkpoint_id: &str,
) -> Result<String, DynamoDbError> {
    let sanitized_table = sanitize_path_component(&checkpoint.table_name);
    let file_name = format!("{}.parquet", checkpoint_id);
    match checkpoint
        .iceberg_metadata
        .as_ref()
        .and_then(|metadata| metadata.files.file_paths.first())
    {
        Some(path) if path.starts_with("s3://") => Ok(format!(
            "{}/dynamodb-write/{}/{}",
            data_access::s3_ingest_base_path(),
            sanitized_table,
            file_name
        )),
        Some(path) if path.contains("://") && !path.starts_with("file://") => {
            Err(DynamoDbError::validation(format!(
                "DynamoDB writes do not support storage path {}",
                path
            )))
        }
        _ => {
            let directory = std::env::temp_dir()
                .join(DYNAMODB_WRITE_LOCAL_DIR)
                .join(sanitized_table);
            fs::create_dir_all(&directory).map_err(|error| {
                DynamoDbError::internal(format!(
                    "Failed to create local DynamoDB write directory: {}",
                    error
                ))
            })?;
            Ok(format!("file://{}", directory.join(file_name).display()))
        }
    }
}

fn sanitize_path_component(name: &str) -> String {
    name.chars()
        .map(|character| match character {
            '/' | '\\' | ':' | ' ' => '_',
            _ => character,
        })
        .collect()
}

async fn write_rows_to_parquet(
    file_path: &str,
    rows: &[Value],
    schema: &PowdrrSchema,
) -> Result<u64, DynamoDbError> {
    let record_batch = record_batch_from_rows(rows, schema)?;
    if file_path.starts_with("s3://") {
        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)
                .map_err(|error| DynamoDbError::internal(error.to_string()))?;
            writer
                .write(&record_batch)
                .map_err(|error| DynamoDbError::internal(error.to_string()))?;
            writer
                .close()
                .map_err(|error| DynamoDbError::internal(error.to_string()))?;
        }
        let bytes = buffer.into_inner();
        data_access::put_s3_file(&file_path.to_string(), &bytes)
            .await
            .map_err(|error| DynamoDbError::internal(error.to_string()))?;
        return Ok(bytes.len() as u64);
    }

    let local_path = file_path.strip_prefix("file://").unwrap_or(file_path);
    if let Some(parent) = std::path::Path::new(local_path).parent() {
        fs::create_dir_all(parent).map_err(|error| {
            DynamoDbError::internal(format!(
                "Failed to create local DynamoDB write directory: {}",
                error
            ))
        })?;
    }
    let file = File::create(local_path).map_err(|error| {
        DynamoDbError::internal(format!("Failed to create local parquet file: {}", error))
    })?;
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), None)
        .map_err(|error| DynamoDbError::internal(error.to_string()))?;
    writer
        .write(&record_batch)
        .map_err(|error| DynamoDbError::internal(error.to_string()))?;
    writer
        .close()
        .map_err(|error| DynamoDbError::internal(error.to_string()))?;
    fs::metadata(local_path)
        .map(|metadata| metadata.len())
        .map_err(|error| {
            DynamoDbError::internal(format!(
                "Failed to stat local parquet file {}: {}",
                local_path, error
            ))
        })
}

fn record_batch_from_rows(
    rows: &[Value],
    schema: &PowdrrSchema,
) -> Result<RecordBatch, DynamoDbError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.to_arrow_schema())));
    }
    let arrow_schema = schema.to_arrow_schema();
    let fields = arrow_schema.fields.as_ref();
    let row_values = rows.to_vec();
    serde_arrow::to_record_batch(fields, &row_values).map_err(|error| {
        DynamoDbError::internal(format!(
            "Failed to convert mutated DynamoDB rows to Arrow: {}",
            error
        ))
    })
}

fn parse_condition_expression(
    expression: Option<&String>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, Value>>,
) -> Result<ParsedFilterExpression, DynamoDbError> {
    let empty_values = HashMap::new();
    parse_filter_expression(expression, names, Some(values.unwrap_or(&empty_values)))
}

fn condition_matches(
    item: Option<&Value>,
    condition_expression: &ParsedFilterExpression,
) -> Result<bool, DynamoDbError> {
    let Some(expression) = condition_expression.expression.as_ref() else {
        return Ok(true);
    };
    let empty = Map::new();
    let object = match item {
        Some(item) => item.as_object().ok_or_else(|| {
            DynamoDbError::internal("DynamoDB condition target was not an object")
        })?,
        None => &empty,
    };
    evaluate_filter_node(object, expression)
}

fn parse_item_key_map(
    item: &Map<String, Value>,
    key_schema: &DynamoDbKeySchemaConfig,
) -> Result<HashMap<String, Value>, DynamoDbError> {
    let mut key = Map::new();
    copy_key_attr(item, &mut key, &key_schema.partition_key)?;
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        copy_key_attr(item, &mut key, sort_key)?;
    }
    parse_key_map(&key, key_schema)
}

fn copy_key_attr(
    item: &Map<String, Value>,
    target: &mut Map<String, Value>,
    field_name: &str,
) -> Result<(), DynamoDbError> {
    target.insert(
        field_name.to_string(),
        item.get(field_name).cloned().ok_or_else(|| {
            DynamoDbError::validation(format!("Item was missing key field {}", field_name))
        })?,
    );
    Ok(())
}

fn split_rows_by_key(
    rows: Vec<Value>,
    key_schema: &DynamoDbKeySchemaConfig,
    key: &HashMap<String, Value>,
) -> Result<(Option<Value>, Vec<Value>), DynamoDbError> {
    let mut existing = None;
    let mut retained = Vec::with_capacity(rows.len());
    for row in rows {
        if row_matches_key(&row, key_schema, key)? {
            if existing.is_none() {
                existing = Some(row.clone());
            }
        } else {
            retained.push(row);
        }
    }
    Ok((existing, retained))
}

fn row_matches_key(
    row: &Value,
    key_schema: &DynamoDbKeySchemaConfig,
    key: &HashMap<String, Value>,
) -> Result<bool, DynamoDbError> {
    let object = row
        .as_object()
        .ok_or_else(|| DynamoDbError::internal("DynamoDB mutation row was not an object"))?;
    if object.get(&key_schema.partition_key) != key.get(&key_schema.partition_key) {
        return Ok(false);
    }
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        if object.get(sort_key) != key.get(sort_key) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn dynamodb_item_to_json_row(item: &Map<String, Value>) -> Result<Value, DynamoDbError> {
    let mut converted = Map::new();
    for (key, value) in item.iter() {
        converted.insert(key.clone(), dynamodb_attr_to_json(value)?);
    }
    Ok(Value::Object(converted))
}

fn validate_put_item_fields(schema: &PowdrrSchema, item: &Value) -> Result<(), DynamoDbError> {
    let Value::Object(object) = item else {
        return Err(DynamoDbError::validation(
            "PutItem payload must encode to a top-level object",
        ));
    };
    let known_fields = schema
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect::<HashSet<_>>();
    let unknown_fields = object
        .keys()
        .filter(|field| !known_fields.contains(*field))
        .cloned()
        .collect::<Vec<_>>();
    if unknown_fields.is_empty() {
        return Ok(());
    }
    Err(DynamoDbError::validation(format!(
        "PutItem currently supports only existing table schema attributes; unknown fields: {}",
        unknown_fields.join(", ")
    )))
}

fn validate_dynamodb_config(
    schema: &PowdrrSchema,
    config: &DynamoDbTableConfig,
) -> Result<(), DynamoDbError> {
    validate_key_schema(schema, &primary_key_schema(config), "table")?;
    let mut seen_index_names = std::collections::HashSet::new();
    for index in config.local_secondary_indexes.iter() {
        if !seen_index_names.insert(index.name.clone()) {
            return Err(DynamoDbError::validation(format!(
                "Duplicate local secondary index name {}",
                index.name
            )));
        }
        validate_local_secondary_index(schema, config, index)?;
    }
    for index in config.global_secondary_indexes.iter() {
        if !seen_index_names.insert(index.name.clone()) {
            return Err(DynamoDbError::validation(format!(
                "Duplicate global secondary index name {}",
                index.name
            )));
        }
        validate_key_schema(
            schema,
            &secondary_index_key_schema(index),
            &format!("global secondary index {}", index.name),
        )?;
    }
    Ok(())
}

fn primary_key_schema(config: &DynamoDbTableConfig) -> DynamoDbKeySchemaConfig {
    DynamoDbKeySchemaConfig {
        partition_key: config.partition_key.clone(),
        sort_key: config.sort_key.clone(),
    }
}

fn secondary_index_key_schema(
    index: &DynamoDbGlobalSecondaryIndexConfig,
) -> DynamoDbKeySchemaConfig {
    DynamoDbKeySchemaConfig {
        partition_key: index.partition_key.clone(),
        sort_key: index.sort_key.clone(),
    }
}

fn local_secondary_index_key_schema(
    config: &DynamoDbTableConfig,
    index: &DynamoDbLocalSecondaryIndexConfig,
) -> DynamoDbKeySchemaConfig {
    DynamoDbKeySchemaConfig {
        partition_key: config.partition_key.clone(),
        sort_key: Some(index.sort_key.clone()),
    }
}

fn query_target(
    config: &DynamoDbTableConfig,
    index_name: Option<&str>,
) -> Result<QueryTarget, DynamoDbError> {
    match index_name {
        Some(index_name) => {
            if let Some(index) = config
                .local_secondary_indexes
                .iter()
                .find(|index| index.name == index_name)
            {
                return Ok(QueryTarget {
                    key_schema: local_secondary_index_key_schema(config, index),
                    unique_lookup: false,
                    index_kind: DynamoDbIndexKind::LocalSecondaryIndex,
                    index_name: Some(index.name.clone()),
                });
            }
            config
                .global_secondary_indexes
                .iter()
                .find(|index| index.name == index_name)
                .map(|index| QueryTarget {
                    key_schema: secondary_index_key_schema(index),
                    unique_lookup: false,
                    index_kind: DynamoDbIndexKind::GlobalSecondaryIndex,
                    index_name: Some(index.name.clone()),
                })
                .ok_or_else(|| {
                    DynamoDbError::validation(format!(
                        "Unknown local or global secondary index {}",
                        index_name
                    ))
                })
        }
        None => Ok(QueryTarget {
            key_schema: primary_key_schema(config),
            unique_lookup: true,
            index_kind: DynamoDbIndexKind::Table,
            index_name: None,
        }),
    }
}

fn validate_local_secondary_index(
    schema: &PowdrrSchema,
    config: &DynamoDbTableConfig,
    index: &DynamoDbLocalSecondaryIndexConfig,
) -> Result<(), DynamoDbError> {
    if config.sort_key.is_none() {
        return Err(DynamoDbError::validation(format!(
            "Local secondary index {} requires the table to declare a sort key",
            index.name
        )));
    }
    if config.sort_key.as_deref() == Some(index.sort_key.as_str()) {
        return Err(DynamoDbError::validation(format!(
            "Local secondary index {} sort_key must differ from the table sort_key",
            index.name
        )));
    }
    validate_key_schema(
        schema,
        &local_secondary_index_key_schema(config, index),
        &format!("local secondary index {}", index.name),
    )
}

fn validate_key_schema(
    schema: &PowdrrSchema,
    key_schema: &DynamoDbKeySchemaConfig,
    label: &str,
) -> Result<(), DynamoDbError> {
    dynamodb_key_type(schema, &key_schema.partition_key)?;
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        if sort_key == &key_schema.partition_key {
            return Err(DynamoDbError::validation(format!(
                "{} sort_key must differ from partition_key",
                label
            )));
        }
        dynamodb_key_type(schema, sort_key)?;
    }
    Ok(())
}

fn append_attribute_definition(
    definitions: &mut Vec<AttributeDefinition>,
    schema: &PowdrrSchema,
    field_name: &str,
) -> Result<(), DynamoDbError> {
    if definitions
        .iter()
        .any(|definition| definition.attribute_name == field_name)
    {
        return Ok(());
    }
    definitions.push(AttributeDefinition {
        attribute_name: field_name.to_string(),
        attribute_type: dynamodb_key_type(schema, field_name)?,
    });
    Ok(())
}

fn key_schema_elements(key_schema: &DynamoDbKeySchemaConfig) -> Vec<KeySchemaElement> {
    let mut elements = vec![KeySchemaElement {
        attribute_name: key_schema.partition_key.clone(),
        key_type: "HASH",
    }];
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        elements.push(KeySchemaElement {
            attribute_name: sort_key.clone(),
            key_type: "RANGE",
        });
    }
    elements
}

fn dynamodb_key_type(
    schema: &PowdrrSchema,
    field_name: &str,
) -> Result<&'static str, DynamoDbError> {
    let schema_map = schema.to_map();
    let field = schema_map
        .get(field_name)
        .ok_or_else(|| DynamoDbError::validation(format!("Unknown key field {}", field_name)))?;
    match field.data_type {
        PowdrrDataType::String => Ok("S"),
        PowdrrDataType::Integer | PowdrrDataType::Float => Ok("N"),
        _ => Err(DynamoDbError::validation(format!(
            "Field {} is not a valid DynamoDB key type",
            field_name
        ))),
    }
}

fn merge_dynamodb_serving_patterns(
    existing: Option<ServingTableConfig>,
    config: &DynamoDbTableConfig,
) -> ServingTableConfig {
    let mut patterns = existing.unwrap_or_default();
    patterns
        .patterns
        .retain(|pattern| !pattern.name.starts_with(DYNAMODB_CONFIG_PATTERN_PREFIX));
    patterns
        .patterns
        .extend(derived_dynamodb_serving_patterns(config));
    patterns
}

fn derived_serving_patterns_for_key_schema(
    prefix: &str,
    key_schema: &DynamoDbKeySchemaConfig,
    include_get_item_pattern: bool,
    include_exact_query_pattern: bool,
) -> Vec<ServingPattern> {
    let mut patterns = vec![];
    if include_get_item_pattern {
        patterns.push(ServingPattern {
            name: format!("{}get_item", prefix),
            eq_fields: match key_schema.sort_key.as_ref() {
                Some(sort_key) => vec![key_schema.partition_key.clone(), sort_key.clone()],
                None => vec![key_schema.partition_key.clone()],
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
        if let Some(sort_key) = key_schema.sort_key.as_ref() {
            patterns.push(ServingPattern {
                name: format!("{}exact_query", prefix),
                eq_fields: vec![key_schema.partition_key.clone(), sort_key.clone()],
                range_field: None,
                order_field: None,
                descending: false,
                max_limit: None,
                projection: None,
                aggregate: None,
            });
        }
    }
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        for descending in [false, true] {
            patterns.push(ServingPattern {
                name: format!(
                    "{}partition_query_{}",
                    prefix,
                    if descending { "desc" } else { "asc" }
                ),
                eq_fields: vec![key_schema.partition_key.clone()],
                range_field: None,
                order_field: Some(sort_key.clone()),
                descending,
                max_limit: None,
                projection: None,
                aggregate: None,
            });
            patterns.push(ServingPattern {
                name: format!(
                    "{}range_query_{}",
                    prefix,
                    if descending { "desc" } else { "asc" }
                ),
                eq_fields: vec![key_schema.partition_key.clone()],
                range_field: Some(sort_key.clone()),
                order_field: Some(sort_key.clone()),
                descending,
                max_limit: None,
                projection: None,
                aggregate: None,
            });
        }
    }
    patterns
}

fn sanitize_serving_pattern_suffix(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn derived_dynamodb_serving_patterns(config: &DynamoDbTableConfig) -> Vec<ServingPattern> {
    let mut patterns = derived_serving_patterns_for_key_schema(
        DYNAMODB_CONFIG_PATTERN_PREFIX,
        &primary_key_schema(config),
        true,
        false,
    );
    for index in config.local_secondary_indexes.iter() {
        let prefix = format!(
            "{}lsi_{}_",
            DYNAMODB_CONFIG_PATTERN_PREFIX,
            sanitize_serving_pattern_suffix(&index.name)
        );
        patterns.extend(derived_serving_patterns_for_key_schema(
            &prefix,
            &local_secondary_index_key_schema(config, index),
            false,
            true,
        ));
    }
    for index in config.global_secondary_indexes.iter() {
        let prefix = format!(
            "{}gsi_{}_",
            DYNAMODB_CONFIG_PATTERN_PREFIX,
            sanitize_serving_pattern_suffix(&index.name)
        );
        patterns.extend(derived_serving_patterns_for_key_schema(
            &prefix,
            &secondary_index_key_schema(index),
            false,
            true,
        ));
    }

    patterns
}

fn parse_target(headers: &HeaderMap) -> Result<String, DynamoDbError> {
    let target = headers
        .get("x-amz-target")
        .ok_or_else(|| DynamoDbError::validation("Missing x-amz-target header"))?
        .to_str()
        .map_err(|_| DynamoDbError::validation("x-amz-target header was not valid ASCII"))?;
    target
        .strip_prefix(DYNAMODB_TARGET_PREFIX)
        .map(|value| value.to_string())
        .ok_or_else(|| DynamoDbError::validation("Unsupported x-amz-target prefix"))
}

async fn authenticate_request(
    headers: &HeaderMap,
    body_bytes: &[u8],
) -> Result<DynamoDbRequestMeta, DynamoDbError> {
    let Some(auth_header) = headers.get(http::header::AUTHORIZATION) else {
        return Err(DynamoDbError::auth("Missing Authorization header"));
    };
    let auth = auth_header
        .to_str()
        .map_err(|_| DynamoDbError::auth("Authorization header was not valid ASCII"))?;
    let parsed = parse_sigv4_authorization(auth)?;
    if parsed.service != "dynamodb" {
        return Err(DynamoDbError::auth(format!(
            "SigV4 service must be dynamodb, got {}",
            parsed.service
        )));
    }
    let secret_access_key = STATE_PROVIDER
        .lookup_secret_access_key(&parsed.access_key_id)
        .await
        .map_err(|error| {
            #[cfg(test)]
            {
                if parsed.access_key_id == "test" {
                    return DynamoDbError::auth("__powdrr_test_fallback__");
                }
            }
            service_error(error)
        })?
        .ok_or_else(|| {
            #[cfg(test)]
            {
                if parsed.access_key_id == "test" {
                    return DynamoDbError::auth("__powdrr_test_fallback__");
                }
            }
            DynamoDbError::auth(format!("Unknown access key {}", parsed.access_key_id))
        })
        .or_else(|error| {
            #[cfg(test)]
            {
                if error.message == "__powdrr_test_fallback__" {
                    return Ok("test".to_string());
                }
            }
            Err(error)
        })?;
    let amz_date = require_header_ascii(headers, "x-amz-date")?;
    validate_sigv4_request_time(&amz_date, &parsed.credential_date)?;
    let payload_hash = sha256_hex(body_bytes);
    if let Some(content_sha256) = optional_header_ascii(headers, "x-amz-content-sha256")? {
        if content_sha256 != payload_hash {
            return Err(DynamoDbError::auth(
                "x-amz-content-sha256 did not match the request body",
            ));
        }
    }
    let signed_headers = parsed.signed_headers.join(";");
    let canonical_headers = canonical_headers(headers, &parsed.signed_headers)?;
    let canonical_request = format!(
        "POST\n/\n\n{}\n{}\n{}",
        canonical_headers, signed_headers, payload_hash
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}/{}/{}/aws4_request\n{}",
        amz_date,
        parsed.credential_date,
        parsed.region,
        parsed.service,
        sha256_hex(canonical_request.as_bytes()),
    );
    let expected_signature = sigv4_signature(
        &secret_access_key,
        &parsed.credential_date,
        &parsed.region,
        &parsed.service,
        &string_to_sign,
    )?;
    if expected_signature != parsed.signature {
        return Err(DynamoDbError::auth("Signature did not match"));
    }
    Ok(DynamoDbRequestMeta {
        _access_key_id: Some(parsed.access_key_id),
    })
}

fn parse_sigv4_authorization(auth: &str) -> Result<ParsedSigV4Authorization, DynamoDbError> {
    let prefix = "AWS4-HMAC-SHA256 ";
    let remainder = auth
        .strip_prefix(prefix)
        .ok_or_else(|| DynamoDbError::auth("Unsupported Authorization algorithm"))?;
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for segment in remainder.split(',') {
        let (key, value) = segment
            .trim()
            .split_once('=')
            .ok_or_else(|| DynamoDbError::auth("Authorization header was malformed"))?;
        match key {
            "Credential" => credential = Some(value.to_string()),
            "SignedHeaders" => signed_headers = Some(value.to_string()),
            "Signature" => signature = Some(value.to_string()),
            _ => {}
        }
    }
    let credential = credential
        .ok_or_else(|| DynamoDbError::auth("Authorization header did not contain a Credential"))?;
    let signed_headers = signed_headers
        .ok_or_else(|| DynamoDbError::auth("Authorization header did not contain SignedHeaders"))?;
    let signature = signature
        .ok_or_else(|| DynamoDbError::auth("Authorization header did not contain Signature"))?;
    let credential_parts = credential.split('/').collect::<Vec<_>>();
    if credential_parts.len() != 5 || credential_parts[4] != "aws4_request" {
        return Err(DynamoDbError::auth("Credential scope was malformed"));
    }
    let signed_headers = signed_headers
        .split(';')
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    if signed_headers.is_empty() {
        return Err(DynamoDbError::auth("SignedHeaders must not be empty"));
    }
    if !signed_headers.iter().any(|header| header == "host") {
        return Err(DynamoDbError::auth("SignedHeaders must include host"));
    }
    if !signed_headers.iter().any(|header| header == "x-amz-date") {
        return Err(DynamoDbError::auth("SignedHeaders must include x-amz-date"));
    }
    Ok(ParsedSigV4Authorization {
        access_key_id: credential_parts[0].to_string(),
        credential_date: credential_parts[1].to_string(),
        region: credential_parts[2].to_string(),
        service: credential_parts[3].to_string(),
        signed_headers,
        signature,
    })
}

fn require_header_ascii(headers: &HeaderMap, name: &str) -> Result<String, DynamoDbError> {
    headers
        .get(name)
        .ok_or_else(|| DynamoDbError::auth(format!("Missing required header {}", name)))?
        .to_str()
        .map(|value| value.to_string())
        .map_err(|_| DynamoDbError::auth(format!("Header {} was not valid ASCII", name)))
}

fn optional_header_ascii(headers: &HeaderMap, name: &str) -> Result<Option<String>, DynamoDbError> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(|value| value.to_string())
                .map_err(|_| DynamoDbError::auth(format!("Header {} was not valid ASCII", name)))
        })
        .transpose()
}

fn canonical_headers(
    headers: &HeaderMap,
    signed_headers: &[String],
) -> Result<String, DynamoDbError> {
    let mut canonical = String::new();
    for name in signed_headers.iter() {
        let value = require_header_ascii(headers, name)?;
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(&canonicalize_header_value(&value));
        canonical.push('\n');
    }
    Ok(canonical)
}

fn canonicalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut sha = Sha256::new();
    sha.update(bytes);
    hex_encode(&sha.finalize())
}

fn sigv4_signature(
    secret_access_key: &str,
    credential_date: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> Result<String, DynamoDbError> {
    let secret = format!("AWS4{}", secret_access_key);
    let k_date = hmac_sha256(secret.as_bytes(), credential_date.as_bytes())?;
    let k_region = hmac_sha256(&k_date, region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, service.as_bytes())?;
    let k_signing = hmac_sha256(&k_service, b"aws4_request")?;
    Ok(hex_encode(&hmac_sha256(
        &k_signing,
        string_to_sign.as_bytes(),
    )?))
}

fn validate_sigv4_request_time(amz_date: &str, credential_date: &str) -> Result<(), DynamoDbError> {
    let parsed = NaiveDateTime::parse_from_str(amz_date, "%Y%m%dT%H%M%SZ")
        .map_err(|_| DynamoDbError::auth("x-amz-date was not a valid SigV4 timestamp"))?;
    let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc);
    if credential_date != timestamp.format("%Y%m%d").to_string() {
        return Err(DynamoDbError::auth(
            "Credential scope date did not match x-amz-date",
        ));
    }
    let skew = Utc::now()
        .signed_duration_since(timestamp)
        .num_minutes()
        .abs();
    if skew > SIGV4_ALLOWED_CLOCK_SKEW_MINUTES {
        return Err(DynamoDbError::auth(
            "x-amz-date was outside the allowed clock skew",
        ));
    }
    Ok(())
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<Vec<u8>, DynamoDbError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|_| DynamoDbError::internal("Failed to initialize HMAC state"))?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        encoded.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
    }
    encoded
}

async fn parse_json_body<T: for<'de> Deserialize<'de>>(
    state: &mut State,
) -> Result<T, DynamoDbError> {
    let bytes = body::to_bytes(Body::take_from(state))
        .await
        .map_err(|error| DynamoDbError::validation(format!("Failed to read body: {}", error)))?;
    serde_json::from_slice(&bytes).map_err(|error| {
        DynamoDbError::validation(format!("Request body was not valid JSON: {}", error))
    })
}

fn parse_projection_expression(
    expression: Option<&String>,
    names: Option<&HashMap<String, String>>,
) -> Result<Option<Vec<String>>, DynamoDbError> {
    expression
        .map(|expression| {
            expression
                .split(',')
                .map(|segment| resolve_attribute_name(segment.trim(), names))
                .collect::<Result<Vec<_>, _>>()
                .map(Some)
        })
        .unwrap_or(Ok(None))
}

fn parse_select_mode(
    select: Option<&String>,
    projection: Option<&Vec<String>>,
    index_name: Option<&str>,
) -> Result<SelectMode, DynamoDbError> {
    if projection.is_some() {
        match select.map(|select| select.as_str()) {
            Some("SPECIFIC_ATTRIBUTES") | None => {}
            Some(_) => {
                return Err(DynamoDbError::validation(
                    "ProjectionExpression can only be used when Select is SPECIFIC_ATTRIBUTES",
                ));
            }
        }
    }
    match select.map(|select| select.as_str()) {
        None => Ok(if projection.is_some() {
            SelectMode::SpecificAttributes
        } else if index_name.is_some() {
            SelectMode::AllProjectedAttributes
        } else {
            SelectMode::AllAttributes
        }),
        Some("ALL_ATTRIBUTES") => Ok(SelectMode::AllAttributes),
        Some("ALL_PROJECTED_ATTRIBUTES") => {
            if index_name.is_none() {
                return Err(DynamoDbError::validation(
                    "ALL_PROJECTED_ATTRIBUTES requires IndexName",
                ));
            }
            Ok(SelectMode::AllProjectedAttributes)
        }
        Some("SPECIFIC_ATTRIBUTES") => {
            if projection.is_none() {
                return Err(DynamoDbError::validation(
                    "SPECIFIC_ATTRIBUTES requires ProjectionExpression",
                ));
            }
            Ok(SelectMode::SpecificAttributes)
        }
        Some("COUNT") => Ok(SelectMode::Count),
        Some(other) => Err(DynamoDbError::validation(format!(
            "Unsupported Select value {}",
            other
        ))),
    }
}

fn parse_return_consumed_capacity(
    value: Option<&String>,
) -> Result<ReturnConsumedCapacityMode, DynamoDbError> {
    match value.map(|value| value.as_str()).unwrap_or("NONE") {
        "NONE" => Ok(ReturnConsumedCapacityMode::None),
        "TOTAL" => Ok(ReturnConsumedCapacityMode::Total),
        "INDEXES" => Ok(ReturnConsumedCapacityMode::Indexes),
        other => Err(DynamoDbError::validation(format!(
            "Unsupported ReturnConsumedCapacity value {}",
            other
        ))),
    }
}

fn parse_write_return_values(
    value: Option<&str>,
    operation: &str,
) -> Result<WriteReturnValues, DynamoDbError> {
    match value.unwrap_or("NONE") {
        "NONE" => Ok(WriteReturnValues::None),
        "ALL_OLD" => Ok(WriteReturnValues::AllOld),
        other => Err(DynamoDbError::validation(format!(
            "Unsupported ReturnValues value {} for {}",
            other, operation
        ))),
    }
}

fn estimate_read_capacity_units(read_count: usize, consistent_read: bool) -> f64 {
    let per_read = if consistent_read { 1.0 } else { 0.5 };
    read_count as f64 * per_read
}

fn consumed_capacity_for_read(
    mode: ReturnConsumedCapacityMode,
    table_name: &str,
    index_kind: DynamoDbIndexKind,
    index_name: Option<&str>,
    capacity_units: f64,
) -> Option<ConsumedCapacity> {
    if mode == ReturnConsumedCapacityMode::None {
        return None;
    }
    let breakdown = CapacityBreakdown {
        capacity_units,
        read_capacity_units: capacity_units,
    };
    let table = if index_kind == DynamoDbIndexKind::Table {
        Some(breakdown.clone())
    } else {
        None
    };
    let local_secondary_indexes = if mode == ReturnConsumedCapacityMode::Indexes
        && index_kind == DynamoDbIndexKind::LocalSecondaryIndex
    {
        Some(HashMap::from([(
            index_name.unwrap_or_default().to_string(),
            breakdown.clone(),
        )]))
    } else {
        None
    };
    let global_secondary_indexes = if mode == ReturnConsumedCapacityMode::Indexes
        && index_kind == DynamoDbIndexKind::GlobalSecondaryIndex
    {
        Some(HashMap::from([(
            index_name.unwrap_or_default().to_string(),
            breakdown.clone(),
        )]))
    } else {
        None
    };
    Some(ConsumedCapacity {
        table_name: table_name.to_string(),
        capacity_units,
        read_capacity_units: capacity_units,
        table,
        local_secondary_indexes,
        global_secondary_indexes,
    })
}

fn query_requested_projection<'a>(
    requested_projection: Option<&'a Vec<String>>,
    select_mode: SelectMode,
) -> Option<&'a Vec<String>> {
    match select_mode {
        SelectMode::SpecificAttributes => requested_projection,
        _ => None,
    }
}

fn query_select_fields(
    requested_projection: Option<&Vec<String>>,
    filter_expression: &ParsedFilterExpression,
    key_schema: &DynamoDbKeySchemaConfig,
    schema: &PowdrrSchema,
) -> Option<Vec<String>> {
    let Some(requested_projection) = requested_projection.cloned() else {
        return None;
    };
    let mut fields = requested_projection;
    append_selected_field(&mut fields, &key_schema.partition_key);
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        append_selected_field(&mut fields, sort_key);
    }
    for field in filter_expression.filter_fields.iter() {
        append_selected_field_if_in_schema(&mut fields, field, schema);
    }
    Some(fields)
}

fn scan_select_fields(
    requested_projection: Option<&Vec<String>>,
    filter_expression: &ParsedFilterExpression,
    order_fields: &[String],
    schema: &PowdrrSchema,
) -> Option<Vec<String>> {
    let Some(requested_projection) = requested_projection.cloned() else {
        return None;
    };
    let mut fields = requested_projection;
    for field in order_fields.iter() {
        append_selected_field(&mut fields, field);
    }
    for field in filter_expression.filter_fields.iter() {
        append_selected_field_if_in_schema(&mut fields, field, schema);
    }
    Some(fields)
}

fn append_selected_field(fields: &mut Vec<String>, field_name: &str) {
    if !fields.iter().any(|field| field == field_name) {
        fields.push(field_name.to_string());
    }
}

fn append_selected_field_if_in_schema(
    fields: &mut Vec<String>,
    field_name: &str,
    schema: &PowdrrSchema,
) {
    if schema.to_map().contains_key(field_name) {
        append_selected_field(fields, field_name);
    }
}

fn query_fetch_limit(
    page_limit: usize,
    sort_is_exact_eq: bool,
    hash_only_key_schema: bool,
    unique_lookup: bool,
) -> usize {
    if unique_lookup && (sort_is_exact_eq || hash_only_key_schema) {
        page_limit.min(1)
    } else {
        page_limit.saturating_add(1)
    }
}

fn parse_key_map(
    key: &Map<String, Value>,
    key_schema: &DynamoDbKeySchemaConfig,
) -> Result<HashMap<String, Value>, DynamoDbError> {
    let mut parsed = HashMap::new();
    parsed.insert(
        key_schema.partition_key.clone(),
        dynamodb_attr_to_json(key.get(&key_schema.partition_key).ok_or_else(|| {
            DynamoDbError::validation(format!(
                "Key must include partition key {}",
                key_schema.partition_key
            ))
        })?)?,
    );
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        parsed.insert(
            sort_key.clone(),
            dynamodb_attr_to_json(key.get(sort_key).ok_or_else(|| {
                DynamoDbError::validation(format!("Key must include sort key {}", sort_key))
            })?)?,
        );
    }
    if key.len() != parsed.len() {
        return Err(DynamoDbError::validation(
            "Key contained attributes that are not part of the table primary key",
        ));
    }
    Ok(parsed)
}

fn key_to_predicates(
    key_schema: &DynamoDbKeySchemaConfig,
    key: &HashMap<String, Value>,
) -> Vec<ServingPredicate> {
    let mut predicates = vec![ServingPredicate {
        field: key_schema.partition_key.clone(),
        eq: key.get(&key_schema.partition_key).cloned(),
        in_values: None,
        gt: None,
        gte: None,
        lt: None,
        lte: None,
    }];
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        predicates.push(ServingPredicate {
            field: sort_key.clone(),
            eq: key.get(sort_key).cloned(),
            in_values: None,
            gt: None,
            gte: None,
            lt: None,
            lte: None,
        });
    }
    predicates
}

struct ParsedQuery {
    partition_value: Value,
    sort_filter: Option<ServingPredicate>,
}

fn parse_key_condition_expression(
    expression: &str,
    key_schema: &DynamoDbKeySchemaConfig,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, Value>>,
) -> Result<ParsedQuery, DynamoDbError> {
    let values = values.ok_or_else(|| {
        DynamoDbError::validation("ExpressionAttributeValues are required for Query")
    })?;
    let partition_field = key_schema.partition_key.clone();
    let partition_prefix = format!("{} = ", partition_field);
    let expression = resolve_expression_names(expression, names)?;
    let (partition_segment, sort_segment) = expression
        .split_once(" AND ")
        .map(|(left, right)| (left.trim(), Some(right.trim())))
        .unwrap_or((expression.trim(), None));

    if !partition_segment.starts_with(&partition_prefix) {
        return Err(DynamoDbError::validation(format!(
            "Query must begin with {} = :value",
            partition_field
        )));
    }
    let partition_token = partition_segment[partition_prefix.len()..].trim();
    let partition_value = lookup_expression_value(partition_token, values)?;

    let sort_filter = match (key_schema.sort_key.as_ref(), sort_segment) {
        (_, None) => None,
        (None, Some(_)) => {
            return Err(DynamoDbError::validation(
                "This table does not define a sort key",
            ));
        }
        (Some(sort_key), Some(segment)) => Some(parse_sort_condition(segment, sort_key, values)?),
    };

    Ok(ParsedQuery {
        partition_value,
        sort_filter,
    })
}

fn parse_filter_expression(
    expression: Option<&String>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, Value>>,
) -> Result<ParsedFilterExpression, DynamoDbError> {
    let Some(expression) = expression else {
        return Ok(ParsedFilterExpression {
            expression: None,
            filter_fields: vec![],
        });
    };
    let resolved_expression = resolve_expression_names(expression, names)?;
    let values = values.ok_or_else(|| {
        DynamoDbError::validation("ExpressionAttributeValues are required for FilterExpression")
    })?;
    let tokens = tokenize_filter_expression(&resolved_expression)?;
    let mut parser = FilterParser::new(tokens, values);
    let expression = parser.parse_expression()?;
    parser.expect_end()?;
    let mut filter_fields = vec![];
    collect_filter_fields(&expression, &mut filter_fields);
    Ok(ParsedFilterExpression {
        expression: Some(expression),
        filter_fields,
    })
}

struct FilterParser<'a> {
    tokens: Vec<FilterToken>,
    index: usize,
    values: &'a HashMap<String, Value>,
}

impl<'a> FilterParser<'a> {
    fn new(tokens: Vec<FilterToken>, values: &'a HashMap<String, Value>) -> Self {
        Self {
            tokens,
            index: 0,
            values,
        }
    }

    fn parse_expression(&mut self) -> Result<FilterNode, DynamoDbError> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<FilterNode, DynamoDbError> {
        let mut nodes = vec![self.parse_and_expression()?];
        while self.consume_token(&FilterToken::Or) {
            nodes.push(self.parse_and_expression()?);
        }
        Ok(if nodes.len() == 1 {
            nodes.pop().unwrap()
        } else {
            FilterNode::Or(nodes)
        })
    }

    fn parse_and_expression(&mut self) -> Result<FilterNode, DynamoDbError> {
        let mut nodes = vec![self.parse_unary_expression()?];
        while self.consume_token(&FilterToken::And) {
            nodes.push(self.parse_unary_expression()?);
        }
        Ok(if nodes.len() == 1 {
            nodes.pop().unwrap()
        } else {
            FilterNode::And(nodes)
        })
    }

    fn parse_unary_expression(&mut self) -> Result<FilterNode, DynamoDbError> {
        if self.consume_token(&FilterToken::Not) {
            return Ok(FilterNode::Not(Box::new(self.parse_unary_expression()?)));
        }
        self.parse_primary_expression()
    }

    fn parse_primary_expression(&mut self) -> Result<FilterNode, DynamoDbError> {
        if self.consume_token(&FilterToken::LParen) {
            let expression = self.parse_expression()?;
            self.expect_token(&FilterToken::RParen)?;
            return Ok(expression);
        }
        self.parse_predicate()
    }

    fn parse_predicate(&mut self) -> Result<FilterNode, DynamoDbError> {
        if self.peek_function("begins_with") {
            let (field, value) = self.parse_field_value_function("begins_with")?;
            let prefix = value.as_str().ok_or_else(|| {
                DynamoDbError::validation("begins_with requires a string AttributeValue")
            })?;
            return Ok(FilterNode::Predicate(FilterPredicate {
                operand: FilterOperand::Field(field),
                kind: FilterPredicateKind::BeginsWith(prefix.to_string()),
            }));
        }
        if self.peek_function("contains") {
            let (field, value) = self.parse_field_value_function("contains")?;
            return Ok(FilterNode::Predicate(FilterPredicate {
                operand: FilterOperand::Field(field),
                kind: FilterPredicateKind::Contains(value),
            }));
        }
        if self.peek_function("attribute_exists") {
            let field = self.parse_single_field_function("attribute_exists")?;
            return Ok(FilterNode::Predicate(FilterPredicate {
                operand: FilterOperand::Field(field),
                kind: FilterPredicateKind::AttributeExists,
            }));
        }
        if self.peek_function("attribute_not_exists") {
            let field = self.parse_single_field_function("attribute_not_exists")?;
            return Ok(FilterNode::Predicate(FilterPredicate {
                operand: FilterOperand::Field(field),
                kind: FilterPredicateKind::AttributeNotExists,
            }));
        }
        if self.peek_function("attribute_type") {
            let (field, value) = self.parse_field_value_function("attribute_type")?;
            let type_name = value.as_str().ok_or_else(|| {
                DynamoDbError::validation("attribute_type requires a string AttributeValue")
            })?;
            return Ok(FilterNode::Predicate(FilterPredicate {
                operand: FilterOperand::Field(field),
                kind: FilterPredicateKind::AttributeType(type_name.to_string()),
            }));
        }

        let operand = self.parse_filter_operand()?;
        if self.consume_token(&FilterToken::Between) {
            let start = self.parse_value_token()?;
            self.expect_token(&FilterToken::And)?;
            let end = self.parse_value_token()?;
            return Ok(FilterNode::Predicate(FilterPredicate {
                operand,
                kind: FilterPredicateKind::Between { start, end },
            }));
        }
        if self.consume_token(&FilterToken::In) {
            self.expect_token(&FilterToken::LParen)?;
            let mut values = vec![self.parse_value_token()?];
            while self.consume_token(&FilterToken::Comma) {
                values.push(self.parse_value_token()?);
            }
            self.expect_token(&FilterToken::RParen)?;
            return Ok(FilterNode::Predicate(FilterPredicate {
                operand,
                kind: FilterPredicateKind::In(values),
            }));
        }

        let kind = if self.consume_token(&FilterToken::Eq) {
            FilterPredicateKind::Eq(self.parse_value_token()?)
        } else if self.consume_token(&FilterToken::Lte) {
            FilterPredicateKind::Lte(self.parse_value_token()?)
        } else if self.consume_token(&FilterToken::Gte) {
            FilterPredicateKind::Gte(self.parse_value_token()?)
        } else if self.consume_token(&FilterToken::Lt) {
            FilterPredicateKind::Lt(self.parse_value_token()?)
        } else if self.consume_token(&FilterToken::Gt) {
            FilterPredicateKind::Gt(self.parse_value_token()?)
        } else {
            return Err(DynamoDbError::validation(
                "Unsupported FilterExpression predicate",
            ));
        };
        Ok(FilterNode::Predicate(FilterPredicate { operand, kind }))
    }

    fn parse_filter_operand(&mut self) -> Result<FilterOperand, DynamoDbError> {
        if self.peek_function("size") {
            self.expect_identifier_ci("size")?;
            self.expect_token(&FilterToken::LParen)?;
            let field = self.parse_identifier()?;
            self.expect_token(&FilterToken::RParen)?;
            return Ok(FilterOperand::Size(field));
        }
        Ok(FilterOperand::Field(self.parse_identifier()?))
    }

    fn parse_field_value_function(&mut self, name: &str) -> Result<(String, Value), DynamoDbError> {
        self.expect_identifier_ci(name)?;
        self.expect_token(&FilterToken::LParen)?;
        let field = self.parse_identifier()?;
        self.expect_token(&FilterToken::Comma)?;
        let value = self.parse_value_token()?;
        self.expect_token(&FilterToken::RParen)?;
        Ok((field, value))
    }

    fn parse_single_field_function(&mut self, name: &str) -> Result<String, DynamoDbError> {
        self.expect_identifier_ci(name)?;
        self.expect_token(&FilterToken::LParen)?;
        let field = self.parse_identifier()?;
        self.expect_token(&FilterToken::RParen)?;
        Ok(field)
    }

    fn parse_identifier(&mut self) -> Result<String, DynamoDbError> {
        match self.next_token() {
            Some(FilterToken::Identifier(value)) => Ok(value.clone()),
            _ => Err(DynamoDbError::validation(
                "FilterExpression expected an attribute name",
            )),
        }
    }

    fn parse_value_token(&mut self) -> Result<Value, DynamoDbError> {
        match self.next_token().cloned() {
            Some(FilterToken::ValueToken(token)) => lookup_expression_value(&token, self.values),
            _ => Err(DynamoDbError::validation(
                "FilterExpression expected an ExpressionAttributeValues token",
            )),
        }
    }

    fn expect_identifier_ci(&mut self, expected: &str) -> Result<(), DynamoDbError> {
        match self.next_token() {
            Some(FilterToken::Identifier(value)) if value.eq_ignore_ascii_case(expected) => Ok(()),
            _ => Err(DynamoDbError::validation(format!(
                "FilterExpression expected function {}",
                expected
            ))),
        }
    }

    fn expect_token(&mut self, expected: &FilterToken) -> Result<(), DynamoDbError> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            Err(DynamoDbError::validation("FilterExpression was malformed"))
        }
    }

    fn consume_token(&mut self, expected: &FilterToken) -> bool {
        if self.peek_token() == Some(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn peek_function(&self, name: &str) -> bool {
        matches!(
            self.peek_token(),
            Some(FilterToken::Identifier(value)) if value.eq_ignore_ascii_case(name)
        )
    }

    fn expect_end(&self) -> Result<(), DynamoDbError> {
        if self.index == self.tokens.len() {
            Ok(())
        } else {
            Err(DynamoDbError::validation(
                "FilterExpression contained trailing tokens",
            ))
        }
    }

    fn peek_token(&self) -> Option<&FilterToken> {
        self.tokens.get(self.index)
    }

    fn next_token(&mut self) -> Option<&FilterToken> {
        let token = self.tokens.get(self.index);
        if token.is_some() {
            self.index += 1;
        }
        token
    }
}

fn tokenize_filter_expression(expression: &str) -> Result<Vec<FilterToken>, DynamoDbError> {
    let mut tokens = vec![];
    let characters = expression.chars().collect::<Vec<_>>();
    let mut index = 0usize;
    while index < characters.len() {
        match characters[index] {
            character if character.is_whitespace() => index += 1,
            '(' => {
                tokens.push(FilterToken::LParen);
                index += 1;
            }
            ')' => {
                tokens.push(FilterToken::RParen);
                index += 1;
            }
            ',' => {
                tokens.push(FilterToken::Comma);
                index += 1;
            }
            '=' => {
                tokens.push(FilterToken::Eq);
                index += 1;
            }
            '<' => {
                if characters.get(index + 1) == Some(&'=') {
                    tokens.push(FilterToken::Lte);
                    index += 2;
                } else {
                    tokens.push(FilterToken::Lt);
                    index += 1;
                }
            }
            '>' => {
                if characters.get(index + 1) == Some(&'=') {
                    tokens.push(FilterToken::Gte);
                    index += 2;
                } else {
                    tokens.push(FilterToken::Gt);
                    index += 1;
                }
            }
            ':' => {
                let start = index;
                index += 1;
                while index < characters.len() && is_filter_identifier_character(characters[index])
                {
                    index += 1;
                }
                tokens.push(FilterToken::ValueToken(
                    characters[start..index].iter().collect(),
                ));
            }
            character if is_filter_identifier_start(character) => {
                let start = index;
                index += 1;
                while index < characters.len() && is_filter_identifier_character(characters[index])
                {
                    index += 1;
                }
                let token = characters[start..index].iter().collect::<String>();
                let keyword = match token.to_ascii_uppercase().as_str() {
                    "AND" => Some(FilterToken::And),
                    "OR" => Some(FilterToken::Or),
                    "NOT" => Some(FilterToken::Not),
                    "BETWEEN" => Some(FilterToken::Between),
                    "IN" => Some(FilterToken::In),
                    _ => None,
                };
                tokens.push(keyword.unwrap_or(FilterToken::Identifier(token)));
            }
            other => {
                return Err(DynamoDbError::validation(format!(
                    "Unsupported FilterExpression token {}",
                    other
                )));
            }
        }
    }
    Ok(tokens)
}

fn is_filter_identifier_start(character: char) -> bool {
    character.is_ascii_alphabetic() || character == '_'
}

fn is_filter_identifier_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_' || character == '-' || character == '.'
}

fn collect_filter_fields(node: &FilterNode, fields: &mut Vec<String>) {
    match node {
        FilterNode::Predicate(predicate) => {
            append_selected_field(fields, predicate.operand.field_name())
        }
        FilterNode::And(nodes) | FilterNode::Or(nodes) => {
            for node in nodes.iter() {
                collect_filter_fields(node, fields);
            }
        }
        FilterNode::Not(node) => collect_filter_fields(node, fields),
    }
}

fn parse_sort_condition(
    segment: &str,
    sort_key: &str,
    values: &HashMap<String, Value>,
) -> Result<ServingPredicate, DynamoDbError> {
    if let Some(rest) = segment.strip_prefix("begins_with") {
        let args = rest
            .trim()
            .strip_prefix('(')
            .and_then(|inner| inner.strip_suffix(')'))
            .ok_or_else(|| DynamoDbError::validation("begins_with must use function syntax"))?;
        let (field, value_token) = args.split_once(',').ok_or_else(|| {
            DynamoDbError::validation("begins_with must include a sort key and value")
        })?;
        if field.trim() != sort_key {
            return Err(DynamoDbError::validation(format!(
                "Only sort key {} can appear after the partition condition",
                sort_key
            )));
        }
        return begins_with_range_predicate(sort_key, value_token.trim(), values);
    }

    if let Some((left, right)) = segment.split_once(" BETWEEN ") {
        if left.trim() != sort_key {
            return Err(DynamoDbError::validation(format!(
                "Only sort key {} can appear after the partition condition",
                sort_key
            )));
        }
        let (start, end) = right
            .split_once(" AND ")
            .ok_or_else(|| DynamoDbError::validation("BETWEEN must include two values"))?;
        return Ok(ServingPredicate {
            field: sort_key.to_string(),
            eq: None,
            in_values: None,
            gt: None,
            gte: Some(lookup_expression_value(start.trim(), values)?),
            lt: None,
            lte: Some(lookup_expression_value(end.trim(), values)?),
        });
    }

    for operator in ["<=", ">=", "<", ">", "="] {
        if let Some((left, right)) = segment.split_once(operator) {
            if left.trim() != sort_key {
                return Err(DynamoDbError::validation(format!(
                    "Only sort key {} can appear after the partition condition",
                    sort_key
                )));
            }
            let value = lookup_expression_value(right.trim(), values)?;
            let mut predicate = ServingPredicate {
                field: sort_key.to_string(),
                eq: None,
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            };
            match operator {
                "=" => predicate.eq = Some(value),
                "<" => predicate.lt = Some(value),
                "<=" => predicate.lte = Some(value),
                ">" => predicate.gt = Some(value),
                ">=" => predicate.gte = Some(value),
                _ => {}
            }
            return Ok(predicate);
        }
    }

    Err(DynamoDbError::validation(
        "Unsupported KeyConditionExpression form",
    ))
}

fn begins_with_range_predicate(
    field: &str,
    value_token: &str,
    values: &HashMap<String, Value>,
) -> Result<ServingPredicate, DynamoDbError> {
    let prefix = lookup_expression_value(value_token, values)?;
    let prefix = prefix
        .as_str()
        .ok_or_else(|| DynamoDbError::validation("begins_with requires a string AttributeValue"))?;
    Ok(ServingPredicate {
        field: field.to_string(),
        eq: None,
        in_values: None,
        gt: None,
        gte: Some(json!(prefix)),
        lt: next_string_prefix_upper_bound(prefix).map(|upper| json!(upper)),
        lte: None,
    })
}

fn next_string_prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars = prefix.chars().collect::<Vec<_>>();
    while let Some(last) = chars.pop() {
        if let Some(next) = next_scalar(last) {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

fn next_scalar(value: char) -> Option<char> {
    let codepoint = value as u32;
    if codepoint == 0x10FFFF {
        return None;
    }
    if codepoint == 0xD7FF {
        return std::char::from_u32(0xE000);
    }
    std::char::from_u32(codepoint + 1)
}

fn apply_exclusive_start_key(
    filters: &mut Vec<ServingPredicate>,
    key_schema: &DynamoDbKeySchemaConfig,
    table_key_schema: &DynamoDbKeySchemaConfig,
    ascending: bool,
    exclusive_start_key: &Map<String, Value>,
) -> Result<(), DynamoDbError> {
    let sort_key = match key_schema.sort_key.as_ref() {
        Some(sort_key) => sort_key,
        None => return Ok(()),
    };
    let parsed_key =
        parse_exclusive_start_key_map(exclusive_start_key, key_schema, table_key_schema)?;
    let partition_value = parsed_key
        .get(&key_schema.partition_key)
        .ok_or_else(|| DynamoDbError::validation("ExclusiveStartKey was missing partition key"))?;
    let partition_filter = filters
        .iter()
        .find(|filter| filter.field == key_schema.partition_key)
        .and_then(|filter| filter.eq.as_ref())
        .ok_or_else(|| DynamoDbError::validation("Partition key filter was missing"))?;
    if partition_filter != partition_value {
        return Err(DynamoDbError::validation(
            "ExclusiveStartKey partition key must match the query partition key",
        ));
    }
    let start_value = parsed_key
        .get(sort_key)
        .cloned()
        .ok_or_else(|| DynamoDbError::validation("ExclusiveStartKey was missing sort key"))?;
    if let Some(filter) = filters.iter_mut().find(|filter| filter.field == *sort_key) {
        if filter.eq.is_some() {
            if filter.eq.as_ref() == Some(&start_value) {
                filter.eq = None;
                filter.gt = Some(start_value.clone());
                filter.lt = Some(start_value);
                return Ok(());
            }
            return Ok(());
        }
        if ascending {
            tighten_lower_bound(filter, start_value)?;
        } else {
            tighten_upper_bound(filter, start_value)?;
        }
        return Ok(());
    }

    let mut predicate = ServingPredicate {
        field: sort_key.clone(),
        eq: None,
        in_values: None,
        gt: None,
        gte: None,
        lt: None,
        lte: None,
    };
    if ascending {
        predicate.gt = Some(start_value);
    } else {
        predicate.lt = Some(start_value);
    }
    filters.push(predicate);
    Ok(())
}

fn tighten_lower_bound(
    filter: &mut ServingPredicate,
    candidate: Value,
) -> Result<(), DynamoDbError> {
    if let Some(existing) = filter.gt.as_ref() {
        if compare_scalars(existing, &candidate)? != std::cmp::Ordering::Less {
            return Ok(());
        }
    }
    if let Some(existing) = filter.gte.as_ref() {
        if compare_scalars(existing, &candidate)? == std::cmp::Ordering::Greater {
            filter.gt = Some(existing.clone());
            filter.gte = None;
            return Ok(());
        }
    }
    filter.gt = Some(candidate);
    filter.gte = None;
    Ok(())
}

fn tighten_upper_bound(
    filter: &mut ServingPredicate,
    candidate: Value,
) -> Result<(), DynamoDbError> {
    if let Some(existing) = filter.lt.as_ref() {
        if compare_scalars(existing, &candidate)? != std::cmp::Ordering::Greater {
            return Ok(());
        }
    }
    if let Some(existing) = filter.lte.as_ref() {
        if compare_scalars(existing, &candidate)? == std::cmp::Ordering::Less {
            filter.lt = Some(existing.clone());
            filter.lte = None;
            return Ok(());
        }
    }
    filter.lt = Some(candidate);
    filter.lte = None;
    Ok(())
}

fn resolve_expression_names(
    expression: &str,
    names: Option<&HashMap<String, String>>,
) -> Result<String, DynamoDbError> {
    let mut resolved = expression.to_string();
    if let Some(names) = names {
        for (alias, field_name) in names.iter() {
            resolved = resolved.replace(alias, field_name);
        }
    }
    Ok(resolved)
}

fn resolve_attribute_name(
    token: &str,
    names: Option<&HashMap<String, String>>,
) -> Result<String, DynamoDbError> {
    if token.starts_with('#') {
        return names
            .and_then(|names| names.get(token).cloned())
            .ok_or_else(|| {
                DynamoDbError::validation(format!(
                    "Missing ExpressionAttributeNames entry for {}",
                    token
                ))
            });
    }
    Ok(token.to_string())
}

fn lookup_expression_value(
    token: &str,
    values: &HashMap<String, Value>,
) -> Result<Value, DynamoDbError> {
    let raw_value = values.get(token).ok_or_else(|| {
        DynamoDbError::validation(format!(
            "Missing ExpressionAttributeValues entry for {}",
            token
        ))
    })?;
    dynamodb_attr_to_json(raw_value)
}

fn json_row_to_dynamodb_item(row: &Value) -> Result<Map<String, Value>, DynamoDbError> {
    let object = row
        .as_object()
        .ok_or_else(|| DynamoDbError::internal("Serving query returned a non-object row"))?;
    let mut item = Map::new();
    for (key, value) in object.iter() {
        item.insert(key.clone(), json_to_dynamodb_attr(value)?);
    }
    Ok(item)
}

fn row_to_key(
    row: &Value,
    key_schema: &DynamoDbKeySchemaConfig,
    table_key_schema: &DynamoDbKeySchemaConfig,
) -> Result<Map<String, Value>, DynamoDbError> {
    let object = row
        .as_object()
        .ok_or_else(|| DynamoDbError::internal("Serving query returned a non-object row"))?;
    let mut key = Map::new();
    append_key_to_map(&mut key, object, &key_schema.partition_key, "partition")?;
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        append_key_to_map(&mut key, object, sort_key, "sort")?;
    }
    if table_key_schema.partition_key != key_schema.partition_key {
        append_key_to_map(
            &mut key,
            object,
            &table_key_schema.partition_key,
            "table partition",
        )?;
    }
    if let Some(sort_key) = table_key_schema.sort_key.as_ref() {
        if key_schema.sort_key.as_ref() != Some(sort_key) {
            append_key_to_map(&mut key, object, sort_key, "table sort")?;
        }
    }
    Ok(key)
}

fn append_key_to_map(
    key: &mut Map<String, Value>,
    object: &Map<String, Value>,
    field_name: &str,
    label: &str,
) -> Result<(), DynamoDbError> {
    if key.contains_key(field_name) {
        return Ok(());
    }
    key.insert(
        field_name.to_string(),
        json_to_dynamodb_attr(object.get(field_name).ok_or_else(|| {
            DynamoDbError::internal(format!(
                "Response item was missing {} key {}",
                label, field_name
            ))
        })?)?,
    );
    Ok(())
}

fn parse_exclusive_start_key_map(
    key: &Map<String, Value>,
    query_key_schema: &DynamoDbKeySchemaConfig,
    table_key_schema: &DynamoDbKeySchemaConfig,
) -> Result<HashMap<String, Value>, DynamoDbError> {
    let mut field_names = vec![query_key_schema.partition_key.clone()];
    if let Some(sort_key) = query_key_schema.sort_key.as_ref() {
        field_names.push(sort_key.clone());
    }
    if table_key_schema.partition_key != query_key_schema.partition_key {
        field_names.push(table_key_schema.partition_key.clone());
    }
    if let Some(sort_key) = table_key_schema.sort_key.as_ref() {
        if query_key_schema.sort_key.as_ref() != Some(sort_key) {
            field_names.push(sort_key.clone());
        }
    }

    let mut parsed = HashMap::new();
    for field_name in field_names.iter() {
        let Some(value) = key.get(field_name) else {
            return Err(DynamoDbError::validation(format!(
                "ExclusiveStartKey was missing key field {}",
                field_name
            )));
        };
        parsed.insert(field_name.clone(), dynamodb_attr_to_json(value)?);
    }
    if key.len() != parsed.len() {
        return Err(DynamoDbError::validation(
            "ExclusiveStartKey contained attributes that are not part of the table or index key schema",
        ));
    }
    Ok(parsed)
}

fn ordered_key_fields(
    key_schema: &DynamoDbKeySchemaConfig,
    table_key_schema: &DynamoDbKeySchemaConfig,
) -> Vec<String> {
    let mut fields = vec![key_schema.partition_key.clone()];
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        fields.push(sort_key.clone());
    }
    if table_key_schema.partition_key != key_schema.partition_key {
        fields.push(table_key_schema.partition_key.clone());
    }
    if let Some(sort_key) = table_key_schema.sort_key.as_ref() {
        if key_schema.sort_key.as_ref() != Some(sort_key) {
            fields.push(sort_key.clone());
        }
    }
    fields
}

async fn execute_scan_query(
    files: &[FileDescriptor],
    projection: &Option<Vec<String>>,
    exclusive_start_key: Option<&HashMap<String, Value>>,
    order_fields: &[String],
    limit: usize,
) -> Result<Vec<Value>, DynamoDbError> {
    let sql_template = build_scan_sql(
        "{table}",
        projection.as_ref(),
        exclusive_start_key,
        order_fields,
        limit,
    )?;
    let mut rows = vec![];
    for file_group in group_files_by_schema(files).into_iter() {
        let mut new_rows = execute_scan_file_group(file_group, &sql_template).await?;
        rows.append(&mut new_rows);
        rows.sort_by(|left, right| compare_scan_rows(left, right, order_fields));
        rows.truncate(limit);
    }
    Ok(rows)
}

async fn execute_scan_file_group(
    files: Vec<FileDescriptor>,
    sql_template: &str,
) -> Result<Vec<Value>, DynamoDbError> {
    let local_name = file_group_table_name(&files);
    let file_paths = files
        .iter()
        .map(|file| file.file_path.clone())
        .collect::<Vec<_>>();
    let total_size = files.iter().map(|file| file.size).sum::<u64>();
    data_access::reserve(&local_name, total_size, vec![]).await;
    let result = async {
        load_files_as_table(&local_name, &file_paths, &files[0].schema.to_arrow_schema())
            .await
            .map_err(|error| DynamoDbError::internal(error.to_string()))?;
        let sql = sql_template.replace("{table}", &local_name);
        let batches = execute_sql_async(&sql)
            .await
            .map_err(|error| DynamoDbError::validation(error.to_string()))?;
        batches_to_serde_value(&batches)
            .await
            .map(|value| value.values)
            .map_err(|error| DynamoDbError::internal(error.message))
    }
    .await;
    data_access::release(&local_name).await;
    result
}

fn build_scan_sql(
    table_name: &str,
    projection: Option<&Vec<String>>,
    exclusive_start_key: Option<&HashMap<String, Value>>,
    order_fields: &[String],
    limit: usize,
) -> Result<String, DynamoDbError> {
    let select = match projection {
        Some(select_fields) => select_fields
            .iter()
            .map(|field| format!("\"{}\"", escape_identifier(field)))
            .collect::<Vec<_>>()
            .join(", "),
        None => "*".to_string(),
    };
    let mut where_clauses = vec![];
    if let Some(exclusive_start_key) = exclusive_start_key {
        where_clauses.push(scan_start_key_clause(order_fields, exclusive_start_key)?);
    }

    let mut sql = format!("SELECT {} FROM {} t", select, table_name);
    if !where_clauses.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_clauses.join(" AND ")));
    }
    if !order_fields.is_empty() {
        let order_clause = order_fields
            .iter()
            .map(|field| format!("\"{}\" ASC", escape_identifier(field)))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(" ORDER BY {}", order_clause));
    }
    sql.push_str(&format!(" LIMIT {}", limit));
    Ok(sql)
}

fn scan_start_key_clause(
    order_fields: &[String],
    key: &HashMap<String, Value>,
) -> Result<String, DynamoDbError> {
    scan_start_key_clause_inner(order_fields, key)
}

fn scan_start_key_clause_inner(
    order_fields: &[String],
    key: &HashMap<String, Value>,
) -> Result<String, DynamoDbError> {
    let Some((field, remaining)) = order_fields.split_first() else {
        return Err(DynamoDbError::validation(
            "ExclusiveStartKey did not include any order fields",
        ));
    };
    let value = key.get(field).ok_or_else(|| {
        DynamoDbError::validation(format!("ExclusiveStartKey was missing key field {}", field))
    })?;
    let field_sql = format!("\"{}\"", escape_identifier(field));
    let value_sql = scan_sql_literal(value)?;
    if remaining.is_empty() {
        return Ok(format!("{} > {}", field_sql, value_sql));
    }
    Ok(format!(
        "({field} > {value} OR ({field} = {value} AND {rest}))",
        field = field_sql,
        value = value_sql,
        rest = scan_start_key_clause_inner(remaining, key)?,
    ))
}

fn scan_sql_literal(value: &Value) -> Result<String, DynamoDbError> {
    match value {
        Value::String(text) => Ok(format!("'{}'", text.replace('\'', "''"))),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(if *boolean {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }),
        Value::Null => Ok("NULL".to_string()),
        _ => Err(DynamoDbError::validation(
            "Only scalar literals are supported in DynamoDB read filters",
        )),
    }
}

fn escape_identifier(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}

fn compare_scan_rows(left: &Value, right: &Value, order_fields: &[String]) -> Ordering {
    let left_object = left.as_object();
    let right_object = right.as_object();
    for field in order_fields.iter() {
        let ordering = compare_scan_row_field(
            left_object.and_then(|object| object.get(field)),
            right_object.and_then(|object| object.get(field)),
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_scan_row_field(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_scalars(left, right)
            .unwrap_or_else(|_| left.to_string().cmp(&right.to_string())),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn group_files_by_schema(files: &[FileDescriptor]) -> Vec<Vec<FileDescriptor>> {
    let mut groups: Vec<Vec<FileDescriptor>> = vec![];
    for file in files.iter().cloned() {
        if let Some(existing_group) = groups.iter_mut().find(|group| {
            group
                .first()
                .map(|existing| existing.schema == file.schema)
                .unwrap_or(false)
        }) {
            existing_group.push(file);
        } else {
            groups.push(vec![file]);
        }
    }
    groups
}

fn file_group_table_name(files: &[FileDescriptor]) -> String {
    let mut file_paths = files
        .iter()
        .map(|file| file.file_path.clone())
        .collect::<Vec<_>>();
    file_paths.sort();
    let mut hasher = DefaultHasher::new();
    for file_path in file_paths.iter() {
        file_path.hash(&mut hasher);
    }
    format!("dynamodb_table_group_{:016x}", hasher.finish())
}

impl FilterOperand {
    fn field_name(&self) -> &str {
        match self {
            FilterOperand::Field(field_name) | FilterOperand::Size(field_name) => field_name,
        }
    }
}

fn apply_filter_expression(
    rows: Vec<Value>,
    filter_expression: &ParsedFilterExpression,
) -> Result<Vec<Value>, DynamoDbError> {
    let Some(expression) = filter_expression.expression.as_ref() else {
        return Ok(rows);
    };
    rows.into_iter()
        .filter_map(
            |row| match row_matches_filter_expression(&row, expression) {
                Ok(true) => Some(Ok(row)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            },
        )
        .collect()
}

fn row_matches_filter_expression(
    row: &Value,
    expression: &FilterNode,
) -> Result<bool, DynamoDbError> {
    let object = row
        .as_object()
        .ok_or_else(|| DynamoDbError::internal("Serving query returned a non-object row"))?;
    evaluate_filter_node(object, expression)
}

fn evaluate_filter_node(
    object: &Map<String, Value>,
    node: &FilterNode,
) -> Result<bool, DynamoDbError> {
    match node {
        FilterNode::Predicate(predicate) => evaluate_filter_predicate(object, predicate),
        FilterNode::And(nodes) => {
            for node in nodes.iter() {
                if !evaluate_filter_node(object, node)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        FilterNode::Or(nodes) => {
            for node in nodes.iter() {
                if evaluate_filter_node(object, node)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        FilterNode::Not(node) => Ok(!evaluate_filter_node(object, node)?),
    }
}

fn evaluate_filter_predicate(
    object: &Map<String, Value>,
    predicate: &FilterPredicate,
) -> Result<bool, DynamoDbError> {
    match &predicate.kind {
        FilterPredicateKind::AttributeExists => {
            Ok(object.contains_key(predicate.operand.field_name()))
        }
        FilterPredicateKind::AttributeNotExists => {
            Ok(!object.contains_key(predicate.operand.field_name()))
        }
        FilterPredicateKind::AttributeType(expected) => {
            let Some(value) = filter_operand_value(object, &predicate.operand) else {
                return Ok(false);
            };
            Ok(filter_value_type_name(value) == Some(expected.as_str()))
        }
        FilterPredicateKind::Contains(expected) => {
            let Some(value) = filter_operand_value(object, &predicate.operand) else {
                return Ok(false);
            };
            Ok(filter_value_contains(value, expected))
        }
        FilterPredicateKind::BeginsWith(prefix) => {
            let Some(value) = filter_operand_value(object, &predicate.operand) else {
                return Ok(false);
            };
            Ok(value
                .as_str()
                .map(|value| value.starts_with(prefix))
                .unwrap_or(false))
        }
        FilterPredicateKind::Eq(expected) => Ok(filter_operand_value(object, &predicate.operand)
            .map(|value| value == expected)
            .unwrap_or(false)),
        FilterPredicateKind::In(expected_values) => {
            Ok(filter_operand_value(object, &predicate.operand)
                .map(|value| expected_values.iter().any(|expected| expected == value))
                .unwrap_or(false))
        }
        FilterPredicateKind::Gt(expected) => {
            compare_filter_operand(object, &predicate.operand, expected, Ordering::Greater)
        }
        FilterPredicateKind::Gte(expected) => {
            compare_filter_operand_at_least(object, &predicate.operand, expected)
        }
        FilterPredicateKind::Lt(expected) => {
            compare_filter_operand(object, &predicate.operand, expected, Ordering::Less)
        }
        FilterPredicateKind::Lte(expected) => {
            compare_filter_operand_at_most(object, &predicate.operand, expected)
        }
        FilterPredicateKind::Between { start, end } => {
            Ok(
                compare_filter_operand_at_least(object, &predicate.operand, start)?
                    && compare_filter_operand_at_most(object, &predicate.operand, end)?,
            )
        }
    }
}

fn filter_operand_value<'a>(
    object: &'a Map<String, Value>,
    operand: &FilterOperand,
) -> Option<&'a Value> {
    match operand {
        FilterOperand::Field(field_name) => object.get(field_name),
        FilterOperand::Size(_) => None,
    }
}

fn filter_operand_scalar(object: &Map<String, Value>, operand: &FilterOperand) -> Option<Value> {
    match operand {
        FilterOperand::Field(field_name) => object.get(field_name).cloned(),
        FilterOperand::Size(field_name) => object
            .get(field_name)
            .and_then(filter_value_size)
            .map(|size| json!(size)),
    }
}

fn compare_filter_operand(
    object: &Map<String, Value>,
    operand: &FilterOperand,
    expected: &Value,
    desired: Ordering,
) -> Result<bool, DynamoDbError> {
    let Some(value) = filter_operand_scalar(object, operand) else {
        return Ok(false);
    };
    Ok(compare_scalars(&value, expected)
        .map(|ordering| ordering == desired)
        .unwrap_or(false))
}

fn compare_filter_operand_at_least(
    object: &Map<String, Value>,
    operand: &FilterOperand,
    expected: &Value,
) -> Result<bool, DynamoDbError> {
    let Some(value) = filter_operand_scalar(object, operand) else {
        return Ok(false);
    };
    Ok(compare_scalars(&value, expected)
        .map(|ordering| ordering != Ordering::Less)
        .unwrap_or(false))
}

fn compare_filter_operand_at_most(
    object: &Map<String, Value>,
    operand: &FilterOperand,
    expected: &Value,
) -> Result<bool, DynamoDbError> {
    let Some(value) = filter_operand_scalar(object, operand) else {
        return Ok(false);
    };
    Ok(compare_scalars(&value, expected)
        .map(|ordering| ordering != Ordering::Greater)
        .unwrap_or(false))
}

fn filter_value_contains(value: &Value, expected: &Value) -> bool {
    match value {
        Value::String(text) => expected
            .as_str()
            .map(|expected| text.contains(expected))
            .unwrap_or(false),
        Value::Array(values) => values.iter().any(|candidate| candidate == expected),
        Value::Object(map) => {
            if let Some(values) = map
                .get(DYNAMODB_STRING_SET_MARKER)
                .and_then(|value| value.as_array())
            {
                return values.iter().any(|candidate| candidate == expected);
            }
            if let Some(values) = map
                .get(DYNAMODB_NUMBER_SET_MARKER)
                .and_then(|value| value.as_array())
            {
                return values.iter().any(|candidate| candidate == expected);
            }
            if let Some(values) = map
                .get(DYNAMODB_BINARY_SET_MARKER)
                .and_then(|value| value.as_array())
            {
                return values.iter().any(|candidate| candidate == expected);
            }
            false
        }
        _ => false,
    }
}

fn filter_value_type_name(value: &Value) -> Option<&'static str> {
    match value {
        Value::Null => Some("NULL"),
        Value::Bool(_) => Some("BOOL"),
        Value::Number(_) => Some("N"),
        Value::String(_) => Some("S"),
        Value::Array(_) => Some("L"),
        Value::Object(map) => {
            if map.contains_key(DYNAMODB_BINARY_MARKER) {
                Some("B")
            } else if map.contains_key(DYNAMODB_STRING_SET_MARKER) {
                Some("SS")
            } else if map.contains_key(DYNAMODB_NUMBER_SET_MARKER) {
                Some("NS")
            } else if map.contains_key(DYNAMODB_BINARY_SET_MARKER) {
                Some("BS")
            } else {
                Some("M")
            }
        }
    }
}

fn filter_value_size(value: &Value) -> Option<usize> {
    match value {
        Value::String(text) => Some(text.len()),
        Value::Array(values) => Some(values.len()),
        Value::Object(map) => {
            if let Some(values) = map
                .get(DYNAMODB_STRING_SET_MARKER)
                .and_then(|value| value.as_array())
            {
                return Some(values.len());
            }
            if let Some(values) = map
                .get(DYNAMODB_NUMBER_SET_MARKER)
                .and_then(|value| value.as_array())
            {
                return Some(values.len());
            }
            if let Some(values) = map
                .get(DYNAMODB_BINARY_SET_MARKER)
                .and_then(|value| value.as_array())
            {
                return Some(values.len());
            }
            if let Some(binary) = map
                .get(DYNAMODB_BINARY_MARKER)
                .and_then(|value| value.as_str())
            {
                return Some(binary.len());
            }
            Some(map.len())
        }
        _ => None,
    }
}

fn project_rows(
    rows: Vec<Value>,
    projection: Option<&Vec<String>>,
) -> Result<Vec<Value>, DynamoDbError> {
    let Some(projection) = projection else {
        return Ok(rows);
    };
    rows.into_iter()
        .map(|row| project_row(&row, projection))
        .collect()
}

fn project_row(row: &Value, projection: &[String]) -> Result<Value, DynamoDbError> {
    let object = row
        .as_object()
        .ok_or_else(|| DynamoDbError::internal("Serving query returned a non-object row"))?;
    let mut projected = Map::new();
    for field in projection.iter() {
        if let Some(value) = object.get(field) {
            projected.insert(field.clone(), value.clone());
        }
    }
    Ok(Value::Object(projected))
}

fn json_to_dynamodb_attr(value: &Value) -> Result<Value, DynamoDbError> {
    match value {
        Value::Null => Ok(json!({ "NULL": true })),
        Value::Bool(boolean) => Ok(json!({ "BOOL": boolean })),
        Value::Number(number) => Ok(json!({ "N": number.to_string() })),
        Value::String(string) => Ok(json!({ "S": string })),
        Value::Array(values) => Ok(json!({
            "L": values
                .iter()
                .map(json_to_dynamodb_attr)
                .collect::<Result<Vec<_>, _>>()?
        })),
        Value::Object(map) => {
            if let Some(value) = map.get(DYNAMODB_BINARY_MARKER) {
                let value = value.as_str().ok_or_else(|| {
                    DynamoDbError::validation("$binary marker must contain a base64 string")
                })?;
                return Ok(json!({ "B": value }));
            }
            if let Some(value) = map.get(DYNAMODB_STRING_SET_MARKER) {
                return Ok(json!({ "SS": string_array_value(value, DYNAMODB_STRING_SET_MARKER)? }));
            }
            if let Some(value) = map.get(DYNAMODB_NUMBER_SET_MARKER) {
                return Ok(json!({ "NS": number_set_value(value)? }));
            }
            if let Some(value) = map.get(DYNAMODB_BINARY_SET_MARKER) {
                return Ok(json!({ "BS": string_array_value(value, DYNAMODB_BINARY_SET_MARKER)? }));
            }
            let mut converted = Map::new();
            for (key, value) in map.iter() {
                converted.insert(key.clone(), json_to_dynamodb_attr(value)?);
            }
            Ok(Value::Object(Map::from_iter([(
                "M".to_string(),
                Value::Object(converted),
            )])))
        }
    }
}

fn dynamodb_attr_to_json(value: &Value) -> Result<Value, DynamoDbError> {
    let object = value
        .as_object()
        .ok_or_else(|| DynamoDbError::validation("DynamoDB AttributeValue must be an object"))?;
    if let Some(value) = object.get("S") {
        return value
            .as_str()
            .map(|value| json!(value))
            .ok_or_else(|| DynamoDbError::validation("AttributeValue.S must be a string"));
    }
    if let Some(value) = object.get("N") {
        let text = value
            .as_str()
            .ok_or_else(|| DynamoDbError::validation("AttributeValue.N must be a string"))?;
        if let Ok(parsed) = text.parse::<i64>() {
            return Ok(json!(parsed));
        }
        if let Ok(parsed) = text.parse::<f64>() {
            return Ok(json!(parsed));
        }
        return Err(DynamoDbError::validation(format!(
            "AttributeValue.N could not be parsed as a number: {}",
            text
        )));
    }
    if let Some(value) = object.get("BOOL") {
        return value
            .as_bool()
            .map(|value| json!(value))
            .ok_or_else(|| DynamoDbError::validation("AttributeValue.BOOL must be a boolean"));
    }
    if let Some(value) = object.get("B") {
        return value
            .as_str()
            .map(|value| json!({ DYNAMODB_BINARY_MARKER: value }))
            .ok_or_else(|| DynamoDbError::validation("AttributeValue.B must be a string"));
    }
    if let Some(value) = object.get("NULL") {
        return if value.as_bool() == Some(true) {
            Ok(Value::Null)
        } else {
            Err(DynamoDbError::validation(
                "AttributeValue.NULL must be true",
            ))
        };
    }
    if let Some(value) = object.get("SS") {
        let values = value
            .as_array()
            .ok_or_else(|| DynamoDbError::validation("AttributeValue.SS must be an array"))?
            .iter()
            .map(|value| {
                value.as_str().map(|value| json!(value)).ok_or_else(|| {
                    DynamoDbError::validation("AttributeValue.SS members must be strings")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(json!({ DYNAMODB_STRING_SET_MARKER: values }));
    }
    if let Some(value) = object.get("NS") {
        let values = value
            .as_array()
            .ok_or_else(|| DynamoDbError::validation("AttributeValue.NS must be an array"))?
            .iter()
            .map(|value| {
                let text = value.as_str().ok_or_else(|| {
                    DynamoDbError::validation("AttributeValue.NS members must be strings")
                })?;
                if let Ok(parsed) = text.parse::<i64>() {
                    return Ok(json!(parsed));
                }
                if let Ok(parsed) = text.parse::<f64>() {
                    return Ok(json!(parsed));
                }
                Err(DynamoDbError::validation(format!(
                    "AttributeValue.NS member could not be parsed as a number: {}",
                    text
                )))
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(json!({ DYNAMODB_NUMBER_SET_MARKER: values }));
    }
    if let Some(value) = object.get("BS") {
        let values = value
            .as_array()
            .ok_or_else(|| DynamoDbError::validation("AttributeValue.BS must be an array"))?
            .iter()
            .map(|value| {
                value.as_str().map(|value| json!(value)).ok_or_else(|| {
                    DynamoDbError::validation("AttributeValue.BS members must be strings")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(json!({ DYNAMODB_BINARY_SET_MARKER: values }));
    }
    if let Some(value) = object.get("L") {
        let array = value
            .as_array()
            .ok_or_else(|| DynamoDbError::validation("AttributeValue.L must be an array"))?;
        return Ok(Value::Array(
            array
                .iter()
                .map(dynamodb_attr_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ));
    }
    if let Some(value) = object.get("M") {
        let map = value
            .as_object()
            .ok_or_else(|| DynamoDbError::validation("AttributeValue.M must be an object"))?;
        let mut converted = Map::new();
        for (key, value) in map.iter() {
            converted.insert(key.clone(), dynamodb_attr_to_json(value)?);
        }
        return Ok(Value::Object(converted));
    }
    Err(DynamoDbError::validation(
        "Unsupported DynamoDB AttributeValue shape",
    ))
}

fn string_array_value(value: &Value, marker: &str) -> Result<Vec<String>, DynamoDbError> {
    value
        .as_array()
        .ok_or_else(|| {
            DynamoDbError::validation(format!("{} marker must contain an array", marker))
        })?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(|value| value.to_string())
                .ok_or_else(|| {
                    DynamoDbError::validation(format!(
                        "{} marker array members must be strings",
                        marker
                    ))
                })
        })
        .collect()
}

fn number_set_value(value: &Value) -> Result<Vec<String>, DynamoDbError> {
    value
        .as_array()
        .ok_or_else(|| DynamoDbError::validation("$number_set marker must contain an array"))?
        .iter()
        .map(|value| match value {
            Value::Number(number) => Ok(number.to_string()),
            _ => Err(DynamoDbError::validation(
                "$number_set marker array members must be numbers",
            )),
        })
        .collect()
}

fn compare_scalars(left: &Value, right: &Value) -> Result<std::cmp::Ordering, DynamoDbError> {
    match (left, right) {
        (Value::String(left), Value::String(right)) => Ok(left.cmp(right)),
        (Value::Number(left), Value::Number(right)) => {
            let left = left
                .as_f64()
                .ok_or_else(|| DynamoDbError::validation("Left scalar was not numeric"))?;
            let right = right
                .as_f64()
                .ok_or_else(|| DynamoDbError::validation("Right scalar was not numeric"))?;
            left.partial_cmp(&right)
                .ok_or_else(|| DynamoDbError::validation("Numeric comparison was not well-defined"))
        }
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        _ => Err(DynamoDbError::validation(
            "ExclusiveStartKey comparison only supports scalar values",
        )),
    }
}

fn json_response(state: &State, status: StatusCode, body: &Value) -> gotham::hyper::Response<Body> {
    let mut response = create_response(
        state,
        status,
        mime::APPLICATION_JSON,
        serde_json::to_string(body).unwrap(),
    );
    response.headers_mut().insert(
        "x-amzn-requestid",
        "powdrr-dynamodb-request".parse().unwrap(),
    );
    response
}

fn dynamodb_error_response(state: &State, error: DynamoDbError) -> gotham::hyper::Response<Body> {
    json_response(
        state,
        error.status,
        &json!({
            "__type": error.type_name,
            "message": error.message,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        apply_filter_expression, dynamodb_attr_to_json, json_to_dynamodb_attr,
        parse_filter_expression, parse_key_condition_expression, primary_key_schema,
        query_select_fields, sha256_hex, sigv4_signature,
    };
    use chrono::Utc;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use gotham::mime;
    use gotham::test::TestServer;
    use powdrr_query_lib::data_contract::{
        DynamoDbTableConfig, FileSetPayload, IcebergMetadata, LicenseType, OrgCreds, OrgSettings,
        TableMetadataCheckpoint,
    };
    use powdrr_query_lib::schema_massager::extract_powdrr_schema;
    use powdrr_query_lib::serving_dataset::read_parquet_documents;
    use powdrr_query_lib::state_provider::STATE_PROVIDER;
    use powdrr_query_lib::test_api::{CompactionMode, IndexingMode, StateMode, TestProcessingMode};
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_attribute_round_trip_for_nested_values() {
        let value = json!({
            "tenant": "acme",
            "count": 4,
            "active": true,
            "nested": { "region": "us" },
            "items": ["a", 2]
        });
        let encoded = json_to_dynamodb_attr(&value).unwrap();
        assert_eq!(dynamodb_attr_to_json(&encoded).unwrap(), value);
    }

    #[test]
    fn test_attribute_round_trip_for_extended_dynamodb_markers() {
        let value = json!({
            "payload": { "$binary": "AQID" },
            "tags": { "$string_set": ["a", "b"] },
            "scores": { "$number_set": [1, 2.5] },
            "attachments": { "$binary_set": ["AQI=", "AwQ="] }
        });
        let encoded = json_to_dynamodb_attr(&value).unwrap();
        assert_eq!(dynamodb_attr_to_json(&encoded).unwrap(), value);
    }

    #[test]
    fn test_parse_query_between_expression() {
        let mut values = HashMap::new();
        values.insert(":tenant".to_string(), json!({ "S": "acme" }));
        values.insert(":start".to_string(), json!({ "N": "10" }));
        values.insert(":end".to_string(), json!({ "N": "20" }));

        let parsed = parse_key_condition_expression(
            "tenant = :tenant AND ts BETWEEN :start AND :end",
            &primary_key_schema(&DynamoDbTableConfig {
                partition_key: "tenant".to_string(),
                sort_key: Some("ts".to_string()),
                local_secondary_indexes: vec![],
                global_secondary_indexes: vec![],
            }),
            None,
            Some(&values),
        )
        .unwrap();

        assert_eq!(parsed.partition_value, json!("acme"));
        let filter = parsed.sort_filter.unwrap();
        assert_eq!(filter.field, "ts");
        assert_eq!(filter.gte, Some(json!(10)));
        assert_eq!(filter.lte, Some(json!(20)));
    }

    #[test]
    fn test_parse_query_begins_with_expression() {
        let mut values = HashMap::new();
        values.insert(":tenant".to_string(), json!({ "S": "acme" }));
        values.insert(":prefix".to_string(), json!({ "S": "evt-" }));

        let parsed = parse_key_condition_expression(
            "tenant = :tenant AND begins_with(event_id, :prefix)",
            &primary_key_schema(&DynamoDbTableConfig {
                partition_key: "tenant".to_string(),
                sort_key: Some("event_id".to_string()),
                local_secondary_indexes: vec![],
                global_secondary_indexes: vec![],
            }),
            None,
            Some(&values),
        )
        .unwrap();

        assert_eq!(parsed.partition_value, json!("acme"));
        let filter = parsed.sort_filter.unwrap();
        assert_eq!(filter.field, "event_id");
        assert_eq!(filter.gte, Some(json!("evt-")));
        assert_eq!(filter.lt, Some(json!("evt.")));
    }

    #[test]
    fn test_parse_filter_expression_supports_in_and_begins_with() {
        let mut values = HashMap::new();
        values.insert(":two".to_string(), json!({ "N": "2" }));
        values.insert(":three".to_string(), json!({ "N": "3" }));
        values.insert(":prefix".to_string(), json!({ "S": "evt-" }));

        let parsed = parse_filter_expression(
            Some(&"#count IN (:two, :three) AND begins_with(event_id, :prefix)".to_string()),
            Some(&HashMap::from([(
                "#count".to_string(),
                "count".to_string(),
            )])),
            Some(&values),
        )
        .unwrap();

        assert_eq!(
            parsed.filter_fields,
            vec!["count".to_string(), "event_id".to_string()]
        );
        let matched = apply_filter_expression(
            vec![json!({
                "count": 2,
                "event_id": "evt-2"
            })],
            &parsed,
        )
        .unwrap();
        assert_eq!(matched.len(), 1);
        let not_matched = apply_filter_expression(
            vec![json!({
                "count": 4,
                "event_id": "evt-2"
            })],
            &parsed,
        )
        .unwrap();
        assert!(not_matched.is_empty());
    }

    #[test]
    fn test_query_select_fields_skips_filter_fields_missing_from_schema() {
        let schema = extract_powdrr_schema(&json!({
            "tenant": "acme",
            "ts": 10,
            "event_id": "evt-1",
            "region": "us"
        }));
        let values = HashMap::new();
        let parsed = parse_filter_expression(
            Some(&"attribute_not_exists(deleted_at) AND attribute_exists(region)".to_string()),
            None,
            Some(&values),
        )
        .unwrap();

        let projection = query_select_fields(
            Some(&vec![
                "tenant".to_string(),
                "ts".to_string(),
                "event_id".to_string(),
            ]),
            &parsed,
            &primary_key_schema(&DynamoDbTableConfig {
                partition_key: "tenant".to_string(),
                sort_key: Some("ts".to_string()),
                local_secondary_indexes: vec![],
                global_secondary_indexes: vec![],
            }),
            &schema,
        )
        .unwrap();

        assert_eq!(
            projection,
            vec![
                "tenant".to_string(),
                "ts".to_string(),
                "event_id".to_string(),
                "region".to_string(),
            ]
        );
    }

    #[test]
    fn test_list_tables_request_rejects_unknown_fields() {
        let error = serde_json::from_value::<super::ListTablesRequest>(json!({
            "UnknownField": true
        }))
        .err()
        .expect("ListTablesRequest should reject unknown fields");
        assert!(
            error.to_string().contains("unknown field `UnknownField`"),
            "{}",
            error
        );
    }

    #[test]
    fn test_dynamodb_put_and_delete_item_smoke() {
        let test_server = TestServer::with_timeout(crate::router::router(true), 1000).unwrap();
        let temp_dir = tempfile::TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("events.parquet");
        write_smoke_parquet(&parquet_path);
        let dataset_path_string = parquet_path.display().to_string();
        let file_path = format!("file://{}", parquet_path.display());
        let file_size = fs::metadata(&parquet_path).unwrap().len();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            ensure_test_org_registered().await;
        });
        let dataset = runtime
            .block_on(read_parquet_documents(&dataset_path_string, Some(10)))
            .unwrap();
        let table_name = format!(
            "dynamo_write_smoke_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let checkpoint = TableMetadataCheckpoint {
            table_name: table_name.clone(),
            original_checkpoint_id: None,
            checkpoint_id: "checkpoint_0".to_string(),
            iceberg_metadata: Some(IcebergMetadata {
                table_schema: dataset.schema.clone(),
                snapshot_id: Some("snapshot_1".to_string()),
                files: FileSetPayload::single(file_path, file_size, dataset.schema.clone()),
                partition_spec: vec![],
                sort_order: vec![],
                column_names: dataset
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
                column_stats: vec![],
                access_artifacts: vec![],
                file_stats: vec![],
            }),
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema: dataset.schema.clone(),
        };
        let mut mode = TestProcessingMode::default();
        mode.state_mode = StateMode::Testing;
        mode.indexing_mode = IndexingMode::Disabled;
        mode.compaction_mode = CompactionMode::Disabled;
        let mode_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_and_processing_mode",
                serde_json::to_string(&mode).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(mode_response.status(), 200);
        let checkpoint_response = test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(checkpoint_response.status(), 200);
        let config_response = test_server
            .client()
            .put(
                format!("http://localhost/{}/_dynamodb/config", table_name),
                serde_json::to_string(&json!({
                    "partition_key": "tenant",
                    "sort_key": "ts"
                }))
                .unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(config_response.status(), 200);

        let key = json!({
            "tenant": { "S": "acme" },
            "ts": { "N": "10" }
        });
        let put_response = perform_dynamodb_request(
            &test_server,
            "PutItem",
            json!({
                "TableName": table_name,
                "Item": {
                    "tenant": { "S": "acme" },
                    "ts": { "N": "10" },
                    "event_id": { "S": "evt-1b" },
                    "region": { "S": "eu" },
                    "count": { "N": "7" }
                },
                "ConditionExpression": "attribute_exists(#pk)",
                "ExpressionAttributeNames": {
                    "#pk": "tenant"
                },
                "ReturnValues": "ALL_OLD"
            }),
        );
        assert_eq!(put_response.status(), 200);
        let put_body =
            serde_json::from_str::<serde_json::Value>(&put_response.read_utf8_body().unwrap())
                .unwrap();
        assert_eq!(
            dynamodb_attr_to_json(&put_body["Attributes"]["event_id"]).unwrap(),
            json!("evt-1")
        );

        let get_item_response = perform_dynamodb_request(
            &test_server,
            "GetItem",
            json!({
                "TableName": table_name,
                "Key": key.clone()
            }),
        );
        assert_eq!(get_item_response.status(), 200);
        let get_item_body =
            serde_json::from_str::<serde_json::Value>(&get_item_response.read_utf8_body().unwrap())
                .unwrap();
        assert_eq!(
            dynamodb_attr_to_json(&get_item_body["Item"]["event_id"]).unwrap(),
            json!("evt-1b")
        );
        assert_eq!(
            dynamodb_attr_to_json(&get_item_body["Item"]["count"]).unwrap(),
            json!(7)
        );

        let delete_response = perform_dynamodb_request(
            &test_server,
            "DeleteItem",
            json!({
                "TableName": table_name,
                "Key": key.clone(),
                "ConditionExpression": "attribute_exists(#pk)",
                "ExpressionAttributeNames": {
                    "#pk": "tenant"
                },
                "ReturnValues": "ALL_OLD"
            }),
        );
        assert_eq!(delete_response.status(), 200);
        let delete_body =
            serde_json::from_str::<serde_json::Value>(&delete_response.read_utf8_body().unwrap())
                .unwrap();
        assert_eq!(
            dynamodb_attr_to_json(&delete_body["Attributes"]["event_id"]).unwrap(),
            json!("evt-1b")
        );

        let missing_item_response = perform_dynamodb_request(
            &test_server,
            "GetItem",
            json!({
                "TableName": table_name,
                "Key": key
            }),
        );
        assert_eq!(missing_item_response.status(), 200);
        let missing_item_body = serde_json::from_str::<serde_json::Value>(
            &missing_item_response.read_utf8_body().unwrap(),
        )
        .unwrap();
        assert!(missing_item_body.get("Item").is_none());
    }

    #[test]
    #[ignore = "requires the full local cache/state-provider harness"]
    fn test_dynamodb_root_operations() {
        let redis_address = "127.0.0.1:6379".parse().unwrap();
        if std::net::TcpStream::connect_timeout(
            &redis_address,
            std::time::Duration::from_millis(200),
        )
        .is_err()
        {
            eprintln!(
                "Skipping DynamoDB root smoke test; Redis is not available on 127.0.0.1:6379"
            );
            return;
        }
        let test_server = TestServer::with_timeout(crate::router::router(true), 1000).unwrap();
        let dataset_path =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/flights.parquet");
        let dataset_path_string = dataset_path.display().to_string();
        let file_path = format!("file://{}", dataset_path.display());
        let file_size = fs::metadata(&dataset_path).unwrap().len();
        let dataset = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(read_parquet_documents(&dataset_path_string, Some(1)))
            .unwrap();
        let first_row = dataset.rows[0].as_object().unwrap().clone();
        let (partition_key, partition_value) = first_row
            .iter()
            .find(|(_, value)| !value.is_null() && !value.is_object() && !value.is_array())
            .map(|(key, value)| (key.clone(), value.clone()))
            .expect("fixture row must contain a scalar field");
        let table_name = format!(
            "dynamo_smoke_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let checkpoint = TableMetadataCheckpoint {
            table_name: table_name.clone(),
            original_checkpoint_id: None,
            checkpoint_id: "checkpoint_0".to_string(),
            iceberg_metadata: Some(IcebergMetadata {
                table_schema: dataset.schema.clone(),
                snapshot_id: Some("snapshot_1".to_string()),
                files: FileSetPayload::single(file_path, file_size, dataset.schema.clone()),
                partition_spec: vec![],
                sort_order: vec![],
                column_names: dataset
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
                column_stats: vec![],
                access_artifacts: vec![],
                file_stats: vec![],
            }),
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema: dataset.schema.clone(),
        };
        let mut mode = TestProcessingMode::default();
        mode.state_mode = StateMode::Testing;
        mode.indexing_mode = IndexingMode::Disabled;
        mode.compaction_mode = CompactionMode::Disabled;
        let mode_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_and_processing_mode",
                serde_json::to_string(&mode).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(mode_response.status(), 200);
        let checkpoint_response = test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(checkpoint_response.status(), 200);

        let config_response = test_server
            .client()
            .put(
                format!("http://localhost/{}/_dynamodb/config", table_name),
                serde_json::to_string(&json!({
                    "partition_key": partition_key.clone(),
                }))
                .unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(config_response.status(), 200);

        let describe_response = perform_dynamodb_request(
            &test_server,
            "DescribeTable",
            json!({ "TableName": table_name }),
        );
        assert_eq!(describe_response.status(), 200);
        let describe_body =
            serde_json::from_str::<serde_json::Value>(&describe_response.read_utf8_body().unwrap())
                .unwrap();
        assert_eq!(
            describe_body["Table"]["KeySchema"][0]["AttributeName"],
            json!(partition_key)
        );
        assert_eq!(describe_body["Table"]["KeySchema"][0]["KeyType"], "HASH");

        let list_tables_response = perform_dynamodb_request(&test_server, "ListTables", json!({}));
        assert_eq!(list_tables_response.status(), 200);
        let list_tables_body = serde_json::from_str::<serde_json::Value>(
            &list_tables_response.read_utf8_body().unwrap(),
        )
        .unwrap();
        assert!(
            list_tables_body["TableNames"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == &json!(table_name))
        );

        let list_tables_unknown_field_response =
            perform_dynamodb_request(&test_server, "ListTables", json!({ "UnknownField": true }));
        assert_eq!(
            list_tables_unknown_field_response.status(),
            400,
            "{}",
            list_tables_unknown_field_response.read_utf8_body().unwrap()
        );

        let get_item_response = perform_dynamodb_request(
            &test_server,
            "GetItem",
            json!({
                "TableName": table_name,
                "Key": HashMap::from([(
                    partition_key.clone(),
                    json_to_dynamodb_attr(&partition_value).unwrap(),
                )])
            }),
        );
        assert_eq!(get_item_response.status(), 200);
        let get_item_body =
            serde_json::from_str::<serde_json::Value>(&get_item_response.read_utf8_body().unwrap())
                .unwrap();
        assert_eq!(
            dynamodb_attr_to_json(&get_item_body["Item"][partition_key.as_str()]).unwrap(),
            partition_value
        );

        let query_response = perform_dynamodb_request(
            &test_server,
            "Query",
            json!({
                "TableName": table_name,
                "KeyConditionExpression": "#pk = :pk",
                "ExpressionAttributeNames": {
                    "#pk": partition_key.clone()
                },
                "ExpressionAttributeValues": {
                    ":pk": json_to_dynamodb_attr(&partition_value).unwrap()
                },
                "Limit": 1
            }),
        );
        let query_status = query_response.status();
        let query_body_text = query_response.read_utf8_body().unwrap();
        assert_eq!(query_status, 200, "{}", query_body_text);
        let query_body = serde_json::from_str::<serde_json::Value>(&query_body_text).unwrap();
        assert_eq!(query_body["Count"], 1);
        assert_eq!(
            dynamodb_attr_to_json(&query_body["Items"][0][partition_key.as_str()]).unwrap(),
            partition_value
        );
    }

    async fn ensure_test_org_registered() {
        if STATE_PROVIDER
            .lookup_secret_access_key(&"test".to_string())
            .await
            .unwrap()
            .as_deref()
            == Some("test")
        {
            return;
        }
        STATE_PROVIDER
            .create_org(&OrgSettings {
                org_id: "dynamodb_test_org".to_string(),
                license_type: LicenseType::Free,
                creds: vec![OrgCreds {
                    access_key_id: "test".to_string(),
                    secret_access_key: "test".to_string(),
                    nickname: Some("dynamodb-test".to_string()),
                }],
            })
            .await
            .unwrap();
    }

    fn write_smoke_parquet(path: &std::path::Path) {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("event_id", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(StringArray::from(vec!["acme", "acme"])) as ArrayRef,
                std::sync::Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                std::sync::Arc::new(StringArray::from(vec!["evt-1", "evt-2"])) as ArrayRef,
                std::sync::Arc::new(StringArray::from(vec!["us", "us"])) as ArrayRef,
                std::sync::Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .unwrap();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn perform_dynamodb_request(
        test_server: &TestServer,
        target: &str,
        body: serde_json::Value,
    ) -> gotham::test::TestResponse {
        let client = test_server.client();
        let mut request = client.post(
            "http://localhost/",
            serde_json::to_string(&body).unwrap(),
            mime::APPLICATION_JSON,
        );
        let amz_date = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let credential_date = Utc::now().format("%Y%m%d").to_string();
        let payload_hash = sha256_hex(serde_json::to_string(&body).unwrap().as_bytes());
        let signed_headers = "content-type;host;x-amz-date;x-amz-target";
        let canonical_request = format!(
            "POST\n/\n\ncontent-type:{}\nhost:{}\nx-amz-date:{}\nx-amz-target:{}\n\n{}\n{}",
            mime::APPLICATION_JSON,
            "localhost",
            amz_date,
            format!("DynamoDB_20120810.{}", target),
            signed_headers,
            payload_hash
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}/us-east-1/dynamodb/aws4_request\n{}",
            amz_date,
            credential_date,
            sha256_hex(canonical_request.as_bytes())
        );
        let signature = sigv4_signature(
            "test",
            &credential_date,
            "us-east-1",
            "dynamodb",
            &string_to_sign,
        )
        .unwrap();
        request.headers_mut().insert(
            "x-amz-target",
            format!("DynamoDB_20120810.{}", target).parse().unwrap(),
        );
        request
            .headers_mut()
            .insert("x-amz-date", amz_date.parse().unwrap());
        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            format!(
                "AWS4-HMAC-SHA256 Credential=test/{}/us-east-1/dynamodb/aws4_request,SignedHeaders={},Signature={}",
                credential_date, signed_headers, signature
            )
            .parse()
            .unwrap(),
        );
        request.perform().unwrap()
    }
}
