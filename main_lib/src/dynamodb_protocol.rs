use std::collections::HashMap;
use std::pin::Pin;

use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{body, Body};
use gotham::mime;
use gotham::state::{FromState, State};
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::data_contract::{
    CreateTable, DynamoDbGlobalSecondaryIndexConfig, DynamoDbTableConfig, ServingPattern,
    ServingTableConfig, TableDescription, TableMetadataCheckpoint,
};
use crate::elastic_search_endpoints::NamePathExtractor;
use crate::lakehouse_serving::{execute_serving_query, ServingQueryError, ServingQueryResponse};
use crate::peers::CheckpointDescriptor;
use crate::schema_massager::{PowdrrDataType, PowdrrSchema};
use crate::serving_plan::{
    ServingPredicate, ServingQueryClassification, ServingRequestPlan, ServingSort,
};
use crate::state_provider::STATE_PROVIDER;
use futures_util::future::FutureExt;

const DYNAMODB_TARGET_PREFIX: &str = "DynamoDB_20120810.";
const DYNAMODB_CONFIG_PATTERN_PREFIX: &str = "_dynamodb_";
const DEFAULT_LIST_TABLES_LIMIT: usize = 100;
const DEFAULT_QUERY_LIMIT: usize = 100;
const DYNAMODB_BINARY_MARKER: &str = "$binary";
const DYNAMODB_STRING_SET_MARKER: &str = "$string_set";
const DYNAMODB_NUMBER_SET_MARKER: &str = "$number_set";
const DYNAMODB_BINARY_SET_MARKER: &str = "$binary_set";

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
}

#[derive(Serialize)]
struct GetItemResponse {
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    item: Option<Map<String, Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")]
struct BatchGetItemRequest {
    request_items: HashMap<String, KeysAndAttributes>,
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
}

#[derive(Serialize)]
struct BatchGetItemResponse {
    #[serde(rename = "Responses")]
    responses: HashMap<String, Vec<Map<String, Value>>>,
    #[serde(rename = "UnprocessedKeys")]
    unprocessed_keys: Map<String, Value>,
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
}

struct DynamoDbTableContext {
    description: TableDescription,
    config: DynamoDbTableConfig,
    schema: PowdrrSchema,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DynamoDbKeySchemaConfig {
    partition_key: String,
    sort_key: Option<String>,
}

#[derive(Clone, Debug)]
struct ParsedFilterExpression {
    filters: Vec<ServingPredicate>,
    filter_fields: Vec<String>,
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
            let _meta = authenticate_request(&headers)?;
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
                "BatchGetItem" => handle_batch_get_item(payload).await?,
                "Query" => handle_query(payload).await?,
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
        },
    })
    .unwrap())
}

async fn handle_get_item(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<GetItemRequest>(payload).map_err(|error| {
        DynamoDbError::validation(format!("Invalid GetItem request: {}", error))
    })?;
    let context = load_dynamodb_table_context(&request.table_name).await?;
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

    Ok(serde_json::to_value(GetItemResponse { item }).unwrap())
}

async fn handle_batch_get_item(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<BatchGetItemRequest>(payload).map_err(|error| {
        DynamoDbError::validation(format!("Invalid BatchGetItem request: {}", error))
    })?;
    let mut responses = HashMap::new();
    for (table_name, keys_and_attributes) in request.request_items.iter() {
        let context = load_dynamodb_table_context(table_name).await?;
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
    }

    Ok(serde_json::to_value(BatchGetItemResponse {
        responses,
        unprocessed_keys: Map::new(),
    })
    .unwrap())
}

async fn handle_query(payload: Value) -> Result<Value, DynamoDbError> {
    let request = serde_json::from_value::<QueryRequest>(payload)
        .map_err(|error| DynamoDbError::validation(format!("Invalid Query request: {}", error)))?;
    let context = load_dynamodb_table_context(&request.table_name).await?;
    let (query_key_schema, unique_lookup) =
        query_target_key_schema(&context.config, request.index_name.as_deref())?;
    let requested_projection = parse_projection_expression(
        request.projection_expression.as_ref(),
        request.expression_attribute_names.as_ref(),
    )?;
    let filter_expression = parse_filter_expression(
        request.filter_expression.as_ref(),
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
    )?;
    let parsed_query = parse_key_condition_expression(
        &request.key_condition_expression,
        &query_key_schema,
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
        field: query_key_schema.partition_key.clone(),
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
            &query_key_schema,
            ascending,
            exclusive_start_key,
        )?;
    }

    let order_by = if sort_is_exact_eq {
        vec![]
    } else {
        query_key_schema
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
        query_key_schema.sort_key.is_none(),
        unique_lookup,
    );
    let query_projection = query_select_fields(
        requested_projection.as_ref(),
        &filter_expression,
        &query_key_schema,
    );

    let response = execute_fast_path_query(
        &request.table_name,
        ServingRequestPlan {
            select: query_projection,
            filters: key_filters,
            order_by,
            limit: Some(effective_limit),
            allow_slow_path: false,
            explain: false,
        },
    )
    .await?;

    let mut evaluated_rows = response.rows;
    let last_evaluated_key = if evaluated_rows.len() > page_limit {
        let key = row_to_key(&evaluated_rows[page_limit - 1], &query_key_schema)?;
        evaluated_rows.truncate(page_limit);
        Some(key)
    } else {
        None
    };
    let scanned_count = evaluated_rows.len();
    let filtered_rows = apply_filter_predicates(evaluated_rows, &filter_expression.filters)?;
    let projected_rows = project_rows(filtered_rows, requested_projection.as_ref())?;
    let rows = projected_rows
        .into_iter()
        .map(|row| json_row_to_dynamodb_item(&row))
        .collect::<Result<Vec<_>, _>>()?;
    let count = rows.len();
    Ok(serde_json::to_value(QueryResponse {
        items: rows,
        count,
        scanned_count,
        last_evaluated_key,
    })
    .unwrap())
}

fn service_error(error: crate::state_provider::ServiceApiError) -> DynamoDbError {
    DynamoDbError::internal(error.to_string())
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
    let checkpoint_id = STATE_PROVIDER
        .get_active_servable_checkpoint(&table_name.to_string())
        .await
        .map_err(service_error)?
        .ok_or_else(|| {
            DynamoDbError::resource_not_found(format!(
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
            DynamoDbError::resource_not_found(format!(
                "Checkpoint metadata was not found for table {}",
                table_name
            ))
        })?;
    schema_from_checkpoint(&checkpoint)
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

fn validate_dynamodb_config(
    schema: &PowdrrSchema,
    config: &DynamoDbTableConfig,
) -> Result<(), DynamoDbError> {
    validate_key_schema(schema, &primary_key_schema(config), "table")?;
    let mut seen_index_names = std::collections::HashSet::new();
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

fn query_target_key_schema(
    config: &DynamoDbTableConfig,
    index_name: Option<&str>,
) -> Result<(DynamoDbKeySchemaConfig, bool), DynamoDbError> {
    match index_name {
        Some(index_name) => config
            .global_secondary_indexes
            .iter()
            .find(|index| index.name == index_name)
            .map(|index| (secondary_index_key_schema(index), false))
            .ok_or_else(|| {
                DynamoDbError::validation(format!("Unknown global secondary index {}", index_name))
            }),
        None => Ok((primary_key_schema(config), true)),
    }
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

fn authenticate_request(headers: &HeaderMap) -> Result<DynamoDbRequestMeta, DynamoDbError> {
    let Some(auth_header) = headers.get(http::header::AUTHORIZATION) else {
        return Ok(DynamoDbRequestMeta {
            _access_key_id: None,
        });
    };
    let auth = auth_header
        .to_str()
        .map_err(|_| DynamoDbError::auth("Authorization header was not valid ASCII"))?;
    let access_key_id = auth
        .split("Credential=")
        .nth(1)
        .and_then(|remainder| remainder.split('/').next())
        .ok_or_else(|| DynamoDbError::auth("Authorization header did not contain a Credential"))?;
    Ok(DynamoDbRequestMeta {
        _access_key_id: Some(access_key_id.to_string()),
    })
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

fn query_select_fields(
    requested_projection: Option<&Vec<String>>,
    filter_expression: &ParsedFilterExpression,
    key_schema: &DynamoDbKeySchemaConfig,
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
        append_selected_field(&mut fields, field);
    }
    Some(fields)
}

fn append_selected_field(fields: &mut Vec<String>, field_name: &str) {
    if !fields.iter().any(|field| field == field_name) {
        fields.push(field_name.to_string());
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
            filters: vec![],
            filter_fields: vec![],
        });
    };
    let resolved_expression = resolve_expression_names(expression, names)?;
    let clauses = split_expression_clauses(&resolved_expression)?;
    let values = values.ok_or_else(|| {
        DynamoDbError::validation("ExpressionAttributeValues are required for FilterExpression")
    })?;
    let mut filters_by_field = HashMap::<String, ServingPredicate>::new();
    let mut filter_fields = vec![];
    for clause in clauses.iter() {
        let filter = parse_filter_clause(clause, values)?;
        let field_name = filter.field.clone();
        if let Some(existing) = filters_by_field.get_mut(&field_name) {
            merge_filter_predicate(existing, filter)?;
        } else {
            filter_fields.push(field_name.clone());
            filters_by_field.insert(field_name, filter);
        }
    }
    let filters = filter_fields
        .iter()
        .filter_map(|field_name| filters_by_field.remove(field_name))
        .collect();
    Ok(ParsedFilterExpression {
        filters,
        filter_fields,
    })
}

fn split_expression_clauses(expression: &str) -> Result<Vec<String>, DynamoDbError> {
    let uppercase = expression.to_ascii_uppercase();
    let bytes = uppercase.as_bytes();
    let mut clauses = vec![];
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut between_pending = false;
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b'(' => {
                depth += 1;
                index += 1;
            }
            b')' => {
                if depth == 0 {
                    return Err(DynamoDbError::validation(
                        "Expression had an unmatched closing parenthesis",
                    ));
                }
                depth -= 1;
                index += 1;
            }
            b' ' if depth == 0 && uppercase[index..].starts_with(" BETWEEN ") => {
                between_pending = true;
                index += " BETWEEN ".len();
            }
            b' ' if depth == 0 && uppercase[index..].starts_with(" AND ") => {
                if between_pending {
                    between_pending = false;
                    index += " AND ".len();
                    continue;
                }
                let clause = expression[start..index].trim();
                if clause.is_empty() {
                    return Err(DynamoDbError::validation(
                        "Expression contained an empty clause",
                    ));
                }
                clauses.push(clause.to_string());
                index += " AND ".len();
                start = index;
            }
            _ => {
                index += 1;
            }
        }
    }

    if depth != 0 {
        return Err(DynamoDbError::validation(
            "Expression had an unmatched opening parenthesis",
        ));
    }

    let trailing_clause = expression[start..].trim();
    if trailing_clause.is_empty() {
        return Err(DynamoDbError::validation(
            "Expression contained an empty clause",
        ));
    }
    clauses.push(trailing_clause.to_string());
    Ok(clauses)
}

fn parse_filter_clause(
    clause: &str,
    values: &HashMap<String, Value>,
) -> Result<ServingPredicate, DynamoDbError> {
    if let Some(rest) = clause.strip_prefix("begins_with") {
        let args = rest
            .trim()
            .strip_prefix('(')
            .and_then(|inner| inner.strip_suffix(')'))
            .ok_or_else(|| DynamoDbError::validation("begins_with must use function syntax"))?;
        let (field, value_token) = args.split_once(',').ok_or_else(|| {
            DynamoDbError::validation("begins_with must include a field and value")
        })?;
        return begins_with_predicate(field.trim(), value_token.trim(), values);
    }

    if let Some((left, right)) = clause.split_once(" BETWEEN ") {
        let (start, end) = right
            .split_once(" AND ")
            .ok_or_else(|| DynamoDbError::validation("BETWEEN must include two values"))?;
        return Ok(ServingPredicate {
            field: left.trim().to_string(),
            eq: None,
            in_values: None,
            gt: None,
            gte: Some(lookup_expression_value(start.trim(), values)?),
            lt: None,
            lte: Some(lookup_expression_value(end.trim(), values)?),
        });
    }

    if let Some((field, values_segment)) = clause.split_once(" IN ") {
        let values_segment = values_segment.trim();
        let value_list = values_segment
            .strip_prefix('(')
            .and_then(|inner| inner.strip_suffix(')'))
            .ok_or_else(|| DynamoDbError::validation("IN must use parenthesized value syntax"))?;
        let in_values = value_list
            .split(',')
            .map(|value_token| lookup_expression_value(value_token.trim(), values))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(ServingPredicate {
            field: field.trim().to_string(),
            eq: None,
            in_values: Some(in_values),
            gt: None,
            gte: None,
            lt: None,
            lte: None,
        });
    }

    for operator in ["<=", ">=", "<", ">", "="] {
        if let Some((left, right)) = clause.split_once(operator) {
            let mut predicate = ServingPredicate {
                field: left.trim().to_string(),
                eq: None,
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            };
            let value = lookup_expression_value(right.trim(), values)?;
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

    Err(DynamoDbError::validation(format!(
        "Unsupported FilterExpression clause {}",
        clause
    )))
}

fn begins_with_predicate(
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

fn merge_filter_predicate(
    existing: &mut ServingPredicate,
    candidate: ServingPredicate,
) -> Result<(), DynamoDbError> {
    if existing.field != candidate.field {
        return Err(DynamoDbError::validation(
            "Cannot merge filter predicates for different fields",
        ));
    }

    if existing.eq.is_some()
        || existing.in_values.is_some()
        || candidate.eq.is_some()
        || candidate.in_values.is_some()
    {
        return Err(DynamoDbError::validation(format!(
            "Multiple filter clauses for {} are not supported unless they tighten a range",
            existing.field
        )));
    }

    if let Some(value) = candidate.gt {
        tighten_lower_bound(existing, value)?;
    }
    if let Some(value) = candidate.gte {
        if existing.gt.is_none() {
            if let Some(existing_value) = existing.gte.as_ref() {
                if compare_scalars(existing_value, &value)? == std::cmp::Ordering::Less {
                    existing.gte = Some(value);
                }
            } else {
                existing.gte = Some(value);
            }
        }
    }
    if let Some(value) = candidate.lt {
        tighten_upper_bound(existing, value)?;
    }
    if let Some(value) = candidate.lte {
        if existing.lt.is_none() {
            if let Some(existing_value) = existing.lte.as_ref() {
                if compare_scalars(existing_value, &value)? == std::cmp::Ordering::Greater {
                    existing.lte = Some(value);
                }
            } else {
                existing.lte = Some(value);
            }
        }
    }
    Ok(())
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
        return begins_with_predicate(sort_key, value_token.trim(), values);
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
    ascending: bool,
    exclusive_start_key: &Map<String, Value>,
) -> Result<(), DynamoDbError> {
    let sort_key = match key_schema.sort_key.as_ref() {
        Some(sort_key) => sort_key,
        None => return Ok(()),
    };
    let parsed_key = parse_key_map(exclusive_start_key, key_schema)?;
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
) -> Result<Map<String, Value>, DynamoDbError> {
    let object = row
        .as_object()
        .ok_or_else(|| DynamoDbError::internal("Serving query returned a non-object row"))?;
    let mut key = Map::new();
    key.insert(
        key_schema.partition_key.clone(),
        json_to_dynamodb_attr(object.get(&key_schema.partition_key).ok_or_else(|| {
            DynamoDbError::internal(format!(
                "Response item was missing partition key {}",
                key_schema.partition_key
            ))
        })?)?,
    );
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        key.insert(
            sort_key.clone(),
            json_to_dynamodb_attr(object.get(sort_key).ok_or_else(|| {
                DynamoDbError::internal(format!("Response item was missing sort key {}", sort_key))
            })?)?,
        );
    }
    Ok(key)
}

fn apply_filter_predicates(
    rows: Vec<Value>,
    filters: &[ServingPredicate],
) -> Result<Vec<Value>, DynamoDbError> {
    if filters.is_empty() {
        return Ok(rows);
    }
    rows.into_iter()
        .filter_map(|row| match row_matches_filters(&row, filters) {
            Ok(true) => Some(Ok(row)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn row_matches_filters(row: &Value, filters: &[ServingPredicate]) -> Result<bool, DynamoDbError> {
    let object = row
        .as_object()
        .ok_or_else(|| DynamoDbError::internal("Serving query returned a non-object row"))?;
    for filter in filters.iter() {
        let Some(value) = object.get(&filter.field) else {
            return Ok(false);
        };
        if !value_matches_filter(value, filter)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn value_matches_filter(value: &Value, filter: &ServingPredicate) -> Result<bool, DynamoDbError> {
    if let Some(expected) = filter.eq.as_ref() {
        return Ok(value == expected);
    }
    if let Some(values) = filter.in_values.as_ref() {
        return Ok(values.iter().any(|candidate| candidate == value));
    }
    if let Some(lower) = filter.gt.as_ref() {
        if compare_scalars(value, lower)? != std::cmp::Ordering::Greater {
            return Ok(false);
        }
    }
    if let Some(lower) = filter.gte.as_ref() {
        let comparison = compare_scalars(value, lower)?;
        if comparison == std::cmp::Ordering::Less {
            return Ok(false);
        }
    }
    if let Some(upper) = filter.lt.as_ref() {
        if compare_scalars(value, upper)? != std::cmp::Ordering::Less {
            return Ok(false);
        }
    }
    if let Some(upper) = filter.lte.as_ref() {
        let comparison = compare_scalars(value, upper)?;
        if comparison == std::cmp::Ordering::Greater {
            return Ok(false);
        }
    }
    Ok(true)
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
        dynamodb_attr_to_json, json_to_dynamodb_attr, parse_filter_expression,
        parse_key_condition_expression, primary_key_schema,
    };
    use crate::data_contract::{
        DynamoDbTableConfig, FileSetPayload, IcebergMetadata, TableMetadataCheckpoint,
    };
    use crate::serving_dataset::read_parquet_documents;
    use gotham::mime;
    use gotham::test::TestServer;
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
        assert_eq!(parsed.filters.len(), 2);
        assert_eq!(parsed.filters[0].field, "count");
        assert_eq!(parsed.filters[0].in_values, Some(vec![json!(2), json!(3)]));
        assert_eq!(parsed.filters[1].field, "event_id");
        assert_eq!(parsed.filters[1].gte, Some(json!("evt-")));
        assert_eq!(parsed.filters[1].lt, Some(json!("evt.")));
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
    fn test_dynamodb_root_operations() {
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
                column_names: dataset
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
                column_stats: vec![],
                file_stats: vec![],
            }),
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema: dataset.schema.clone(),
        };
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
        assert!(list_tables_body["TableNames"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == &json!(table_name)));

        let list_tables_unknown_field_response = perform_dynamodb_request(
            &test_server,
            "ListTables",
            json!({ "UnknownField": true }),
        );
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
        request.headers_mut().insert(
            "x-amz-target",
            format!("DynamoDB_20120810.{}", target).parse().unwrap(),
        );
        request.perform().unwrap()
    }
}
