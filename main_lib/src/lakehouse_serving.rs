use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;

use futures::stream::{self, StreamExt};
use futures::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{body, Body};
use gotham::mime;
use gotham::state::{FromState, State};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::data_access::{self, execute_sql_async, load_file_as_table, path_to_table_name};
use crate::data_contract::{
    CreateTable, FileDescriptor, ServingPattern, ServingTableConfig, TableDescription,
    TableMetadataCheckpoint,
};
use crate::elastic_search_endpoints::NamePathExtractor;
use crate::peers::CheckpointDescriptor;
use crate::schema_massager::{PowdrrDataType, PowdrrSchema};
use crate::search_runtime::batches_to_serde_value;
use crate::serving_plan::{ServingPredicate, ServingQueryClassification, ServingRequestPlan};
use crate::state_provider::STATE_PROVIDER;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const MAX_IN_VALUES: usize = 32;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServingQueryResponse {
    pub table: String,
    pub classification: ServingQueryClassification,
    #[serde(default)]
    pub matched_pattern: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    pub files_considered: usize,
    pub files_selected: usize,
    pub estimated_bytes: u64,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub rows: Vec<Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServingConfigResponse {
    pub acknowledged: bool,
    pub table: String,
    pub serving: ServingTableConfig,
}

struct ServingExecutionContext {
    description: TableDescription,
    checkpoint: TableMetadataCheckpoint,
    schema: PowdrrSchema,
    files: Vec<FileDescriptor>,
    snapshot_id: Option<String>,
}

struct ServingPlan {
    classification: ServingQueryClassification,
    matched_pattern: Option<String>,
    reason: Option<String>,
    limit: usize,
    sql: String,
    files_considered: usize,
    files_selected: usize,
    estimated_bytes: u64,
}

pub fn get_serving_config(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state);
        match STATE_PROVIDER.describe_table(&path.name).await {
            Ok(Some(description)) => match description.serving {
                Some(serving) => {
                    let response = json_response(
                        &state,
                        StatusCode::OK,
                        &ServingConfigResponse {
                            acknowledged: true,
                            table: description.name,
                            serving,
                        },
                    );
                    Ok((state, response))
                }
                None => {
                    let response = json_response(
                        &state,
                        StatusCode::NOT_FOUND,
                        &json_error("No serving config declared for table"),
                    );
                    Ok((state, response))
                }
            },
            Ok(None) => {
                let response =
                    json_response(&state, StatusCode::NOT_FOUND, &json_error("Table not found"));
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn put_serving_config(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let body = match parse_json_body::<ServingTableConfig>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        let tags = match STATE_PROVIDER.describe_table(&path).await {
            Ok(Some(description)) => description.tags,
            Ok(None) => HashMap::new(),
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                return Ok((state, response));
            }
        };

        let request = CreateTable {
            name: path.clone(),
            tags,
            serving: Some(body.clone()),
            dynamodb: None,
        };

        match STATE_PROVIDER.upsert_table_metadata(&request).await {
            Ok(_) => {
                let response = json_response(
                    &state,
                    StatusCode::OK,
                    &ServingConfigResponse {
                        acknowledged: true,
                        table: path,
                        serving: body,
                    },
                );
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn serve_query(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let request = match parse_json_body::<ServingRequestPlan>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        match execute_serving_query(&path, request).await {
            Ok(response) => {
                let status = match response.classification {
                    ServingQueryClassification::FastPath => StatusCode::OK,
                    ServingQueryClassification::SlowPath => StatusCode::OK,
                    ServingQueryClassification::Rejected => StatusCode::UNPROCESSABLE_ENTITY,
                };
                let response = json_response(&state, status, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response =
                    json_response(&state, error.status, &json_error(&error.message));
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub async fn execute_serving_query(
    table_name: &str,
    request: ServingRequestPlan,
) -> Result<ServingQueryResponse, ServingQueryError> {
    let context = load_serving_context(table_name).await?;
    validate_request(&request, &context.schema)?;

    if context
        .checkpoint
        .deletes_metadata
        .as_ref()
        .map(|deletes| !deletes.files.is_empty())
        .unwrap_or(false)
    {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: ServingQueryClassification::Rejected,
            matched_pattern: None,
            snapshot_id: context.snapshot_id,
            reason: Some("Delete-aware serving is not implemented yet".to_string()),
            files_considered: context.files.len(),
            files_selected: 0,
            estimated_bytes: 0,
            sql: None,
            rows: vec![],
        });
    }

    let plan = plan_request(&context, &request)?;
    if request.explain {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: plan.classification,
            matched_pattern: plan.matched_pattern,
            snapshot_id: context.snapshot_id,
            reason: plan.reason,
            files_considered: plan.files_considered,
            files_selected: plan.files_selected,
            estimated_bytes: plan.estimated_bytes,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    if plan.classification == ServingQueryClassification::Rejected {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: plan.classification,
            matched_pattern: plan.matched_pattern,
            snapshot_id: context.snapshot_id,
            reason: plan.reason,
            files_considered: plan.files_considered,
            files_selected: plan.files_selected,
            estimated_bytes: plan.estimated_bytes,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    if plan.classification == ServingQueryClassification::SlowPath && !request.allow_slow_path {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: plan.classification,
            matched_pattern: plan.matched_pattern,
            snapshot_id: context.snapshot_id,
            reason: Some(
                "Query requires slow path. Set allow_slow_path=true to execute it".to_string(),
            ),
            files_considered: plan.files_considered,
            files_selected: plan.files_selected,
            estimated_bytes: plan.estimated_bytes,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    let mut rows = execute_plan(&context, &request, &plan.sql, plan.limit).await?;
    if request.order_by.is_empty() {
        rows.truncate(plan.limit);
    }

    Ok(ServingQueryResponse {
        table: context.description.name,
        classification: plan.classification,
        matched_pattern: plan.matched_pattern,
        snapshot_id: context.snapshot_id,
        reason: plan.reason,
        files_considered: plan.files_considered,
        files_selected: plan.files_selected,
        estimated_bytes: plan.estimated_bytes,
        sql: Some(plan.sql),
        rows,
    })
}

async fn load_serving_context(table_name: &str) -> Result<ServingExecutionContext, ServingQueryError> {
    let description = match STATE_PROVIDER.describe_table(&table_name.to_string()).await {
        Ok(Some(description)) => description,
        Ok(None) => {
            return Err(ServingQueryError::new(
                StatusCode::NOT_FOUND,
                "Table not found",
            ))
        }
        Err(error) => {
            return Err(ServingQueryError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            ))
        }
    };

    let checkpoint_id = match STATE_PROVIDER
        .get_latest_checkpoint(&description.name, None)
        .await
    {
        Ok(Some(checkpoint_id)) => checkpoint_id,
        Ok(None) => {
            return Err(ServingQueryError::new(
                StatusCode::NOT_FOUND,
                "No checkpoint available for table",
            ))
        }
        Err(error) => {
            return Err(ServingQueryError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            ))
        }
    };

    let checkpoint = match STATE_PROVIDER
        .get_checkpoint(CheckpointDescriptor::new(
            description.name.clone(),
            checkpoint_id,
        ))
        .await
    {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => {
            return Err(ServingQueryError::new(
                StatusCode::NOT_FOUND,
                "Checkpoint metadata was not found",
            ))
        }
        Err(error) => {
            return Err(ServingQueryError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            ))
        }
    };

    let iceberg_metadata = match checkpoint.iceberg_metadata.clone() {
        Some(metadata) => metadata,
        None => {
            return Err(ServingQueryError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Serving queries currently require Iceberg-backed storage",
            ))
        }
    };

    let files = iceberg_metadata.files.as_file_tuples();
    Ok(ServingExecutionContext {
        description,
        schema: iceberg_metadata.table_schema.clone(),
        snapshot_id: iceberg_metadata.snapshot_id.clone(),
        checkpoint,
        files,
    })
}

fn plan_request(
    context: &ServingExecutionContext,
    request: &ServingRequestPlan,
) -> Result<ServingPlan, ServingQueryError> {
    let serving = context
        .description
        .serving
        .clone()
        .unwrap_or_default();
    let limit = request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let sql = build_sql("{table}", request, limit)?;
    let estimated_bytes = context.files.iter().map(|file| file.size).sum::<u64>();

    if !request_supported(request) {
        return Ok(ServingPlan {
            classification: ServingQueryClassification::Rejected,
            matched_pattern: None,
            reason: Some("Unsupported serving query shape".to_string()),
            limit,
            sql,
            files_considered: context.files.len(),
            files_selected: 0,
            estimated_bytes: 0,
        });
    }

    if let Some(pattern) = serving
        .patterns
        .iter()
        .find(|pattern| request_matches_pattern(request, pattern, limit))
    {
        return Ok(ServingPlan {
            classification: ServingQueryClassification::FastPath,
            matched_pattern: Some(pattern.name.clone()),
            reason: None,
            limit,
            sql,
            files_considered: context.files.len(),
            files_selected: context.files.len(),
            estimated_bytes,
        });
    }

    Ok(ServingPlan {
        classification: ServingQueryClassification::SlowPath,
        matched_pattern: None,
        reason: Some("No declared serving pattern matched this query".to_string()),
        limit,
        sql,
        files_considered: context.files.len(),
        files_selected: context.files.len(),
        estimated_bytes,
    })
}

async fn execute_plan(
    context: &ServingExecutionContext,
    request: &ServingRequestPlan,
    sql: &str,
    limit: usize,
) -> Result<Vec<Value>, ServingQueryError> {
    let mut rows = vec![];
    let sql_template = sql.to_string();
    let concurrency = context.files.len().clamp(1, serving_file_parallelism());
    let mut results = stream::iter(context.files.iter().cloned().map(|file| {
        let local_sql_template = sql_template.clone();
        async move { execute_file_plan(file, &local_sql_template).await }
    }))
    .buffer_unordered(concurrency);

    while let Some(result) = results.next().await {
        merge_rows(&mut rows, result?, request, limit);
    }

    Ok(rows)
}

fn merge_rows(
    rows: &mut Vec<Value>,
    new_rows: Vec<Value>,
    request: &ServingRequestPlan,
    limit: usize,
) {
    rows.extend(new_rows);

    if let Some(sort) = request.order_by.first() {
        rows.sort_by(|left, right| {
            compare_row_values(left.get(&sort.field), right.get(&sort.field), sort.descending)
        });
        rows.truncate(limit);
        return;
    }

    // Bound unsorted fan-in while preserving enough slack for stable comparisons.
    let unsorted_cap = limit.saturating_mul(4).max(limit);
    if rows.len() > unsorted_cap {
        rows.truncate(unsorted_cap);
    }
}

fn serving_file_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().clamp(1, 8))
        .unwrap_or(4)
}

async fn execute_file_plan(
    file: FileDescriptor,
    sql_template: &str,
) -> Result<Vec<Value>, ServingQueryError> {
    let local_name = path_to_table_name(&file.file_path);
    data_access::reserve(&local_name, file.size, vec![]).await;
    let result = async {
        load_file_as_table(&local_name, &file.file_path, true, None)
            .await
            .map_err(|error| {
                ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &error.to_string())
            })?;

        let local_sql = sql_template.replace("{table}", &local_name);
        let batches = execute_sql_async(&local_sql).await.map_err(|error| {
            ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string())
        })?;
        let serde_result = batches_to_serde_value(&batches).await.map_err(|error| {
            ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.message)
        })?;
        Ok::<Vec<Value>, ServingQueryError>(serde_result.values)
    }
    .await;
    data_access::release(&local_name).await;
    result
}

fn validate_request(
    request: &ServingRequestPlan,
    schema: &PowdrrSchema,
) -> Result<(), ServingQueryError> {
    let schema_map = schema.to_map();
    let mut seen_filter_fields = HashSet::new();

    if request.order_by.len() > 1 {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            "Only one ORDER BY field is supported in the serving MVP",
        ));
    }

    if let Some(select_fields) = normalized_select(request.select.clone()) {
        for field in select_fields.iter() {
            if !schema_map.contains_key(field) {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Unknown select field {}", field),
                ));
            }
        }
    }

    for sort in request.order_by.iter() {
        if !schema_map.contains_key(&sort.field) {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Unknown sort field {}", sort.field),
            ));
        }
    }

    for filter in request.filters.iter() {
        if !seen_filter_fields.insert(filter.field.clone()) {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                "Duplicate filters on the same field are not supported",
            ));
        }
        let field = match schema_map.get(&filter.field) {
            Some(field) => field,
            None => {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Unknown filter field {}", filter.field),
                ))
            }
        };
        validate_filter(filter, &field.data_type)?;
    }

    if request.limit.unwrap_or(DEFAULT_LIMIT) > MAX_LIMIT {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Limit must be <= {}", MAX_LIMIT),
        ));
    }

    Ok(())
}

fn validate_filter(
    filter: &ServingPredicate,
    data_type: &PowdrrDataType,
) -> Result<(), ServingQueryError> {
    let eq_like = usize::from(filter.eq.is_some()) + usize::from(filter.in_values.is_some());
    let range_like = usize::from(filter.gt.is_some())
        + usize::from(filter.gte.is_some())
        + usize::from(filter.lt.is_some())
        + usize::from(filter.lte.is_some());

    if eq_like == 0 && range_like == 0 {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Filter for {} has no operator", filter.field),
        ));
    }
    if eq_like > 1 || (eq_like > 0 && range_like > 0) {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Filter for {} mixes incompatible operators", filter.field),
        ));
    }
    if let Some(values) = filter.in_values.as_ref() {
        if values.is_empty() || values.len() > MAX_IN_VALUES {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("IN filter for {} must have 1-{} values", filter.field, MAX_IN_VALUES),
            ));
        }
        for value in values.iter() {
            validate_literal(value, data_type, &filter.field)?;
        }
    }
    if let Some(value) = filter.eq.as_ref() {
        validate_literal(value, data_type, &filter.field)?;
    }
    for range_value in [&filter.gt, &filter.gte, &filter.lt, &filter.lte] {
        if let Some(value) = range_value.as_ref() {
            validate_literal(value, data_type, &filter.field)?;
        }
    }

    Ok(())
}

fn validate_literal(
    value: &Value,
    data_type: &PowdrrDataType,
    field_name: &str,
) -> Result<(), ServingQueryError> {
    match data_type {
        PowdrrDataType::Boolean => {
            if !value.is_boolean() {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Field {} expects a boolean literal", field_name),
                ));
            }
        }
        PowdrrDataType::Float | PowdrrDataType::Integer => {
            if !value.is_number() {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Field {} expects a numeric literal", field_name),
                ));
            }
        }
        PowdrrDataType::String => {
            if !value.is_string() {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Field {} expects a string literal", field_name),
                ));
            }
        }
        PowdrrDataType::Object(_) | PowdrrDataType::Array(_) | PowdrrDataType::Null => {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Field {} is not supported by the serving MVP", field_name),
            ))
        }
    }
    Ok(())
}

fn request_supported(request: &ServingRequestPlan) -> bool {
    request.order_by.len() <= 1
}

fn request_matches_pattern(
    request: &ServingRequestPlan,
    pattern: &ServingPattern,
    limit: usize,
) -> bool {
    if pattern
        .max_limit
        .map(|max_limit| limit > max_limit as usize)
        .unwrap_or(false)
    {
        return false;
    }

    let select_fields = normalized_select(request.select.clone());
    if let (Some(select_fields), Some(projection)) = (select_fields.as_ref(), pattern.projection.as_ref()) {
        if !select_fields.iter().all(|field| projection.contains(field)) {
            return false;
        }
    } else if request.select.is_none() && pattern.projection.is_some() {
        return false;
    }

    if let Some(order_field) = pattern.order_field.as_ref() {
        match request.order_by.first() {
            Some(sort) if &sort.field == order_field && sort.descending == pattern.descending => {}
            _ => return false,
        }
    } else if !request.order_by.is_empty() {
        return false;
    }

    let eq_field_set = pattern.eq_fields.iter().cloned().collect::<HashSet<_>>();
    let mut seen_fields = HashSet::new();
    for filter in request.filters.iter() {
        seen_fields.insert(filter.field.clone());
        let is_eq = filter.eq.is_some() || filter.in_values.is_some();
        let is_range = filter.gt.is_some() || filter.gte.is_some() || filter.lt.is_some() || filter.lte.is_some();
        if is_eq && !eq_field_set.contains(&filter.field) {
            return false;
        }
        if is_range && pattern.range_field.as_deref() != Some(filter.field.as_str()) {
            return false;
        }
    }

    if !pattern.eq_fields.iter().all(|field| seen_fields.contains(field)) {
        return false;
    }
    if let Some(range_field) = pattern.range_field.as_ref() {
        if !seen_fields.contains(range_field) {
            return false;
        }
    }

    true
}

fn build_sql(
    table_name: &str,
    request: &ServingRequestPlan,
    limit: usize,
) -> Result<String, ServingQueryError> {
    let select = match normalized_select(request.select.clone()) {
        Some(select_fields) => select_fields
            .iter()
            .map(|field| format!("\"{}\"", escape_identifier(field)))
            .collect::<Vec<_>>()
            .join(", "),
        None => "*".to_string(),
    };

    let where_clauses = request
        .filters
        .iter()
        .map(sql_for_filter)
        .collect::<Result<Vec<_>, _>>()?;
    let mut sql = format!("SELECT {} FROM {} t", select, table_name);
    if !where_clauses.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_clauses.join(" AND ")));
    }
    if let Some(sort) = request.order_by.first() {
        sql.push_str(&format!(
            " ORDER BY \"{}\" {}",
            escape_identifier(&sort.field),
            if sort.descending { "DESC" } else { "ASC" }
        ));
    }
    sql.push_str(&format!(" LIMIT {}", limit));
    Ok(sql)
}

fn sql_for_filter(filter: &ServingPredicate) -> Result<String, ServingQueryError> {
    let field = format!("\"{}\"", escape_identifier(&filter.field));
    if let Some(value) = filter.eq.as_ref() {
        return Ok(format!("{} = {}", field, sql_literal(value)?));
    }
    if let Some(values) = filter.in_values.as_ref() {
        let sql_values = values
            .iter()
            .map(sql_literal)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(format!("{} IN ({})", field, sql_values.join(", ")));
    }

    let mut parts = vec![];
    if let Some(value) = filter.gt.as_ref() {
        parts.push(format!("{} > {}", field, sql_literal(value)?));
    }
    if let Some(value) = filter.gte.as_ref() {
        parts.push(format!("{} >= {}", field, sql_literal(value)?));
    }
    if let Some(value) = filter.lt.as_ref() {
        parts.push(format!("{} < {}", field, sql_literal(value)?));
    }
    if let Some(value) = filter.lte.as_ref() {
        parts.push(format!("{} <= {}", field, sql_literal(value)?));
    }
    Ok(parts.join(" AND "))
}

fn sql_literal(value: &Value) -> Result<String, ServingQueryError> {
    match value {
        Value::String(text) => Ok(format!("'{}'", text.replace('\'', "''"))),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(if *boolean { "TRUE".to_string() } else { "FALSE".to_string() }),
        _ => Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            "Only scalar literals are supported in serving queries",
        )),
    }
}

fn normalized_select(select: Option<Vec<String>>) -> Option<Vec<String>> {
    match select {
        Some(fields) if fields.len() == 1 && fields[0] == "*" => None,
        Some(fields) => Some(fields),
        None => None,
    }
}

fn compare_row_values(left: Option<&Value>, right: Option<&Value>, descending: bool) -> Ordering {
    let ordering = compare_values(left.unwrap_or(&Value::Null), right.unwrap_or(&Value::Null));
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => {
            if let (Some(left_number), Some(right_number)) = (left.as_f64(), right.as_f64()) {
                return left_number
                    .partial_cmp(&right_number)
                    .unwrap_or(Ordering::Equal);
            }
            if let (Some(left_string), Some(right_string)) = (left.as_str(), right.as_str()) {
                return left_string.cmp(right_string);
            }
            if let (Some(left_bool), Some(right_bool)) = (left.as_bool(), right.as_bool()) {
                return left_bool.cmp(&right_bool);
            }
            left.to_string().cmp(&right.to_string())
        }
    }
}

fn escape_identifier(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}

async fn parse_json_body<T: for<'de> Deserialize<'de>>(
    state: &mut State,
) -> Result<T, String> {
    let valid_body = body::to_bytes(Body::take_from(state))
        .await
        .map_err(|error| error.to_string())?;
    serde_json::from_slice::<T>(&valid_body).map_err(|error| error.to_string())
}

fn json_response<T: Serialize>(state: &State, status: StatusCode, body: &T) -> gotham::hyper::Response<Body> {
    create_response(
        state,
        status,
        mime::APPLICATION_JSON,
        serde_json::to_string(body).unwrap(),
    )
}

fn json_error(message: &str) -> Value {
    serde_json::json!({ "error": message })
}

#[derive(Debug)]
pub struct ServingQueryError {
    pub status: StatusCode,
    pub message: String,
}

impl ServingQueryError {
    fn new(status: StatusCode, message: &str) -> Self {
        Self {
            status,
            message: message.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_sql, request_matches_pattern,
    };
    use crate::data_contract::ServingPattern;
    use crate::serving_plan::{ServingPredicate, ServingRequestPlan, ServingSort};
    use serde_json::json;

    #[test]
    fn test_build_sql_for_range_top_n() {
        let sql = build_sql(
            "{table}",
            &ServingRequestPlan {
                select: Some(vec!["title".to_string()]),
                filters: vec![ServingPredicate {
                    field: "snippet".to_string(),
                    eq: Some(json!("hello")),
                    in_values: None,
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                }],
                order_by: vec![ServingSort {
                    field: "title".to_string(),
                    descending: false,
                }],
                limit: Some(5),
                allow_slow_path: false,
                explain: false,
            },
            5,
        )
        .unwrap();

        assert_eq!(
            sql,
            "SELECT \"title\" FROM {table} t WHERE \"snippet\" = 'hello' ORDER BY \"title\" ASC LIMIT 5"
        );
    }

    #[test]
    fn test_request_matches_pattern() {
        let request = ServingRequestPlan {
            select: Some(vec!["title".to_string()]),
            filters: vec![],
            order_by: vec![ServingSort {
                field: "title".to_string(),
                descending: false,
            }],
            limit: Some(3),
            allow_slow_path: false,
            explain: false,
        };
        let pattern = ServingPattern {
            name: "recent_titles".to_string(),
            eq_fields: vec![],
            range_field: None,
            order_field: Some("title".to_string()),
            descending: false,
            max_limit: Some(10),
            projection: Some(vec!["title".to_string()]),
        };

        assert!(request_matches_pattern(&request, &pattern, 3));
    }
}
