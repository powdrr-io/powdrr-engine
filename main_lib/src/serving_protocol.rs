use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::serving_plan::{ServingPredicate, ServingRequestPlan};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct MongoFindCommand {
    pub find: String,
    #[serde(default = "default_mongo_filter")]
    pub filter: Value,
    #[serde(default)]
    pub projection: Option<Value>,
    #[serde(default)]
    pub sort: Option<Value>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub skip: Option<u64>,
    #[serde(default, rename = "batchSize")]
    pub batch_size: Option<i64>,
    #[serde(default, rename = "singleBatch")]
    pub single_batch: Option<bool>,
    #[serde(default, rename = "noCursorTimeout")]
    pub no_cursor_timeout: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct MongoFindRequest {
    pub filter: Value,
    #[serde(default)]
    pub projection: Option<Value>,
    #[serde(default)]
    pub sort: Option<Value>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongoProtocolError {
    pub message: String,
}

impl MongoProtocolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for MongoProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for MongoProtocolError {}

fn default_mongo_filter() -> Value {
    Value::Object(Map::new())
}

pub fn to_elasticsearch_search(plan: &ServingRequestPlan) -> Value {
    let mut body = Map::new();
    if let Some(select) = plan.select.as_ref() {
        body.insert("_source".to_string(), json!(select));
    }
    if let Some(limit) = plan.limit {
        body.insert("size".to_string(), json!(limit));
    }
    if !plan.order_by.is_empty() {
        body.insert(
            "sort".to_string(),
            Value::Array(
                plan.order_by
                    .iter()
                    .map(|sort| {
                        json!({
                            sort.field.clone(): {
                                "order": if sort.descending { "desc" } else { "asc" }
                            }
                        })
                    })
                    .collect(),
            ),
        );
    }

    let query = if plan.filters.is_empty() {
        json!({ "match_all": {} })
    } else {
        json!({
            "bool": {
                "filter": plan.filters.iter().map(elasticsearch_filter).collect::<Vec<_>>(),
            }
        })
    };
    body.insert("query".to_string(), query);
    Value::Object(body)
}

pub fn to_mongodb_find(plan: &ServingRequestPlan) -> MongoFindRequest {
    let mut filter = Map::new();
    for predicate in plan.filters.iter() {
        filter.insert(predicate.field.clone(), mongodb_filter(predicate));
    }

    let projection = Some(match plan.select.as_ref() {
        Some(fields) => {
            let mut projection = fields
                .iter()
                .map(|field| (field.clone(), json!(1)))
                .collect::<Map<String, Value>>();
            projection.insert("_id".to_string(), json!(0));
            Value::Object(projection)
        }
        None => json!({ "_id": 0 }),
    });

    let sort = if plan.order_by.is_empty() {
        None
    } else {
        Some(Value::Object(
            plan.order_by
                .iter()
                .map(|item| {
                    (
                        item.field.clone(),
                        json!(if item.descending { -1 } else { 1 }),
                    )
                })
                .collect::<Map<String, Value>>(),
        ))
    };

    MongoFindRequest {
        filter: Value::Object(filter),
        projection,
        sort,
        limit: plan.limit,
    }
}

pub fn from_mongodb_find(
    command: &MongoFindCommand,
) -> Result<ServingRequestPlan, MongoProtocolError> {
    if command.find.trim().is_empty() {
        return Err(MongoProtocolError::new(
            "find command must include a non-empty collection name",
        ));
    }
    if command.skip.unwrap_or(0) != 0 {
        return Err(MongoProtocolError::new(
            "skip is not supported by the serving request planner yet",
        ));
    }

    Ok(ServingRequestPlan {
        select: parse_mongodb_projection(command.projection.as_ref())?,
        filters: parse_mongodb_filter_document(&command.filter)?,
        aggregate: None,
        order_by: parse_mongodb_sort(command.sort.as_ref())?,
        limit: parse_mongodb_limit(command.limit)?,
        allow_slow_path: false,
        explain: false,
    })
}

fn elasticsearch_filter(predicate: &ServingPredicate) -> Value {
    if let Some(eq) = predicate.eq.as_ref() {
        return json!({
            "term": {
                predicate.field.clone(): eq.clone(),
            }
        });
    }
    if let Some(values) = predicate.in_values.as_ref() {
        return json!({
            "terms": {
                predicate.field.clone(): values.clone(),
            }
        });
    }

    let mut range = Map::new();
    if let Some(value) = predicate.gt.as_ref() {
        range.insert("gt".to_string(), value.clone());
    }
    if let Some(value) = predicate.gte.as_ref() {
        range.insert("gte".to_string(), value.clone());
    }
    if let Some(value) = predicate.lt.as_ref() {
        range.insert("lt".to_string(), value.clone());
    }
    if let Some(value) = predicate.lte.as_ref() {
        range.insert("lte".to_string(), value.clone());
    }
    json!({
        "range": {
            predicate.field.clone(): Value::Object(range),
        }
    })
}

fn mongodb_filter(predicate: &ServingPredicate) -> Value {
    if let Some(eq) = predicate.eq.as_ref() {
        return eq.clone();
    }
    if let Some(values) = predicate.in_values.as_ref() {
        return json!({ "$in": values.clone() });
    }

    let mut range = Map::new();
    if let Some(value) = predicate.gt.as_ref() {
        range.insert("$gt".to_string(), value.clone());
    }
    if let Some(value) = predicate.gte.as_ref() {
        range.insert("$gte".to_string(), value.clone());
    }
    if let Some(value) = predicate.lt.as_ref() {
        range.insert("$lt".to_string(), value.clone());
    }
    if let Some(value) = predicate.lte.as_ref() {
        range.insert("$lte".to_string(), value.clone());
    }
    Value::Object(range)
}

fn parse_mongodb_filter_document(
    value: &Value,
) -> Result<Vec<ServingPredicate>, MongoProtocolError> {
    let document = value
        .as_object()
        .ok_or_else(|| MongoProtocolError::new("Mongo filter must be a document"))?;
    let mut predicates = vec![];
    for (field, value) in document.iter() {
        match field.as_str() {
            "$and" => {
                let clauses = value
                    .as_array()
                    .ok_or_else(|| MongoProtocolError::new("$and must be an array of documents"))?;
                for clause in clauses.iter() {
                    predicates.extend(parse_mongodb_filter_document(clause)?);
                }
            }
            operator if operator.starts_with('$') => {
                return Err(MongoProtocolError::new(format!(
                    "Unsupported top-level Mongo filter operator {}",
                    operator
                )));
            }
            _ => predicates.push(parse_mongodb_field_predicate(field, value)?),
        }
    }
    Ok(predicates)
}

fn parse_mongodb_field_predicate(
    field: &str,
    value: &Value,
) -> Result<ServingPredicate, MongoProtocolError> {
    let mut predicate = ServingPredicate {
        field: field.to_string(),
        eq: None,
        in_values: None,
        gt: None,
        gte: None,
        lt: None,
        lte: None,
    };

    let Some(value_map) = value.as_object() else {
        predicate.eq = Some(value.clone());
        return Ok(predicate);
    };

    if value_map.is_empty() || value_map.keys().all(|key| !key.starts_with('$')) {
        predicate.eq = Some(value.clone());
        return Ok(predicate);
    }

    if value_map.keys().any(|key| !key.starts_with('$')) {
        return Err(MongoProtocolError::new(format!(
            "Mongo filter field {} mixed operator and literal keys",
            field
        )));
    }

    for (operator, operand) in value_map.iter() {
        match operator.as_str() {
            "$eq" => predicate.eq = Some(operand.clone()),
            "$in" => {
                predicate.in_values = Some(
                    operand
                        .as_array()
                        .ok_or_else(|| {
                            MongoProtocolError::new(format!(
                                "Mongo filter field {} uses $in with a non-array operand",
                                field
                            ))
                        })?
                        .clone(),
                );
            }
            "$gt" => predicate.gt = Some(operand.clone()),
            "$gte" => predicate.gte = Some(operand.clone()),
            "$lt" => predicate.lt = Some(operand.clone()),
            "$lte" => predicate.lte = Some(operand.clone()),
            unsupported => {
                return Err(MongoProtocolError::new(format!(
                    "Unsupported Mongo filter operator {} for field {}",
                    unsupported, field
                )));
            }
        }
    }

    if predicate.eq.is_some()
        && (predicate.in_values.is_some()
            || predicate.gt.is_some()
            || predicate.gte.is_some()
            || predicate.lt.is_some()
            || predicate.lte.is_some())
    {
        return Err(MongoProtocolError::new(format!(
            "Mongo filter field {} mixes $eq with other operators",
            field
        )));
    }
    if predicate.in_values.is_some()
        && (predicate.gt.is_some()
            || predicate.gte.is_some()
            || predicate.lt.is_some()
            || predicate.lte.is_some())
    {
        return Err(MongoProtocolError::new(format!(
            "Mongo filter field {} mixes $in with range operators",
            field
        )));
    }

    Ok(predicate)
}

fn parse_mongodb_projection(
    projection: Option<&Value>,
) -> Result<Option<Vec<String>>, MongoProtocolError> {
    let Some(projection) = projection else {
        return Ok(None);
    };
    let projection_map = projection
        .as_object()
        .ok_or_else(|| MongoProtocolError::new("Mongo projection must be a document"))?;

    let mut included_fields = vec![];
    let mut saw_inclusion = false;
    let mut saw_exclusion = false;
    for (field, value) in projection_map.iter() {
        let mode = mongodb_projection_mode(value).ok_or_else(|| {
            MongoProtocolError::new(format!(
                "Mongo projection field {} must be 0/1 or false/true",
                field
            ))
        })?;
        match (field.as_str(), mode) {
            ("_id", false) => {}
            (_, true) => {
                saw_inclusion = true;
                included_fields.push(field.clone());
            }
            (_, false) => saw_exclusion = true,
        }
    }

    if saw_exclusion {
        return Err(MongoProtocolError::new(
            "Mongo exclusion projection is not supported yet except for `_id: 0`",
        ));
    }
    if !saw_inclusion {
        return Ok(None);
    }
    Ok(Some(included_fields))
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

fn parse_mongodb_sort(
    sort: Option<&Value>,
) -> Result<Vec<crate::serving_plan::ServingSort>, MongoProtocolError> {
    let Some(sort) = sort else {
        return Ok(vec![]);
    };
    let sort_map = sort
        .as_object()
        .ok_or_else(|| MongoProtocolError::new("Mongo sort must be a document"))?;
    let mut order_by = vec![];
    for (field, value) in sort_map.iter() {
        let direction = value
            .as_i64()
            .or_else(|| value.as_u64().map(|number| number as i64));
        match direction {
            Some(1) => order_by.push(crate::serving_plan::ServingSort {
                field: field.clone(),
                descending: false,
            }),
            Some(-1) => order_by.push(crate::serving_plan::ServingSort {
                field: field.clone(),
                descending: true,
            }),
            _ => {
                return Err(MongoProtocolError::new(format!(
                    "Mongo sort field {} must use 1 or -1",
                    field
                )));
            }
        }
    }
    Ok(order_by)
}

fn parse_mongodb_limit(limit: Option<i64>) -> Result<Option<usize>, MongoProtocolError> {
    match limit {
        None | Some(0) => Ok(None),
        Some(value) if value > 0 => Ok(Some(value as usize)),
        Some(_) => Err(MongoProtocolError::new(
            "Mongo limit must be a non-negative integer",
        )),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::serving_plan::{ServingPredicate, ServingRequestPlan, ServingSort};

    use super::{
        MongoFindCommand, MongoFindRequest, from_mongodb_find, to_elasticsearch_search,
        to_mongodb_find,
    };

    #[test]
    fn test_to_elasticsearch_search() {
        let plan = ServingRequestPlan {
            select: Some(vec!["title".to_string(), "price".to_string()]),
            filters: vec![
                ServingPredicate {
                    field: "tenant".to_string(),
                    eq: Some(json!("acme")),
                    in_values: None,
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                },
                ServingPredicate {
                    field: "price".to_string(),
                    eq: None,
                    in_values: None,
                    gt: None,
                    gte: Some(json!(10)),
                    lt: Some(json!(25)),
                    lte: None,
                },
            ],
            aggregate: None,
            order_by: vec![ServingSort {
                field: "price".to_string(),
                descending: true,
            }],
            limit: Some(25),
            allow_slow_path: false,
            explain: false,
        };

        assert_eq!(
            to_elasticsearch_search(&plan),
            json!({
                "_source": ["title", "price"],
                "size": 25,
                "sort": [{ "price": { "order": "desc" } }],
                "query": {
                    "bool": {
                        "filter": [
                            { "term": { "tenant": "acme" } },
                            { "range": { "price": { "gte": 10, "lt": 25 } } }
                        ]
                    }
                }
            })
        );
    }

    #[test]
    fn test_to_mongodb_find() {
        let plan = ServingRequestPlan {
            select: Some(vec!["tenant".to_string(), "title".to_string()]),
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: None,
                in_values: Some(vec![json!("acme"), json!("globex")]),
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![ServingSort {
                field: "title".to_string(),
                descending: false,
            }],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        assert_eq!(
            to_mongodb_find(&plan),
            MongoFindRequest {
                filter: json!({ "tenant": { "$in": ["acme", "globex"] } }),
                projection: Some(json!({ "_id": 0, "tenant": 1, "title": 1 })),
                sort: Some(json!({ "title": 1 })),
                limit: Some(10),
            }
        );
    }

    #[test]
    fn test_from_mongodb_find() {
        let command = MongoFindCommand {
            find: "events".to_string(),
            filter: json!({
                "$and": [
                    { "tenant": "acme" },
                    { "status": { "$in": ["open", "closed"] } },
                    { "price": { "$gte": 10, "$lt": 25 } }
                ]
            }),
            projection: Some(json!({ "_id": 0, "tenant": 1, "price": 1 })),
            sort: Some(json!({ "price": -1 })),
            limit: Some(25),
            skip: None,
            batch_size: None,
            single_batch: None,
            no_cursor_timeout: None,
        };

        assert_eq!(
            from_mongodb_find(&command).unwrap(),
            ServingRequestPlan {
                select: Some(vec!["tenant".to_string(), "price".to_string()]),
                filters: vec![
                    ServingPredicate {
                        field: "tenant".to_string(),
                        eq: Some(json!("acme")),
                        in_values: None,
                        gt: None,
                        gte: None,
                        lt: None,
                        lte: None,
                    },
                    ServingPredicate {
                        field: "status".to_string(),
                        eq: None,
                        in_values: Some(vec![json!("open"), json!("closed")]),
                        gt: None,
                        gte: None,
                        lt: None,
                        lte: None,
                    },
                    ServingPredicate {
                        field: "price".to_string(),
                        eq: None,
                        in_values: None,
                        gt: None,
                        gte: Some(json!(10)),
                        lt: Some(json!(25)),
                        lte: None,
                    },
                ],
                aggregate: None,
                order_by: vec![ServingSort {
                    field: "price".to_string(),
                    descending: true,
                }],
                limit: Some(25),
                allow_slow_path: false,
                explain: false,
            }
        );
    }

    #[test]
    fn test_from_mongodb_find_projection_only_excludes_id() {
        let command = MongoFindCommand {
            find: "events".to_string(),
            filter: json!({}),
            projection: Some(json!({ "_id": 0 })),
            sort: None,
            limit: Some(0),
            skip: None,
            batch_size: None,
            single_batch: None,
            no_cursor_timeout: None,
        };

        assert_eq!(from_mongodb_find(&command).unwrap().select, None);
        assert_eq!(from_mongodb_find(&command).unwrap().limit, None);
    }

    #[test]
    fn test_from_mongodb_find_rejects_skip() {
        let command = MongoFindCommand {
            find: "events".to_string(),
            filter: json!({}),
            projection: None,
            sort: None,
            limit: None,
            skip: Some(5),
            batch_size: None,
            single_batch: None,
            no_cursor_timeout: None,
        };

        assert!(
            from_mongodb_find(&command)
                .unwrap_err()
                .message
                .contains("skip is not supported")
        );
    }

    #[test]
    fn test_from_mongodb_find_rejects_unsupported_operator() {
        let command = MongoFindCommand {
            find: "events".to_string(),
            filter: json!({ "message": { "$regex": "failed" } }),
            projection: None,
            sort: None,
            limit: None,
            skip: None,
            batch_size: None,
            single_batch: None,
            no_cursor_timeout: None,
        };

        assert!(
            from_mongodb_find(&command)
                .unwrap_err()
                .message
                .contains("Unsupported Mongo filter operator $regex")
        );
    }
}
