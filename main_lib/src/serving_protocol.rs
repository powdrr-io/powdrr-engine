use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::serving_plan::{ServingPredicate, ServingRequestPlan};

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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::serving_plan::{ServingPredicate, ServingRequestPlan, ServingSort};

    use super::{MongoFindRequest, to_elasticsearch_search, to_mongodb_find};

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
}
