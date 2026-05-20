use crate::data_contract::ServingAggregateSpec;
use crate::query_path::QueryPredicate;
use crate::serving_plan::{ServingPredicate, ServingRequestPlan, ServingSort};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct ReadPredicate {
    pub field: String,
    pub eq: Option<Value>,
    pub in_values: Option<Vec<Value>>,
    pub gt: Option<Value>,
    pub gte: Option<Value>,
    pub lt: Option<Value>,
    pub lte: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSort {
    pub field: String,
    pub descending: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReadPlan {
    pub select: Option<Vec<String>>,
    pub filters: Vec<ReadPredicate>,
    pub aggregate: Option<ServingAggregateSpec>,
    pub order_by: Vec<ReadSort>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub search_after: Option<Vec<Value>>,
    pub allow_slow_path: bool,
    pub explain: bool,
}

impl ReadPlan {
    pub fn normalized_limit(&self, default_limit: usize, max_limit: usize) -> usize {
        self.limit.unwrap_or(default_limit).min(max_limit)
    }
}

impl From<&ServingPredicate> for ReadPredicate {
    fn from(predicate: &ServingPredicate) -> Self {
        Self {
            field: predicate.field.clone(),
            eq: predicate.eq.clone(),
            in_values: predicate.in_values.clone(),
            gt: predicate.gt.clone(),
            gte: predicate.gte.clone(),
            lt: predicate.lt.clone(),
            lte: predicate.lte.clone(),
        }
    }
}

impl From<&ReadPredicate> for QueryPredicate {
    fn from(predicate: &ReadPredicate) -> Self {
        Self {
            field: predicate.field.clone(),
            eq: predicate.eq.clone(),
            in_values: predicate.in_values.clone(),
            gt: predicate.gt.clone(),
            gte: predicate.gte.clone(),
            lt: predicate.lt.clone(),
            lte: predicate.lte.clone(),
        }
    }
}

impl From<&ServingSort> for ReadSort {
    fn from(sort: &ServingSort) -> Self {
        Self {
            field: sort.field.clone(),
            descending: sort.descending,
        }
    }
}

impl From<&ServingRequestPlan> for ReadPlan {
    fn from(plan: &ServingRequestPlan) -> Self {
        Self {
            select: plan.select.clone(),
            filters: plan.filters.iter().map(ReadPredicate::from).collect(),
            aggregate: plan.aggregate.clone(),
            order_by: plan.order_by.iter().map(ReadSort::from).collect(),
            limit: plan.limit,
            offset: 0,
            search_after: None,
            allow_slow_path: plan.allow_slow_path,
            explain: plan.explain,
        }
    }
}
