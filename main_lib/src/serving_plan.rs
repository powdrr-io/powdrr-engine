use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServingQueryClassification {
    FastPath,
    SlowPath,
    Rejected,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServingPredicate {
    pub field: String,
    #[serde(default)]
    pub eq: Option<Value>,
    #[serde(default, rename = "in")]
    pub in_values: Option<Vec<Value>>,
    #[serde(default)]
    pub gt: Option<Value>,
    #[serde(default)]
    pub gte: Option<Value>,
    #[serde(default)]
    pub lt: Option<Value>,
    #[serde(default)]
    pub lte: Option<Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServingSort {
    pub field: String,
    #[serde(default)]
    pub descending: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServingRequestPlan {
    #[serde(default)]
    pub select: Option<Vec<String>>,
    #[serde(default)]
    pub filters: Vec<ServingPredicate>,
    #[serde(default)]
    pub order_by: Vec<ServingSort>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub allow_slow_path: bool,
    #[serde(default)]
    pub explain: bool,
}
