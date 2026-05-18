use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ILMPolicyDeleteAction {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ILMPolicyDelete {
    pub min_age: String,
    pub actions: ILMPolicyActions,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ILMPolicyRolloverAction {
    pub max_size: Option<String>,
    pub max_age: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ILMPolicyActions {
    pub rollover: Option<ILMPolicyRolloverAction>,
    pub delete: Option<ILMPolicyDeleteAction>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ILMPolicyHot {
    pub actions: ILMPolicyActions,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ILMPolicyPhases {
    pub hot: Option<ILMPolicyHot>,
    pub delete: Option<ILMPolicyDelete>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ILMPolicyMeta {
    pub managed: bool,
    pub index_patterns: Option<Vec<String>>,
    pub version: Option<i64>,
    pub created_by: Option<String>,
    pub updated_by: Option<String>,
    pub description: Option<String>,
    pub generation: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ILMPolicyPolicy {
    pub _meta: Option<ILMPolicyMeta>,
    pub phases: ILMPolicyPhases,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ILMPolicyDefinition {
    pub policy: ILMPolicyPolicy,
}
