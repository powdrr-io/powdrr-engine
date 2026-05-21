use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct FileFilter {
    operator: String,
    value: String,
}
