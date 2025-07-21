use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize)]
pub struct FileFilter {
    operator: String,
    value: String,
}

