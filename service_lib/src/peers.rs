use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct CheckpointDescriptor {
    pub table_name: String,
    pub checkpoint_id: String,
    pub original_checkpoint_id: Option<String>,
}

impl CheckpointDescriptor {
    pub fn new(table_name: String, checkpoint_id: String) -> Self {
        CheckpointDescriptor {
            table_name,
            checkpoint_id,
            original_checkpoint_id: None,
        }
    }

    pub fn from_full_name(full_name: &str) -> Self {
        let parts: Vec<&str> = full_name.split(':').collect();
        if parts.len() == 2 {
            CheckpointDescriptor {
                table_name: parts[0].to_string(),
                checkpoint_id: parts[1].to_string(),
                original_checkpoint_id: None,
            }
        } else if parts.len() == 3 {
            CheckpointDescriptor {
                table_name: parts[0].to_string(),
                checkpoint_id: parts[2].to_string(),
                original_checkpoint_id: Some(parts[1].to_string()),
            }
        } else {
            panic!("Invalid checkpoint descriptor: {}", full_name);
        }
    }

    pub(crate) fn full_checkpoint_id(&self) -> String {
        match &self.original_checkpoint_id {
            Some(original_checkpoint_id) => {
                format!("{}:{}", original_checkpoint_id, self.checkpoint_id)
            }
            None => self.checkpoint_id.clone(),
        }
    }

    pub fn full_name(&self) -> String {
        match &self.original_checkpoint_id {
            Some(original_checkpoint_id) => format!(
                "{}:{}:{}",
                self.table_name, original_checkpoint_id, self.checkpoint_id
            ),
            None => format!("{}:{}", self.table_name, self.checkpoint_id),
        }
    }
}

impl Display for CheckpointDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.table_name, self.checkpoint_id)
    }
}
