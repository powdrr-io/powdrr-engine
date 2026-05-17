use serde::{Deserialize, Serialize};
use serde_json::Value;

use datafusion::prelude::{ParquetReadOptions, SessionContext};
use crate::schema_massager::PowdrrSchema;
use crate::search_runtime::batches_to_serde_value;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ParquetDocumentSet {
    pub rows: Vec<Value>,
    pub schema: PowdrrSchema,
}

pub async fn read_parquet_documents(
    file_path: &str,
    limit: Option<usize>,
) -> Result<ParquetDocumentSet, String> {
    let local_name = "serving_dataset_input".to_string();
    let context = SessionContext::new();
    context
        .register_parquet(&local_name, file_path, ParquetReadOptions::new())
        .await
        .map_err(|error| error.to_string())?;

    let sql = match limit {
        Some(limit) => format!("SELECT * FROM {} LIMIT {}", local_name, limit),
        None => format!("SELECT * FROM {}", local_name),
    };
    let batches = context
        .sql(&sql)
        .await
        .map_err(|error| error.to_string())?
        .collect()
        .await
        .map_err(|error| error.to_string())?;
    let serde_result = batches_to_serde_value(&batches)
        .await
        .map_err(|error| error.message)?;
    let schema = serde_result
        .schema
        .ok_or_else(|| "No schema was inferred from parquet rows".to_string())?;
    Ok(ParquetDocumentSet {
        rows: serde_result.values,
        schema,
    })
}
