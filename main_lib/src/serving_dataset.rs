use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::data_access::{self, execute_sql_async, load_file_as_table, path_to_table_name};
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
    let file_path = file_path.to_string();
    let local_name = format!("benchmark_{}", path_to_table_name(&file_path));
    data_access::reserve(&local_name, 0, vec![]).await;

    let result = async {
        data_access::drop(&local_name).await;
        load_file_as_table(&local_name, &file_path, true, None)
            .await
            .map_err(|error| error.to_string())?;

        let sql = match limit {
            Some(limit) => format!("SELECT * FROM {} LIMIT {}", local_name, limit),
            None => format!("SELECT * FROM {}", local_name),
        };
        let batches = execute_sql_async(&sql)
            .await
            .map_err(|error| error.to_string())?;
        let serde_result = batches_to_serde_value(&batches)
            .await
            .map_err(|error| error.message)?;
        let schema = serde_result
            .schema
            .ok_or_else(|| "No schema was inferred from parquet rows".to_string())?;
        Ok::<ParquetDocumentSet, String>(ParquetDocumentSet {
            rows: serde_result.values,
            schema,
        })
    }
    .await;

    data_access::drop(&local_name).await;
    data_access::release(&local_name).await;
    result
}
