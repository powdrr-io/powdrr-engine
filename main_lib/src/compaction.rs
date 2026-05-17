use arrow_array_55::RecordBatch as IcebergRecordBatch;
use arrow_ipc_55::reader::FileReader as IcebergFileReader;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::ipc::writer::FileWriter as DataFusionFileWriter;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use futures_util::FutureExt;
use gotham::mime;
use http::StatusCode;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, FileNameGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use idgenerator::IdInstance;
use parquet_55::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{error::Error, fmt};

use crate::data_access::{IcebergLibMetadata, execute_sql};
use crate::data_contract::{
    CompactionCommit, CompactionWorkItem, FileSetPayload, IcebergCommit, IcebergMetadata,
    SpeedboatCommitTableInfo,
};
use crate::elastic_search_common::{
    Command, CommandContext, CommandError, ElasticSearchResponse, ResultGeneratorFuture,
    execute_command,
};
use crate::elastic_search_ingest::{WriteBuffer, write_to_file};
use crate::peers::{PrivateCompactionInvocation, PrivateInvocation};
use crate::schema_massager::{PowdrrSchema, SqlBuilder};
use crate::search_runtime::df_to_serde_value;
use crate::state_provider::ServiceApiError;
use crate::{data_access, state_provider::STATE_PROVIDER};

#[derive(Debug)]
pub(crate) struct CompactionError {
    pub message: String,
}

impl Error for CompactionError {}

impl fmt::Display for CompactionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CompactionResult {
    None,
    Iceberg {
        num_records: usize,
    },
    Speedboat {
        file_location: String,
        num_records: usize,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompactionResponse {
    pub table_name: String,
    pub lib_metadata: IcebergLibMetadata,
    pub schema: PowdrrSchema,
    pub deletes_table_info: Option<SpeedboatCommitTableInfo>,
    pub compactions: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompactionCommand {
    table: String,
    work_item: CompactionWorkItem,
    compaction_id: String,
    last_snapshot_id: i64,
    parquet_file_name: String,
}

#[derive(Clone, Debug)]
struct PowdrrFileNameGenerator {
    file_name: String,
    count: Arc<AtomicU64>,
}

impl PowdrrFileNameGenerator {
    fn new(file_name: &String) -> Self {
        Self {
            file_name: file_name.clone(),
            count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn create_file_name() -> String {
        format!("{}-00000.parquet", IdInstance::next_id())
    }
}

impl FileNameGenerator for PowdrrFileNameGenerator {
    fn generate_file_name(&self) -> String {
        let count = self.count.fetch_add(1, Ordering::Relaxed);
        assert_eq!(count, 0);
        self.file_name.clone()
    }
}

impl CompactionCommand {
    fn to_iceberg_batches(data: &[RecordBatch]) -> Result<Vec<IcebergRecordBatch>, iceberg::Error> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let mut bytes = Vec::new();
        let schema = data[0].schema();
        let mut writer = DataFusionFileWriter::try_new(&mut bytes, &schema).map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to create Arrow IPC writer for Iceberg conversion",
            )
            .with_source(e)
        })?;

        for batch in data {
            writer.write(batch).map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "Failed to serialize DataFusion batches for Iceberg conversion",
                )
                .with_source(e)
            })?;
        }

        writer.finish().map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to finalize Arrow IPC writer for Iceberg conversion",
            )
            .with_source(e)
        })?;

        IcebergFileReader::try_new(Cursor::new(bytes), None)
            .map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "Failed to open Arrow IPC reader for Iceberg conversion",
                )
                .with_source(e)
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "Failed to deserialize Iceberg-compatible record batches",
                )
                .with_source(e)
            })
    }

    async fn append_iceberg_table(
        namespace: &String,
        name: &String,
        iceberg_schema: iceberg::spec::Schema,
        compaction_id: &String,
        data: &[IcebergRecordBatch],
        parquet_file_name: &String,
    ) -> Result<(), iceberg::Error> {
        let table = data_access::ensure_iceberg_table(namespace, name, &iceberg_schema).await?;
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            Arc::new(iceberg_schema),
            table.file_io().clone(),
            location_generator.clone(),
            PowdrrFileNameGenerator::new(parquet_file_name),
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
        let mut data_file_writer = data_file_writer_builder.build().await.unwrap();

        for batch in data.iter() {
            match data_file_writer.write(batch.clone()).await {
                Ok(_) => (),
                Err(e) => return Err(e),
            }
        }
        let data_files = match data_file_writer.close().await {
            Ok(df) => df,
            Err(e) => return Err(e),
        };

        data_access::commit_iceberg_transaction(namespace, name, compaction_id, &data_files).await
    }

    async fn update_iceberg(
        data: &Vec<RecordBatch>,
        table_name: &String,
        compaction_id: &String,
        parquet_file_name: &String,
    ) -> Result<(), iceberg::Error> {
        if data.len() == 0 {
            return Ok(());
        }

        let iceberg_batches = Self::to_iceberg_batches(data)?;

        let converted_schema = match arrow_schema_to_schema(iceberg_batches[0].schema().as_ref()) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        Self::append_iceberg_table(
            &"default".to_string(),
            table_name,
            converted_schema,
            compaction_id,
            &iceberg_batches,
            parquet_file_name,
        )
        .await
    }

    async fn do_iceberg_commit(
        compaction_response: &CompactionResponse,
    ) -> Result<i64, ServiceApiError> {
        let metadata = IcebergMetadata {
            table_schema: compaction_response.schema.clone(),
            snapshot_id: Some(compaction_response.lib_metadata.snapshot_id.to_string()),
            files: FileSetPayload {
                file_paths: compaction_response.lib_metadata.files.clone(),
                schemas: compaction_response
                    .lib_metadata
                    .schemas
                    .iter()
                    .map(|s| {
                        PowdrrSchema::from_iceberg(
                            &compaction_response.lib_metadata.table_schema,
                            s,
                        )
                    })
                    .collect(),
                file_schemas: compaction_response
                    .lib_metadata
                    .files
                    .iter()
                    .enumerate()
                    .map(|(x, _)| x as u64)
                    .collect(),
                sizes: compaction_response.lib_metadata.sizes.clone(),
            },
            column_names: compaction_response.lib_metadata.column_names.clone(),
            column_stats: compaction_response.lib_metadata.column_stats.clone(),
        };
        metadata.files.validate();

        match STATE_PROVIDER
            .iceberg_commit(
                &compaction_response.table_name,
                &IcebergCommit {
                    metadata,
                    compactions: compaction_response.lib_metadata.compactions.clone(),
                    deletes_table_info: compaction_response.deletes_table_info.clone(),
                },
            )
            .await
        {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        Ok(compaction_response.lib_metadata.snapshot_id)
    }
}

#[async_trait]
impl Command for CompactionCommand {
    async fn get_private_invocation(&self) -> PrivateInvocation {
        PrivateInvocation::Compaction(PrivateCompactionInvocation {
            sql: SqlBuilder::for_compaction().build(),
            speedboat_files: self.work_item.speedboat_files.clone(),
            table_schema: self.work_item.table_schema.clone(),
            delete_files: self.work_item.delete_files.clone(),
        })
    }

    fn result_generator(
        &self,
        result_table_name: Option<String>,
    ) -> Pin<Box<ResultGeneratorFuture>> {
        let public_table_name = self.table.clone();
        let compactions = vec![self.compaction_id.clone()];
        let schema = self.work_item.table_schema.clone();
        let old_snapshot_id = self.last_snapshot_id;
        let parquet_file_name = self.parquet_file_name.clone();
        async move {
            let internal_df_table_name = match result_table_name {
                Some(t) => t,
                None => {
                    // TODO: After this compaction there is....nothing?
                    // Maybe this should panic since it shouldn't be possible to get here.
                    return Err(CommandError{ message: "Nothing to compact".to_string() })
                }
            };
            let remaining_deletes_data_frame = match execute_sql(&format!("select _dt_id_seq_no as _id_seq_no from {internal_df_table_name} where _id is null and _dt_id_seq_no is not null")).await {
                Ok(df) => df,
                Err(e) => return Err(CommandError{ message: e.to_string() })
            };
            let results_data_frame = match execute_sql(&format!("select * from {internal_df_table_name} where _id is not null and _dt_id_seq_no is null")).await {
                Ok(df) => {
                    match df.drop_columns(&["_dt_id", "_dt_seq_no", "_dt_id_seq_no"]) {
                        Ok(df) => df,
                        Err(e) => return Err(CommandError{ message: e.to_string() })
                    }
                },
                Err(e) => return Err(CommandError{ message: e.to_string() })
            };

            let deletes_serde_result = df_to_serde_value(&remaining_deletes_data_frame).await?;
            let deletes_buffer = WriteBuffer::delete(deletes_serde_result.values.iter().map(|x| x.clone()).collect());

            match results_data_frame.clone().count().await {
                Ok(c) => assert!(c > 0),
                Err(_) => panic!("Results data frame count failed"),
            };

            tracing::info!("!!!!!!!!!!!!!!!!!!!! Compacting to Iceberg !!!!!!!!!!!!!!!!!!!!!!!");
            // TODO: if nulls can come out there that probably means we need to intercept earlier
            // in the pipeline to cast nulls to real types.
            let null_fields = results_data_frame.schema().fields().iter()
                .filter(|f| f.data_type() == &DataType::Null || f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none())
                .map(|f|f.name().as_str())
                .collect::<Vec<&str>>();
            let non_null_results = match results_data_frame.clone().drop_columns(null_fields.as_slice()) {
                Ok(df) => df,
                Err(e) => return Err(CommandError{ message: e.to_string() })
            };

            let data = match non_null_results.collect().await {
                Ok(d) => d,
                Err(e) => return Err(CommandError{ message: e.to_string() })
            };

            assert_eq!(compactions.len(), 1);
            match Self::update_iceberg(
                &data,
                &public_table_name,
                &compactions[0],
                &parquet_file_name,
            ).await {
                Ok(_) => (),
                Err(e) => {
                    let error = format!("{}", e);
                    tracing::info!("Iceberg Update failed: {}", error);
                    return Err(CommandError{ message: e.to_string() })
                }
            }

            let lib_metadata = match data_access::load_iceberg_table_metadata(
                &"default".to_string(),
                &public_table_name,
                old_snapshot_id
            ).await {
                Ok(m) => m,
                Err(e) => {
                    let error = format!("{}", e);
                    tracing::info!("Iceberg Metadata load failed: {}", error);
                    return Err(CommandError{ message: e.to_string() })
                },
            };

            let deletes_table_info = if deletes_buffer.num_records() > 0 {
                let (deletes_path, size) = match write_to_file(&deletes_buffer, &public_table_name, &"delete".to_string()).await {
                    Ok(file) => file,
                    Err(e) => {
                        return Err(CommandError{ message: e.to_string() })
                    }
                };
                Some(SpeedboatCommitTableInfo {
                    commit_type: "delete".to_string(),
                    table_name: public_table_name.clone(),
                    files: vec!(deletes_path),
                    sizes: vec!(size),
                    schema: deletes_buffer.schema(),
                })
            } else {
                None
            };

            let command_response = CompactionResponse {
                table_name: public_table_name.clone(),
                lib_metadata,
                schema,
                deletes_table_info,
                compactions,
            };

            Ok(ElasticSearchResponse {
                status: StatusCode::OK,
                mime: mime::APPLICATION_JSON,
                body: serde_json::to_string(&command_response).unwrap(),
                headers: vec![],
            })
        }.boxed()
    }
}

pub(crate) async fn compact_logs(
    command: Arc<dyn Command>,
) -> Result<ElasticSearchResponse, CompactionError> {
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Compacting Start !!!!!!!!!!!!!!!!!!!!!!!");
    let response = execute_command(CommandContext {}, command).await;
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Compacting End !!!!!!!!!!!!!!!!!!!!!!!");
    Ok(response)
}

pub(crate) async fn perform_compaction(
    work_items: Vec<(String, CompactionWorkItem)>,
    last_snapshot_id: i64,
) -> Result<i64, CompactionError> {
    let mut new_last_snapshot_id = last_snapshot_id;
    for (table_name, work_item) in work_items.iter() {
        let compaction_id = work_item.id.clone();

        // NOTE: the api commit must happen before the iceberg commit. The main_lib is designed to understand that
        // a compaction commit might get committed to it but fail afterwards. If we commit to Iceberg and fail to
        // record that in the main_lib then that leads to correctness errors that aren't really possible to fix.
        let parquet_file_name = PowdrrFileNameGenerator::create_file_name();
        match STATE_PROVIDER
            .compaction_commit(
                table_name,
                &CompactionCommit {
                    removed_speedboat_files: work_item.speedboat_files.file_paths.clone(),
                    compaction_id: compaction_id.clone(),
                    checkpoint_id_to_replace: work_item.checkpoint_id_to_replace.clone(),
                    removed_delete_files: work_item.delete_files.clone(),
                    checkpoints_to_delete: work_item.checkpoints_to_delete.clone(),
                    parquet_file_name: parquet_file_name.clone(),
                },
            )
            .await
        {
            Ok(_) => (),
            Err(e) => {
                return Err(CompactionError {
                    message: format!("api call failed: {}", e),
                });
            }
        }

        let command = CompactionCommand {
            table: table_name.clone(),
            work_item: work_item.clone(),
            compaction_id: compaction_id.clone(),
            last_snapshot_id,
            parquet_file_name,
        };

        let peers = STATE_PROVIDER.get_peer_clients().await;
        assert!(peers.len() > 0);
        let response_maybe = match peers[0].private_compaction_leader(&command).await {
            Ok(success) => success,
            Err(e) => {
                return Err(CompactionError {
                    message: e.to_string(),
                });
            }
        };

        new_last_snapshot_id = match response_maybe {
            Some(response) => match CompactionCommand::do_iceberg_commit(&response).await {
                Ok(id) => id,
                Err(e) => {
                    return Err(CompactionError {
                        message: e.to_string(),
                    });
                }
            },
            None => new_last_snapshot_id,
        };
    }

    Ok(new_last_snapshot_id)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::error::ArrowError;
    use datafusion::parquet::data_type::AsBytes;
    use gotham::test::Server;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::io::BufReader;
    use std::sync::Arc;

    use super::{CompactionCommand, PowdrrFileNameGenerator};
    use crate::data_access;
    use crate::elastic_search_storage_schema::{FullRecord, RecordInput, SpeedboatCommitBuilder};
    use crate::router::tests::TEST_SERVER;
    use iceberg::io::{
        FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
    };

    #[test]
    fn test_iceberg_catalog_list_all_tables() {
        let test_server = &*TEST_SERVER;

        test_server.run_future(test_iceberg_catalog_list_all_tables_worker());
    }

    async fn test_iceberg_catalog_list_all_tables_worker() {
        data_access::drop_all_iceberg_tables(&"default".to_string())
            .await
            .unwrap();

        let iceberg_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        match data_access::ensure_iceberg_table(
            &"default".to_string(),
            &"test_table".to_string(),
            &iceberg_schema,
        )
        .await
        {
            Ok(_) => (),
            Err(e) => {
                panic!("oh no = {}", e)
            }
        };

        match data_access::drop_iceberg_table(&"default".to_string(), &"test_table".to_string())
            .await
        {
            Ok(_) => (),
            Err(_) => {}
        }
    }

    #[test]
    fn test_iceberg_compact_simple() {
        let test_server = &*TEST_SERVER;

        test_server.run_future(test_iceberg_compact_simple_worker());
    }

    async fn test_iceberg_compact_simple_worker() {
        data_access::drop_all_iceberg_tables(&"default".to_string())
            .await
            .unwrap();

        let file_content = include_str!("../tests/data/logs.json");
        let mut values = vec![];
        for split_str in file_content.split("\n") {
            if split_str.len() == 0 {
                continue;
            }
            let parsed_val = match serde_json::from_str(split_str) {
                Ok(v) => v,
                Err(e) => {
                    let _error = format!("{}", e);
                    panic!("oh no");
                }
            };
            values.push(parsed_val)
        }
        let mut builder = SpeedboatCommitBuilder::new(&"simple".to_string());
        let records = values
            .iter()
            .map(|x| FullRecord::from_record(x).record_input)
            .collect::<Vec<RecordInput>>();
        for record in records.iter() {
            builder.insert(record)
        }
        let (insert_buffer, _, _) = builder.build_buffers();
        let json = arrow_json::ReaderBuilder::new(Arc::new(
            insert_buffer.schema().unwrap().to_arrow_schema(),
        ))
        .build(BufReader::new(file_content.as_bytes()))
        .unwrap();
        let batch = json
            .collect::<Result<Vec<RecordBatch>, ArrowError>>()
            .unwrap();

        match CompactionCommand::update_iceberg(
            &batch,
            &"simple".to_string(),
            &"thing1".to_string(),
            &PowdrrFileNameGenerator::create_file_name(),
        )
        .await
        {
            Ok(_) => (),
            Err(e) => {
                panic!("oh no = {}", e)
            }
        }

        let metadata = match data_access::load_iceberg_table_metadata(
            &"default".to_string(),
            &"simple".to_string(),
            -1,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                panic!("nope {}", e)
            }
        };

        assert_eq!(metadata.files.len(), 1);
        assert_eq!(metadata.compactions.len(), 1);
        assert_eq!(metadata.column_names.len(), 0);
        assert_eq!(metadata.column_stats.len(), 0);

        match data_access::drop_iceberg_table(&"default".to_string(), &"simple".to_string()).await {
            Ok(_) => (),
            Err(_) => {}
        }
    }

    #[test]
    fn test_iceberg_compact_okta() {
        let test_server = &*TEST_SERVER;

        test_server.run_future(test_iceberg_compact_okta_worker());
    }

    async fn test_iceberg_compact_okta_worker() {
        data_access::drop_all_iceberg_tables(&"default".to_string())
            .await
            .unwrap();

        let okta_1 = include_str!("../tests/data/okta_system_log_1.json").replace("\n", "");
        let okta_2 = include_str!("../tests/data/okta_system_log_2.json").replace("\n", "");
        let okta_3 = include_str!("../tests/data/okta_system_log_3.json").replace("\n", "");
        let okta_4 = include_str!("../tests/data/okta_system_log_4.json").replace("\n", "");
        let values = vec![
            serde_json::from_str::<serde_json::Value>(&okta_1).unwrap(),
            serde_json::from_str::<serde_json::Value>(&okta_2).unwrap(),
            serde_json::from_str::<serde_json::Value>(&okta_3).unwrap(),
            serde_json::from_str::<serde_json::Value>(&okta_4).unwrap(),
        ];
        let mut builder = SpeedboatCommitBuilder::new(&"simple".to_string());
        let records = values
            .iter()
            .enumerate()
            .map(|(id, x)| RecordInput::new(format!("id_{}", id), 1, x, None))
            .collect::<Vec<RecordInput>>();
        for record in records.iter() {
            builder.insert(record)
        }
        let (insert_buffer, _, _) = builder.build_buffers();
        let insert_buffer_vec = insert_buffer.as_byte_vec();
        let arrow_schema = insert_buffer.schema().unwrap().to_arrow_schema();
        let json = arrow_json::ReaderBuilder::new(Arc::new(arrow_schema))
            .build(BufReader::new(insert_buffer_vec.as_bytes()))
            .unwrap();
        let batch = json
            .collect::<Result<Vec<RecordBatch>, ArrowError>>()
            .unwrap();

        match CompactionCommand::update_iceberg(
            &batch,
            &"okta".to_string(),
            &"thing1".to_string(),
            &PowdrrFileNameGenerator::create_file_name(),
        )
        .await
        {
            Ok(_) => (),
            Err(e) => {
                panic!("oh no = {}", e)
            }
        }

        let metadata = match data_access::load_iceberg_table_metadata(
            &"default".to_string(),
            &"okta".to_string(),
            -1,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                panic!("nope {}", e)
            }
        };

        assert_eq!(metadata.files.len(), 1);
        assert_eq!(metadata.compactions.len(), 1);
        assert_eq!(metadata.column_names.len(), 0);
        assert_eq!(metadata.column_stats.len(), 0);

        match data_access::drop_iceberg_table(&"default".to_string(), &"okta".to_string()).await {
            Ok(_) => (),
            Err(_) => {}
        }
    }

    async fn test_s3_file_io_worker() {
        data_access::drop_all_iceberg_tables(&"default".to_string())
            .await
            .unwrap();

        let file_io = FileIOBuilder::new("s3")
            .with_props(vec![
                (S3_ENDPOINT, "http://localhost:9000".to_string()),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
            ])
            .build()
            .unwrap();
        file_io.delete("s3://default/test_input.txt").await.unwrap();
        assert!(file_io.exists("s3://default/test_input.txt").await.unwrap() == false);

        let output_file = file_io.new_output("s3://default/test_input.txt").unwrap();
        {
            output_file
                .write("testing stuff is fun and useful".into())
                .await
                .unwrap();
        }

        let input_file = file_io.new_input("s3://default/test_input.txt").unwrap();

        {
            let buffer = input_file.read().await.unwrap();
            assert_eq!(buffer, "testing stuff is fun and useful".as_bytes());
        }

        file_io.delete("s3://default/test_input.txt").await.unwrap();
        assert!(file_io.exists("s3://default/test_input.txt").await.unwrap() == false);
    }

    #[test]
    fn test_s3_file_io() {
        let test_server = &*TEST_SERVER;

        test_server.run_future(test_s3_file_io_worker());
    }
}
