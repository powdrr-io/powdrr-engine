
use iceberg::Catalog;
use std::{error::Error, fmt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::DataType;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::parquet::file::properties::WriterProperties;
use futures_util::{FutureExt, TryStreamExt};
use gotham::mime;
use http::StatusCode;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use iceberg::table::Table;
use iceberg::transaction::ApplyTransactionAction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use idgenerator::IdInstance;
use serde::{Deserialize, Serialize};

use crate::{state_hosted_service::{API_SERVICE_CLIENT}};
use crate::data_access::execute_sql;
use crate::elastic_search_commands::df_to_serde_value;
use crate::elastic_search_common::{execute_command, Command, CommandContext, CommandError, ElasticSearchResponse, ResultGeneratorFuture};
use crate::elastic_search_ingest::{write_to_file, WriteBuffer};
use crate::schema_massager::{PowdrrSchema, SqlBuilder};
use crate::data_contract::{CompactionCommit, CompactionWorkItem, FileSetPayload, IcebergCommit, IcebergMetadata, SpeedboatCommit, SpeedboatCommitTableInfo};
use crate::state_hosted_service::ServiceApiError;
use crate::state_peers::{PrivateCompactionInvocation, PrivateInvocation};


const REST_CATALOG_IP: &str = "localhost";
const REST_CATALOG_PORT: i16 = 8181;
const S3_ENDPOINT_VALUE: &str = "http://localhost:9000";
const S3_ACCESS_KEY_ID_VALUE: &str = "admin";
const S3_SECRET_ACCESS_KEY_VALUE: &str = "password";
const S3_REGION_VALUE: &str = "us-east-1"; // TODO: figure out a good number here


fn get_iceberg_catalog_config() -> RestCatalogConfig {
    RestCatalogConfig::builder()
        .uri(format!("http://{}:{}", REST_CATALOG_IP, REST_CATALOG_PORT))
        .props(HashMap::from([
            (S3_ENDPOINT.to_string(), S3_ENDPOINT_VALUE.to_string()),
            (S3_ACCESS_KEY_ID.to_string(), S3_ACCESS_KEY_ID_VALUE.to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), S3_SECRET_ACCESS_KEY_VALUE.to_string()),
            (S3_REGION.to_string(), S3_REGION_VALUE.to_string()),
        ]))
        .build()
}

#[allow(dead_code)]
fn get_catalog() -> RestCatalog {
    RestCatalog::new(get_iceberg_catalog_config())
}


static REST_CATALOG: LazyLock<Arc<RestCatalog>> = LazyLock::new(|| Arc::new(get_catalog()));


#[allow(dead_code)]
async fn list_all_tables(namespace: &String) -> Result<Vec<TableIdent>, iceberg::Error> {
    let catalog = REST_CATALOG.clone();
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    match catalog.get_namespace(&namespace_ident).await {
        Ok(_) => catalog.list_tables(&namespace_ident).await,
        Err(_) => Ok(vec!())
    }
}

#[cfg(test)]
async fn drop_table(namespace: &String, name: &String) -> Result<(), iceberg::Error> {
    let catalog = REST_CATALOG.clone();

    let namespace_ident = NamespaceIdent::new(namespace.clone());

    let table_ident = TableIdent {
        namespace: namespace_ident.clone(),
        name: name.clone()
    };

    catalog.drop_table(&table_ident).await
}

pub(crate) async fn drop_all_tables(namespace: &String) -> Result<(), iceberg::Error> {
    let catalog = REST_CATALOG.clone();
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    let all_tables: Vec<TableIdent> = match catalog.get_namespace(&namespace_ident).await {
        Ok(_) => catalog.list_tables(&namespace_ident).await?,
        Err(_) => vec!()
    };
    for table_ident in all_tables.iter() {
        catalog.drop_table(table_ident).await?
    }
    Ok(())
}

async fn ensure_table(namespace: &String, name: &String, iceberg_schema: &iceberg::spec::Schema) -> Result<Table, iceberg::Error> {
    let catalog = REST_CATALOG.clone();

    let namespace_ident = NamespaceIdent::new(namespace.clone());

    let table_ident = TableIdent {
        namespace: namespace_ident.clone(),
        name: name.clone()
    };

    match catalog.get_namespace(&namespace_ident).await {
        Err(_) => {
            catalog.create_namespace(&namespace_ident,  HashMap::new()).await?;
        },
        Ok(_) => ()
    };

    match catalog.load_table(&table_ident).await {
        Ok(t) => Ok(t),
        Err(_) => {
            let creation = TableCreation::builder()
                .name(name.clone())
                .schema(iceberg_schema.clone())
                .build();

            match catalog.create_table(&namespace_ident, creation).await {
                Ok(t) => Ok(t),
                Err(e) => {
                    tracing::info!("Failed to create table {}: {}", name, e);
                    Err(e)
                }
            }
        }
    }

}


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
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct IcebergLibMetadata {
    pub snapshot_id: i64,
    pub files: Vec<String>,
    pub sizes: Vec<u64>,
    pub compactions: Vec<String>,
    pub column_names: Vec<String>,
    // per file, per column lower and upper bounds
    // TODO: this needs to be generalized to support bloom filters
    pub column_stats: Vec<(String, String)>,
}


pub async fn load_table_metadata(namespace: &String, name: &String, last_snapshot_id: i64) -> Result<IcebergLibMetadata, iceberg::Error> {
    let catalog = REST_CATALOG.clone();

    let namespace_ident = NamespaceIdent::new(namespace.clone());

    let table_ident = TableIdent {
        namespace: namespace_ident.clone(),
        name: name.clone()
    };

    let table: Table = match catalog.load_table(&table_ident).await {
        Ok(t) => t,
        Err(_) => {
            return Err(iceberg::Error::new(iceberg::ErrorKind::DataInvalid, format!("No such table {}", name)))
        }
    };

    let snapshot_log = Vec::from_iter(table.metadata().history());
    let mut compactions = vec!();
    for snapshot_info in snapshot_log.iter().rev() {
        let snapshot = match table.metadata().snapshot_by_id(snapshot_info.snapshot_id) {
            Some(s) => s,
            None => {
                tracing::info!("Unable to find iceberg snapshot {}", snapshot_info.snapshot_id);
                return Err(iceberg::Error::new(iceberg::ErrorKind::DataInvalid, format!("Unable to find iceberg snapshot {}", snapshot_info.snapshot_id)))
            }
        };

        if snapshot_info.snapshot_id == last_snapshot_id {
            break;
        }

        let summary = snapshot.summary();
        match summary.additional_properties.get("compaction") {
            Some(c) => compactions.push(c.clone()),
            None => ()
        };
    }

    let current_snapshot = match table.metadata().current_snapshot() {
        Some(c) => c,
        None => return Err(iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "No snapshot for this table"))
    };

    let table_scan = match table.scan().select_all().build() {
        Ok(s) => s,
        Err(e) => {
            return Err(iceberg::Error::new(iceberg::ErrorKind::DataInvalid, format!("No table scan task generated, {}", e)))
        }
    };

    let plan_files = match table_scan.plan_files().await {
        Ok(p) => p,
        Err(_) => {
            return Err(iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "No plan files task generated"))
        }
    };

    let files_result = plan_files.map_ok(|f| (f.data_file_path, f.length))
        .map_err(|err| iceberg::Error::new(iceberg::ErrorKind::Unexpected, format!("file scan task generate failed, {}", err)).with_source(err))
        .try_collect::<Vec<_>>()
        .await;

    let (files, sizes) = match files_result {
        Ok(r) => {
            (
                r.iter().map(|(f, _)| f.clone()).collect(),
                r.iter().map(|(_, s)| *s).collect(),
            )
        },
        Err(e) => return Err(e),
    };

    Ok(IcebergLibMetadata {
        snapshot_id: current_snapshot.snapshot_id(),
        files: files,
        sizes: sizes,
        compactions: compactions,
        column_names: vec!(),
        column_stats: vec!(),
    })
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompactionResponse {
    pub table_name: String,
    pub lib_metadata: IcebergLibMetadata,
    pub schema: PowdrrSchema,
    pub deletes_table_info: Option<SpeedboatCommitTableInfo>,
    pub compactions: Vec<String>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompactionCommand {
    table: String,
    work_item: CompactionWorkItem,
    compaction_id: String,
    last_snapshot_id: i64,
}

impl CompactionCommand {
    async fn append_iceberg_table(
        namespace: &String,
        name: &String,
        iceberg_schema: iceberg::spec::Schema,
        compaction_id: &String,
        data: &Vec<RecordBatch>
    ) -> Result<(), iceberg::Error> {
        let table = ensure_table(namespace, name, &iceberg_schema).await?;
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let file_name_generator = DefaultFileNameGenerator::new(
            IdInstance::next_id().to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            Arc::new(iceberg_schema),
            table.file_io().clone(),
            location_generator.clone(),
            file_name_generator.clone(),
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
        let mut data_file_writer = data_file_writer_builder.build().await.unwrap();

        for batch in data.iter() {
            match data_file_writer.write(batch.clone()).await {
                Ok(_) => (),
                Err(e) => {
                    return Err(e)
                }
            }
        }
        let data_files = match data_file_writer.close().await {
            Ok(df) => df,
            Err(e) => {
                return Err(e)
            }
        };

        let tx = iceberg::transaction::Transaction::new(&table);
        let mut action = tx.fast_append();
        action = action.set_snapshot_properties(HashMap::from([("compaction".to_string(), compaction_id.clone())]));
        action = action.add_data_files(data_files.clone());
        let catalog = REST_CATALOG.clone();
        action.apply(tx)?.commit(catalog.as_ref()).await?;

        Ok(())
    }

    async fn update_iceberg(data: &Vec<RecordBatch>, table_name: &String, compaction_id: &String) -> Result<(), iceberg::Error> {
        let converted_schema = match arrow_schema_to_schema(&data[0].schema()) {
            Ok(s) => s,
            Err(e) => {
                return Err(e)
            },
        };

        Self::append_iceberg_table(
            &"default".to_string(),
            table_name,
            converted_schema,
            compaction_id,
            &data,
        ).await
    }

    async fn do_iceberg_commit(compaction_response: &CompactionResponse) -> Result<i64, ServiceApiError> {
        API_SERVICE_CLIENT.iceberg_commit(
            &compaction_response.table_name,
            &IcebergCommit {
                metadata: IcebergMetadata {
                    table_schema: compaction_response.schema.clone(),
                    snapshot_id: compaction_response.lib_metadata.snapshot_id.to_string(),
                    files: FileSetPayload {
                        file_paths: compaction_response.lib_metadata.files.clone(),
                        schemas: vec!(compaction_response.schema.clone()),
                        file_schemas: compaction_response.lib_metadata.files.iter().map(|_|0).collect(),
                        sizes: compaction_response.lib_metadata.sizes.clone()
                    },
                    column_names: compaction_response.lib_metadata.column_names.clone(),
                    column_stats: compaction_response.lib_metadata.column_stats.clone(),
                },
                compactions: compaction_response.lib_metadata.compactions.clone(),
            }
        ).await?;

        // Note: the Iceberg and Speedboat commits are done separately here and are
        // therefore NOT ATOMIC. The Speedboat commit here is just deletions where
        // the new file contains a subset of deletes from the existing files. If this update is lost the
        // worst case is that the next compaction sees all the same deletes in the same
        // files and once again tries to compact them.

        if compaction_response.deletes_table_info.is_some() {
            API_SERVICE_CLIENT.speedboat_commit(&SpeedboatCommit {
                type_files: vec!(compaction_response.deletes_table_info.as_ref().unwrap().clone()),
                compactions: compaction_response.compactions.clone(),
            }).await?
        }

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

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>> {
        let public_table_name = self.table.clone();
        let compactions = vec!(self.compaction_id.clone());
        let schema = self.work_item.table_schema.clone();
        let old_snapshot_id = self.last_snapshot_id;
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
            Self::update_iceberg(
                &data,
                &public_table_name,
                &compactions[0]
            ).await.unwrap();

            let lib_metadata = match load_table_metadata(
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
                let (deletes_path, size) = match write_to_file(&deletes_buffer, &public_table_name, &"delete".to_string()) {
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
                mime: mime::TEXT_PLAIN,
                body: serde_json::to_string(&command_response).unwrap(),
                headers: vec![],
            })
        }.boxed()
    }
}


pub(crate) async fn compact_logs(command: Arc<dyn Command>) -> Result<ElasticSearchResponse, CompactionError> {
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Compacting Start !!!!!!!!!!!!!!!!!!!!!!!");
    let response = execute_command(CommandContext{}, command).await;
    tracing::info!("!!!!!!!!!!!!!!!!!!!! Compacting End !!!!!!!!!!!!!!!!!!!!!!!");
    Ok(response)
}


pub(crate) async fn perform_compaction(work_items: Vec<(String, CompactionWorkItem)>, last_snapshot_id: i64) -> Result<i64, CompactionError> {
    let mut new_last_snapshot_id = last_snapshot_id;
    for (table_name, work_item) in work_items.iter() {
        let compaction_id = IdInstance::next_id().to_string();

        // NOTE: the api commit must happen before the iceberg commit. The main_lib is designed to understand that
        // a compaction commit might get committed to it but fail afterwards. If we commit to Iceberg and fail to
        // record that in the main_lib then that leads to correctness errors that aren't really possible to fix.
        match API_SERVICE_CLIENT.compaction_commit(
            table_name,
            &CompactionCommit {
                removed_speedboat_files: work_item.speedboat_files.file_paths.clone(),
                compaction_id: compaction_id.clone(),
                removed_delete_files: work_item.delete_files.clone(),
            }
        ).await {
            Ok(_) => (),
            Err(e) => {
                return Err(CompactionError { message: format!("api call failed: {}", e) })
            },
        }

        let command = CompactionCommand {
            table: table_name.clone(),
            work_item: work_item.clone(),
            compaction_id: compaction_id.clone(),
            last_snapshot_id,
        };

        let peers = API_SERVICE_CLIENT.get_peer_clients().await;
        assert!(peers.len() > 0);
        let response_maybe = match peers[0].private_compaction_leader(&command).await {
            Ok(success) => success,
            Err(e) => return Err(CompactionError{ message: e.to_string() })
        };

        new_last_snapshot_id = match response_maybe {
            Some(response) => {
                match CompactionCommand::do_iceberg_commit(&response).await {
                    Ok(id) => id,
                    Err(e) => return Err(CompactionError{ message: e.to_string() })
                }
            },
            None => new_last_snapshot_id
        };
    }
   
    Ok(new_last_snapshot_id)
}


#[cfg(test)]
mod tests {
    use std::io::BufReader;
    use std::sync::Arc;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::error::ArrowError;
    use datafusion::parquet::data_type::AsBytes;
    use gotham::test::Server;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::{drop_table, ensure_table, load_table_metadata, CompactionCommand};
    use iceberg::io::{
        FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
    };
    use crate::elastic_search_storage_schema::{FullRecord, RecordInput, SpeedboatCommitBuilder};
    use crate::router::tests::TEST_SERVER;
    use super::list_all_tables;

    #[test]
    fn test_iceberg_catalog_list_all_tables() {
        let test_server = &*TEST_SERVER;

        test_server.run_future(test_iceberg_catalog_list_all_tables_worker());
    }

    async fn test_iceberg_catalog_list_all_tables_worker() {
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

        match ensure_table(&"default".to_string(), &"test_table".to_string(), &iceberg_schema).await {
            Ok(_) => (),
            Err(e) => {
                panic!("oh no = {}", e)
            }
        };

        match list_all_tables(&"default".to_string()).await {
            Ok(tables) => {
                match tables.len() {
                    0 => panic!("Table creation or listing failed"),
                    _ => ()
                }
            }
            Err(e) => panic!("oh no = {}", e)
        }

        match drop_table(&"default".to_string(), &"test_table".to_string()).await {
            Ok(_) => (),
            Err(_) => {
            }
        }
    }

    #[test]
    fn test_iceberg_compact_simple() {
        let test_server = &*TEST_SERVER;

        test_server.run_future(test_iceberg_compact_simple_worker());
    }

    async fn test_iceberg_compact_simple_worker() {
        match drop_table(&"default".to_string(), &"simple".to_string()).await {
            Ok(_) => (),
            Err(_) => {
            }
        }

        let file_content = include_str!("../tests/data/logs.json");
        let mut values = vec!();
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
        let records = values.iter().map(|x|FullRecord::from_record(x).record_input).collect::<Vec<RecordInput>>();
        for record in records.iter() {
            builder.insert(record)
        }
        let (insert_buffer, _, _) = builder.build_buffers();
        let json = arrow_json::ReaderBuilder::new(Arc::new(insert_buffer.schema().unwrap().to_arrow_schema())).build(BufReader::new(file_content.as_bytes())).unwrap();
        let batch = json.collect::<Result<Vec<RecordBatch>, ArrowError>>().unwrap();

        match CompactionCommand::update_iceberg(
            &batch,
            &"simple".to_string(),
            &"thing1".to_string()
        ).await {
            Ok(_) => (),
            Err(e) => {
                panic!("oh no = {}", e)
            }
        }

        let metadata = match load_table_metadata(&"default".to_string(), &"simple".to_string(), -1).await {
            Ok(m) => m,
            Err(e) => {
                panic!("nope {}", e)
            },
        };

        assert_eq!(metadata.files.len(), 1);
        assert_eq!(metadata.compactions.len(), 1);
        assert_eq!(metadata.column_names.len(), 0);
        assert_eq!(metadata.column_stats.len(), 0);

        match drop_table(&"default".to_string(), &"simple".to_string()).await {
            Ok(_) => (),
            Err(_) => {
            }
        }
    }

    #[test]
    fn test_iceberg_compact_okta() {
        let test_server = &*TEST_SERVER;

        test_server.run_future(test_iceberg_compact_okta_worker());
    }

    async fn test_iceberg_compact_okta_worker() {
        match drop_table(&"default".to_string(), &"okta".to_string()).await {
            Ok(_) => (),
            Err(_) => {
            }
        }

        let okta_1 = include_str!("../tests/data/okta_system_log_1.json").replace("\n", "");
        let okta_2 = include_str!("../tests/data/okta_system_log_2.json").replace("\n", "");
        let okta_3 = include_str!("../tests/data/okta_system_log_3.json").replace("\n", "");
        let okta_4 = include_str!("../tests/data/okta_system_log_4.json").replace("\n", "");
        let values = vec!(
            serde_json::from_str::<serde_json::Value>(&okta_1).unwrap(),
            serde_json::from_str::<serde_json::Value>(&okta_2).unwrap(),
            serde_json::from_str::<serde_json::Value>(&okta_3).unwrap(),
            serde_json::from_str::<serde_json::Value>(&okta_4).unwrap(),
        );
        let mut builder = SpeedboatCommitBuilder::new(&"simple".to_string());
        let records = values.iter().enumerate().map(|(id, x)|RecordInput::new(format!("id_{}", id), 1, x, None)).collect::<Vec<RecordInput>>();
        for record in records.iter() {
            builder.insert(record)
        }
        let (insert_buffer, _, _) = builder.build_buffers();
        let insert_buffer_vec = insert_buffer.as_byte_vec();
        let arrow_schema = insert_buffer.schema().unwrap().to_arrow_schema();
        let json = arrow_json::ReaderBuilder::new(Arc::new(arrow_schema)).build(BufReader::new(insert_buffer_vec.as_bytes())).unwrap();
        let batch = json.collect::<Result<Vec<RecordBatch>, ArrowError>>().unwrap();

        match CompactionCommand::update_iceberg(
            &batch,
            &"okta".to_string(),
            &"thing1".to_string()
        ).await {
            Ok(_) => (),
            Err(e) => {
                panic!("oh no = {}", e)
            }
        }

        let metadata = match load_table_metadata(&"default".to_string(), &"okta".to_string(), -1).await {
            Ok(m) => m,
            Err(e) => {
                panic!("nope {}", e)
            },
        };

        assert_eq!(metadata.files.len(), 1);
        assert_eq!(metadata.compactions.len(), 1);
        assert_eq!(metadata.column_names.len(), 0);
        assert_eq!(metadata.column_stats.len(), 0);

        match drop_table(&"default".to_string(), &"okta".to_string()).await {
            Ok(_) => (),
            Err(_) => {
            }
        }
    }

    async fn test_s3_file_io_worker() {
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
        assert!(
            file_io.exists("s3://default/test_input.txt").await.unwrap() == false
        );

        let output_file = file_io.new_output("s3://default/test_input.txt").unwrap();
        {
            output_file.write("testing stuff is fun and useful".into()).await.unwrap();
        }

        let input_file = file_io.new_input("s3://default/test_input.txt").unwrap();

        {
            let buffer = input_file.read().await.unwrap();
            assert_eq!(buffer, "testing stuff is fun and useful".as_bytes());
        }

        file_io.delete("s3://default/test_input.txt").await.unwrap();
        assert!(
            file_io.exists("s3://default/test_input.txt").await.unwrap() == false
        );
    }

    #[test]
    fn test_s3_file_io() {
        let test_server = &*TEST_SERVER;

        test_server.run_future(test_s3_file_io_worker());
    }

}
