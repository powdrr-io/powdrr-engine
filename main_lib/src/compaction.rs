
use iceberg::Catalog;
use std::{error::Error, fmt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaBuilder};
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
use idgenerator::{IdGeneratorOptions, IdInstance};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::error::RecvError;

use crate::{elastic_search_ingest, state_hosted_service::{CompactionCommit, API_SERVICE_CLIENT}};
use crate::data_access::execute_sql;
use crate::elastic_search_commands::to_serde_value;
use crate::elastic_search_common::{execute_command, Command, CommandContext, ElasticSearchResponse, ResultGeneratorFuture};
use crate::elastic_search_ingest::WriteBuffer;
use crate::schema_massager::{PowdrrSchema, SqlBuilder};
use crate::state_hosted_service::{CompactionWorkItem, IcebergCommit, IcebergMetadata};
use crate::state_peers::{PrivateCompactionInvocation, PrivateInvocation};


const REST_CATALOG_IP: &str = "localhost";
const REST_CATALOG_PORT: i16 = 8181;
const S3_ENDPOINT_VALUE: &str = "http://localhost:9000";
const S3_ACCESS_KEY_ID_VALUE: &str = "admin";
const S3_SECRET_ACCESS_KEY_VALUE: &str = "password";
const S3_REGION_VALUE: &str = "us-east-1";
const MIN_PARQUET_SIZE: usize = 2; // TODO: figure out a good number here


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

#[allow(dead_code)]
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

            catalog.create_table(&namespace_ident, creation).await
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

#[derive(Clone)]
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
            return Err(iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "No such table"))
        }
    };

    let snapshot_log = Vec::from_iter(table.metadata().history());
    let mut compactions = vec!();
    for snapshot_info in snapshot_log.iter().rev() {
        let snapshot = match table.metadata().snapshot_by_id(snapshot_info.snapshot_id) {
            Some(s) => s,
            None => panic!("That's weird")
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


struct CompactionCommand {
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
        let options = IdGeneratorOptions::new().worker_id(1).worker_id_bit_len(6);
        match IdInstance::init(options) {
            Ok(_) => (),
            Err(_) => panic!("What happened?")
        }

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
                let error = format!("{}", e);
                println!("{}", error);
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

    #[allow(dead_code)] // Used in testing
    fn fix_fields(fields: &Fields) -> Schema {
        let mut builder = SchemaBuilder::new();
        for (index, field) in fields.iter().enumerate() {
            let new_field = Field::new(
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
            ).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                (index + 1).to_string()
            )]));
            builder.push(new_field);
        }
        builder.finish()
    }


    async fn update_iceberg(data: &Vec<RecordBatch>, table_name: &String, compaction_id: &String) -> () {
        let converted_schema = match arrow_schema_to_schema(&data[0].schema()) {
            Ok(s) => s,
            Err(e) => {
                let error = format!("{}", e);
                panic!("oh no = {}", error);
            },
        };

        match Self::append_iceberg_table(
            &"default".to_string(),
            table_name,
            converted_schema,
            compaction_id,
            &data,
        ).await {
            Ok(_) => (),
            Err(_) => {
                panic!("nope");
            },
        }

    }

    async fn do_iceberg_commit(table_name: &String, schema: &PowdrrSchema, last_snapshot_id: i64) -> Result<i64, RecvError> {
        let lib_metadata = match load_table_metadata(
            &"default".to_string(),
            table_name,
            last_snapshot_id
        ).await {
            Ok(m) => m,
            Err(_) => panic!("nope"),
        };

        match API_SERVICE_CLIENT.iceberg_commit(
            table_name,
            &IcebergCommit {
                metadata: IcebergMetadata {
                    table_schema: schema.clone(),
                    snapshot_id: lib_metadata.snapshot_id.to_string(),
                    files: lib_metadata.files.clone(),
                    sizes: lib_metadata.sizes,
                    column_names: lib_metadata.column_names,
                    column_stats: lib_metadata.column_stats,
                    schemas: vec!(schema.clone()),
                    file_schemas: lib_metadata.files.iter().map(|_|0).collect(),
                },
                compactions: lib_metadata.compactions,
            }
        ).await {
            Ok(_) => (),
            Err(e) => return Err(e)
        };

        return Ok(lib_metadata.snapshot_id)
    }

}

#[async_trait]
impl Command for CompactionCommand {
    async fn get_private_invocation(&self) -> PrivateInvocation {
        assert_eq!(self.work_item.iceberg_files.len(), 0, "Iceberg file compaction is not yet implemented");
        PrivateInvocation::Compaction(PrivateCompactionInvocation {
            sql: SqlBuilder::for_compaction().build(),
            speedboat_files: self.work_item.speedboat_files.clone(),
            schemas: self.work_item.schemas.clone(),
            file_schemas: self.work_item.file_schemas.clone(),
            table_schema: self.work_item.table_schema.clone(),
            delete_files: self.work_item.delete_files.clone(),
        })
    }

    async fn result_generator(&self, result_table_name: Option<String>) -> ElasticSearchResponse {
        let table = self.table.clone();
        let compactions = vec!(self.compaction_id.clone());
        let schema = self.work_item.table_schema.clone();
        let old_snapshot_id = self.last_snapshot_id;
        let table_name = match result_table_name {
            Some(t) => t,
            None => {
                // TODO: Need to commit that after this compaction there is....nothing?
                // Maybe this should panic since it shouldn't be possible to get here.
                let none = CompactionResult::None;
                return ElasticSearchResponse {
                    status: StatusCode::OK,
                    mime: mime::APPLICATION_JSON,
                    body: serde_json::to_string(&none).unwrap(),
                    headers: vec![],
                };
            }
        };
        let remaining_deletes_data_frame = match execute_sql(&format!("select _dt_id_seq_no as _id_seq_no from {table_name} where _id is null and _dt_id_seq_no is not null")).await {
            Ok(df) => df,
            Err(_) => panic!("nope")
        };
        let results_data_frame = match execute_sql(&format!("select * from {table_name} where _id is not null and _dt_id_seq_no is null")).await {
            Ok(df) => {
                match df.drop_columns(&["_dt_id", "_dt_seq_no", "_dt_id_seq_no"]) {
                    Ok(df) => df,
                    Err(_) => panic!("nope")
                }
            },
            Err(_) => panic!("nope")
        };

        let (compacted_deletes, _) = to_serde_value(&remaining_deletes_data_frame).await;
        if compacted_deletes.len() != 0 {
            let mut deletes_buffer = WriteBuffer::new();
            deletes_buffer.push_many(compacted_deletes.iter().map(|x| serde_json::to_string(x).unwrap()).collect());
            match elastic_search_ingest::commit_general_compactions(&deletes_buffer, &table, &"delete".to_string(), &compactions).await {
                Ok(_) => (),
                Err(_) => panic!("nope"),
            };
        }

        let results_count = match results_data_frame.clone().count().await {
            Ok(c) => c,
            Err(_) => panic!("nope")
        };

        let new_snapshot_id: i64 = if results_count < MIN_PARQUET_SIZE {
            let (compacted_results, _) = to_serde_value(&results_data_frame).await;

            let mut result_buffer = WriteBuffer::new();
            result_buffer.schema = Some(schema);
            result_buffer.push_many(compacted_results.iter().map(|x| serde_json::to_string(x).unwrap()).collect());
            match elastic_search_ingest::commit_general_compactions(&result_buffer, &table, &"compact".to_string(), &compactions).await {
                Ok(_) => (),
                Err(_) => panic!("nope"),
            };

            old_snapshot_id
        } else {
            // TODO: if nulls can come out there that probably means we need to intercept earlier
            // in the pipeline to cast nulls to real types.
            let null_fields = results_data_frame.schema().fields().iter()
                .filter(|f| f.data_type() == &DataType::Null || f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none())
                .map(|f|f.name().as_str())
                .collect::<Vec<&str>>();
            let non_null_results = match results_data_frame.clone().drop_columns(null_fields.as_slice()) {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };

            let data = match non_null_results.collect().await {
                Ok(d) => d,
                Err(_) => panic!("nope")
            };
            Self::update_iceberg(
                &data,
                &table,
                &compactions[0]
            ).await;

            Self::do_iceberg_commit(
                &table,
                &schema,
                0
            ).await.unwrap()
        };

        ElasticSearchResponse {
            status: StatusCode::OK,
            mime: mime::TEXT_PLAIN,
            body: new_snapshot_id.to_string(),
            headers: vec![],
        }
    }
}


async fn compact_logs(command: Arc<dyn Command>) -> Result<i64, CompactionError>{
    let response = execute_command(CommandContext{}, command).await;
    Ok(response.body.parse::<i64>().unwrap())
}


pub(crate) async fn perform_compaction(work_items: Vec<(String, CompactionWorkItem)>, last_snapshot_id: i64) -> Result<i64, CompactionError> {
    let new_last_snapshot_id = last_snapshot_id;
    for (table_name, work_item) in work_items.iter() {
        assert_eq!(work_item.iceberg_files.len(), 0, "Iceberg file compaction is not yet implemented");

        let compaction_id = IdInstance::next_id().to_string();

        // NOTE: the api commit must happen before the iceberg commit. The main_lib is designed to understand that
        // a compaction commit might get committed to it but fail afterwards. If we commit to Iceberg and fail to
        // record that in the main_lib then that leads to correctness errors that aren't really possible to fix.
        match API_SERVICE_CLIENT.compaction_commit(
            table_name,
            &CompactionCommit {
                removed_speedboat_files: work_item.speedboat_files.clone(),
                removed_iceberg_files: work_item.iceberg_files.clone(),
                compaction_id: compaction_id.clone(),
                removed_delete_files: work_item.delete_files.clone(),
            }
        ).await {
            Ok(_) => (),
            Err(_) => return Err(CompactionError { message: "api call failed".to_string() }),
        }

        let command = CompactionCommand {
            table: table_name.clone(),
            work_item: work_item.clone(),
            compaction_id: compaction_id.clone(),
            last_snapshot_id: last_snapshot_id,
        };

        compact_logs(Arc::new(command)).await?;
        // TODO: need to figure out the last snapshot stuff when iceberg is implemented
    }
   
    Ok(new_last_snapshot_id)
}


#[cfg(test)]
mod tests {
    use std::io::BufReader;
    use std::sync::Arc;
    use arrow_json::reader::infer_json_schema;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::error::ArrowError;
    use gotham::test::Server;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::{drop_table, ensure_table, load_table_metadata, CompactionCommand};
    use iceberg::io::{
        FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
    };
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
        match drop_table(&"default".to_string(), &"logs".to_string()).await {
            Ok(_) => (),
            Err(_) => {
            }
        }

        let file_content = include_str!("../tests/data/logs.json");
        let (schema, _) = infer_json_schema(BufReader::new(file_content.as_bytes()), None).unwrap();
        let fixed_schema = CompactionCommand::fix_fields(&schema.fields);
        let json = arrow_json::ReaderBuilder::new(Arc::new(fixed_schema)).build(BufReader::new(file_content.as_bytes())).unwrap();
        let batch = json.collect::<Result<Vec<RecordBatch>, ArrowError>>().unwrap();

        CompactionCommand::update_iceberg(
            &batch,
            &"logs".to_string(),
            &"thing1".to_string()
        ).await;

        let metadata = match load_table_metadata(&"default".to_string(), &"logs".to_string(), -1).await {
            Ok(m) => m,
            Err(e) => {
                panic!("nope {}", e)
            },
        };

        assert_eq!(metadata.files.len(), 1);
        assert_eq!(metadata.compactions.len(), 1);
        assert_eq!(metadata.column_names.len(), 0);
        assert_eq!(metadata.column_stats.len(), 0);

        match drop_table(&"default".to_string(), &"logs".to_string()).await {
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
