use std::{collections::HashMap, fs::File, future::Future, io::{BufReader, Seek}, pin::Pin, sync::{Arc, LazyLock}};

use arrow_array::RecordBatch;
use arrow_json::reader::infer_json_schema;
use arrow_schema::ArrowError;
use futures::{future::try_join_all, FutureExt, TryStreamExt};
use iceberg::{arrow::arrow_schema_to_schema, table::Table, writer::{base_writer::data_file_writer::DataFileWriterBuilder, file_writer::{location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator}, ParquetWriterBuilder}, IcebergWriter, IcebergWriterBuilder}, Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use idgenerator::{IdGeneratorOptions, IdInstance};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::properties::WriterProperties};

const REST_CATALOG_IP: &str = "localhost";
const REST_CATALOG_PORT: i16 = 8181;
const S3_ENDPOINT_VALUE: &str = "http://localhost:9000";
const S3_ACCESS_KEY_ID_VALUE: &str = "admin";
const S3_SECRET_ACCESS_KEY_VALUE: &str = "password";
const S3_REGION_VALUE: &str = "us-east-1";



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


fn get_catalog() -> RestCatalog {
    RestCatalog::new(get_iceberg_catalog_config())
}


static REST_CATALOG: LazyLock<Arc<RestCatalog>> = LazyLock::new(|| Arc::new(get_catalog()));


async fn _list_all_tables(namespace: &String) -> Result<Vec<TableIdent>, iceberg::Error> {
    let catalog = REST_CATALOG.clone();
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    match catalog.get_namespace(&namespace_ident).await {
        Ok(_) => catalog.list_tables(&namespace_ident).await,
        Err(_) => Ok(vec!())
    }
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


pub type RecordBatchFuture = dyn Future<Output = Result<Vec<RecordBatch>, iceberg::Error>> + Send;


async fn append_iceberg_table(
    namespace: &String, 
    name: &String, 
    iceberg_schema: &iceberg::spec::Schema, 
    compaction_id: &String,
    data: &Vec<RecordBatch>
) -> Result<(), iceberg::Error> {
    let options = IdGeneratorOptions::new().worker_id(1).worker_id_bit_len(6);
    match IdInstance::init(options) {
        Ok(_) => (),
        Err(_) => panic!("What happened?")
    }
        
    let table = ensure_table(namespace, name, iceberg_schema).await?;
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        IdInstance::next_id().to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
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
                let error = format!("{}", e);
                println!("{}", error);
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
    let mut action = tx.fast_append_with_properties(
        None, 
        vec![],
        HashMap::from([("compaction".to_string(), compaction_id.clone())])).unwrap();
    match action.add_data_files(data_files.clone()) {
        Ok(_) => (),
        Err(e) => {
            let error = format!("{}", e);
            println!("{}", error);
            return Err(e)               
        }
    }
    let catalog = REST_CATALOG.clone();
    action.apply().await.unwrap().commit(catalog.as_ref()).await.unwrap();

    Ok(())
}


async fn dump_as_json(file_path: &String, data: &Vec<RecordBatch>) -> Result<(), iceberg::Error> {
    let buf = File::create_new(file_path).unwrap();
    let mut writer = arrow_json::LineDelimitedWriter::new(buf);
    data.iter().for_each(|b|writer.write(b).unwrap());
    writer.finish().unwrap();       
    Ok(()) 
}


async fn load_json_file(file_path: &String) -> Result<Vec<RecordBatch>, iceberg::Error> {
    let file = File::open(file_path).unwrap();
    let mut reader = BufReader::new(file);
    let inferred_schema = infer_json_schema(&mut reader, Some(1)).unwrap();
    let _ = reader.seek(std::io::SeekFrom::Start(0));

    let json = arrow_json::ReaderBuilder::new(Arc::new(inferred_schema.0)).build(reader).unwrap();
    let result: Result<Vec<RecordBatch>, ArrowError> = json.into_iter().collect();
    match result {
        Ok(b) => Ok(b),
        Err(e) => Err(iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))
    }
}

async fn load_parquet_file(file_path: &String) -> Result<Vec<RecordBatch>, iceberg::Error> {
    let file = File::open(file_path).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();
    
    let result: Result<Vec<RecordBatch>, ArrowError> = reader.into_iter().collect();
    match result {
        Ok(b) => Ok(b),
        Err(e) => Err(iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))
    }
}


async fn load_file(file_path: &String, parquet: bool) -> Result<Vec<RecordBatch>, iceberg::Error> {
    if parquet {
        load_parquet_file(file_path).await
    } else {
        load_json_file(file_path).await
    }
}


fn load_all_files(
    json_file_paths: Vec<String>, 
    parquet_file_paths: Vec<String>
) -> Pin<Box<RecordBatchFuture>> {
    async move {
        let json_calls = json_file_paths.iter().map(|f| load_file(f, false));
        let parquet_calls = parquet_file_paths.iter().map(|f| load_file(f, true));
        let all_calls = json_calls.chain(parquet_calls);
        let result = try_join_all(all_calls).await;
        match result {
            Ok(r) => Ok(r.iter().flatten().map(|r|r.clone()).collect()),
            Err(e) => Err(iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))
        }
    }.boxed()
}

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

pub async fn compact_logs(
    namespace: &String, 
    name: &String,
    compaction_id: &String,
    json_files: &Vec<String>,
    parquet_files: &Vec<String>,
    iceberg_threshold: usize,
) -> Result<CompactionResult, iceberg::Error> {
    compact(namespace, name, compaction_id, json_files, parquet_files, iceberg_threshold).await
}


fn new_file_name(table: &String, old_file_name: &String) -> String {
    let last_slash_index = old_file_name.rfind("/").unwrap();

    return format!("{}/compact-{}-{}.json", &old_file_name[..last_slash_index], table, IdInstance::next_id().to_string())
}


pub async fn compact(
    namespace: &String, 
    name: &String,
    compaction_id: &String,
    json_files: &Vec<String>,
    parquet_files: &Vec<String>,
    iceberg_threshold: usize,
) -> Result<CompactionResult, iceberg::Error> {
    if json_files.len() == 0 {
        panic!("You must pass in at least one json file");
    }
    let data = match load_all_files(json_files.clone(), parquet_files.clone()).await {
        Ok(d) => d,
        Err(e) => return Err(e)
    };

    let total_rows = match data.iter().map(|d| d.num_rows()).reduce(|x, y| x + y) {
        Some(num) => num,
        None => return Ok(CompactionResult::None)
    };

    if total_rows >= iceberg_threshold {
        let schema = data.get(0).unwrap().schema();
        let converted_schema = match arrow_schema_to_schema(schema.clone().as_ref()) {
            Ok(s) => s,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                return Err(e)
            }
        };

        match append_iceberg_table(
            namespace,
            name,
            &converted_schema,
            compaction_id,
            &data,
        ).await {
            Ok(_) => Ok(CompactionResult::Iceberg{ num_records: total_rows }),
            Err(e) => Err(e)
        }
    } else {
        let file_path = new_file_name(name, json_files.get(0).unwrap());
        match dump_as_json(
            &file_path,
            &data
        ).await {
            Ok(_) => Ok(CompactionResult::Speedboat { file_location: file_path, num_records: total_rows }),
            Err(e) => Err(e),
        }
    }
}


#[derive(Clone)]
pub struct IcebergLibMetadata {
    pub snapshot_id: i64,
    pub files: Vec<String>,
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

    let table = match catalog.load_table(&table_ident).await {
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

    let plan_files = table
        .scan()
        .select_all()
        .build()
        .unwrap().plan_files().await.unwrap();

    let files_stream_raw = plan_files
        .map_ok(|f| f.data_file_path)
        .map_err(|err| iceberg::Error::new(iceberg::ErrorKind::Unexpected, "file scan task generate failed").with_source(err));

    let files_stream = Box::new(files_stream_raw);
    let files: Vec<String> = match files_stream.try_collect().await {
        Ok(f) => f,
        Err(e) => return Err(e),
    };

    Ok(IcebergLibMetadata { 
        snapshot_id: current_snapshot.snapshot_id(), 
        files: files, 
        compactions: compactions,
        column_names: vec!(), 
        column_stats: vec!(),
    })
}


#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
  
    use crate::iceberg::{compact, ensure_table, load_table_metadata, CompactionResult};
    use iceberg::io::{
        FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
    };

    use super::_list_all_tables;

    #[tokio::test]
    async fn test_iceberg_catalog_list_all_tables() {
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

        match ensure_table(&"default".to_string(), &"logs".to_string(), &iceberg_schema).await {
            Ok(_) => (),
            Err(e) => panic!("oh no = {}", e)
        };

        match _list_all_tables(&"default".to_string()).await {
            Ok(tables) => {
                match tables.len() {
                    0 => panic!("Table creation or listing failed"),
                    _ => ()
                }
            }
            Err(e) => panic!("oh no = {}", e)
        }
    }

    #[tokio::test]
    async fn test_iceberg_compact_simple() {
        match compact(
            &"default".to_string(), 
            &"simple".to_string(), 
            &"1234".to_string(),
            &vec!("/Users/greg.fee/code/monolith-rust-workspace/iceberg_lib/tests/data/simple_1.json".to_string()),
            &vec!(),
            0,
        ).await {
            Ok(result) => match result {
                CompactionResult::Iceberg{num_records} => assert_eq!(num_records, 3),
                _ => panic!("wrong")
            },
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("{}", error);
            }
        }

        let metadata = load_table_metadata(
            &"default".to_string(), 
            &"simple".to_string(),
            0,             
        ).await.unwrap();

        assert!(metadata.files.len() > 0);
    }   

    #[tokio::test]
    async fn test_s3_file_io() {
        let file_io = FileIOBuilder::new("s3")
            .with_props(vec![
                (S3_ENDPOINT, "http://localhost:9000".to_string()),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
            ])
            .build()
            .unwrap();   

        let output_file = file_io.new_output("s3://icebergdata/test_input").unwrap();
        {
            output_file.write("test_input".into()).await.unwrap();
        }

        let input_file = file_io.new_input("s3://icebergdata/test_input").unwrap();

        {
            let buffer = input_file.read().await.unwrap();
            assert_eq!(buffer, "test_input".as_bytes());
        }        
    } 
}
