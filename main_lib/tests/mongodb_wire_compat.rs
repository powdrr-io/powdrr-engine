use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use futures_util::future;
use gotham::bind_server;
use mongodb::Client as MongoClient;
use mongodb::bson::{Document, doc};
use powdrr_lib::data_contract::{
    FileSetPayload, IcebergFileStats, IcebergMetadata, TableMetadataCheckpoint,
};
use powdrr_lib::mongodb_wire_protocol::serve_mongodb_wire;
use powdrr_lib::router::router;
use powdrr_lib::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema};
use powdrr_lib::test_api::{
    CacheMode, CompactionMode, IndexingMode, PeerMode, PrefetchMode, StateMode, StorageMode,
    TestProcessingMode,
};
use reqwest::Client as HttpClient;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct HttpPowdrrServer {
    base_url: String,
    task: JoinHandle<()>,
}

impl HttpPowdrrServer {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            bind_server(listener, router(true), |socket| future::ok::<_, ()>(socket)).await;
        });

        Self {
            base_url: format!("http://{}", address),
            task,
        }
    }
}

impl Drop for HttpPowdrrServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct MongoWireServer {
    uri: String,
    task: JoinHandle<()>,
}

impl MongoWireServer {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            serve_mongodb_wire(listener).await.unwrap();
        });

        Self {
            uri: format!(
                "mongodb://127.0.0.1:{}/?directConnection=true&serverSelectionTimeoutMS=2000",
                address.port()
            ),
            task,
        }
    }
}

impl Drop for MongoWireServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[test]
fn mongodb_rust_driver_can_ping_and_find_over_powdrr_wire() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        run_mongodb_wire_compat_test().await;
    });
}

async fn run_mongodb_wire_compat_test() {
    let temp_dir = TempDir::new().unwrap();
    let parquet_path = temp_dir.path().join("mongo_wire_logs.parquet");
    write_mongo_test_parquet(&parquet_path);

    let table_name = unique_name("mongo_wire_logs");
    let database = unique_name("mongo_wire_db");
    let collection = "logs";

    let http_server = HttpPowdrrServer::spawn().await;
    let mongo_server = MongoWireServer::spawn().await;
    configure_testing_mode(&http_server.base_url).await;
    add_checkpoint(&http_server.base_url, &table_name, &parquet_path).await;
    configure_serving(&http_server.base_url, &table_name).await;
    configure_mongo(&http_server.base_url, &table_name, &database, collection).await;

    let client = MongoClient::with_uri_str(&mongo_server.uri).await.unwrap();

    let ping_result = client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .unwrap();
    assert_eq!(ping_result.get_f64("ok").unwrap(), 1.0);

    let list_databases = client
        .database("admin")
        .run_command(doc! { "listDatabases": 1 })
        .await
        .unwrap();
    assert!(
        list_databases["databases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| {
                entry
                    .as_document()
                    .and_then(|entry| entry.get_str("name").ok())
                    == Some(database.as_str())
            }),
        "Mongo wire listDatabases did not include {}: {:?}",
        database,
        list_databases
    );

    let collection = client
        .database(&database)
        .collection::<Document>(collection);
    let found = collection
        .find_one(doc! { "_id": "1_1" })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.get_str("_id").unwrap(), "1_1");
    assert_eq!(found.get_str("message").unwrap(), "Login attempt failed");
}

async fn configure_testing_mode(base_url: &str) {
    let client = HttpClient::new();
    client
        .put(format!(
            "{}/_test/v1/_testing_and_processing_mode",
            base_url
        ))
        .json(&TestProcessingMode {
            state_mode: StateMode::Testing,
            storage_mode: StorageMode::default(),
            cache_mode: CacheMode::Redis(None),
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Disabled,
            compaction_mode: CompactionMode::Disabled,
            prefetch_mode: PrefetchMode::Disabled,
        })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

async fn add_checkpoint(base_url: &str, table_name: &str, parquet_path: &Path) {
    let schema = PowdrrSchema::from(&vec![
        PowdrrField {
            name: "_id_seq_no".to_string(),
            data_type: PowdrrDataType::String,
        },
        PowdrrField {
            name: "message".to_string(),
            data_type: PowdrrDataType::String,
        },
    ]);
    let file_path = format!("file://{}", parquet_path.display());
    let file_size = fs::metadata(parquet_path).unwrap().len();
    let files = FileSetPayload {
        file_paths: vec![file_path.clone()],
        schemas: vec![schema.clone()],
        file_schemas: vec![0],
        sizes: vec![file_size],
    };
    let checkpoint = TableMetadataCheckpoint {
        table_name: table_name.to_string(),
        original_checkpoint_id: None,
        checkpoint_id: "checkpoint_0".to_string(),
        iceberg_metadata: Some(IcebergMetadata {
            table_schema: schema.clone(),
            snapshot_id: Some("snapshot_1".to_string()),
            files,
            column_names: vec![],
            column_stats: vec![],
            file_stats: vec![IcebergFileStats {
                file_path,
                record_count: Some(2),
                columns: vec![],
                row_groups: vec![],
            }],
        }),
        speedboat_metadata: None,
        deletes_metadata: None,
        extension_metadata: HashMap::new(),
        schema,
    };

    HttpClient::new()
        .post(format!("{}/_test/v1/_add_checkpoint", base_url))
        .json(&checkpoint)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

async fn configure_serving(base_url: &str, table_name: &str) {
    HttpClient::new()
        .put(format!("{}/{}/_serve/config", base_url, table_name))
        .json(&serde_json::json!({
            "patterns": [
                {
                    "name": "mongo_wire_id_lookup",
                    "eq_fields": ["_id_seq_no"],
                    "max_limit": 10
                }
            ]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

async fn configure_mongo(base_url: &str, table_name: &str, database: &str, collection: &str) {
    HttpClient::new()
        .put(format!("{}/{}/_mongo/config", base_url, table_name))
        .json(&serde_json::json!({
            "enabled": true,
            "database": database,
            "collection": collection,
            "id": { "field": "_id_seq_no" }
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

fn write_mongo_test_parquet(path: &Path) {
    let schema = Schema::new(vec![
        Field::new("_id_seq_no", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        std::sync::Arc::new(schema.clone()),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["1_1", "2_1"])) as ArrayRef,
            std::sync::Arc::new(StringArray::from(vec![
                "Login attempt failed",
                "Login successful",
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, std::sync::Arc::new(schema), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn unique_name(prefix: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}_{}", prefix, suffix)
}
