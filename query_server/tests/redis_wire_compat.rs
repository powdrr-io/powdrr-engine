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
use powdrr_query_lib::data_contract::{
    FileSetPayload, IcebergFileStats, IcebergMetadata, TableMetadataCheckpoint,
};
use powdrr_query_lib::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema};
use powdrr_query_runtime::test_api::{
    CacheMode, CompactionMode, IndexingMode, PeerMode, PrefetchMode, StateMode, StorageMode,
    TestProcessingMode,
};
use powdrr_query_server::redis_wire_protocol::serve_redis_wire;
use powdrr_query_server::router::router;
use redis::Commands;
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

struct RedisWireServer {
    url: String,
    task: JoinHandle<()>,
}

impl RedisWireServer {
    async fn spawn(database: u32) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            serve_redis_wire(listener).await.unwrap();
        });

        Self {
            url: format!("redis://127.0.0.1:{}/{}", address.port(), database),
            task,
        }
    }
}

impl Drop for RedisWireServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[test]
fn redis_rust_client_can_ping_get_and_mget_over_powdrr_wire() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        run_redis_wire_compat_test().await;
    });
}

async fn run_redis_wire_compat_test() {
    let temp_dir = TempDir::new().unwrap();
    let parquet_path = temp_dir.path().join("redis_wire_cache.parquet");
    write_redis_test_parquet(&parquet_path);

    let table_name = unique_name("redis_wire_cache");
    let database = 7u32;

    let http_server = HttpPowdrrServer::spawn().await;
    let redis_server = RedisWireServer::spawn(database).await;
    configure_testing_mode(&http_server.base_url).await;
    add_checkpoint(&http_server.base_url, &table_name, &parquet_path).await;
    configure_serving(&http_server.base_url, &table_name).await;
    configure_redis(&http_server.base_url, &table_name, database).await;

    let client = redis::Client::open(redis_server.url.clone()).unwrap();
    let (ping, alpha, missing, values, exists_count) =
        tokio::task::spawn_blocking(move || -> (String, Option<String>, Option<String>, Vec<Option<String>>, i64) {
            let mut connection = client.get_connection().unwrap();
            let ping: String = redis::cmd("PING").query(&mut connection).unwrap();
            let alpha: Option<String> = connection.get("alpha").unwrap();
            let missing: Option<String> = connection.get("missing").unwrap();
            let values: Vec<Option<String>> =
                connection.get(&["alpha", "missing", "bravo"]).unwrap();
            let exists_count: i64 = redis::cmd("EXISTS")
                .arg(&["alpha", "missing", "bravo"])
                .query(&mut connection)
                .unwrap();
            (ping, alpha, missing, values, exists_count)
        })
        .await
        .unwrap();

    assert_eq!(ping, "PONG");
    assert_eq!(alpha, Some("first".to_string()));
    assert_eq!(missing, None);
    assert_eq!(
        values,
        vec![Some("first".to_string()), None, Some("second".to_string())]
    );
    assert_eq!(exists_count, 2);
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
            name: "key".to_string(),
            data_type: PowdrrDataType::String,
        },
        PowdrrField {
            name: "value".to_string(),
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
            partition_spec: vec![],
            sort_order: vec![],
            column_names: vec![],
            column_stats: vec![],
            access_artifacts: vec![],
            file_stats: vec![IcebergFileStats {
                file_path,
                record_count: Some(3),
                columns: vec![],
                partition_values: vec![],
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
                    "name": "redis_key_lookup",
                    "eq_fields": ["key"],
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

async fn configure_redis(base_url: &str, table_name: &str, database: u32) {
    HttpClient::new()
        .put(format!("{}/{}/_redis/config", base_url, table_name))
        .json(&serde_json::json!({
            "enabled": true,
            "database": database,
            "key_field": "key",
            "value_field": "value"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

fn write_redis_test_parquet(path: &Path) {
    let schema = Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        std::sync::Arc::new(schema.clone()),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["alpha", "bravo", "charlie"])) as ArrayRef,
            std::sync::Arc::new(StringArray::from(vec!["first", "second", "third"])) as ArrayRef,
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
