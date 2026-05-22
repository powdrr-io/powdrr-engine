use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use futures_util::future;
use gotham::bind_server;
use powdrr_query_lib::data_contract::{
    FileSetPayload, IcebergFileStats, IcebergMetadata, TableMetadataCheckpoint,
};
use powdrr_query_lib::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema};
use powdrr_query_runtime::state_provider::STATE_PROVIDER;
use powdrr_query_runtime::test_api::{
    ApiMode, CacheMode, CompactionMode, IndexingMode, PeerMode, PrefetchMode, StateMode,
    StorageMode, TestProcessingMode,
};
use powdrr_query_server::redis_wire_protocol::serve_redis_wire;
use powdrr_query_server::router::router;
use redis::Commands;
use reqwest::Client as HttpClient;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static TEST_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

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

    TEST_RUNTIME.block_on(async {
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
    let (ping, alpha, missing, values, exists_count, city, features, all_fields, hexists_city, hexists_missing) =
        tokio::task::spawn_blocking(move || -> (String, Option<String>, Option<String>, Vec<Option<String>>, i64, Option<String>, Vec<Option<String>>, HashMap<String, String>, i64, i64) {
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
            let city: Option<String> = redis::cmd("HGET")
                .arg("alpha")
                .arg("city")
                .query(&mut connection)
                .unwrap();
            let features: Vec<Option<String>> = redis::cmd("HMGET")
                .arg("alpha")
                .arg(&["city", "plan", "score", "missing_feature"])
                .query(&mut connection)
                .unwrap();
            let all_fields: HashMap<String, String> = redis::cmd("HGETALL")
                .arg("alpha")
                .query(&mut connection)
                .unwrap();
            let hexists_city: i64 = redis::cmd("HEXISTS")
                .arg("alpha")
                .arg("city")
                .query(&mut connection)
                .unwrap();
            let hexists_missing: i64 = redis::cmd("HEXISTS")
                .arg("alpha")
                .arg("missing_feature")
                .query(&mut connection)
                .unwrap();
            (
                ping,
                alpha,
                missing,
                values,
                exists_count,
                city,
                features,
                all_fields,
                hexists_city,
                hexists_missing,
            )
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
    assert_eq!(city, Some("honolulu".to_string()));
    assert_eq!(
        features,
        vec![
            Some("honolulu".to_string()),
            Some("pro".to_string()),
            Some("7".to_string()),
            None,
        ]
    );
    assert_eq!(all_fields.get("key"), Some(&"alpha".to_string()));
    assert_eq!(all_fields.get("value"), Some(&"first".to_string()));
    assert_eq!(all_fields.get("city"), Some(&"honolulu".to_string()));
    assert_eq!(all_fields.get("plan"), Some(&"pro".to_string()));
    assert_eq!(all_fields.get("score"), Some(&"7".to_string()));
    assert_eq!(hexists_city, 1);
    assert_eq!(hexists_missing, 0);
}

#[test]
fn redis_rust_client_receives_readonly_on_write_commands() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    TEST_RUNTIME.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("redis_wire_cache.parquet");
        write_redis_test_parquet(&parquet_path);

        let table_name = unique_name("redis_wire_cache_readonly");
        let database = 8u32;

        let http_server = HttpPowdrrServer::spawn().await;
        let redis_server = RedisWireServer::spawn(database).await;
        configure_testing_mode_with_api_mode(&http_server.base_url, ApiMode::ReadWrite).await;
        add_checkpoint(&http_server.base_url, &table_name, &parquet_path).await;
        configure_serving(&http_server.base_url, &table_name).await;
        configure_redis(&http_server.base_url, &table_name, database).await;
        STATE_PROVIDER.set_api_mode(ApiMode::ReadOnly).await;

        let client = redis::Client::open(redis_server.url.clone()).unwrap();
        let error = tokio::task::spawn_blocking(move || -> String {
            let mut connection = client.get_connection().unwrap();
            let result: redis::RedisResult<()> = connection.set("alpha", "updated");
            result
                .expect_err("SET should fail in read-only mode")
                .to_string()
        })
        .await
        .unwrap();

        assert!(
            error.contains("READONLY") || error.contains("ReadOnly"),
            "{error}"
        );
        assert!(error.contains("read-only mode"), "{error}");
    });
}

#[test]
fn redis_hash_commands_work_without_value_field_config() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    TEST_RUNTIME.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("redis_wire_hash_only.parquet");
        write_redis_test_parquet(&parquet_path);

        let table_name = unique_name("redis_wire_hash_only");
        let database = 9u32;

        let http_server = HttpPowdrrServer::spawn().await;
        let redis_server = RedisWireServer::spawn(database).await;
        configure_testing_mode(&http_server.base_url).await;
        add_checkpoint(&http_server.base_url, &table_name, &parquet_path).await;
        configure_serving(&http_server.base_url, &table_name).await;
        configure_redis_hash_only(&http_server.base_url, &table_name, database).await;

        let client = redis::Client::open(redis_server.url.clone()).unwrap();
        let (fields, missing) =
            tokio::task::spawn_blocking(move || -> (Vec<Option<String>>, String) {
                let mut connection = client.get_connection().unwrap();
                let fields: Vec<Option<String>> = redis::cmd("HMGET")
                    .arg("alpha")
                    .arg(&["city", "plan", "score"])
                    .query(&mut connection)
                    .unwrap();
                let missing = redis::cmd("GET")
                    .arg("alpha")
                    .query::<Option<String>>(&mut connection)
                    .expect_err("GET should fail when value_field is not configured")
                    .to_string();
                (fields, missing)
            })
            .await
            .unwrap();

        assert_eq!(
            fields,
            vec![
                Some("honolulu".to_string()),
                Some("pro".to_string()),
                Some("7".to_string()),
            ]
        );
        assert!(missing.contains("value_field"), "{missing}");
        assert!(
            missing.contains("HGET") || missing.contains("HMGET"),
            "{missing}"
        );
    });
}

async fn configure_testing_mode(base_url: &str) {
    configure_testing_mode_with_api_mode(base_url, ApiMode::ReadWrite).await;
}

async fn configure_testing_mode_with_api_mode(base_url: &str, api_mode: ApiMode) {
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
            api_mode,
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
        PowdrrField {
            name: "city".to_string(),
            data_type: PowdrrDataType::String,
        },
        PowdrrField {
            name: "plan".to_string(),
            data_type: PowdrrDataType::String,
        },
        PowdrrField {
            name: "score".to_string(),
            data_type: PowdrrDataType::Integer,
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

async fn configure_redis_hash_only(base_url: &str, table_name: &str, database: u32) {
    HttpClient::new()
        .put(format!("{}/{}/_redis/config", base_url, table_name))
        .json(&serde_json::json!({
            "enabled": true,
            "database": database,
            "key_field": "key"
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
        Field::new("city", DataType::Utf8, false),
        Field::new("plan", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
    ]);
    let batch = RecordBatch::try_new(
        std::sync::Arc::new(schema.clone()),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["alpha", "bravo", "charlie"])) as ArrayRef,
            std::sync::Arc::new(StringArray::from(vec!["first", "second", "third"])) as ArrayRef,
            std::sync::Arc::new(StringArray::from(vec!["honolulu", "oakland", "seattle"]))
                as ArrayRef,
            std::sync::Arc::new(StringArray::from(vec!["pro", "free", "team"])) as ArrayRef,
            std::sync::Arc::new(Int64Array::from(vec![7, 3, 11])) as ArrayRef,
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
