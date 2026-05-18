use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::future;
use gotham::bind_server;
use powdrr_lib::router::router;
use reqwest::Client as HttpClient;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct PowdrrServer {
    base_url: String,
    task: JoinHandle<()>,
}

impl PowdrrServer {
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

impl Drop for PowdrrServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct JsSmokeFixture {
    index: String,
    alias: String,
    marker: i64,
}

#[test]
fn elasticsearch_js_client_matches_read_only_subset() {
    let _guard = lock_test_environment();

    let Err(reason) = ensure_js_smoke_dependencies() else {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            run_js_client_smoke().await;
        });
        return;
    };

    eprintln!("Skipping Elasticsearch JS client smoke run; {}", reason);
}

async fn run_js_client_smoke() {
    let powdrr_server = PowdrrServer::spawn().await;
    let marker_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let fixture = JsSmokeFixture {
        index: unique_name("js_client_smoke"),
        alias: unique_name("js_client_alias"),
        marker: marker_seed,
    };
    let external_base_url = "http://127.0.0.1:9200";

    configure_powdrr_testing_mode(&powdrr_server.base_url).await;
    seed_backend(&powdrr_server.base_url, &fixture, true).await;
    seed_backend(external_base_url, &fixture, false).await;

    let status = Command::new("node")
        .arg(js_smoke_script_path())
        .env("POWDRR_ES_JS_LOCAL_URL", &powdrr_server.base_url)
        .env("POWDRR_ES_JS_EXTERNAL_URL", external_base_url)
        .env("POWDRR_ES_JS_INDEX", &fixture.index)
        .env("POWDRR_ES_JS_ALIAS", &fixture.alias)
        .env("POWDRR_ES_JS_MARKER", fixture.marker.to_string())
        .status()
        .expect("failed to spawn Elasticsearch JS client smoke script");

    assert!(
        status.success(),
        "Elasticsearch JS client smoke script failed with status {:?}",
        status.code()
    );
}

fn lock_test_environment() -> MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn ensure_js_smoke_dependencies() -> Result<(), String> {
    require_local_service("LocalStack/DynamoDB", "127.0.0.1:4566")?;
    require_local_service("Redis", "127.0.0.1:6379")?;
    require_local_service("MinIO", "127.0.0.1:9000")?;
    require_local_service("Iceberg REST catalog", "127.0.0.1:8181")?;
    require_local_service("Elasticsearch", "127.0.0.1:9200")?;
    require_command("node", "--version")?;
    require_command("npm", "--version")?;

    let package_root = js_smoke_package_root();
    let package_json = package_root.join("package.json");
    if !package_json.exists() {
        return Err(format!(
            "missing JS smoke package at {}",
            package_json.display()
        ));
    }

    let client_package = package_root
        .join("node_modules")
        .join("@elastic")
        .join("elasticsearch")
        .join("package.json");
    if !client_package.exists() {
        return Err(format!(
            "requires npm ci --prefix {}",
            package_root.display()
        ));
    }

    Ok(())
}

fn require_local_service(name: &str, address: &str) -> Result<(), String> {
    let socket_address: SocketAddr = address.parse().unwrap();
    TcpStream::connect_timeout(&socket_address, Duration::from_millis(200))
        .map(|_| ())
        .map_err(|err| format!("requires {} at {} ({})", name, address, err))
}

fn require_command(command: &str, arg: &str) -> Result<(), String> {
    Command::new(command)
        .arg(arg)
        .status()
        .map_err(|err| format!("requires command '{}' ({})", command, err))
        .and_then(|status| {
            if status.success() {
                Ok(())
            } else {
                Err(format!(
                    "requires command '{}' to exit successfully for '{}'",
                    command, arg
                ))
            }
        })
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn js_smoke_package_root() -> PathBuf {
    workspace_root().join("tests").join("es_js_client")
}

fn js_smoke_script_path() -> PathBuf {
    js_smoke_package_root().join("smoke.mjs")
}

async fn configure_powdrr_testing_mode(base_url: &str) {
    let client = HttpClient::new();
    let response = client
        .put(format!("{}/_test/v1/_testing_mode", base_url))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "failed to configure Powdrr testing mode: status={}, body={}",
        response.status(),
        response.text().await.unwrap_or_default()
    );
}

async fn seed_backend(base_url: &str, fixture: &JsSmokeFixture, is_powdrr: bool) {
    let client = HttpClient::new();
    let create_index_response = client
        .put(format!("{}/{}", base_url, fixture.index))
        .json(&json!({
            "aliases": {
                fixture.alias.clone(): {}
            },
            "mappings": {
                "properties": {
                    "@timestamp": { "type": "date" },
                    "message": { "type": "text" },
                    "index_col": { "type": "long" },
                    "js_client_text": { "type": "text" },
                    "js_client_counter": { "type": "long" },
                    "js_client_marker": { "type": "long" }
                }
            },
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(
        create_index_response.status().is_success(),
        "failed to create index on {}: status={}, body={}",
        base_url,
        create_index_response.status(),
        create_index_response.text().await.unwrap_or_default()
    );

    let bulk_body = format!(
        concat!(
            "{{\"create\":{{\"_index\":\"{index}\",\"_id\":\"doc-1\"}}}}\n",
            "{{\"@timestamp\":\"2099-03-08T11:04:05.000Z\",\"index_col\":1,\"message\":\"Login attempt failed\",\"js_client_text\":\"Login attempt failed\",\"js_client_counter\":10,\"js_client_marker\":{marker}}}\n",
            "{{\"create\":{{\"_index\":\"{index}\",\"_id\":\"doc-2\"}}}}\n",
            "{{\"@timestamp\":\"2099-03-08T11:06:07.000Z\",\"index_col\":2,\"message\":\"Login successful\",\"js_client_text\":\"Login successful\",\"js_client_counter\":20,\"js_client_marker\":{marker}}}\n",
            "{{\"create\":{{\"_index\":\"{index}\",\"_id\":\"doc-3\"}}}}\n",
            "{{\"@timestamp\":\"2099-03-09T11:07:08.000Z\",\"index_col\":3,\"message\":\"Logout successful\",\"js_client_text\":\"Logout successful\",\"js_client_counter\":30,\"js_client_marker\":{other_marker}}}\n"
        ),
        index = fixture.index,
        marker = fixture.marker,
        other_marker = fixture.marker + 2
    );
    let bulk_response = client
        .post(format!("{}/_bulk", base_url))
        .header("content-type", "application/x-ndjson")
        .body(bulk_body)
        .send()
        .await
        .unwrap();
    assert!(
        bulk_response.status().is_success(),
        "failed to bulk index docs on {}: status={}, body={}",
        base_url,
        bulk_response.status(),
        bulk_response.text().await.unwrap_or_default()
    );

    let visibility_response = if is_powdrr {
        client
            .put(format!("{}/_test/v1/_process_work", base_url))
            .send()
            .await
            .unwrap()
    } else {
        client
            .post(format!("{}/{}/_refresh", base_url, fixture.index))
            .send()
            .await
            .unwrap()
    };
    assert!(
        visibility_response.status().is_success(),
        "failed to make fixture docs visible on {}: status={}, body={}",
        base_url,
        visibility_response.status(),
        visibility_response.text().await.unwrap_or_default()
    );
}

fn unique_name(prefix: &str) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("compat_{}_{}", prefix, timestamp)
}
