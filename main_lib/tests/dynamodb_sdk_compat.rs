use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    KeyType, KeysAndAttributes, LocalSecondaryIndex, Projection, ProjectionType,
    ReturnConsumedCapacity, ReturnValue, ScalarAttributeType, Select, TableDescription,
    TableStatus,
};
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use futures_util::future;
use gotham::bind_server;
use powdrr_lib::data_contract::{
    DynamoDbGlobalSecondaryIndexConfig, DynamoDbLocalSecondaryIndexConfig, DynamoDbTableConfig,
    FileSetPayload, IcebergMetadata, LicenseType, OrgCreds, OrgSettings, TableMetadataCheckpoint,
};
use powdrr_lib::router::router;
use powdrr_lib::serving_dataset::read_parquet_documents;
use powdrr_lib::state_provider::STATE_PROVIDER;
use powdrr_lib::test_api::{CompactionMode, IndexingMode, StateMode, TestProcessingMode};
use reqwest::Client as HttpClient;
use serde::Serialize;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Clone, Debug, Serialize)]
struct EventRow {
    tenant: String,
    ts: i64,
    event_id: String,
    region: String,
    active: bool,
    count: i64,
}

#[derive(Clone)]
struct TestGlobalSecondaryIndex {
    name: String,
    partition_key: String,
    partition_key_type: ScalarAttributeType,
    sort_key: Option<(String, ScalarAttributeType)>,
}

#[derive(Clone)]
struct TestLocalSecondaryIndex {
    name: String,
    sort_key: String,
    sort_key_type: ScalarAttributeType,
}

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

#[test]
fn dynamodb_sdk_compat_matches_localstack_for_read_only_mvp() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let Err(reason) = ensure_local_engine_dependencies() else {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            run_dynamodb_sdk_compat_test().await;
        });
        return;
    };

    eprintln!("Skipping DynamoDB SDK compatibility run; {}", reason);
}

async fn run_dynamodb_sdk_compat_test() {
    let temp_dir = TempDir::new().unwrap();
    let parquet_path = temp_dir.path().join("events.parquet");
    let rows = fixture_rows();
    write_test_parquet(&parquet_path, &rows);

    let table_name = unique_table_name("dynamo_sdk_compat");
    let begins_with_table_name = unique_table_name("dynamo_sdk_begins_with");
    let write_table_name = unique_table_name("dynamo_sdk_write");
    let region_event_id_index = "region-event-id-index".to_string();
    let tenant_count_index = "tenant-count-index".to_string();
    let powdrr_server = PowdrrServer::spawn().await;
    configure_powdrr_testing_mode(&powdrr_server.base_url).await;
    register_powdrr_test_org().await;
    configure_powdrr_table(
        &powdrr_server.base_url,
        &table_name,
        &parquet_path,
        &DynamoDbTableConfig {
            partition_key: "tenant".to_string(),
            sort_key: Some("ts".to_string()),
            local_secondary_indexes: vec![DynamoDbLocalSecondaryIndexConfig {
                name: tenant_count_index.clone(),
                sort_key: "count".to_string(),
            }],
            global_secondary_indexes: vec![DynamoDbGlobalSecondaryIndexConfig {
                name: region_event_id_index.clone(),
                partition_key: "region".to_string(),
                sort_key: Some("event_id".to_string()),
            }],
        },
    )
    .await;
    configure_powdrr_table(
        &powdrr_server.base_url,
        &begins_with_table_name,
        &parquet_path,
        &DynamoDbTableConfig {
            partition_key: "tenant".to_string(),
            sort_key: Some("event_id".to_string()),
            local_secondary_indexes: vec![],
            global_secondary_indexes: vec![],
        },
    )
    .await;
    configure_powdrr_table(
        &powdrr_server.base_url,
        &write_table_name,
        &parquet_path,
        &DynamoDbTableConfig {
            partition_key: "tenant".to_string(),
            sort_key: Some("ts".to_string()),
            local_secondary_indexes: vec![],
            global_secondary_indexes: vec![],
        },
    )
    .await;

    let powdrr_client = dynamodb_client(&powdrr_server.base_url).await;
    let localstack_client = dynamodb_client("http://127.0.0.1:4566").await;
    create_localstack_table(
        &localstack_client,
        &table_name,
        &rows,
        "ts",
        ScalarAttributeType::N,
        vec![TestLocalSecondaryIndex {
            name: tenant_count_index.clone(),
            sort_key: "count".to_string(),
            sort_key_type: ScalarAttributeType::N,
        }],
        vec![TestGlobalSecondaryIndex {
            name: region_event_id_index.clone(),
            partition_key: "region".to_string(),
            partition_key_type: ScalarAttributeType::S,
            sort_key: Some(("event_id".to_string(), ScalarAttributeType::S)),
        }],
    )
    .await;
    create_localstack_table(
        &localstack_client,
        &begins_with_table_name,
        &rows,
        "event_id",
        ScalarAttributeType::S,
        vec![],
        vec![],
    )
    .await;
    create_localstack_table(
        &localstack_client,
        &write_table_name,
        &rows,
        "ts",
        ScalarAttributeType::N,
        vec![],
        vec![],
    )
    .await;

    let powdrr_tables = powdrr_client.list_tables().limit(100).send().await.unwrap();
    assert!(
        powdrr_tables
            .table_names()
            .iter()
            .any(|name| name == &table_name),
        "Powdrr ListTables did not include {}; tables={:?}",
        table_name,
        powdrr_tables.table_names()
    );

    let localstack_tables = localstack_client
        .list_tables()
        .limit(100)
        .send()
        .await
        .unwrap();
    assert!(
        localstack_tables
            .table_names()
            .iter()
            .any(|name| name == &table_name),
        "LocalStack ListTables did not include {}; tables={:?}",
        table_name,
        localstack_tables.table_names()
    );

    let powdrr_description = powdrr_client
        .describe_table()
        .table_name(&table_name)
        .send()
        .await
        .unwrap();
    let localstack_description = localstack_client
        .describe_table()
        .table_name(&table_name)
        .send()
        .await
        .unwrap();
    compare_table_descriptions(
        powdrr_description.table().unwrap(),
        localstack_description.table().unwrap(),
    );

    let get_key = primary_key_item(&rows[1]);
    let expected_get_item = json!({
        "tenant": "acme",
        "ts": 20,
        "event_id": "evt-2",
        "count": 2,
    });
    let powdrr_get_item = powdrr_client
        .get_item()
        .table_name(&table_name)
        .set_key(Some(get_key.clone()))
        .projection_expression("#pk, ts, event_id, #count")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#count", "count")
        .send()
        .await
        .unwrap();
    let localstack_get_item = localstack_client
        .get_item()
        .table_name(&table_name)
        .set_key(Some(get_key))
        .projection_expression("#pk, ts, event_id, #count")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#count", "count")
        .send()
        .await
        .unwrap();
    let powdrr_get_json = optional_item_to_json(powdrr_get_item.item());
    let localstack_get_json = optional_item_to_json(localstack_get_item.item());
    assert_eq!(powdrr_get_json, Some(expected_get_item.clone()));
    assert_eq!(powdrr_get_json, localstack_get_json);

    let powdrr_get_capacity = powdrr_client
        .get_item()
        .table_name(&table_name)
        .set_key(Some(primary_key_item(&rows[1])))
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();
    let localstack_get_capacity = localstack_client
        .get_item()
        .table_name(&table_name)
        .set_key(Some(primary_key_item(&rows[1])))
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();
    assert_eq!(
        powdrr_get_capacity
            .consumed_capacity()
            .and_then(|capacity| capacity.table_name()),
        Some(table_name.as_str())
    );
    assert!(
        powdrr_get_capacity
            .consumed_capacity()
            .and_then(|capacity| capacity.capacity_units())
            .is_some()
    );
    assert!(
        localstack_get_capacity
            .consumed_capacity()
            .and_then(|capacity| capacity.capacity_units())
            .is_some()
    );

    let batch_keys = vec![primary_key_item(&rows[1]), primary_key_item(&rows[3])];
    let expected_batch_items = vec![
        json!({
            "tenant": "acme",
            "ts": 20,
            "event_id": "evt-2",
        }),
        json!({
            "tenant": "globex",
            "ts": 15,
            "event_id": "evt-4",
        }),
    ];
    let batch_request = KeysAndAttributes::builder()
        .set_keys(Some(batch_keys.clone()))
        .projection_expression("#pk, ts, event_id")
        .expression_attribute_names("#pk", "tenant")
        .build()
        .unwrap();
    let powdrr_batch = powdrr_client
        .batch_get_item()
        .request_items(table_name.clone(), batch_request.clone())
        .send()
        .await
        .unwrap();
    let localstack_batch = localstack_client
        .batch_get_item()
        .request_items(table_name.clone(), batch_request)
        .send()
        .await
        .unwrap();
    assert_eq!(
        powdrr_batch.unprocessed_keys().map(|keys| keys.len()),
        Some(0)
    );
    assert_eq!(
        normalize_item_list(batch_output_items(&powdrr_batch, &table_name)),
        normalize_item_list(expected_batch_items.clone())
    );
    assert_eq!(
        normalize_item_list(batch_output_items(&powdrr_batch, &table_name)),
        normalize_item_list(batch_output_items(&localstack_batch, &table_name))
    );

    let expected_query_page_one = vec![
        json!({
            "tenant": "acme",
            "ts": 10,
            "event_id": "evt-1",
            "region": "us-east-1",
        }),
        json!({
            "tenant": "acme",
            "ts": 20,
            "event_id": "evt-2",
            "region": "us-west-2",
        }),
    ];
    let powdrr_query_page_one = query_page(&powdrr_client, &table_name, None).await;
    let localstack_query_page_one = query_page(&localstack_client, &table_name, None).await;
    assert_eq!(powdrr_query_page_one.count(), 2);
    assert_eq!(powdrr_query_page_one.scanned_count(), 2);
    assert_eq!(
        normalize_item_list(items_to_json(powdrr_query_page_one.items())),
        normalize_item_list(expected_query_page_one)
    );
    assert_eq!(
        normalize_item_list(items_to_json(powdrr_query_page_one.items())),
        normalize_item_list(items_to_json(localstack_query_page_one.items()))
    );
    let expected_last_key = json!({
        "tenant": "acme",
        "ts": 20,
    });
    let powdrr_last_key = optional_item_to_json(powdrr_query_page_one.last_evaluated_key());
    let localstack_last_key = optional_item_to_json(localstack_query_page_one.last_evaluated_key());
    assert_eq!(powdrr_last_key, Some(expected_last_key.clone()));
    assert_eq!(powdrr_last_key, localstack_last_key);

    let powdrr_query_page_two = query_page(
        &powdrr_client,
        &table_name,
        powdrr_query_page_one.last_evaluated_key().cloned(),
    )
    .await;
    let localstack_query_page_two = query_page(
        &localstack_client,
        &table_name,
        localstack_query_page_one.last_evaluated_key().cloned(),
    )
    .await;
    let expected_query_page_two = vec![json!({
        "tenant": "acme",
        "ts": 30,
        "event_id": "evt-3",
        "region": "eu-central-1",
    })];
    assert_eq!(powdrr_query_page_two.count(), 1);
    assert_eq!(powdrr_query_page_two.scanned_count(), 1);
    assert_eq!(
        normalize_item_list(items_to_json(powdrr_query_page_two.items())),
        normalize_item_list(expected_query_page_two)
    );
    assert_eq!(
        normalize_item_list(items_to_json(powdrr_query_page_two.items())),
        normalize_item_list(items_to_json(localstack_query_page_two.items()))
    );
    assert_eq!(powdrr_query_page_two.last_evaluated_key(), None);
    assert_eq!(localstack_query_page_two.last_evaluated_key(), None);

    let powdrr_filter_page_one = query_with_filter_page(&powdrr_client, &table_name, None).await;
    let localstack_filter_page_one =
        query_with_filter_page(&localstack_client, &table_name, None).await;
    let expected_filter_page_one = vec![json!({
        "tenant": "acme",
        "ts": 20,
        "event_id": "evt-2",
        "count": 2,
    })];
    assert_eq!(
        items_to_json(powdrr_filter_page_one.items()),
        expected_filter_page_one
    );
    assert_eq!(powdrr_filter_page_one.count(), 1);
    assert_eq!(powdrr_filter_page_one.scanned_count(), 2);
    assert_eq!(
        items_to_json(powdrr_filter_page_one.items()),
        items_to_json(localstack_filter_page_one.items())
    );
    assert_eq!(
        powdrr_filter_page_one.count(),
        localstack_filter_page_one.count()
    );
    assert_eq!(
        powdrr_filter_page_one.scanned_count(),
        localstack_filter_page_one.scanned_count()
    );
    let expected_filter_last_key = json!({
        "tenant": "acme",
        "ts": 20,
    });
    let powdrr_filter_last_key = optional_item_to_json(powdrr_filter_page_one.last_evaluated_key());
    let localstack_filter_last_key =
        optional_item_to_json(localstack_filter_page_one.last_evaluated_key());
    assert_eq!(
        powdrr_filter_last_key,
        Some(expected_filter_last_key.clone())
    );
    assert_eq!(powdrr_filter_last_key, localstack_filter_last_key);

    let powdrr_filter_page_two = query_with_filter_page(
        &powdrr_client,
        &table_name,
        powdrr_filter_page_one.last_evaluated_key().cloned(),
    )
    .await;
    let localstack_filter_page_two = query_with_filter_page(
        &localstack_client,
        &table_name,
        localstack_filter_page_one.last_evaluated_key().cloned(),
    )
    .await;
    let expected_filter_page_two = vec![json!({
        "tenant": "acme",
        "ts": 30,
        "event_id": "evt-3",
        "count": 3,
    })];
    assert_eq!(
        items_to_json(powdrr_filter_page_two.items()),
        expected_filter_page_two
    );
    assert_eq!(powdrr_filter_page_two.count(), 1);
    assert_eq!(powdrr_filter_page_two.scanned_count(), 1);
    assert_eq!(
        items_to_json(powdrr_filter_page_two.items()),
        items_to_json(localstack_filter_page_two.items())
    );
    assert_eq!(
        powdrr_filter_page_two.count(),
        localstack_filter_page_two.count()
    );
    assert_eq!(
        powdrr_filter_page_two.scanned_count(),
        localstack_filter_page_two.scanned_count()
    );
    assert_eq!(powdrr_filter_page_two.last_evaluated_key(), None);
    assert_eq!(localstack_filter_page_two.last_evaluated_key(), None);

    let expected_begins_with_page_one = vec![
        json!({
            "tenant": "acme",
            "event_id": "evt-1",
            "region": "us-east-1",
            "ts": 10,
        }),
        json!({
            "tenant": "acme",
            "event_id": "evt-2",
            "region": "us-west-2",
            "ts": 20,
        }),
    ];
    let powdrr_begins_with_page_one =
        query_begins_with_page(&powdrr_client, &begins_with_table_name, None).await;
    let localstack_begins_with_page_one =
        query_begins_with_page(&localstack_client, &begins_with_table_name, None).await;
    assert_eq!(
        items_to_json(powdrr_begins_with_page_one.items()),
        expected_begins_with_page_one
    );
    assert_eq!(
        items_to_json(powdrr_begins_with_page_one.items()),
        items_to_json(localstack_begins_with_page_one.items())
    );
    let expected_begins_with_last_key = json!({
        "tenant": "acme",
        "event_id": "evt-2",
    });
    let powdrr_begins_with_last_key =
        optional_item_to_json(powdrr_begins_with_page_one.last_evaluated_key());
    let localstack_begins_with_last_key =
        optional_item_to_json(localstack_begins_with_page_one.last_evaluated_key());
    assert_eq!(
        powdrr_begins_with_last_key,
        Some(expected_begins_with_last_key.clone())
    );
    assert_eq!(powdrr_begins_with_last_key, localstack_begins_with_last_key);

    let expected_begins_with_page_two = vec![json!({
        "tenant": "acme",
        "event_id": "evt-3",
        "region": "eu-central-1",
        "ts": 30,
    })];
    let powdrr_begins_with_page_two = query_begins_with_page(
        &powdrr_client,
        &begins_with_table_name,
        powdrr_begins_with_page_one.last_evaluated_key().cloned(),
    )
    .await;
    let localstack_begins_with_page_two = query_begins_with_page(
        &localstack_client,
        &begins_with_table_name,
        localstack_begins_with_page_one
            .last_evaluated_key()
            .cloned(),
    )
    .await;
    assert_eq!(
        items_to_json(powdrr_begins_with_page_two.items()),
        expected_begins_with_page_two
    );
    assert_eq!(
        items_to_json(powdrr_begins_with_page_two.items()),
        items_to_json(localstack_begins_with_page_two.items())
    );
    assert_eq!(powdrr_begins_with_page_two.last_evaluated_key(), None);
    assert_eq!(localstack_begins_with_page_two.last_evaluated_key(), None);

    let powdrr_gsi_query =
        query_region_index_page(&powdrr_client, &table_name, &region_event_id_index).await;
    let localstack_gsi_query =
        query_region_index_page(&localstack_client, &table_name, &region_event_id_index).await;
    let expected_gsi_items = vec![
        json!({
            "tenant": "acme",
            "region": "us-east-1",
            "event_id": "evt-1",
            "ts": 10,
        }),
        json!({
            "tenant": "initech",
            "region": "us-east-1",
            "event_id": "evt-5",
            "ts": 25,
        }),
    ];
    assert_eq!(items_to_json(powdrr_gsi_query.items()), expected_gsi_items);
    assert_eq!(
        items_to_json(powdrr_gsi_query.items()),
        items_to_json(localstack_gsi_query.items())
    );

    let powdrr_gsi_capacity =
        query_region_index_page_with_capacity(&powdrr_client, &table_name, &region_event_id_index)
            .await;
    assert!(
        powdrr_gsi_capacity
            .consumed_capacity()
            .and_then(|capacity| capacity.global_secondary_indexes())
            .and_then(|indexes| indexes.get(&region_event_id_index))
            .is_some()
    );

    let powdrr_lsi_query =
        query_local_index_page(&powdrr_client, &table_name, &tenant_count_index).await;
    let localstack_lsi_query =
        query_local_index_page(&localstack_client, &table_name, &tenant_count_index).await;
    let expected_lsi_items = vec![
        json!({
            "tenant": "acme",
            "count": 2,
            "event_id": "evt-2",
            "ts": 20,
        }),
        json!({
            "tenant": "acme",
            "count": 3,
            "event_id": "evt-3",
            "ts": 30,
        }),
    ];
    assert_eq!(items_to_json(powdrr_lsi_query.items()), expected_lsi_items);
    assert_eq!(
        items_to_json(powdrr_lsi_query.items()),
        items_to_json(localstack_lsi_query.items())
    );
    assert!(
        powdrr_lsi_query
            .consumed_capacity()
            .and_then(|capacity| capacity.local_secondary_indexes())
            .and_then(|indexes| indexes.get(&tenant_count_index))
            .is_some()
    );

    let powdrr_or_filter = query_with_or_not_filter_page(&powdrr_client, &table_name).await;
    let localstack_or_filter = query_with_or_not_filter_page(&localstack_client, &table_name).await;
    assert_eq!(
        items_to_json(powdrr_or_filter.items()),
        vec![
            json!({
                "tenant": "acme",
                "ts": 20,
                "event_id": "evt-2",
                "region": "us-west-2",
                "active": false,
            }),
            json!({
                "tenant": "acme",
                "ts": 30,
                "event_id": "evt-3",
                "region": "eu-central-1",
                "active": true,
            }),
        ]
    );
    assert_eq!(
        items_to_json(powdrr_or_filter.items()),
        items_to_json(localstack_or_filter.items())
    );

    let powdrr_attribute_filter =
        query_with_attribute_meta_filter_page(&powdrr_client, &table_name).await;
    let localstack_attribute_filter =
        query_with_attribute_meta_filter_page(&localstack_client, &table_name).await;
    assert_eq!(
        items_to_json(powdrr_attribute_filter.items()),
        vec![
            json!({
                "tenant": "acme",
                "ts": 20,
                "event_id": "evt-2",
                "region": "us-west-2",
                "count": 2,
            }),
            json!({
                "tenant": "acme",
                "ts": 30,
                "event_id": "evt-3",
                "region": "eu-central-1",
                "count": 3,
            }),
        ]
    );
    assert_eq!(
        items_to_json(powdrr_attribute_filter.items()),
        items_to_json(localstack_attribute_filter.items())
    );

    let expected_scan_items = normalize_item_list(vec![
        json!({
            "tenant": "acme",
            "ts": 10,
            "event_id": "evt-1",
            "region": "us-east-1",
            "active": true,
            "count": 1,
        }),
        json!({
            "tenant": "acme",
            "ts": 20,
            "event_id": "evt-2",
            "region": "us-west-2",
            "active": false,
            "count": 2,
        }),
        json!({
            "tenant": "acme",
            "ts": 30,
            "event_id": "evt-3",
            "region": "eu-central-1",
            "active": true,
            "count": 3,
        }),
        json!({
            "tenant": "globex",
            "ts": 15,
            "event_id": "evt-4",
            "region": "ap-southeast-2",
            "active": true,
            "count": 4,
        }),
        json!({
            "tenant": "initech",
            "ts": 25,
            "event_id": "evt-5",
            "region": "us-east-1",
            "active": false,
            "count": 5,
        }),
    ]);
    let powdrr_full_scan = scan_primary_page(&powdrr_client, &table_name, None, None).await;
    let localstack_full_scan = scan_primary_page(&localstack_client, &table_name, None, None).await;
    assert_eq!(
        normalize_item_list(items_to_json(powdrr_full_scan.items())),
        expected_scan_items
    );
    assert_eq!(
        normalize_item_list(items_to_json(powdrr_full_scan.items())),
        normalize_item_list(items_to_json(localstack_full_scan.items()))
    );

    let powdrr_scan_pages = collect_scan_pages(&powdrr_client, &table_name, Some(2)).await;
    let localstack_scan_pages = collect_scan_pages(&localstack_client, &table_name, Some(2)).await;
    assert_eq!(
        normalize_item_list(flatten_scan_pages(&powdrr_scan_pages)),
        expected_scan_items
    );
    assert_eq!(
        normalize_item_list(flatten_scan_pages(&powdrr_scan_pages)),
        normalize_item_list(flatten_scan_pages(&localstack_scan_pages))
    );
    assert!(
        powdrr_scan_pages
            .first()
            .and_then(|page| page.last_evaluated_key())
            .is_some()
    );
    assert!(
        powdrr_scan_pages
            .last()
            .and_then(|page| page.last_evaluated_key())
            .is_none()
    );

    let powdrr_scan_count = scan_active_count(&powdrr_client, &table_name).await;
    let localstack_scan_count = scan_active_count(&localstack_client, &table_name).await;
    assert_eq!(powdrr_scan_count.count(), 3);
    assert_eq!(powdrr_scan_count.scanned_count(), 5);
    assert_eq!(powdrr_scan_count.count(), localstack_scan_count.count());
    assert_eq!(
        powdrr_scan_count.scanned_count(),
        localstack_scan_count.scanned_count()
    );
    assert_eq!(powdrr_scan_count.items().len(), 0);

    let powdrr_scan_capacity = scan_active_count_with_capacity(&powdrr_client, &table_name).await;
    assert_eq!(
        powdrr_scan_capacity
            .consumed_capacity()
            .and_then(|capacity| capacity.table_name()),
        Some(table_name.as_str())
    );
    assert!(
        powdrr_scan_capacity
            .consumed_capacity()
            .and_then(|capacity| capacity.capacity_units())
            .is_some()
    );

    let powdrr_put_replace = put_item_replace_existing(&powdrr_client, &write_table_name).await;
    let localstack_put_replace =
        put_item_replace_existing(&localstack_client, &write_table_name).await;
    assert_eq!(
        optional_item_to_json(powdrr_put_replace.attributes()),
        Some(json!({
            "tenant": "acme",
            "ts": 20,
            "event_id": "evt-2",
            "region": "us-west-2",
            "active": false,
            "count": 2,
        }))
    );
    assert_eq!(
        optional_item_to_json(powdrr_put_replace.attributes()),
        optional_item_to_json(localstack_put_replace.attributes())
    );

    let powdrr_put_insert = put_item_insert_new(&powdrr_client, &write_table_name).await;
    let localstack_put_insert = put_item_insert_new(&localstack_client, &write_table_name).await;
    assert_eq!(powdrr_put_insert.attributes(), None);
    assert_eq!(
        powdrr_put_insert.attributes(),
        localstack_put_insert.attributes()
    );

    let powdrr_after_insert = powdrr_client
        .get_item()
        .table_name(&write_table_name)
        .set_key(Some(primary_key_item_from_parts("acme", json!(40))))
        .send()
        .await
        .unwrap();
    let localstack_after_insert = localstack_client
        .get_item()
        .table_name(&write_table_name)
        .set_key(Some(primary_key_item_from_parts("acme", json!(40))))
        .send()
        .await
        .unwrap();
    assert_eq!(
        optional_item_to_json(powdrr_after_insert.item()),
        Some(json!({
            "tenant": "acme",
            "ts": 40,
            "event_id": "evt-40",
            "region": "us-east-2",
            "active": false,
            "count": 40,
            "note": "new",
        }))
    );
    assert_eq!(
        optional_item_to_json(powdrr_after_insert.item()),
        optional_item_to_json(localstack_after_insert.item())
    );

    let powdrr_put_condition_failure =
        put_item_condition_failure(&powdrr_client, &write_table_name)
            .await
            .unwrap_err()
            .into_service_error();
    let localstack_put_condition_failure =
        put_item_condition_failure(&localstack_client, &write_table_name)
            .await
            .unwrap_err()
            .into_service_error();
    assert_eq!(
        powdrr_put_condition_failure.meta().code(),
        Some("ConditionalCheckFailedException")
    );
    assert_eq!(
        powdrr_put_condition_failure.meta().code(),
        localstack_put_condition_failure.meta().code()
    );

    let powdrr_delete_existing = delete_item_existing(&powdrr_client, &write_table_name).await;
    let localstack_delete_existing =
        delete_item_existing(&localstack_client, &write_table_name).await;
    assert_eq!(
        optional_item_to_json(powdrr_delete_existing.attributes()),
        Some(json!({
            "tenant": "acme",
            "ts": 40,
            "event_id": "evt-40",
            "region": "us-east-2",
            "active": false,
            "count": 40,
            "note": "new",
        }))
    );
    assert_eq!(
        optional_item_to_json(powdrr_delete_existing.attributes()),
        optional_item_to_json(localstack_delete_existing.attributes())
    );

    let powdrr_delete_missing = delete_item_missing(&powdrr_client, &write_table_name).await;
    let localstack_delete_missing =
        delete_item_missing(&localstack_client, &write_table_name).await;
    assert_eq!(powdrr_delete_missing.attributes(), None);
    assert_eq!(
        powdrr_delete_missing.attributes(),
        localstack_delete_missing.attributes()
    );

    let powdrr_delete_condition_failure =
        delete_item_condition_failure(&powdrr_client, &write_table_name)
            .await
            .unwrap_err()
            .into_service_error();
    let localstack_delete_condition_failure =
        delete_item_condition_failure(&localstack_client, &write_table_name)
            .await
            .unwrap_err()
            .into_service_error();
    assert_eq!(
        powdrr_delete_condition_failure.meta().code(),
        Some("ConditionalCheckFailedException")
    );
    assert_eq!(
        powdrr_delete_condition_failure.meta().code(),
        localstack_delete_condition_failure.meta().code()
    );
}

fn ensure_local_engine_dependencies() -> Result<(), String> {
    require_local_service("LocalStack/DynamoDB", "127.0.0.1:4566")?;
    require_local_service("Redis", "127.0.0.1:6379")?;
    require_local_service("MinIO", "127.0.0.1:9000")?;
    require_local_service("Iceberg REST catalog", "127.0.0.1:8181")?;
    Ok(())
}

fn require_local_service(name: &str, address: &str) -> Result<(), String> {
    let socket_address = address.parse().unwrap();
    std::net::TcpStream::connect_timeout(&socket_address, Duration::from_millis(200))
        .map(|_| ())
        .map_err(|error| format!("requires {} at {} ({})", name, address, error))
}

async fn configure_powdrr_table(
    base_url: &str,
    table_name: &str,
    parquet_path: &Path,
    config: &DynamoDbTableConfig,
) {
    let client = HttpClient::new();
    let checkpoint = checkpoint_from_parquet(table_name, parquet_path).await;

    client
        .post(format!("{}/_test/v1/_add_checkpoint", base_url))
        .json(&checkpoint)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let url = format!("{}/{}/_dynamodb/config", base_url, table_name);
    let mut last_status = None;
    let mut last_body = String::new();

    for _ in 0..25 {
        let response = client.put(&url).json(config).send().await.unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        if status.is_success() {
            return;
        }
        if body.contains("Checkpoint did not contain a usable schema")
            || body.contains("No checkpoint was available")
            || body.contains("Checkpoint metadata was not found")
        {
            last_status = Some(status);
            last_body = body;
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        panic!(
            "PUT /{}/_dynamodb/config failed with status {} and body {}",
            table_name, status, body
        );
    }

    panic!(
        "PUT /{}/_dynamodb/config never became ready; last status {:?}, last body {}",
        table_name, last_status, last_body
    );
}

async fn configure_powdrr_testing_mode(base_url: &str) {
    let client = HttpClient::new();
    let mut mode = TestProcessingMode::default();
    mode.state_mode = StateMode::Testing;
    mode.indexing_mode = IndexingMode::Disabled;
    mode.compaction_mode = CompactionMode::Disabled;

    client
        .put(format!(
            "{}/_test/v1/_testing_and_processing_mode",
            base_url
        ))
        .json(&mode)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

async fn register_powdrr_test_org() {
    let access_key = "test".to_string();
    if STATE_PROVIDER
        .lookup_secret_access_key(&access_key)
        .await
        .unwrap()
        .as_deref()
        == Some("test")
    {
        return;
    }
    STATE_PROVIDER
        .create_org(&OrgSettings {
            org_id: "dynamodb_test_org".to_string(),
            license_type: LicenseType::Free,
            creds: vec![OrgCreds {
                access_key_id: access_key,
                secret_access_key: "test".to_string(),
                nickname: Some("dynamodb-test".to_string()),
            }],
        })
        .await
        .unwrap();
}

async fn checkpoint_from_parquet(table_name: &str, parquet_path: &Path) -> TableMetadataCheckpoint {
    let dataset_path = parquet_path.display().to_string();
    let dataset = read_parquet_documents(&dataset_path, None).await.unwrap();
    let file_size = fs::metadata(parquet_path).unwrap().len();
    let file_path = format!("file://{}", parquet_path.display());

    TableMetadataCheckpoint {
        table_name: table_name.to_string(),
        original_checkpoint_id: None,
        checkpoint_id: "checkpoint_0".to_string(),
        iceberg_metadata: Some(IcebergMetadata {
            table_schema: dataset.schema.clone(),
            snapshot_id: Some("snapshot_1".to_string()),
            files: FileSetPayload {
                file_paths: vec![file_path],
                schemas: vec![dataset.schema.clone()],
                file_schemas: vec![0],
                sizes: vec![file_size],
            },
            partition_spec: vec![],
            sort_order: vec![],
            column_names: dataset
                .schema
                .fields()
                .iter()
                .map(|field| field.name.clone())
                .collect(),
            column_stats: vec![],
            access_artifacts: vec![],
            file_stats: vec![],
        }),
        speedboat_metadata: None,
        deletes_metadata: None,
        extension_metadata: HashMap::new(),
        schema: dataset.schema,
    }
}

async fn dynamodb_client(endpoint_url: &str) -> DynamoClient {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .credentials_provider(Credentials::new("test", "test", None, None, "static"))
        .load()
        .await;
    DynamoClient::new(&config)
}

async fn create_localstack_table(
    client: &DynamoClient,
    table_name: &str,
    rows: &[EventRow],
    sort_key_name: &str,
    sort_key_type: ScalarAttributeType,
    local_secondary_indexes: Vec<TestLocalSecondaryIndex>,
    global_secondary_indexes: Vec<TestGlobalSecondaryIndex>,
) {
    let _ = client.delete_table().table_name(table_name).send().await;

    let mut create_table = client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("tenant")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(sort_key_name)
                .attribute_type(sort_key_type.clone())
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("tenant")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(sort_key_name)
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest);

    let mut declared_attributes = vec![
        ("tenant".to_string(), ScalarAttributeType::S),
        (sort_key_name.to_string(), sort_key_type),
    ];
    for index in local_secondary_indexes.iter() {
        if !declared_attributes
            .iter()
            .any(|(name, _)| name == &index.sort_key)
        {
            create_table = create_table.attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(index.sort_key.clone())
                    .attribute_type(index.sort_key_type.clone())
                    .build()
                    .unwrap(),
            );
            declared_attributes.push((index.sort_key.clone(), index.sort_key_type.clone()));
        }
        let lsi = LocalSecondaryIndex::builder()
            .index_name(index.name.clone())
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("tenant")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(index.sort_key.clone())
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .projection(
                Projection::builder()
                    .projection_type(ProjectionType::All)
                    .build(),
            )
            .build()
            .unwrap();
        create_table = create_table.local_secondary_indexes(lsi);
    }
    for index in global_secondary_indexes.iter() {
        if !declared_attributes
            .iter()
            .any(|(name, _)| name == &index.partition_key)
        {
            create_table = create_table.attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(index.partition_key.clone())
                    .attribute_type(index.partition_key_type.clone())
                    .build()
                    .unwrap(),
            );
            declared_attributes.push((
                index.partition_key.clone(),
                index.partition_key_type.clone(),
            ));
        }
        if let Some((sort_key, sort_key_type)) = index.sort_key.as_ref() {
            if !declared_attributes.iter().any(|(name, _)| name == sort_key) {
                create_table = create_table.attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(sort_key.clone())
                        .attribute_type(sort_key_type.clone())
                        .build()
                        .unwrap(),
                );
                declared_attributes.push((sort_key.clone(), sort_key_type.clone()));
            }
        }
        let mut gsi = GlobalSecondaryIndex::builder()
            .index_name(index.name.clone())
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(index.partition_key.clone())
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .projection(
                Projection::builder()
                    .projection_type(ProjectionType::All)
                    .build(),
            );
        if let Some((sort_key, _)) = index.sort_key.as_ref() {
            gsi = gsi.key_schema(
                KeySchemaElement::builder()
                    .attribute_name(sort_key.clone())
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            );
        }
        create_table = create_table.global_secondary_indexes(gsi.build().unwrap());
    }

    create_table.send().await.unwrap();

    client
        .wait_until_table_exists()
        .table_name(table_name)
        .wait(Duration::from_secs(5))
        .await
        .unwrap();

    for row in rows {
        client
            .put_item()
            .table_name(table_name)
            .set_item(Some(
                serde_dynamo::aws_sdk_dynamodb_1::to_item(row.clone()).unwrap(),
            ))
            .send()
            .await
            .unwrap();
    }
}

async fn query_page(
    client: &DynamoClient,
    table_name: &str,
    exclusive_start_key: Option<HashMap<String, AttributeValue>>,
) -> aws_sdk_dynamodb::operation::query::QueryOutput {
    client
        .query()
        .table_name(table_name)
        .key_condition_expression("#pk = :pk AND #sk BETWEEN :start AND :end")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "ts")
        .expression_attribute_names("#region", "region")
        .expression_attribute_values(":pk", AttributeValue::S("acme".to_string()))
        .expression_attribute_values(":start", AttributeValue::N("10".to_string()))
        .expression_attribute_values(":end", AttributeValue::N("30".to_string()))
        .projection_expression("#pk, #sk, event_id, #region")
        .limit(2)
        .scan_index_forward(true)
        .set_exclusive_start_key(exclusive_start_key)
        .send()
        .await
        .unwrap()
}

async fn query_begins_with_page(
    client: &DynamoClient,
    table_name: &str,
    exclusive_start_key: Option<HashMap<String, AttributeValue>>,
) -> aws_sdk_dynamodb::operation::query::QueryOutput {
    client
        .query()
        .table_name(table_name)
        .key_condition_expression("#pk = :pk AND begins_with(#sk, :prefix)")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "event_id")
        .expression_attribute_names("#region", "region")
        .expression_attribute_values(":pk", AttributeValue::S("acme".to_string()))
        .expression_attribute_values(":prefix", AttributeValue::S("evt-".to_string()))
        .projection_expression("#pk, #sk, #region, ts")
        .limit(2)
        .scan_index_forward(true)
        .set_exclusive_start_key(exclusive_start_key)
        .send()
        .await
        .unwrap()
}

async fn query_with_filter_page(
    client: &DynamoClient,
    table_name: &str,
    exclusive_start_key: Option<HashMap<String, AttributeValue>>,
) -> aws_sdk_dynamodb::operation::query::QueryOutput {
    client
        .query()
        .table_name(table_name)
        .key_condition_expression("#pk = :pk AND #sk BETWEEN :start AND :end")
        .filter_expression("#count IN (:two, :three) AND begins_with(#event_id, :prefix)")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "ts")
        .expression_attribute_names("#count", "count")
        .expression_attribute_names("#event_id", "event_id")
        .expression_attribute_values(":pk", AttributeValue::S("acme".to_string()))
        .expression_attribute_values(":start", AttributeValue::N("10".to_string()))
        .expression_attribute_values(":end", AttributeValue::N("30".to_string()))
        .expression_attribute_values(":two", AttributeValue::N("2".to_string()))
        .expression_attribute_values(":three", AttributeValue::N("3".to_string()))
        .expression_attribute_values(":prefix", AttributeValue::S("evt-".to_string()))
        .projection_expression("#pk, #sk, #event_id, #count")
        .limit(2)
        .scan_index_forward(true)
        .set_exclusive_start_key(exclusive_start_key)
        .send()
        .await
        .unwrap()
}

async fn query_region_index_page(
    client: &DynamoClient,
    table_name: &str,
    index_name: &str,
) -> aws_sdk_dynamodb::operation::query::QueryOutput {
    client
        .query()
        .table_name(table_name)
        .index_name(index_name)
        .key_condition_expression("#pk = :pk AND begins_with(#sk, :prefix)")
        .expression_attribute_names("#pk", "region")
        .expression_attribute_names("#sk", "event_id")
        .expression_attribute_names("#tenant", "tenant")
        .expression_attribute_values(":pk", AttributeValue::S("us-east-1".to_string()))
        .expression_attribute_values(":prefix", AttributeValue::S("evt-".to_string()))
        .projection_expression("#tenant, #pk, #sk, ts")
        .scan_index_forward(true)
        .send()
        .await
        .unwrap()
}

async fn query_region_index_page_with_capacity(
    client: &DynamoClient,
    table_name: &str,
    index_name: &str,
) -> aws_sdk_dynamodb::operation::query::QueryOutput {
    client
        .query()
        .table_name(table_name)
        .index_name(index_name)
        .key_condition_expression("#pk = :pk AND begins_with(#sk, :prefix)")
        .expression_attribute_names("#pk", "region")
        .expression_attribute_names("#sk", "event_id")
        .expression_attribute_names("#tenant", "tenant")
        .expression_attribute_values(":pk", AttributeValue::S("us-east-1".to_string()))
        .expression_attribute_values(":prefix", AttributeValue::S("evt-".to_string()))
        .projection_expression("#tenant, #pk, #sk, ts")
        .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
        .scan_index_forward(true)
        .send()
        .await
        .unwrap()
}

async fn query_local_index_page(
    client: &DynamoClient,
    table_name: &str,
    index_name: &str,
) -> aws_sdk_dynamodb::operation::query::QueryOutput {
    client
        .query()
        .table_name(table_name)
        .index_name(index_name)
        .consistent_read(true)
        .key_condition_expression("#pk = :pk AND #sk BETWEEN :start AND :end")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "count")
        .expression_attribute_names("#event", "event_id")
        .expression_attribute_values(":pk", AttributeValue::S("acme".to_string()))
        .expression_attribute_values(":start", AttributeValue::N("2".to_string()))
        .expression_attribute_values(":end", AttributeValue::N("3".to_string()))
        .projection_expression("#pk, #sk, #event, ts")
        .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
        .send()
        .await
        .unwrap()
}

async fn query_with_or_not_filter_page(
    client: &DynamoClient,
    table_name: &str,
) -> aws_sdk_dynamodb::operation::query::QueryOutput {
    client
        .query()
        .table_name(table_name)
        .key_condition_expression("#pk = :pk AND #sk BETWEEN :start AND :end")
        .filter_expression("contains(#region, :prefix) OR NOT #active = :active")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "ts")
        .expression_attribute_names("#region", "region")
        .expression_attribute_names("#active", "active")
        .expression_attribute_values(":pk", AttributeValue::S("acme".to_string()))
        .expression_attribute_values(":start", AttributeValue::N("10".to_string()))
        .expression_attribute_values(":end", AttributeValue::N("30".to_string()))
        .expression_attribute_values(":prefix", AttributeValue::S("eu".to_string()))
        .expression_attribute_values(":active", AttributeValue::Bool(true))
        .projection_expression("#pk, #sk, event_id, #region, #active")
        .send()
        .await
        .unwrap()
}

async fn query_with_attribute_meta_filter_page(
    client: &DynamoClient,
    table_name: &str,
) -> aws_sdk_dynamodb::operation::query::QueryOutput {
    client
        .query()
        .table_name(table_name)
        .key_condition_expression("#pk = :pk AND #sk BETWEEN :start AND :end")
        .filter_expression(
            "attribute_exists(#region) AND attribute_not_exists(deleted_at) AND attribute_type(#region, :type) AND size(event_id) > :min_size AND #count >= :min_count",
        )
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "ts")
        .expression_attribute_names("#region", "region")
        .expression_attribute_names("#count", "count")
        .expression_attribute_names("#event", "event_id")
        .expression_attribute_values(":pk", AttributeValue::S("acme".to_string()))
        .expression_attribute_values(":start", AttributeValue::N("10".to_string()))
        .expression_attribute_values(":end", AttributeValue::N("30".to_string()))
        .expression_attribute_values(":type", AttributeValue::S("S".to_string()))
        .expression_attribute_values(":min_size", AttributeValue::N("4".to_string()))
        .expression_attribute_values(":min_count", AttributeValue::N("2".to_string()))
        .projection_expression("#pk, #sk, #event, #region, #count")
        .send()
        .await
        .unwrap()
}

async fn scan_primary_page(
    client: &DynamoClient,
    table_name: &str,
    limit: Option<i32>,
    exclusive_start_key: Option<HashMap<String, AttributeValue>>,
) -> aws_sdk_dynamodb::operation::scan::ScanOutput {
    let mut request = client
        .scan()
        .table_name(table_name)
        .expression_attribute_names("#tenant", "tenant")
        .expression_attribute_names("#ts", "ts")
        .expression_attribute_names("#event", "event_id")
        .expression_attribute_names("#region", "region")
        .expression_attribute_names("#active", "active")
        .expression_attribute_names("#count", "count")
        .projection_expression("#tenant, #ts, #event, #region, #active, #count");
    if let Some(limit) = limit {
        request = request.limit(limit);
    }
    if let Some(exclusive_start_key) = exclusive_start_key {
        request = request.set_exclusive_start_key(Some(exclusive_start_key));
    }
    request.send().await.unwrap()
}

async fn scan_active_count(
    client: &DynamoClient,
    table_name: &str,
) -> aws_sdk_dynamodb::operation::scan::ScanOutput {
    client
        .scan()
        .table_name(table_name)
        .expression_attribute_names("#active", "active")
        .expression_attribute_values(":active", AttributeValue::Bool(true))
        .filter_expression("#active = :active")
        .select(Select::Count)
        .send()
        .await
        .unwrap()
}

async fn scan_active_count_with_capacity(
    client: &DynamoClient,
    table_name: &str,
) -> aws_sdk_dynamodb::operation::scan::ScanOutput {
    client
        .scan()
        .table_name(table_name)
        .expression_attribute_names("#active", "active")
        .expression_attribute_values(":active", AttributeValue::Bool(true))
        .filter_expression("#active = :active")
        .select(Select::Count)
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap()
}

async fn collect_scan_pages(
    client: &DynamoClient,
    table_name: &str,
    limit: Option<i32>,
) -> Vec<aws_sdk_dynamodb::operation::scan::ScanOutput> {
    let mut pages = vec![];
    let mut exclusive_start_key = None;
    loop {
        let page = scan_primary_page(client, table_name, limit, exclusive_start_key).await;
        exclusive_start_key = page.last_evaluated_key().cloned();
        let done = exclusive_start_key.is_none();
        pages.push(page);
        if done {
            return pages;
        }
    }
}

async fn put_item_replace_existing(
    client: &DynamoClient,
    table_name: &str,
) -> aws_sdk_dynamodb::operation::put_item::PutItemOutput {
    client
        .put_item()
        .table_name(table_name)
        .set_item(Some(item_from_json(json!({
            "tenant": "acme",
            "ts": 20,
            "event_id": "evt-2-replaced",
            "region": "us-west-1",
            "active": true,
            "count": 22,
            "note": "replaced",
        }))))
        .condition_expression("attribute_exists(#pk) AND #count = :count")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#count", "count")
        .expression_attribute_values(":count", AttributeValue::N("2".to_string()))
        .return_values(ReturnValue::AllOld)
        .send()
        .await
        .unwrap()
}

async fn put_item_insert_new(
    client: &DynamoClient,
    table_name: &str,
) -> aws_sdk_dynamodb::operation::put_item::PutItemOutput {
    client
        .put_item()
        .table_name(table_name)
        .set_item(Some(item_from_json(json!({
            "tenant": "acme",
            "ts": 40,
            "event_id": "evt-40",
            "region": "us-east-2",
            "active": false,
            "count": 40,
            "note": "new",
        }))))
        .condition_expression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "ts")
        .return_values(ReturnValue::None)
        .send()
        .await
        .unwrap()
}

async fn put_item_condition_failure(
    client: &DynamoClient,
    table_name: &str,
) -> Result<
    aws_sdk_dynamodb::operation::put_item::PutItemOutput,
    aws_sdk_dynamodb::error::SdkError<aws_sdk_dynamodb::operation::put_item::PutItemError>,
> {
    client
        .put_item()
        .table_name(table_name)
        .set_item(Some(item_from_json(json!({
            "tenant": "acme",
            "ts": 40,
            "event_id": "evt-40-fail",
            "region": "us-east-2",
            "active": true,
            "count": 41,
        }))))
        .condition_expression("attribute_not_exists(#pk)")
        .expression_attribute_names("#pk", "tenant")
        .send()
        .await
}

async fn delete_item_existing(
    client: &DynamoClient,
    table_name: &str,
) -> aws_sdk_dynamodb::operation::delete_item::DeleteItemOutput {
    client
        .delete_item()
        .table_name(table_name)
        .set_key(Some(primary_key_item_from_parts("acme", json!(40))))
        .condition_expression("attribute_exists(note)")
        .return_values(ReturnValue::AllOld)
        .send()
        .await
        .unwrap()
}

async fn delete_item_missing(
    client: &DynamoClient,
    table_name: &str,
) -> aws_sdk_dynamodb::operation::delete_item::DeleteItemOutput {
    client
        .delete_item()
        .table_name(table_name)
        .set_key(Some(primary_key_item_from_parts("acme", json!(999))))
        .return_values(ReturnValue::AllOld)
        .send()
        .await
        .unwrap()
}

async fn delete_item_condition_failure(
    client: &DynamoClient,
    table_name: &str,
) -> Result<
    aws_sdk_dynamodb::operation::delete_item::DeleteItemOutput,
    aws_sdk_dynamodb::error::SdkError<aws_sdk_dynamodb::operation::delete_item::DeleteItemError>,
> {
    client
        .delete_item()
        .table_name(table_name)
        .set_key(Some(primary_key_item_from_parts("acme", json!(10))))
        .condition_expression("attribute_not_exists(region)")
        .send()
        .await
}

fn compare_table_descriptions(powdrr: &TableDescription, localstack: &TableDescription) {
    assert_eq!(powdrr.table_name(), localstack.table_name());
    assert_eq!(
        powdrr.table_status(),
        Some(&TableStatus::Active),
        "Powdrr DescribeTable should surface ACTIVE status"
    );
    assert_eq!(
        table_key_schema(powdrr),
        table_key_schema(localstack),
        "key schema mismatch"
    );
    assert_eq!(
        table_attribute_definitions(powdrr),
        table_attribute_definitions(localstack),
        "attribute definitions mismatch"
    );
    assert_eq!(
        powdrr
            .billing_mode_summary()
            .and_then(|summary| summary.billing_mode()),
        Some(&BillingMode::PayPerRequest),
    );
    assert_eq!(
        powdrr
            .billing_mode_summary()
            .and_then(|summary| summary.billing_mode()),
        localstack
            .billing_mode_summary()
            .and_then(|summary| summary.billing_mode()),
    );
    assert_eq!(
        table_global_secondary_indexes(powdrr),
        table_global_secondary_indexes(localstack),
        "global secondary index mismatch"
    );
    assert_eq!(
        table_local_secondary_indexes(powdrr),
        table_local_secondary_indexes(localstack),
        "local secondary index mismatch"
    );
}

fn flatten_scan_pages(pages: &[aws_sdk_dynamodb::operation::scan::ScanOutput]) -> Vec<Value> {
    pages
        .iter()
        .flat_map(|page| items_to_json(page.items()))
        .collect()
}

fn table_key_schema(description: &TableDescription) -> Vec<(String, String)> {
    description
        .key_schema()
        .iter()
        .map(|element| {
            (
                element.attribute_name().to_string(),
                element.key_type().as_str().to_string(),
            )
        })
        .collect()
}

fn table_attribute_definitions(description: &TableDescription) -> Vec<(String, String)> {
    let mut definitions = description
        .attribute_definitions()
        .iter()
        .map(|definition| {
            (
                definition.attribute_name().to_string(),
                definition.attribute_type().as_str().to_string(),
            )
        })
        .collect::<Vec<_>>();
    definitions.sort();
    definitions
}

fn table_global_secondary_indexes(
    description: &TableDescription,
) -> Vec<(String, Vec<(String, String)>)> {
    let mut indexes = description
        .global_secondary_indexes()
        .iter()
        .map(|index| {
            (
                index.index_name().unwrap_or_default().to_string(),
                index
                    .key_schema()
                    .iter()
                    .map(|element| {
                        (
                            element.attribute_name().to_string(),
                            element.key_type().as_str().to_string(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    indexes.sort_by(|left, right| left.0.cmp(&right.0));
    indexes
}

fn table_local_secondary_indexes(
    description: &TableDescription,
) -> Vec<(String, Vec<(String, String)>)> {
    let mut indexes = description
        .local_secondary_indexes()
        .iter()
        .map(|index| {
            (
                index.index_name().unwrap_or_default().to_string(),
                index
                    .key_schema()
                    .iter()
                    .map(|element| {
                        (
                            element.attribute_name().to_string(),
                            element.key_type().as_str().to_string(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    indexes.sort_by(|left, right| left.0.cmp(&right.0));
    indexes
}

fn batch_output_items(
    output: &aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemOutput,
    table_name: &str,
) -> Vec<Value> {
    output
        .responses()
        .and_then(|responses| responses.get(table_name))
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(item_map_to_json)
        .collect()
}

fn items_to_json(items: &[HashMap<String, AttributeValue>]) -> Vec<Value> {
    items.iter().cloned().map(item_map_to_json).collect()
}

fn optional_item_to_json(item: Option<&HashMap<String, AttributeValue>>) -> Option<Value> {
    item.cloned().map(item_map_to_json)
}

fn item_map_to_json(item: HashMap<String, AttributeValue>) -> Value {
    serde_dynamo::aws_sdk_dynamodb_1::from_item(item).unwrap()
}

fn item_from_json(item: Value) -> HashMap<String, AttributeValue> {
    serde_dynamo::aws_sdk_dynamodb_1::to_item(item).unwrap()
}

fn normalize_item_list(items: Vec<Value>) -> Vec<Value> {
    let mut keyed = items
        .into_iter()
        .map(|item| {
            let key = item_sort_key(&item);
            (key, item)
        })
        .collect::<Vec<_>>();
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    keyed.into_iter().map(|(_, item)| item).collect()
}

fn item_sort_key(item: &Value) -> (String, i64) {
    let object = item.as_object().unwrap();
    let tenant = object
        .get("tenant")
        .and_then(Value::as_str)
        .unwrap()
        .to_string();
    let ts = object.get("ts").and_then(Value::as_i64).unwrap();
    (tenant, ts)
}

fn primary_key_item(row: &EventRow) -> HashMap<String, AttributeValue> {
    serde_dynamo::aws_sdk_dynamodb_1::to_item(json!({
        "tenant": row.tenant.clone(),
        "ts": row.ts,
    }))
    .unwrap()
}

fn primary_key_item_from_parts(tenant: &str, ts: Value) -> HashMap<String, AttributeValue> {
    serde_dynamo::aws_sdk_dynamodb_1::to_item(json!({
        "tenant": tenant,
        "ts": ts,
    }))
    .unwrap()
}

fn fixture_rows() -> Vec<EventRow> {
    vec![
        EventRow {
            tenant: "acme".to_string(),
            ts: 10,
            event_id: "evt-1".to_string(),
            region: "us-east-1".to_string(),
            active: true,
            count: 1,
        },
        EventRow {
            tenant: "acme".to_string(),
            ts: 20,
            event_id: "evt-2".to_string(),
            region: "us-west-2".to_string(),
            active: false,
            count: 2,
        },
        EventRow {
            tenant: "acme".to_string(),
            ts: 30,
            event_id: "evt-3".to_string(),
            region: "eu-central-1".to_string(),
            active: true,
            count: 3,
        },
        EventRow {
            tenant: "globex".to_string(),
            ts: 15,
            event_id: "evt-4".to_string(),
            region: "ap-southeast-2".to_string(),
            active: true,
            count: 4,
        },
        EventRow {
            tenant: "initech".to_string(),
            ts: 25,
            event_id: "evt-5".to_string(),
            region: "us-east-1".to_string(),
            active: false,
            count: 5,
        },
    ]
}

fn write_test_parquet(path: &Path, rows: &[EventRow]) {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.tenant.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            std::sync::Arc::new(Int64Array::from(
                rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
            )) as ArrayRef,
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.event_id.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.region.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            std::sync::Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.active).collect::<Vec<_>>(),
            )) as ArrayRef,
            std::sync::Arc::new(Int64Array::from(
                rows.iter().map(|row| row.count).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();

    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn unique_table_name(prefix: &str) -> String {
    format!(
        "{}_{}",
        prefix,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
