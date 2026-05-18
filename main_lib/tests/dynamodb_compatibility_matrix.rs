use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::net::TcpStream;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::operation::query::QueryOutput;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    KeyType, KeysAndAttributes, Projection, ProjectionType, ScalarAttributeType, TableDescription,
    TableStatus,
};
use aws_sdk_dynamodb::Client as DynamoClient;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use futures_util::future;
use gotham::bind_server;
use powdrr_lib::data_contract::{
    DynamoDbGlobalSecondaryIndexConfig, DynamoDbTableConfig, FileSetPayload, IcebergMetadata,
    TableMetadataCheckpoint,
};
use powdrr_lib::router::router;
use powdrr_lib::serving_dataset::read_parquet_documents;
use powdrr_lib::test_api::{CompactionMode, IndexingMode, StateMode, TestProcessingMode};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const CASES_JSON: &str = include_str!("data/dynamodb_compat_cases.json");

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const ALL_DYNAMODB_OPERATIONS: &[&str] = &[
    "BatchExecuteStatement",
    "BatchGetItem",
    "BatchWriteItem",
    "CreateBackup",
    "CreateGlobalTable",
    "CreateTable",
    "DeleteBackup",
    "DeleteItem",
    "DeleteResourcePolicy",
    "DeleteTable",
    "DescribeBackup",
    "DescribeContinuousBackups",
    "DescribeContributorInsights",
    "DescribeEndpoints",
    "DescribeExport",
    "DescribeGlobalTable",
    "DescribeGlobalTableSettings",
    "DescribeImport",
    "DescribeKinesisStreamingDestination",
    "DescribeLimits",
    "DescribeTable",
    "DescribeTableReplicaAutoScaling",
    "DescribeTimeToLive",
    "DisableKinesisStreamingDestination",
    "EnableKinesisStreamingDestination",
    "ExecuteStatement",
    "ExecuteTransaction",
    "ExportTableToPointInTime",
    "GetItem",
    "GetResourcePolicy",
    "ImportTable",
    "ListBackups",
    "ListContributorInsights",
    "ListExports",
    "ListGlobalTables",
    "ListImports",
    "ListTables",
    "ListTagsOfResource",
    "PutItem",
    "PutResourcePolicy",
    "Query",
    "RestoreTableFromBackup",
    "RestoreTableToPointInTime",
    "Scan",
    "TagResource",
    "TransactGetItems",
    "TransactWriteItems",
    "UntagResource",
    "UpdateContinuousBackups",
    "UpdateContributorInsights",
    "UpdateGlobalTable",
    "UpdateGlobalTableSettings",
    "UpdateItem",
    "UpdateKinesisStreamingDestination",
    "UpdateTable",
    "UpdateTableReplicaAutoScaling",
    "UpdateTimeToLive",
];

const SUPPORTED_DYNAMODB_OPERATIONS: &[&str] = &[
    "BatchGetItem",
    "DescribeTable",
    "GetItem",
    "ListTables",
    "Query",
];

#[derive(Debug, Deserialize)]
struct CaseFile {
    operations: Vec<OperationCase>,
}

#[derive(Debug, Deserialize)]
struct OperationCase {
    operation: String,
    mode: CoverageMode,
    #[serde(rename = "description")]
    _description: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CoverageMode {
    DifferentialSupported,
    ExplicitError,
}

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

struct DifferentialFixture {
    _temp_dir: TempDir,
    powdrr_client: DynamoClient,
    localstack_client: DynamoClient,
    rows: Vec<EventRow>,
    primary_table_name: String,
    begins_with_table_name: String,
    hidden_table_name: String,
    region_event_id_index: String,
}

#[test]
fn compatibility_matrix_operations_are_complete_and_unique() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let cases = load_cases();
    assert!(
        !cases.is_empty(),
        "expected at least one DynamoDB compatibility operation case"
    );

    let mut operation_set = BTreeSet::new();
    let mut supported = BTreeSet::new();
    for case in cases {
        assert!(
            operation_set.insert(case.operation.clone()),
            "duplicate DynamoDB operation coverage entry '{}'",
            case.operation
        );
        if case.mode == CoverageMode::DifferentialSupported {
            supported.insert(case.operation.clone());
        }
    }

    let expected_operations = ALL_DYNAMODB_OPERATIONS
        .iter()
        .map(|operation| operation.to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        operation_set, expected_operations,
        "DynamoDB compatibility matrix must enumerate the full SDK operation surface"
    );

    let expected_supported = SUPPORTED_DYNAMODB_OPERATIONS
        .iter()
        .map(|operation| operation.to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        supported, expected_supported,
        "supported operation coverage drifted from the tracked contract"
    );
}

#[test]
fn compatibility_matrix_wire_contract_locally() {
    let _guard = TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let server = PowdrrServer::spawn().await;
        let fixture = match ensure_local_engine_dependencies() {
            Ok(()) => Some(build_differential_fixture(&server.base_url).await),
            Err(reason) => {
                eprintln!(
                    "Skipping DynamoDB stateful and differential contract run; {}",
                    reason
                );
                None
            }
        };

        assert_unsupported_operations_explicit(&server.base_url).await;
        assert_parser_rejections_explicit(&server.base_url).await;
        if let Some(fixture) = fixture {
            assert_stateful_query_errors(&server.base_url, &fixture.primary_table_name).await;
            for case in load_cases()
                .into_iter()
                .filter(|case| case.mode == CoverageMode::DifferentialSupported)
            {
                eprintln!("dynamo differential operation: {}", case.operation);
                match case.operation.as_str() {
                    "ListTables" => compare_list_tables(&fixture).await,
                    "DescribeTable" => compare_describe_table(&fixture).await,
                    "GetItem" => compare_get_item(&fixture).await,
                    "BatchGetItem" => compare_batch_get_item(&fixture).await,
                    "Query" => compare_query_operation(&fixture).await,
                    other => panic!(
                        "missing supported DynamoDB differential executor for {}",
                        other
                    ),
                }
            }
        }
    });
}

fn load_cases() -> Vec<OperationCase> {
    serde_json::from_str::<CaseFile>(CASES_JSON)
        .unwrap()
        .operations
}

async fn build_differential_fixture(base_url: &str) -> DifferentialFixture {
    let temp_dir = TempDir::new().unwrap();
    let parquet_path = temp_dir.path().join("events.parquet");
    let rows = fixture_rows();
    write_test_parquet(&parquet_path, &rows);

    let primary_table_name = unique_table_name("dynamo_matrix_primary");
    let begins_with_table_name = unique_table_name("dynamo_matrix_begins_with");
    let hidden_table_name = unique_table_name("dynamo_matrix_hidden");
    let region_event_id_index = "region-event-id-index".to_string();

    configure_powdrr_testing_mode(base_url).await;
    configure_powdrr_table(
        base_url,
        &primary_table_name,
        &parquet_path,
        &DynamoDbTableConfig {
            partition_key: "tenant".to_string(),
            sort_key: Some("ts".to_string()),
            global_secondary_indexes: vec![DynamoDbGlobalSecondaryIndexConfig {
                name: region_event_id_index.clone(),
                partition_key: "region".to_string(),
                sort_key: Some("event_id".to_string()),
            }],
        },
    )
    .await;
    configure_powdrr_table(
        base_url,
        &begins_with_table_name,
        &parquet_path,
        &DynamoDbTableConfig {
            partition_key: "tenant".to_string(),
            sort_key: Some("event_id".to_string()),
            global_secondary_indexes: vec![],
        },
    )
    .await;
    add_powdrr_checkpoint(base_url, &hidden_table_name, &parquet_path).await;

    let powdrr_client = dynamodb_client(base_url).await;
    let localstack_client = dynamodb_client("http://127.0.0.1:4566").await;
    create_localstack_table(
        &localstack_client,
        &primary_table_name,
        &rows,
        "ts",
        ScalarAttributeType::N,
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
    )
    .await;
    wait_for_powdrr_rows(
        &powdrr_client,
        &primary_table_name,
        "ts",
        &[&rows[1], &rows[3]],
    )
    .await;
    wait_for_powdrr_rows(
        &powdrr_client,
        &begins_with_table_name,
        "event_id",
        &[&rows[0], &rows[2]],
    )
    .await;

    DifferentialFixture {
        _temp_dir: temp_dir,
        powdrr_client,
        localstack_client,
        rows,
        primary_table_name,
        begins_with_table_name,
        hidden_table_name,
        region_event_id_index,
    }
}

async fn assert_unsupported_operations_explicit(base_url: &str) {
    for case in load_cases()
        .into_iter()
        .filter(|case| case.mode == CoverageMode::ExplicitError)
    {
        let response = raw_dynamodb_request(base_url, &case.operation, &json!({})).await;
        assert_eq!(
            response.status, 400,
            "{} should fail explicitly, got status {} body {}",
            case.operation, response.status, response.body
        );
        assert_eq!(
            response.body["__type"],
            json!("ValidationException"),
            "{} should return a ValidationException body, got {}",
            case.operation,
            response.body
        );
        assert_eq!(
            response.body["message"],
            json!(format!("Unsupported x-amz-target {}", case.operation)),
            "{} should surface the unsupported x-amz-target explicitly, got {}",
            case.operation,
            response.body
        );
    }
}

async fn assert_parser_rejections_explicit(base_url: &str) {
    let table_name = "parser_only_table".to_string();
    let explicit_error_cases = vec![
        (
            "ListTables",
            json!({ "UnknownField": true }),
            "unknown field `UnknownField`",
        ),
        (
            "DescribeTable",
            json!({
                "TableName": table_name,
                "ReturnConsumedCapacity": "TOTAL"
            }),
            "unknown field `ReturnConsumedCapacity`",
        ),
        (
            "GetItem",
            json!({
                "TableName": table_name,
                "Key": {
                    "tenant": { "S": "acme" },
                    "ts": { "N": "10" }
                },
                "ConsistentRead": true
            }),
            "unknown field `ConsistentRead`",
        ),
        (
            "BatchGetItem",
            json!({
                "RequestItems": {
                    table_name.clone(): {
                        "Keys": [{
                            "tenant": { "S": "acme" },
                            "ts": { "N": "10" }
                        }]
                    }
                },
                "ReturnConsumedCapacity": "TOTAL"
            }),
            "unknown field `ReturnConsumedCapacity`",
        ),
        (
            "Query",
            json!({
                "TableName": table_name,
                "KeyConditionExpression": "#pk = :pk",
                "ExpressionAttributeNames": {
                    "#pk": "tenant"
                },
                "ExpressionAttributeValues": {
                    ":pk": { "S": "acme" }
                },
                "Select": "COUNT"
            }),
            "unknown field `Select`",
        ),
    ];

    for (operation, body, expected_message_fragment) in explicit_error_cases {
        let response = raw_dynamodb_request(base_url, operation, &body).await;
        assert_eq!(
            response.status, 400,
            "{} should fail explicitly, got status {} body {}",
            operation, response.status, response.body
        );
        assert_eq!(
            response.body["__type"],
            json!("ValidationException"),
            "{} should return ValidationException, got {}",
            operation,
            response.body
        );
        let message = response.body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected_message_fragment),
            "{} should surface '{}', got '{}'",
            operation,
            expected_message_fragment,
            message
        );
    }
}

async fn assert_stateful_query_errors(base_url: &str, table_name: &str) {
    let explicit_error_cases = vec![
        (
            "Query",
            json!({
                "TableName": table_name,
                "KeyConditionExpression": "#pk = :pk AND #sk BETWEEN :start AND :end",
                "ExpressionAttributeNames": {
                    "#pk": "tenant",
                    "#sk": "ts",
                    "#region": "region"
                },
                "ExpressionAttributeValues": {
                    ":pk": { "S": "acme" },
                    ":start": { "N": "10" },
                    ":end": { "N": "30" },
                    ":prefix": { "S": "us" }
                },
                "FilterExpression": "contains(#region, :prefix)"
            }),
            "Unsupported FilterExpression clause contains(region, :prefix)",
        ),
        (
            "Query",
            json!({
                "TableName": table_name,
                "KeyConditionExpression": "#pk = :pk AND #sk IN (:one, :two)",
                "ExpressionAttributeNames": {
                    "#pk": "tenant",
                    "#sk": "ts"
                },
                "ExpressionAttributeValues": {
                    ":pk": { "S": "acme" },
                    ":one": { "N": "10" },
                    ":two": { "N": "20" }
                }
            }),
            "Unsupported KeyConditionExpression form",
        ),
    ];

    for (operation, body, expected_message_fragment) in explicit_error_cases {
        let response = raw_dynamodb_request(base_url, operation, &body).await;
        assert_eq!(
            response.status, 400,
            "{} should fail explicitly, got status {} body {}",
            operation, response.status, response.body
        );
        assert_eq!(
            response.body["__type"],
            json!("ValidationException"),
            "{} should return ValidationException, got {}",
            operation,
            response.body
        );
        let message = response.body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected_message_fragment),
            "{} should surface '{}', got '{}'",
            operation,
            expected_message_fragment,
            message
        );
    }
}

async fn compare_list_tables(fixture: &DifferentialFixture) {
    let powdrr_tables = fixture
        .powdrr_client
        .list_tables()
        .limit(100)
        .send()
        .await
        .unwrap();
    let localstack_tables = fixture
        .localstack_client
        .list_tables()
        .limit(100)
        .send()
        .await
        .unwrap();

    let mut powdrr_names = powdrr_tables.table_names().to_vec();
    let mut localstack_names = localstack_tables.table_names().to_vec();
    powdrr_names.sort();
    localstack_names.sort();

    assert!(
        !powdrr_names
            .iter()
            .any(|name| name == &fixture.hidden_table_name),
        "Powdrr ListTables exposed non-Dynamo table {}; tables={:?}",
        fixture.hidden_table_name,
        powdrr_names
    );
    assert_eq!(powdrr_names, localstack_names);
}

async fn compare_describe_table(fixture: &DifferentialFixture) {
    let powdrr_description = fixture
        .powdrr_client
        .describe_table()
        .table_name(&fixture.primary_table_name)
        .send()
        .await
        .unwrap();
    let localstack_description = fixture
        .localstack_client
        .describe_table()
        .table_name(&fixture.primary_table_name)
        .send()
        .await
        .unwrap();
    compare_table_descriptions(
        powdrr_description.table().unwrap(),
        localstack_description.table().unwrap(),
    );
}

async fn compare_get_item(fixture: &DifferentialFixture) {
    let get_key = primary_key_item(&fixture.rows[1]);
    let expected_get_item = json!({
        "tenant": "acme",
        "ts": 20,
        "event_id": "evt-2",
        "count": 2,
    });
    let powdrr_get_item = fixture
        .powdrr_client
        .get_item()
        .table_name(&fixture.primary_table_name)
        .set_key(Some(get_key.clone()))
        .projection_expression("#pk, ts, event_id, #count")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#count", "count")
        .send()
        .await
        .unwrap();
    let localstack_get_item = fixture
        .localstack_client
        .get_item()
        .table_name(&fixture.primary_table_name)
        .set_key(Some(get_key))
        .projection_expression("#pk, ts, event_id, #count")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#count", "count")
        .send()
        .await
        .unwrap();

    let powdrr_json = optional_item_to_json(powdrr_get_item.item());
    let localstack_json = optional_item_to_json(localstack_get_item.item());
    assert_eq!(powdrr_json, Some(expected_get_item));
    assert_eq!(powdrr_json, localstack_json);
}

async fn compare_batch_get_item(fixture: &DifferentialFixture) {
    let batch_keys = vec![
        primary_key_item(&fixture.rows[1]),
        primary_key_item(&fixture.rows[3]),
    ];
    let expected_batch_items = normalize_item_list(vec![
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
    ]);
    let batch_request = KeysAndAttributes::builder()
        .set_keys(Some(batch_keys.clone()))
        .projection_expression("#pk, ts, event_id")
        .expression_attribute_names("#pk", "tenant")
        .build()
        .unwrap();

    let powdrr_output = fixture
        .powdrr_client
        .batch_get_item()
        .request_items(fixture.primary_table_name.clone(), batch_request.clone())
        .send()
        .await
        .unwrap();
    let localstack_output = fixture
        .localstack_client
        .batch_get_item()
        .request_items(fixture.primary_table_name.clone(), batch_request)
        .send()
        .await
        .unwrap();

    assert_eq!(
        normalize_item_list(batch_output_items(
            &powdrr_output,
            &fixture.primary_table_name
        )),
        expected_batch_items
    );
    assert_eq!(
        normalize_item_list(batch_output_items(
            &powdrr_output,
            &fixture.primary_table_name
        )),
        normalize_item_list(batch_output_items(
            &localstack_output,
            &fixture.primary_table_name
        ))
    );
}

async fn compare_query_operation(fixture: &DifferentialFixture) {
    let powdrr_first_page =
        query_page(&fixture.powdrr_client, &fixture.primary_table_name, None).await;
    let localstack_first_page = query_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        None,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_first_page,
        &localstack_first_page,
        normalize_item_list(vec![
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
        ]),
    );

    let powdrr_second_page = query_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        powdrr_first_page.last_evaluated_key().cloned(),
    )
    .await;
    let localstack_second_page = query_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        localstack_first_page.last_evaluated_key().cloned(),
    )
    .await;
    compare_query_page_outputs(
        &powdrr_second_page,
        &localstack_second_page,
        normalize_item_list(vec![json!({
            "tenant": "acme",
            "ts": 30,
            "event_id": "evt-3",
            "region": "eu-central-1",
        })]),
    );

    let powdrr_begins_with = query_begins_with_page(
        &fixture.powdrr_client,
        &fixture.begins_with_table_name,
        None,
    )
    .await;
    let localstack_begins_with = query_begins_with_page(
        &fixture.localstack_client,
        &fixture.begins_with_table_name,
        None,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_begins_with,
        &localstack_begins_with,
        normalize_item_list(vec![
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
        ]),
    );

    let powdrr_filtered =
        query_with_filter_page(&fixture.powdrr_client, &fixture.primary_table_name, None).await;
    let localstack_filtered = query_with_filter_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        None,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_filtered,
        &localstack_filtered,
        normalize_item_list(vec![json!({
            "tenant": "acme",
            "ts": 20,
            "event_id": "evt-2",
            "count": 2,
        })]),
    );

    let powdrr_filtered_second_page = query_with_filter_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        powdrr_filtered.last_evaluated_key().cloned(),
    )
    .await;
    let localstack_filtered_second_page = query_with_filter_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        localstack_filtered.last_evaluated_key().cloned(),
    )
    .await;
    compare_query_page_outputs(
        &powdrr_filtered_second_page,
        &localstack_filtered_second_page,
        normalize_item_list(vec![json!({
            "tenant": "acme",
            "ts": 30,
            "event_id": "evt-3",
            "count": 3,
        })]),
    );

    let powdrr_gsi = query_region_index_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        &fixture.region_event_id_index,
    )
    .await;
    let localstack_gsi = query_region_index_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        &fixture.region_event_id_index,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_gsi,
        &localstack_gsi,
        vec![json!({
            "tenant": "acme",
            "region": "us-east-1",
            "event_id": "evt-1",
            "ts": 10,
        })],
    );
}

fn compare_query_page_outputs(
    powdrr: &QueryOutput,
    localstack: &QueryOutput,
    expected_items: Vec<Value>,
) {
    let powdrr_items = normalize_item_list(items_to_json(powdrr.items()));
    let localstack_items = normalize_item_list(items_to_json(localstack.items()));
    assert_eq!(powdrr_items, expected_items);
    assert_eq!(powdrr_items, localstack_items);
    assert_eq!(powdrr.count(), localstack.count());
    assert_eq!(powdrr.scanned_count(), localstack.scanned_count());
    assert_eq!(
        optional_item_to_json(powdrr.last_evaluated_key()),
        optional_item_to_json(localstack.last_evaluated_key())
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
    TcpStream::connect_timeout(&socket_address, Duration::from_millis(200))
        .map(|_| ())
        .map_err(|error| format!("requires {} at {} ({})", name, address, error))
}

async fn add_powdrr_checkpoint(base_url: &str, table_name: &str, parquet_path: &Path) {
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
}

async fn put_powdrr_dynamodb_config(
    base_url: &str,
    table_name: &str,
    config: &DynamoDbTableConfig,
) {
    let url = format!("{}/{}/_dynamodb/config", base_url, table_name);
    let mut last_status = None;
    let mut last_body = String::new();

    for _ in 0..25 {
        let response = HttpClient::new()
            .put(&url)
            .json(config)
            .send()
            .await
            .unwrap();
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

async fn configure_powdrr_table(
    base_url: &str,
    table_name: &str,
    parquet_path: &Path,
    config: &DynamoDbTableConfig,
) {
    add_powdrr_checkpoint(base_url, table_name, parquet_path).await;
    put_powdrr_dynamodb_config(base_url, table_name, config).await;
}

async fn configure_powdrr_testing_mode(base_url: &str) {
    let mut mode = TestProcessingMode::default();
    mode.state_mode = StateMode::Testing;
    mode.indexing_mode = IndexingMode::Disabled;
    mode.compaction_mode = CompactionMode::Disabled;

    HttpClient::new()
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
            column_names: dataset
                .schema
                .fields()
                .iter()
                .map(|field| field.name.clone())
                .collect(),
            column_stats: vec![],
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

async fn wait_for_powdrr_rows(
    client: &DynamoClient,
    table_name: &str,
    sort_key_name: &str,
    rows: &[&EventRow],
) {
    for _ in 0..20 {
        let mut all_present = true;
        for row in rows {
            let key = serde_dynamo::aws_sdk_dynamodb_1::to_item(json!({
                "tenant": row.tenant,
                sort_key_name: match sort_key_name {
                    "ts" => Value::from(row.ts),
                    "event_id" => Value::from(row.event_id.clone()),
                    other => panic!("unsupported sort key {}", other),
                },
            }))
            .unwrap();
            let output = client
                .get_item()
                .table_name(table_name)
                .set_key(Some(key))
                .send()
                .await
                .unwrap();
            if output.item().is_none() {
                all_present = false;
                break;
            }
        }
        if all_present {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!(
        "Powdrr table {} did not become readable for sort key {} in time",
        table_name, sort_key_name
    );
}

async fn create_localstack_table(
    client: &DynamoClient,
    table_name: &str,
    rows: &[EventRow],
    sort_key_name: &str,
    sort_key_type: ScalarAttributeType,
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

async fn raw_dynamodb_request(base_url: &str, operation: &str, body: &Value) -> HttpResponseRecord {
    let response = HttpClient::new()
        .post(format!("{}/", base_url.trim_end_matches('/')))
        .header("x-amz-target", format!("DynamoDB_20120810.{}", operation))
        .json(body)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = response.text().await.unwrap();
    HttpResponseRecord {
        status,
        body: serde_json::from_str(&body).unwrap_or_else(|_| json!({ "raw": body })),
    }
}

struct HttpResponseRecord {
    status: u16,
    body: Value,
}

async fn query_page(
    client: &DynamoClient,
    table_name: &str,
    exclusive_start_key: Option<HashMap<String, AttributeValue>>,
) -> QueryOutput {
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
) -> QueryOutput {
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
) -> QueryOutput {
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
) -> QueryOutput {
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

fn item_sort_key(item: &Value) -> (String, String, i64) {
    let object = item.as_object().unwrap();
    let tenant = object
        .get("tenant")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let event_id = object
        .get("event_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let ts = object.get("ts").and_then(Value::as_i64).unwrap_or_default();
    (tenant, event_id, ts)
}

fn primary_key_item(row: &EventRow) -> HashMap<String, AttributeValue> {
    serde_dynamo::aws_sdk_dynamodb_1::to_item(json!({
        "tenant": row.tenant.clone(),
        "ts": row.ts,
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
