use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::net::TcpStream;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::operation::query::QueryOutput;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    KeyType, KeysAndAttributes, LocalSecondaryIndex, Projection, ProjectionType,
    ReturnConsumedCapacity, ScalarAttributeType, Select, TableDescription, TableStatus,
};
use chrono::Utc;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use futures_util::future;
use gotham::bind_server;
use hmac::{Hmac, Mac};
use powdrr_lib::data_contract::{
    DynamoDbGlobalSecondaryIndexConfig, DynamoDbLocalSecondaryIndexConfig, DynamoDbTableConfig,
    FileSetPayload, IcebergMetadata, LicenseType, OrgCreds, OrgSettings,
    TableMetadataCheckpoint,
};
use powdrr_lib::router::router;
use powdrr_lib::serving_dataset::read_parquet_documents;
use powdrr_lib::state_provider::STATE_PROVIDER;
use powdrr_lib::test_api::{
    CacheMode, CompactionMode, IndexingMode, StateMode, TestProcessingMode,
};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use url::Url;

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
    "Scan",
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

struct DifferentialFixture {
    _temp_dir: TempDir,
    powdrr_client: DynamoClient,
    localstack_client: DynamoClient,
    rows: Vec<EventRow>,
    primary_table_name: String,
    begins_with_table_name: String,
    hidden_table_name: String,
    region_event_id_index: String,
    tenant_count_index: String,
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
        register_powdrr_test_org().await;
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
        assert_auth_errors_explicit(&server.base_url).await;
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
                    "Scan" => compare_scan_operation(&fixture).await,
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
    let tenant_count_index = "tenant-count-index".to_string();

    eprintln!("dynamo fixture: configuring Powdrr testing mode");
    configure_powdrr_testing_mode(base_url).await;
    register_powdrr_test_org().await;
    eprintln!("dynamo fixture: configuring Powdrr primary table");
    configure_powdrr_table(
        base_url,
        &primary_table_name,
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
    eprintln!("dynamo fixture: configuring Powdrr begins_with table");
    configure_powdrr_table(
        base_url,
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
    eprintln!("dynamo fixture: adding hidden Powdrr checkpoint");
    add_powdrr_checkpoint(base_url, &hidden_table_name, &parquet_path).await;

    let powdrr_client = dynamodb_client(base_url).await;
    let localstack_client = dynamodb_client("http://127.0.0.1:4566").await;
    eprintln!("dynamo fixture: creating LocalStack primary table");
    create_localstack_table(
        &localstack_client,
        &primary_table_name,
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
    eprintln!("dynamo fixture: creating LocalStack begins_with table");
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
    eprintln!("dynamo fixture: waiting for Powdrr primary rows");
    wait_for_powdrr_rows(
        &powdrr_client,
        &primary_table_name,
        "ts",
        &[&rows[1], &rows[3]],
    )
    .await;
    eprintln!("dynamo fixture: waiting for Powdrr begins_with rows");
    wait_for_powdrr_rows(
        &powdrr_client,
        &begins_with_table_name,
        "event_id",
        &[&rows[0], &rows[2]],
    )
    .await;
    eprintln!("dynamo fixture: ready");

    DifferentialFixture {
        _temp_dir: temp_dir,
        powdrr_client,
        localstack_client,
        rows,
        primary_table_name,
        begins_with_table_name,
        hidden_table_name,
        region_event_id_index,
        tenant_count_index,
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

async fn assert_auth_errors_explicit(base_url: &str) {
    let response = HttpClient::new()
        .post(format!("{}/", base_url.trim_end_matches('/')))
        .header("content-type", "application/json")
        .header("x-amz-target", "DynamoDB_20120810.ListTables")
        .body("{}".to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 403);
    let body = serde_json::from_str::<Value>(&response.text().await.unwrap()).unwrap();
    assert_eq!(body["__type"], json!("UnrecognizedClientException"));
    assert_eq!(body["message"], json!("Missing Authorization header"));
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
                    "#region": "region",
                    "#count": "count"
                },
                "ExpressionAttributeValues": {
                    ":pk": { "S": "acme" },
                    ":start": { "N": "10" },
                    ":end": { "N": "30" },
                    ":prefix": { "S": "us" },
                    ":threshold": { "N": "2" }
                },
                "FilterExpression": "contains(#region, :prefix) OR NOT #count > :threshold"
            }),
            "",
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
                "ReturnConsumedCapacity": "MAYBE"
            }),
            "Unsupported ReturnConsumedCapacity value MAYBE",
        ),
        (
            "Scan",
            json!({
                "TableName": table_name,
                "ProjectionExpression": "tenant",
                "Select": "COUNT"
            }),
            "ProjectionExpression can only be used when Select is SPECIFIC_ATTRIBUTES",
        ),
        (
            "Query",
            json!({
                "TableName": table_name,
                "IndexName": "region-event-id-index",
                "ConsistentRead": true,
                "KeyConditionExpression": "#pk = :pk",
                "ExpressionAttributeNames": {
                    "#pk": "region"
                },
                "ExpressionAttributeValues": {
                    ":pk": { "S": "us-east-1" }
                }
            }),
            "Consistent reads are not supported on global secondary indexes",
        ),
    ];

    for (operation, body, expected_message_fragment) in explicit_error_cases {
        let response = raw_dynamodb_request(base_url, operation, &body).await;
        if expected_message_fragment.is_empty() {
            assert_eq!(
                response.status, 200,
                "{} should now be supported, got status {} body {}",
                operation, response.status, response.body
            );
        } else {
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

    let mut powdrr_all_names = powdrr_tables.table_names().to_vec();
    let mut localstack_all_names = localstack_tables.table_names().to_vec();
    powdrr_all_names.sort();
    localstack_all_names.sort();
    let powdrr_names = fixture_table_names(powdrr_tables.table_names());
    let localstack_names = fixture_table_names(localstack_tables.table_names());
    let expected_names = fixture_table_names(&[
        fixture.primary_table_name.clone(),
        fixture.begins_with_table_name.clone(),
    ]);

    assert!(
        !powdrr_names
            .iter()
            .any(|name| name == &fixture.hidden_table_name),
        "Powdrr ListTables exposed non-Dynamo table {}; tables={:?}",
        fixture.hidden_table_name,
        powdrr_names
    );
    assert_eq!(powdrr_names, expected_names);
    for expected_name in expected_names.iter() {
        assert!(
            localstack_names.iter().any(|name| name == expected_name),
            "LocalStack ListTables did not include expected fixture table {}; tables={:?}",
            expected_name,
            localstack_names
        );
    }

    let powdrr_first_page = list_tables_page(
        &fixture.powdrr_client,
        Some(1),
        predecessor_table_name(&powdrr_all_names, &expected_names[0]),
    )
    .await;
    let localstack_first_page = list_tables_page(
        &fixture.localstack_client,
        Some(1),
        predecessor_table_name(&localstack_all_names, &expected_names[0]),
    )
    .await;
    assert_eq!(
        powdrr_first_page.table_names(),
        &[expected_names[0].clone()],
        "Powdrr ListTables first page should preserve Dynamo ordering"
    );
    assert_eq!(
        powdrr_first_page.table_names(),
        localstack_first_page.table_names()
    );
    assert_eq!(
        powdrr_first_page.last_evaluated_table_name(),
        localstack_first_page.last_evaluated_table_name()
    );

    let powdrr_second_page = list_tables_page(
        &fixture.powdrr_client,
        Some(1),
        predecessor_table_name(&powdrr_all_names, &expected_names[1]),
    )
    .await;
    let localstack_second_page = list_tables_page(
        &fixture.localstack_client,
        Some(1),
        predecessor_table_name(&localstack_all_names, &expected_names[1]),
    )
    .await;
    assert_eq!(
        powdrr_second_page.table_names(),
        &[expected_names[1].clone()],
        "Powdrr ListTables second page should honor ExclusiveStartTableName"
    );
    assert_eq!(
        powdrr_second_page.table_names(),
        localstack_second_page.table_names()
    );
    assert_eq!(
        powdrr_second_page.last_evaluated_table_name(),
        localstack_second_page.last_evaluated_table_name()
    );
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

    let powdrr_begins_with_description = fixture
        .powdrr_client
        .describe_table()
        .table_name(&fixture.begins_with_table_name)
        .send()
        .await
        .unwrap();
    let localstack_begins_with_description = fixture
        .localstack_client
        .describe_table()
        .table_name(&fixture.begins_with_table_name)
        .send()
        .await
        .unwrap();
    compare_table_descriptions(
        powdrr_begins_with_description.table().unwrap(),
        localstack_begins_with_description.table().unwrap(),
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

    let missing_key = primary_key_item_from_parts("acme", json!(999));
    let powdrr_missing = fixture
        .powdrr_client
        .get_item()
        .table_name(&fixture.primary_table_name)
        .set_key(Some(missing_key.clone()))
        .send()
        .await
        .unwrap();
    let localstack_missing = fixture
        .localstack_client
        .get_item()
        .table_name(&fixture.primary_table_name)
        .set_key(Some(missing_key))
        .send()
        .await
        .unwrap();
    assert_eq!(powdrr_missing.item(), None);
    assert_eq!(powdrr_missing.item(), localstack_missing.item());
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

    let multi_table_output = fixture
        .powdrr_client
        .batch_get_item()
        .request_items(
            fixture.primary_table_name.clone(),
            KeysAndAttributes::builder()
                .set_keys(Some(vec![
                    primary_key_item(&fixture.rows[0]),
                    primary_key_item_from_parts("acme", json!(999)),
                ]))
                .projection_expression("tenant, ts, event_id")
                .build()
                .unwrap(),
        )
        .request_items(
            fixture.begins_with_table_name.clone(),
            KeysAndAttributes::builder()
                .set_keys(Some(vec![string_sort_key_item(
                    &fixture.rows[2].tenant,
                    "event_id",
                    &fixture.rows[2].event_id,
                )]))
                .projection_expression("tenant, event_id, #region")
                .expression_attribute_names("#region", "region")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let localstack_multi_table_output = fixture
        .localstack_client
        .batch_get_item()
        .request_items(
            fixture.primary_table_name.clone(),
            KeysAndAttributes::builder()
                .set_keys(Some(vec![
                    primary_key_item(&fixture.rows[0]),
                    primary_key_item_from_parts("acme", json!(999)),
                ]))
                .projection_expression("tenant, ts, event_id")
                .build()
                .unwrap(),
        )
        .request_items(
            fixture.begins_with_table_name.clone(),
            KeysAndAttributes::builder()
                .set_keys(Some(vec![string_sort_key_item(
                    &fixture.rows[2].tenant,
                    "event_id",
                    &fixture.rows[2].event_id,
                )]))
                .projection_expression("tenant, event_id, #region")
                .expression_attribute_names("#region", "region")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        normalize_item_list(batch_output_items(
            &multi_table_output,
            &fixture.primary_table_name
        )),
        normalize_item_list(vec![json!({
            "tenant": "acme",
            "ts": 10,
            "event_id": "evt-1",
        })])
    );
    assert_eq!(
        normalize_item_list(batch_output_items(
            &multi_table_output,
            &fixture.primary_table_name
        )),
        normalize_item_list(batch_output_items(
            &localstack_multi_table_output,
            &fixture.primary_table_name
        ))
    );
    assert_eq!(
        normalize_item_list(batch_output_items(
            &multi_table_output,
            &fixture.begins_with_table_name
        )),
        normalize_item_list(vec![json!({
            "tenant": "acme",
            "event_id": "evt-3",
            "region": "eu-central-1",
        })])
    );
    assert_eq!(
        normalize_item_list(batch_output_items(
            &multi_table_output,
            &fixture.begins_with_table_name
        )),
        normalize_item_list(batch_output_items(
            &localstack_multi_table_output,
            &fixture.begins_with_table_name
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

    let powdrr_begins_with_second_page = query_begins_with_page(
        &fixture.powdrr_client,
        &fixture.begins_with_table_name,
        powdrr_begins_with.last_evaluated_key().cloned(),
    )
    .await;
    let localstack_begins_with_second_page = query_begins_with_page(
        &fixture.localstack_client,
        &fixture.begins_with_table_name,
        localstack_begins_with.last_evaluated_key().cloned(),
    )
    .await;
    compare_query_page_outputs(
        &powdrr_begins_with_second_page,
        &localstack_begins_with_second_page,
        vec![json!({
            "tenant": "acme",
            "event_id": "evt-3",
            "region": "eu-central-1",
            "ts": 30,
        })],
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

    let powdrr_filtered_projection = query_with_filter_projection_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        None,
    )
    .await;
    let localstack_filtered_projection = query_with_filter_projection_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        None,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_filtered_projection,
        &localstack_filtered_projection,
        vec![json!({
            "event_id": "evt-2",
        })],
    );

    let powdrr_filtered_projection_second_page = query_with_filter_projection_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        powdrr_filtered_projection.last_evaluated_key().cloned(),
    )
    .await;
    let localstack_filtered_projection_second_page = query_with_filter_projection_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        localstack_filtered_projection.last_evaluated_key().cloned(),
    )
    .await;
    compare_query_page_outputs(
        &powdrr_filtered_projection_second_page,
        &localstack_filtered_projection_second_page,
        vec![json!({
            "event_id": "evt-3",
        })],
    );

    let powdrr_descending =
        query_descending_page(&fixture.powdrr_client, &fixture.primary_table_name, None).await;
    let localstack_descending = query_descending_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        None,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_descending,
        &localstack_descending,
        vec![
            json!({
                "tenant": "acme",
                "ts": 30,
                "event_id": "evt-3",
                "region": "eu-central-1",
            }),
            json!({
                "tenant": "acme",
                "ts": 20,
                "event_id": "evt-2",
                "region": "us-west-2",
            }),
        ],
    );

    let powdrr_descending_second_page = query_descending_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        powdrr_descending.last_evaluated_key().cloned(),
    )
    .await;
    let localstack_descending_second_page = query_descending_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        localstack_descending.last_evaluated_key().cloned(),
    )
    .await;
    compare_query_page_outputs(
        &powdrr_descending_second_page,
        &localstack_descending_second_page,
        vec![json!({
            "tenant": "acme",
            "ts": 10,
            "event_id": "evt-1",
            "region": "us-east-1",
        })],
    );

    let powdrr_exact = query_exact_page(&fixture.powdrr_client, &fixture.primary_table_name).await;
    let localstack_exact =
        query_exact_page(&fixture.localstack_client, &fixture.primary_table_name).await;
    compare_query_page_outputs(
        &powdrr_exact,
        &localstack_exact,
        vec![json!({
            "tenant": "acme",
            "ts": 20,
            "event_id": "evt-2",
        })],
    );

    let powdrr_empty = query_empty_page(&fixture.powdrr_client, &fixture.primary_table_name).await;
    let localstack_empty =
        query_empty_page(&fixture.localstack_client, &fixture.primary_table_name).await;
    compare_query_page_outputs(&powdrr_empty, &localstack_empty, vec![]);

    let powdrr_gsi = query_region_index_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        &fixture.region_event_id_index,
        None,
        None,
    )
    .await;
    let localstack_gsi = query_region_index_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        &fixture.region_event_id_index,
        None,
        None,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_gsi,
        &localstack_gsi,
        vec![
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
        ],
    );

    let powdrr_gsi_first_page = query_region_index_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        &fixture.region_event_id_index,
        Some(1),
        None,
    )
    .await;
    let localstack_gsi_first_page = query_region_index_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        &fixture.region_event_id_index,
        Some(1),
        None,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_gsi_first_page,
        &localstack_gsi_first_page,
        vec![json!({
            "tenant": "acme",
            "region": "us-east-1",
            "event_id": "evt-1",
            "ts": 10,
        })],
    );

    let powdrr_gsi_second_page = query_region_index_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        &fixture.region_event_id_index,
        Some(1),
        powdrr_gsi_first_page.last_evaluated_key().cloned(),
    )
    .await;
    let localstack_gsi_second_page = query_region_index_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        &fixture.region_event_id_index,
        Some(1),
        localstack_gsi_first_page.last_evaluated_key().cloned(),
    )
    .await;
    compare_query_page_outputs(
        &powdrr_gsi_second_page,
        &localstack_gsi_second_page,
        vec![json!({
            "tenant": "initech",
            "region": "us-east-1",
            "event_id": "evt-5",
            "ts": 25,
        })],
    );

    let powdrr_gsi_capacity = query_region_index_page_with_capacity(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        &fixture.region_event_id_index,
    )
    .await;
    assert!(
        powdrr_gsi_capacity
            .consumed_capacity()
            .and_then(|capacity| capacity.global_secondary_indexes())
            .and_then(|indexes| indexes.get(&fixture.region_event_id_index))
            .is_some()
    );

    let powdrr_lsi = query_local_index_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
        &fixture.tenant_count_index,
    )
    .await;
    let localstack_lsi = query_local_index_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
        &fixture.tenant_count_index,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_lsi,
        &localstack_lsi,
        vec![
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
        ],
    );
    assert!(
        powdrr_lsi
            .consumed_capacity()
            .and_then(|capacity| capacity.local_secondary_indexes())
            .and_then(|indexes| indexes.get(&fixture.tenant_count_index))
            .is_some()
    );

    let powdrr_or_filter =
        query_with_or_not_filter_page(&fixture.powdrr_client, &fixture.primary_table_name).await;
    let localstack_or_filter = query_with_or_not_filter_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_or_filter,
        &localstack_or_filter,
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
        ],
    );

    let powdrr_attribute_filter = query_with_attribute_meta_filter_page(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
    )
    .await;
    let localstack_attribute_filter = query_with_attribute_meta_filter_page(
        &fixture.localstack_client,
        &fixture.primary_table_name,
    )
    .await;
    compare_query_page_outputs(
        &powdrr_attribute_filter,
        &localstack_attribute_filter,
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
        ],
    );

    let powdrr_count = query_count_page(&fixture.powdrr_client, &fixture.primary_table_name).await;
    let localstack_count =
        query_count_page(&fixture.localstack_client, &fixture.primary_table_name).await;
    assert_eq!(powdrr_count.count(), 3);
    assert_eq!(powdrr_count.scanned_count(), 3);
    assert_eq!(powdrr_count.count(), localstack_count.count());
    assert_eq!(powdrr_count.scanned_count(), localstack_count.scanned_count());
    assert_eq!(powdrr_count.items().len(), 0);
}

async fn compare_scan_operation(fixture: &DifferentialFixture) {
    let powdrr_full = scan_page(&fixture.powdrr_client, &fixture.primary_table_name, None, None).await;
    let localstack_full =
        scan_page(&fixture.localstack_client, &fixture.primary_table_name, None, None).await;
    let expected_full = normalize_item_list(vec![
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
    let powdrr_full_items = normalize_item_list(items_to_json(powdrr_full.items()));
    let localstack_full_items = normalize_item_list(items_to_json(localstack_full.items()));
    assert_eq!(powdrr_full_items, expected_full);
    assert_eq!(powdrr_full_items, localstack_full_items);
    assert_eq!(powdrr_full.count(), localstack_full.count());
    assert_eq!(powdrr_full.scanned_count(), localstack_full.scanned_count());

    let powdrr_pages =
        collect_scan_pages(&fixture.powdrr_client, &fixture.primary_table_name, Some(2)).await;
    let localstack_pages =
        collect_scan_pages(&fixture.localstack_client, &fixture.primary_table_name, Some(2)).await;
    assert_eq!(
        normalize_item_list(flatten_scan_pages(&powdrr_pages)),
        expected_full,
        "Powdrr paginated Scan should cover the full item set",
    );
    assert_eq!(
        normalize_item_list(flatten_scan_pages(&powdrr_pages)),
        normalize_item_list(flatten_scan_pages(&localstack_pages)),
        "paginated Scan should cover the same full item set as LocalStack",
    );
    assert!(
        powdrr_pages.first().and_then(|page| page.last_evaluated_key()).is_some(),
        "first paginated Scan page should expose a continuation key"
    );
    assert!(
        powdrr_pages.last().and_then(|page| page.last_evaluated_key()).is_none(),
        "final paginated Scan page should not expose a continuation key"
    );

    let powdrr_count = scan_count_page(&fixture.powdrr_client, &fixture.primary_table_name).await;
    let localstack_count =
        scan_count_page(&fixture.localstack_client, &fixture.primary_table_name).await;
    assert_eq!(powdrr_count.count(), 3);
    assert_eq!(powdrr_count.scanned_count(), 5);
    assert_eq!(powdrr_count.count(), localstack_count.count());
    assert_eq!(powdrr_count.scanned_count(), localstack_count.scanned_count());
    assert_eq!(powdrr_count.items().len(), 0);

    let powdrr_count_with_capacity = scan_count_page_with_capacity(
        &fixture.powdrr_client,
        &fixture.primary_table_name,
    )
    .await;
    assert_eq!(
        powdrr_count_with_capacity
            .consumed_capacity()
            .and_then(|capacity| capacity.table_name()),
        Some(fixture.primary_table_name.as_str())
    );
}

fn compare_query_page_outputs(
    powdrr: &QueryOutput,
    localstack: &QueryOutput,
    expected_items: Vec<Value>,
) {
    let powdrr_items = items_to_json(powdrr.items());
    let localstack_items = items_to_json(localstack.items());
    assert_eq!(powdrr_items, expected_items);
    assert_eq!(powdrr_items, localstack_items);
    assert_eq!(powdrr.count(), localstack.count());
    assert_eq!(powdrr.scanned_count(), localstack.scanned_count());
    assert_eq!(
        optional_item_to_json(powdrr.last_evaluated_key()),
        optional_item_to_json(localstack.last_evaluated_key())
    );
}

fn flatten_scan_pages(pages: &[aws_sdk_dynamodb::operation::scan::ScanOutput]) -> Vec<Value> {
    pages.iter()
        .flat_map(|page| items_to_json(page.items()))
        .collect()
}

fn ensure_local_engine_dependencies() -> Result<(), String> {
    require_local_service("LocalStack/DynamoDB", "127.0.0.1:4566")?;
    let (redis_host, redis_port) = test_redis_endpoint();
    require_local_service("Redis", &format!("{}:{}", redis_host, redis_port))?;
    require_local_service("MinIO", "127.0.0.1:9000")?;
    require_local_service("Iceberg REST catalog", "127.0.0.1:8181")?;
    Ok(())
}

fn configured_test_redis_url() -> Option<String> {
    std::env::var("POWDRR_TEST_REDIS_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

fn test_redis_endpoint() -> (String, u16) {
    let Some(redis_url) = configured_test_redis_url() else {
        return ("127.0.0.1".to_string(), 6379);
    };
    let parsed = Url::parse(&redis_url)
        .unwrap_or_else(|error| panic!("invalid POWDRR_TEST_REDIS_URL {}: {}", redis_url, error));
    let host = parsed.host_str().unwrap_or("127.0.0.1").to_string();
    let port = parsed.port().unwrap_or(6379);
    (host, port)
}

fn require_local_service(name: &str, address: &str) -> Result<(), String> {
    let socket_address = address.parse().unwrap();
    TcpStream::connect_timeout(&socket_address, Duration::from_millis(200))
        .map(|_| ())
        .map_err(|error| format!("requires {} at {} ({})", name, address, error))
}

async fn add_powdrr_checkpoint(base_url: &str, table_name: &str, parquet_path: &Path) {
    let client = powdrr_http_client();
    let checkpoint = checkpoint_from_parquet(table_name, parquet_path).await;
    let url = format!("{}/_test/v1/_add_checkpoint", base_url);
    let mut last_error = String::new();
    for _ in 0..20 {
        match client.post(&url).json(&checkpoint).send().await {
            Ok(response) => match response.error_for_status() {
                Ok(_) => return,
                Err(error) => last_error = error.to_string(),
            },
            Err(error) => last_error = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!(
        "POST /_test/v1/_add_checkpoint for {} failed after retries: {}",
        table_name, last_error
    );
}

async fn put_powdrr_dynamodb_config(
    base_url: &str,
    table_name: &str,
    config: &DynamoDbTableConfig,
) {
    let url = format!("{}/{}/_dynamodb/config", base_url, table_name);
    let client = powdrr_http_client();
    let mut last_status = None;
    let mut last_body = String::new();

    for _ in 0..25 {
        let response = match client.put(&url).json(config).send().await {
            Ok(response) => response,
            Err(error) => {
                last_status = None;
                last_body = error.to_string();
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
        };
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
    if let Some(redis_url) = configured_test_redis_url() {
        mode.cache_mode = CacheMode::Redis(Some(redis_url));
    }
    let client = powdrr_http_client();
    let url = format!("{}/_test/v1/_testing_and_processing_mode", base_url);
    let mut last_error = String::new();

    for _ in 0..20 {
        match client.put(&url).json(&mode).send().await {
            Ok(response) => match response.error_for_status() {
                Ok(_) => return,
                Err(error) => last_error = error.to_string(),
            },
            Err(error) => last_error = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    panic!(
        "PUT /_test/v1/_testing_and_processing_mode failed after retries: {}",
        last_error
    );
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

fn powdrr_http_client() -> HttpClient {
    HttpClient::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

async fn wait_for_powdrr_rows(
    client: &DynamoClient,
    table_name: &str,
    sort_key_name: &str,
    rows: &[&EventRow],
) {
    let started = Instant::now();
    for attempt in 0..240 {
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
        if attempt > 0 && attempt % 30 == 0 {
            eprintln!(
                "dynamo fixture: still waiting for {} rows on {} after {:?}",
                table_name,
                sort_key_name,
                started.elapsed()
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!(
        "Powdrr table {} did not become readable for sort key {} after {:?}",
        table_name,
        sort_key_name,
        started.elapsed()
    );
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

async fn raw_dynamodb_request(base_url: &str, operation: &str, body: &Value) -> HttpResponseRecord {
    let body_text = serde_json::to_string(body).unwrap();
    let target = format!("DynamoDB_20120810.{}", operation);
    let amz_date = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let credential_date = Utc::now().format("%Y%m%d").to_string();
    let parsed_base_url = Url::parse(base_url).unwrap();
    let host = match parsed_base_url.port() {
        Some(port) => format!("{}:{}", parsed_base_url.host_str().unwrap(), port),
        None => parsed_base_url.host_str().unwrap().to_string(),
    };
    let payload_hash = sha256_hex(body_text.as_bytes());
    let signed_headers = "content-type;host;x-amz-date;x-amz-target";
    let canonical_request = format!(
        "POST\n/\n\ncontent-type:{}\nhost:{}\nx-amz-date:{}\nx-amz-target:{}\n\n{}\n{}",
        "application/json",
        host,
        amz_date,
        target,
        signed_headers,
        payload_hash
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}/us-east-1/dynamodb/aws4_request\n{}",
        amz_date,
        credential_date,
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = sigv4_signature(
        "test",
        &credential_date,
        "us-east-1",
        "dynamodb",
        &string_to_sign,
    );
    let response = HttpClient::new()
        .post(format!("{}/", base_url.trim_end_matches('/')))
        .header("content-type", "application/json")
        .header("host", host)
        .header("x-amz-date", amz_date)
        .header("x-amz-target", target)
        .header(
            "Authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential=test/{}/us-east-1/dynamodb/aws4_request,SignedHeaders={},Signature={}",
                credential_date, signed_headers, signature
            ),
        )
        .body(body_text)
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

fn sha256_hex(bytes: &[u8]) -> String {
    let mut sha = Sha256::new();
    sha.update(bytes);
    hex_encode(&sha.finalize())
}

fn sigv4_signature(
    secret_access_key: &str,
    credential_date: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    let secret = format!("AWS4{}", secret_access_key);
    let k_date = hmac_sha256(secret.as_bytes(), credential_date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    hex_encode(&hmac_sha256(&k_signing, string_to_sign.as_bytes()))
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        encoded.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
    }
    encoded
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

async fn query_with_filter_projection_page(
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
        .projection_expression("#event_id")
        .limit(2)
        .scan_index_forward(true)
        .set_exclusive_start_key(exclusive_start_key)
        .send()
        .await
        .unwrap()
}

async fn query_descending_page(
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
        .scan_index_forward(false)
        .set_exclusive_start_key(exclusive_start_key)
        .send()
        .await
        .unwrap()
}

async fn query_exact_page(client: &DynamoClient, table_name: &str) -> QueryOutput {
    client
        .query()
        .table_name(table_name)
        .key_condition_expression("#pk = :pk AND #sk = :sk")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "ts")
        .expression_attribute_names("#event_id", "event_id")
        .expression_attribute_values(":pk", AttributeValue::S("acme".to_string()))
        .expression_attribute_values(":sk", AttributeValue::N("20".to_string()))
        .projection_expression("#pk, #sk, #event_id")
        .limit(5)
        .send()
        .await
        .unwrap()
}

async fn query_empty_page(client: &DynamoClient, table_name: &str) -> QueryOutput {
    client
        .query()
        .table_name(table_name)
        .key_condition_expression("#pk = :pk AND #sk BETWEEN :start AND :end")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "ts")
        .expression_attribute_values(":pk", AttributeValue::S("acme".to_string()))
        .expression_attribute_values(":start", AttributeValue::N("100".to_string()))
        .expression_attribute_values(":end", AttributeValue::N("200".to_string()))
        .projection_expression("#pk, #sk, event_id")
        .limit(5)
        .send()
        .await
        .unwrap()
}

async fn query_region_index_page(
    client: &DynamoClient,
    table_name: &str,
    index_name: &str,
    limit: Option<i32>,
    exclusive_start_key: Option<HashMap<String, AttributeValue>>,
) -> QueryOutput {
    let mut request = client
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
        .scan_index_forward(true);
    if let Some(limit) = limit {
        request = request.limit(limit);
    }
    request
        .set_exclusive_start_key(exclusive_start_key)
        .send()
        .await
        .unwrap()
}

async fn query_region_index_page_with_capacity(
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
) -> QueryOutput {
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

async fn query_with_or_not_filter_page(client: &DynamoClient, table_name: &str) -> QueryOutput {
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
) -> QueryOutput {
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

async fn query_count_page(client: &DynamoClient, table_name: &str) -> QueryOutput {
    client
        .query()
        .table_name(table_name)
        .key_condition_expression("#pk = :pk AND #sk BETWEEN :start AND :end")
        .expression_attribute_names("#pk", "tenant")
        .expression_attribute_names("#sk", "ts")
        .expression_attribute_values(":pk", AttributeValue::S("acme".to_string()))
        .expression_attribute_values(":start", AttributeValue::N("10".to_string()))
        .expression_attribute_values(":end", AttributeValue::N("30".to_string()))
        .select(Select::Count)
        .limit(5)
        .send()
        .await
        .unwrap()
}

async fn scan_page(
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
    request
        .set_exclusive_start_key(exclusive_start_key)
        .send()
        .await
        .unwrap()
}

async fn scan_count_page(
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

async fn scan_count_page_with_capacity(
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
        let page = scan_page(client, table_name, limit, exclusive_start_key).await;
        exclusive_start_key = page.last_evaluated_key().cloned();
        let done = exclusive_start_key.is_none();
        pages.push(page);
        if done {
            return pages;
        }
    }
}

async fn list_tables_page(
    client: &DynamoClient,
    limit: Option<i32>,
    exclusive_start_table_name: Option<String>,
) -> aws_sdk_dynamodb::operation::list_tables::ListTablesOutput {
    let mut request = client.list_tables();
    if let Some(limit) = limit {
        request = request.limit(limit);
    }
    request
        .set_exclusive_start_table_name(exclusive_start_table_name)
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
    assert_eq!(
        table_local_secondary_indexes(powdrr),
        table_local_secondary_indexes(localstack),
        "local secondary index mismatch"
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

fn fixture_table_names(table_names: &[String]) -> Vec<String> {
    let mut fixture_tables = table_names
        .iter()
        .filter(|name| name.starts_with("dynamo_matrix_"))
        .cloned()
        .collect::<Vec<_>>();
    fixture_tables.sort();
    fixture_tables
}

fn predecessor_table_name(table_names: &[String], target: &str) -> Option<String> {
    table_names
        .iter()
        .position(|name| name == target)
        .and_then(|index| index.checked_sub(1))
        .map(|index| table_names[index].clone())
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
    primary_key_item_from_parts(&row.tenant, json!(row.ts))
}

fn primary_key_item_from_parts(tenant: &str, ts: Value) -> HashMap<String, AttributeValue> {
    serde_dynamo::aws_sdk_dynamodb_1::to_item(json!({
        "tenant": tenant,
        "ts": ts,
    }))
    .unwrap()
}

fn string_sort_key_item(
    tenant: &str,
    sort_key_name: &str,
    sort_key_value: &str,
) -> HashMap<String, AttributeValue> {
    serde_dynamo::aws_sdk_dynamodb_1::to_item(json!({
        "tenant": tenant,
        sort_key_name: sort_key_value,
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
