use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::TryStreamExt;
use gotham::mime;
use gotham::plain::test::AsyncTestServer;
use mongodb::bson::{self, Document};
use mongodb::{Client as MongoClient, Collection, IndexModel};
use powdrr_lib::data_contract::{
    FileSetPayload, IcebergMetadata, ServingPattern, TableMetadataCheckpoint,
};
use powdrr_lib::lakehouse_serving::ServingQueryResponse;
use powdrr_lib::router::router;
use powdrr_lib::schema_massager::{PowdrrDataType, PowdrrSchema};
use powdrr_lib::serving_dataset::{ParquetDocumentSet, read_parquet_documents};
use powdrr_lib::serving_plan::{
    ServingPredicate, ServingQueryClassification, ServingRequestPlan, ServingSort,
};
use powdrr_lib::serving_protocol::{to_elasticsearch_search, to_mongodb_find};
use powdrr_lib::test_api::{
    CacheMode, CompactionMode, IndexingMode, PeerMode, PrefetchMode, StateMode, StorageMode,
    TestProcessingMode,
};
use reqwest::Client as HttpClient;
use serde_json::{Map, Value, json};

const DEFAULT_LIMIT: usize = 25;
const DEFAULT_ITERATIONS: usize = 20;
const DEFAULT_WARMUP: usize = 5;
const DEFAULT_DATASET: &str = "main_lib/tests/data/flights.parquet";
const BENCH_BASE_URL: &str = "http://localhost";

#[derive(Clone, Debug)]
struct BenchmarkConfig {
    dataset_path: PathBuf,
    limit: usize,
    iterations: usize,
    warmup: usize,
    es_url: String,
    mongo_uri: String,
    skip_es: bool,
    skip_mongo: bool,
}

impl BenchmarkConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            dataset_path: env::var("POWDRR_SERVE_BENCH_DATASET")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(DEFAULT_DATASET)),
            limit: env::var("POWDRR_SERVE_BENCH_LIMIT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(DEFAULT_LIMIT),
            iterations: env::var("POWDRR_SERVE_BENCH_ITERATIONS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(DEFAULT_ITERATIONS),
            warmup: env::var("POWDRR_SERVE_BENCH_WARMUP")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(DEFAULT_WARMUP),
            es_url: env::var("POWDRR_SERVE_BENCH_ES_URL")
                .unwrap_or_else(|_| "http://localhost:9200".to_string()),
            mongo_uri: env::var("POWDRR_SERVE_BENCH_MONGO_URI")
                .unwrap_or_else(|_| "mongodb://localhost:27017".to_string()),
            skip_es: env::var("POWDRR_SERVE_BENCH_SKIP_ES")
                .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            skip_mongo: env::var("POWDRR_SERVE_BENCH_SKIP_MONGO")
                .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        })
    }
}

#[derive(Clone, Debug)]
struct WorkloadShape {
    projection: Vec<String>,
    sort_field: String,
    eq_field: Option<String>,
    eq_values: Vec<Value>,
    range_field: Option<String>,
    range_lower_bound: Option<Value>,
}

#[derive(Clone, Debug)]
struct BenchmarkCase {
    name: String,
    plan: ServingRequestPlan,
}

#[derive(Clone, Debug)]
struct BackendQueryResult {
    rows: Vec<Value>,
    classification: Option<ServingQueryClassification>,
}

#[derive(Clone, Debug)]
struct LatencySummary {
    avg_ms: f64,
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = BenchmarkConfig::from_env()?;
    let dataset_path = canonical_dataset_path(&config.dataset_path)?;
    let dataset = read_parquet_documents(dataset_path.to_str().unwrap(), None)
        .await
        .map_err(|error| anyhow!(error))
        .with_context(|| format!("failed to read parquet dataset at {}", dataset_path.display()))?;
    let workload = infer_workload(&dataset)?;
    let cases = build_benchmark_cases(&workload, config.limit);
    if cases.is_empty() {
        bail!("no benchmark cases could be inferred from dataset");
    }

    println!("Dataset: {}", dataset_path.display());
    println!("Rows loaded: {}", dataset.rows.len());
    println!("Projection: {}", workload.projection.join(", "));
    println!("Sort field: {}", workload.sort_field);
    if let Some(eq_field) = workload.eq_field.as_ref() {
        println!("Equality field: {}", eq_field);
    }
    if let Some(range_field) = workload.range_field.as_ref() {
        println!("Range field: {}", range_field);
    }

    let table_name = unique_name("serve_bench");
    let test_server = AsyncTestServer::new(router(true))
        .await
        .context("failed to start in-process Powdrr benchmark server")?;
    setup_powdrr(&test_server, &table_name, &dataset_path, &dataset.schema, &cases).await?;

    let http_client = HttpClient::new();
    let mut es_target = None;
    if !config.skip_es {
        let index_name = unique_name("serve_bench_es");
        setup_elasticsearch(&http_client, &config.es_url, &index_name, &dataset).await?;
        es_target = Some((config.es_url.clone(), index_name));
    }

    let mut mongo_target = None;
    if !config.skip_mongo {
        let db_name = unique_name("serve_bench_mongo");
        let collection_name = "docs".to_string();
        let collection =
            setup_mongodb(&config.mongo_uri, &db_name, &collection_name, &dataset, &cases).await?;
        mongo_target = Some((db_name, collection_name, collection));
    }

    println!();
    println!(
        "{:<20} {:<10} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "case", "backend", "avg", "p50", "p95", "min", "max"
    );

    for case in cases.iter() {
        let powdrr_baseline = run_powdrr_case(&test_server, &table_name, &case.plan).await?;
        if powdrr_baseline.classification != Some(ServingQueryClassification::FastPath) {
            bail!(
                "Powdrr case {} did not stay on fast path: {:?}",
                case.name,
                powdrr_baseline.classification
            );
        }

        let (powdrr_result, powdrr_latency) = measure_runner(config.warmup, config.iterations, || {
            run_powdrr_case(&test_server, &table_name, &case.plan).boxed()
        })
        .await?;
        print_summary(&case.name, "powdrr", &powdrr_latency);

        if let Some((es_url, index_name)) = es_target.as_ref() {
            let es_first = run_es_case(&http_client, es_url, index_name, &case.plan).await?;
            assert_same_rows("powdrr", &powdrr_baseline, "elasticsearch", &es_first, &case.name)?;
            let (_, es_latency) = measure_runner(config.warmup, config.iterations, || {
                run_es_case(&http_client, es_url, index_name, &case.plan).boxed()
            })
            .await?;
            print_summary(&case.name, "elasticsearch", &es_latency);
        }

        if let Some((_, _, collection)) = mongo_target.as_ref() {
            let mongo_first = run_mongo_case(collection, &case.plan).await?;
            assert_same_rows("powdrr", &powdrr_baseline, "mongodb", &mongo_first, &case.name)?;
            let (_, mongo_latency) = measure_runner(config.warmup, config.iterations, || {
                run_mongo_case(collection, &case.plan).boxed()
            })
            .await?;
            print_summary(&case.name, "mongodb", &mongo_latency);
        }

        assert_eq!(
            normalize_rows(&powdrr_baseline.rows),
            normalize_rows(&powdrr_result.rows)
        );
    }

    if let Some((es_url, index_name)) = es_target.as_ref() {
        cleanup_elasticsearch(&http_client, es_url, index_name).await?;
    }
    if let Some((db_name, _, collection)) = mongo_target.as_ref() {
        collection
            .drop()
            .await
            .with_context(|| format!("failed to drop Mongo collection in {}", db_name))?;
    }

    Ok(())
}

fn canonical_dataset_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(env::current_dir()?.join(path))
}

fn infer_workload(dataset: &ParquetDocumentSet) -> Result<WorkloadShape> {
    if dataset.rows.is_empty() {
        bail!("dataset had no rows");
    }

    let schema_map = dataset.schema.to_map();
    let string_fields: Vec<String> = dataset
        .schema
        .fields()
        .iter()
        .filter(|field| field.data_type == PowdrrDataType::String)
        .map(|field| field.name.clone())
        .collect();
    let numeric_fields: Vec<String> = dataset
        .schema
        .fields()
        .iter()
        .filter(|field| {
            field.data_type == PowdrrDataType::Integer || field.data_type == PowdrrDataType::Float
        })
        .map(|field| field.name.clone())
        .collect();

    let (eq_field, eq_values) = infer_eq_field(&dataset.rows, &string_fields);
    let sort_field = numeric_fields
        .first()
        .cloned()
        .or_else(|| string_fields.first().cloned())
        .ok_or_else(|| anyhow!("expected at least one sortable field"))?;

    let range_field = numeric_fields.first().cloned();
    let range_lower_bound = range_field.as_ref().and_then(|field| {
        let field_type = schema_map.get(field).map(|value| value.data_type.clone())?;
        infer_numeric_lower_bound(&dataset.rows, field, &field_type)
    });

    let mut projection = vec![sort_field.clone()];
    if let Some(eq_field) = eq_field.as_ref() {
        projection.push(eq_field.clone());
    }
    for field in dataset.schema.fields().iter().map(|field| field.name.clone()) {
        if projection.len() >= 3 {
            break;
        }
        if !projection.contains(&field) {
            projection.push(field);
        }
    }
    projection.sort();
    projection.dedup();

    Ok(WorkloadShape {
        projection,
        sort_field,
        eq_field,
        eq_values,
        range_field,
        range_lower_bound,
    })
}

fn infer_eq_field(rows: &[Value], string_fields: &[String]) -> (Option<String>, Vec<Value>) {
    for field in string_fields.iter() {
        let mut counts = HashMap::<String, usize>::new();
        for row in rows.iter().take(5_000) {
            if let Some(text) = row.get(field).and_then(Value::as_str) {
                *counts.entry(text.to_string()).or_insert(0) += 1;
            }
        }
        let mut values: Vec<(String, usize)> =
            counts.into_iter().filter(|(_, count)| *count > 1).collect();
        values.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        if !values.is_empty() {
            return (
                Some(field.clone()),
                values
                    .into_iter()
                    .take(3)
                    .map(|(value, _)| Value::String(value))
                    .collect(),
            );
        }
    }

    (None, vec![])
}

fn infer_numeric_lower_bound(
    rows: &[Value],
    field_name: &str,
    field_type: &PowdrrDataType,
) -> Option<Value> {
    match field_type {
        PowdrrDataType::Integer => {
            let mut values = rows
                .iter()
                .filter_map(|row| row.get(field_name).and_then(Value::as_i64))
                .collect::<Vec<_>>();
            if values.is_empty() {
                return None;
            }
            values.sort();
            Some(json!(values[values.len() / 3]))
        }
        PowdrrDataType::Float => {
            let mut values = rows
                .iter()
                .filter_map(|row| row.get(field_name).and_then(Value::as_f64))
                .collect::<Vec<_>>();
            if values.is_empty() {
                return None;
            }
            values.sort_by(|left, right| left.partial_cmp(right).unwrap());
            Some(json!(values[values.len() / 3]))
        }
        _ => None,
    }
}

fn build_benchmark_cases(workload: &WorkloadShape, limit: usize) -> Vec<BenchmarkCase> {
    let mut cases = vec![BenchmarkCase {
        name: "top_n".to_string(),
        plan: ServingRequestPlan {
            select: Some(workload.projection.clone()),
            filters: vec![],
            order_by: vec![ServingSort {
                field: workload.sort_field.clone(),
                descending: false,
            }],
            limit: Some(limit),
            allow_slow_path: true,
            explain: false,
        },
    }];

    if let Some(eq_field) = workload.eq_field.as_ref() {
        if let Some(eq_value) = workload.eq_values.first() {
            cases.push(BenchmarkCase {
                name: "eq_top_n".to_string(),
                plan: ServingRequestPlan {
                    select: Some(workload.projection.clone()),
                    filters: vec![ServingPredicate {
                        field: eq_field.clone(),
                        eq: Some(eq_value.clone()),
                        in_values: None,
                        gt: None,
                        gte: None,
                        lt: None,
                        lte: None,
                    }],
                    order_by: vec![ServingSort {
                        field: workload.sort_field.clone(),
                        descending: false,
                    }],
                    limit: Some(limit),
                    allow_slow_path: true,
                    explain: false,
                },
            });
        }

        if workload.eq_values.len() >= 2 {
            cases.push(BenchmarkCase {
                name: "in_top_n".to_string(),
                plan: ServingRequestPlan {
                    select: Some(workload.projection.clone()),
                    filters: vec![ServingPredicate {
                        field: eq_field.clone(),
                        eq: None,
                        in_values: Some(workload.eq_values.clone()),
                        gt: None,
                        gte: None,
                        lt: None,
                        lte: None,
                    }],
                    order_by: vec![ServingSort {
                        field: workload.sort_field.clone(),
                        descending: false,
                    }],
                    limit: Some(limit),
                    allow_slow_path: true,
                    explain: false,
                },
            });
        }
    }

    if let (Some(range_field), Some(range_lower_bound)) = (
        workload.range_field.as_ref(),
        workload.range_lower_bound.as_ref(),
    ) {
        cases.push(BenchmarkCase {
            name: "range_top_n".to_string(),
            plan: ServingRequestPlan {
                select: Some(workload.projection.clone()),
                filters: vec![ServingPredicate {
                    field: range_field.clone(),
                    eq: None,
                    in_values: None,
                    gt: None,
                    gte: Some(range_lower_bound.clone()),
                    lt: None,
                    lte: None,
                }],
                order_by: vec![ServingSort {
                    field: workload.sort_field.clone(),
                    descending: false,
                }],
                limit: Some(limit),
                allow_slow_path: true,
                explain: false,
            },
        });
    }

    cases
}

async fn setup_powdrr(
    test_server: &AsyncTestServer,
    table_name: &str,
    dataset_path: &Path,
    schema: &PowdrrSchema,
    cases: &[BenchmarkCase],
) -> Result<()> {
    let mode = TestProcessingMode {
        state_mode: StateMode::Testing,
        storage_mode: StorageMode::default(),
        cache_mode: CacheMode::Redis(None),
        peer_mode: PeerMode::SelfOnly,
        indexing_mode: IndexingMode::Disabled,
        compaction_mode: CompactionMode::Disabled,
        prefetch_mode: PrefetchMode::Disabled,
    };

    let response = test_server
        .client()
        .put(format!("{}/_test/v1/_testing_and_processing_mode", BENCH_BASE_URL))
        .mime(mime::APPLICATION_JSON)
        .body(serde_json::to_string(&mode)?)
        .perform()
        .await?;
    if response.status() != 200 {
        bail!(
            "failed to set Powdrr benchmark mode: {}",
            response.read_utf8_body().await?
        );
    }

    let file_size = fs::metadata(dataset_path)?.len();
    let file_url = format!("file://{}", dataset_path.display());
    let checkpoint = TableMetadataCheckpoint {
        table_name: table_name.to_string(),
        original_checkpoint_id: None,
        checkpoint_id: unique_name("checkpoint"),
        iceberg_metadata: Some(IcebergMetadata {
            table_schema: schema.clone(),
            snapshot_id: Some(unique_name("snapshot")),
            files: FileSetPayload {
                file_paths: vec![file_url],
                sizes: vec![file_size],
                file_schemas: vec![0],
                schemas: vec![schema.clone()],
            },
            column_names: schema.fields().iter().map(|field| field.name.clone()).collect(),
            column_stats: vec![],
        }),
        speedboat_metadata: None,
        deletes_metadata: None,
        extension_metadata: HashMap::new(),
        schema: schema.clone(),
    };

    let checkpoint_response = test_server
        .client()
        .post(format!("{}/_test/v1/_add_checkpoint", BENCH_BASE_URL))
        .mime(mime::APPLICATION_JSON)
        .body(serde_json::to_string(&checkpoint)?)
        .perform()
        .await?;
    if checkpoint_response.status() != 200 {
        bail!(
            "failed to add Powdrr checkpoint: {}",
            checkpoint_response.read_utf8_body().await?
        );
    }

    let config_response = test_server
        .client()
        .put(format!("{}/{}/_serve/config", BENCH_BASE_URL, table_name))
        .mime(mime::APPLICATION_JSON)
        .body(serde_json::to_string(&json!({
            "patterns": cases
                .iter()
                .map(|case| pattern_from_case(&case.name, &case.plan))
                .collect::<Vec<_>>()
        }))?)
        .perform()
        .await?;
    if config_response.status() != 200 {
        bail!(
            "failed to set Powdrr serving config: {}",
            config_response.read_utf8_body().await?
        );
    }

    Ok(())
}

fn pattern_from_case(name: &str, plan: &ServingRequestPlan) -> ServingPattern {
    let mut eq_fields = vec![];
    let mut range_field = None;
    for filter in plan.filters.iter() {
        if filter.eq.is_some() || filter.in_values.is_some() {
            eq_fields.push(filter.field.clone());
        }
        if filter.gt.is_some() || filter.gte.is_some() || filter.lt.is_some() || filter.lte.is_some()
        {
            range_field = Some(filter.field.clone());
        }
    }
    ServingPattern {
        name: name.to_string(),
        eq_fields,
        range_field,
        order_field: plan.order_by.first().map(|sort| sort.field.clone()),
        descending: plan
            .order_by
            .first()
            .map(|sort| sort.descending)
            .unwrap_or(false),
        max_limit: plan.limit.map(|limit| limit as u64),
        projection: plan.select.clone(),
    }
}

async fn setup_elasticsearch(
    http_client: &HttpClient,
    es_url: &str,
    index_name: &str,
    dataset: &ParquetDocumentSet,
) -> Result<()> {
    let _ = http_client
        .delete(format!("{}/{}", es_url, index_name))
        .send()
        .await;

    let mapping = elasticsearch_mapping(&dataset.schema);
    let create_response = http_client
        .put(format!("{}/{}", es_url, index_name))
        .json(&mapping)
        .send()
        .await?;
    if !create_response.status().is_success() {
        bail!("failed to create Elasticsearch index {}", create_response.text().await?);
    }

    let mut bulk_payload = String::new();
    for row in dataset.rows.iter() {
        bulk_payload.push_str(&format!(
            "{}\n{}\n",
            json!({ "index": { "_index": index_name } }),
            serde_json::to_string(row)?
        ));
    }

    let bulk_response = http_client
        .post(format!("{}/_bulk?refresh=true", es_url))
        .header("content-type", "application/x-ndjson")
        .body(bulk_payload)
        .send()
        .await?;
    if !bulk_response.status().is_success() {
        bail!(
            "failed to bulk load Elasticsearch data {}",
            bulk_response.text().await?
        );
    }

    Ok(())
}

fn elasticsearch_mapping(schema: &PowdrrSchema) -> Value {
    let mut properties = Map::new();
    for field in schema.fields().iter() {
        let field_mapping = match field.data_type {
            PowdrrDataType::Boolean => json!({ "type": "boolean" }),
            PowdrrDataType::Float => json!({ "type": "double" }),
            PowdrrDataType::Integer => json!({ "type": "long" }),
            PowdrrDataType::String => json!({ "type": "keyword" }),
            _ => continue,
        };
        properties.insert(field.name.clone(), field_mapping);
    }

    json!({
        "mappings": {
            "dynamic": "strict",
            "properties": Value::Object(properties),
        }
    })
}

async fn setup_mongodb(
    mongo_uri: &str,
    db_name: &str,
    collection_name: &str,
    dataset: &ParquetDocumentSet,
    cases: &[BenchmarkCase],
) -> Result<Collection<Document>> {
    let client = MongoClient::with_uri_str(mongo_uri).await?;
    let database = client.database(db_name);
    let collection = database.collection::<Document>(collection_name);

    let docs = dataset
        .rows
        .iter()
        .map(json_value_to_document)
        .collect::<Result<Vec<_>>>()?;
    for chunk in docs.chunks(500) {
        collection
            .insert_many(chunk.to_vec())
            .await
            .context("failed to insert Mongo benchmark chunk")?;
    }

    let mut seen_indexes = HashSet::new();
    for case in cases.iter() {
        let mut index_keys = Document::new();
        for filter in case.plan.filters.iter() {
            if filter.eq.is_some() || filter.in_values.is_some() {
                index_keys.insert(filter.field.clone(), 1);
            } else if filter.gt.is_some()
                || filter.gte.is_some()
                || filter.lt.is_some()
                || filter.lte.is_some()
            {
                index_keys.insert(filter.field.clone(), 1);
            }
        }
        for sort in case.plan.order_by.iter() {
            index_keys.insert(sort.field.clone(), if sort.descending { -1 } else { 1 });
        }
        if index_keys.is_empty() {
            continue;
        }

        let signature = format!("{:?}", index_keys);
        if !seen_indexes.insert(signature) {
            continue;
        }
        collection
            .create_index(IndexModel::builder().keys(index_keys).build())
            .await
            .context("failed to create Mongo benchmark index")?;
    }

    Ok(collection)
}

async fn run_powdrr_case(
    test_server: &AsyncTestServer,
    table_name: &str,
    plan: &ServingRequestPlan,
) -> Result<BackendQueryResult> {
    let response = test_server
        .client()
        .post(format!("{}/{}/_serve", BENCH_BASE_URL, table_name))
        .mime(mime::APPLICATION_JSON)
        .body(serde_json::to_string(plan)?)
        .perform()
        .await?;
    if response.status() != 200 {
        bail!("Powdrr query failed: {}", response.read_utf8_body().await?);
    }

    let parsed: ServingQueryResponse = serde_json::from_str(&response.read_utf8_body().await?)?;
    Ok(BackendQueryResult {
        rows: parsed.rows,
        classification: Some(parsed.classification),
    })
}

async fn run_es_case(
    http_client: &HttpClient,
    es_url: &str,
    index_name: &str,
    plan: &ServingRequestPlan,
) -> Result<BackendQueryResult> {
    let response = http_client
        .post(format!("{}/{}/_search", es_url, index_name))
        .json(&to_elasticsearch_search(plan))
        .send()
        .await?;
    if !response.status().is_success() {
        bail!("Elasticsearch query failed: {}", response.text().await?);
    }

    let json: Value = response.json().await?;
    let rows = json["hits"]["hits"]
        .as_array()
        .ok_or_else(|| anyhow!("missing hits array from Elasticsearch response"))?
        .iter()
        .map(|hit| hit["_source"].clone())
        .collect();

    Ok(BackendQueryResult {
        rows,
        classification: None,
    })
}

async fn run_mongo_case(
    collection: &Collection<Document>,
    plan: &ServingRequestPlan,
) -> Result<BackendQueryResult> {
    let mongo_request = to_mongodb_find(plan);
    let mut action = collection.find(json_value_to_document(&mongo_request.filter)?);
    if let Some(projection) = mongo_request.projection.as_ref() {
        action = action.projection(json_value_to_document(projection)?);
    }
    if let Some(sort) = mongo_request.sort.as_ref() {
        action = action.sort(json_value_to_document(sort)?);
    }
    if let Some(limit) = mongo_request.limit {
        action = action.limit(limit as i64);
    }
    let rows = action
        .await?
        .try_collect::<Vec<Document>>()
        .await?
        .into_iter()
        .map(|doc| serde_json::to_value(doc).unwrap())
        .collect();

    Ok(BackendQueryResult {
        rows,
        classification: None,
    })
}

async fn measure_runner<'a, F>(
    warmup: usize,
    iterations: usize,
    mut run: F,
) -> Result<(BackendQueryResult, LatencySummary)>
where
    F: FnMut() -> BoxFuture<'a, Result<BackendQueryResult>>,
{
    for _ in 0..warmup {
        let _ = run().await?;
    }

    let mut durations = vec![];
    let mut last_result = None;
    for _ in 0..iterations {
        let start = Instant::now();
        let result = run().await?;
        durations.push(start.elapsed());
        last_result = Some(result);
    }

    Ok((
        last_result.ok_or_else(|| anyhow!("no benchmark iterations were executed"))?,
        summarize_latencies(&durations),
    ))
}

fn summarize_latencies(durations: &[Duration]) -> LatencySummary {
    let mut millis = durations
        .iter()
        .map(|duration| duration.as_secs_f64() * 1000.0)
        .collect::<Vec<_>>();
    millis.sort_by(|left, right| left.partial_cmp(right).unwrap());

    let avg_ms = millis.iter().sum::<f64>() / millis.len() as f64;
    let percentile = |value: f64| -> f64 {
        let index = ((millis.len() - 1) as f64 * value).round() as usize;
        millis[index]
    };

    LatencySummary {
        avg_ms,
        min_ms: *millis.first().unwrap(),
        p50_ms: percentile(0.50),
        p95_ms: percentile(0.95),
        max_ms: *millis.last().unwrap(),
    }
}

fn print_summary(case_name: &str, backend_name: &str, summary: &LatencySummary) {
    println!(
        "{:<20} {:<10} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1}",
        case_name,
        backend_name,
        summary.avg_ms,
        summary.p50_ms,
        summary.p95_ms,
        summary.min_ms,
        summary.max_ms,
    );
}

fn normalize_rows(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|row| serde_json::to_string(&canonical_value(row)).unwrap())
        .collect()
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_value).collect()),
        Value::Object(map) => {
            let mut ordered = BTreeMap::new();
            for (key, value) in map.iter() {
                ordered.insert(key.clone(), canonical_value(value));
            }
            Value::Object(ordered.into_iter().collect::<Map<String, Value>>())
        }
        _ => value.clone(),
    }
}

fn assert_same_rows(
    left_name: &str,
    left_result: &BackendQueryResult,
    right_name: &str,
    right_result: &BackendQueryResult,
    case_name: &str,
) -> Result<()> {
    let left_rows = normalize_rows(&left_result.rows);
    let right_rows = normalize_rows(&right_result.rows);
    if left_rows != right_rows {
        bail!(
            "result mismatch for case {} between {} and {}",
            case_name,
            left_name,
            right_name
        );
    }
    Ok(())
}

fn json_value_to_document(value: &Value) -> Result<Document> {
    bson::to_document(value).context("failed to convert JSON value to BSON document")
}

async fn cleanup_elasticsearch(
    http_client: &HttpClient,
    es_url: &str,
    index_name: &str,
) -> Result<()> {
    let response = http_client
        .delete(format!("{}/{}", es_url, index_name))
        .send()
        .await?;
    if !(response.status().is_success() || response.status().as_u16() == 404) {
        bail!(
            "failed to delete Elasticsearch benchmark index {}",
            response.text().await?
        );
    }
    Ok(())
}

fn unique_name(prefix: &str) -> String {
    format!("{}_{}", prefix, unix_timestamp_millis())
}

fn unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
