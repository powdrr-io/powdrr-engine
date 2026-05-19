use std::cmp::Ordering;
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
use reqwest::{Client as HttpClient, Method as HttpMethod};
use serde::Serialize;
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
    powdrr_url: Option<String>,
    es_url: String,
    mongo_uri: String,
    sort_field: Option<String>,
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
            powdrr_url: env::var("POWDRR_SERVE_BENCH_POWDRR_URL").ok(),
            es_url: env::var("POWDRR_SERVE_BENCH_ES_URL")
                .unwrap_or_else(|_| "http://localhost:9200".to_string()),
            mongo_uri: env::var("POWDRR_SERVE_BENCH_MONGO_URI")
                .unwrap_or_else(|_| "mongodb://localhost:27017".to_string()),
            sort_field: env::var("POWDRR_SERVE_BENCH_SORT_FIELD").ok(),
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
    range_upper_bound: Option<Value>,
}

#[derive(Clone, Debug)]
struct BenchmarkCase {
    name: String,
    plan: ServingRequestPlan,
    comparable_limit: usize,
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

enum PowdrrTarget {
    InProcess(AsyncTestServer),
    External(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = BenchmarkConfig::from_env()?;
    let dataset_path = canonical_dataset_path(&config.dataset_path)?;
    let dataset = read_parquet_documents(dataset_path.to_str().unwrap(), None)
        .await
        .map_err(|error| anyhow!(error))
        .with_context(|| {
            format!(
                "failed to read parquet dataset at {}",
                dataset_path.display()
            )
        })?;
    let workload = infer_workload(&dataset, config.limit, config.sort_field.as_deref())?;
    let cases = build_benchmark_cases(&workload, &dataset.rows, config.limit);
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
    println!(
        "Case limits: {}",
        cases
            .iter()
            .map(|case| format!("{}={}", case.name, case.comparable_limit))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let table_name = unique_name("serve_bench");
    let http_client = HttpClient::new();
    let powdrr_target = match config.powdrr_url.as_ref() {
        Some(powdrr_url) => PowdrrTarget::External(powdrr_url.clone()),
        None => PowdrrTarget::InProcess(
            AsyncTestServer::new(router(true))
                .await
                .context("failed to start in-process Powdrr benchmark server")?,
        ),
    };
    setup_powdrr(
        &powdrr_target,
        &http_client,
        &table_name,
        &dataset_path,
        &dataset.schema,
        &cases,
    )
    .await?;

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
        let collection = setup_mongodb(
            &config.mongo_uri,
            &db_name,
            &collection_name,
            &dataset,
            &cases,
        )
        .await?;
        mongo_target = Some((db_name, collection_name, collection));
    }

    println!();
    println!(
        "{:<20} {:<10} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "case", "backend", "avg", "p50", "p95", "min", "max"
    );

    for case in cases.iter() {
        let powdrr_baseline =
            run_powdrr_case(&powdrr_target, &http_client, &table_name, &case.plan).await?;
        if powdrr_baseline.classification != Some(ServingQueryClassification::FastPath) {
            bail!(
                "Powdrr case {} did not stay on fast path: {:?}",
                case.name,
                powdrr_baseline.classification
            );
        }

        let (powdrr_result, powdrr_latency) =
            measure_runner(config.warmup, config.iterations, || {
                run_powdrr_case(&powdrr_target, &http_client, &table_name, &case.plan).boxed()
            })
            .await?;
        print_summary(&case.name, "powdrr", &powdrr_latency);

        if let Some((es_url, index_name)) = es_target.as_ref() {
            let es_first = run_es_case(&http_client, es_url, index_name, &case.plan).await?;
            assert_same_rows(
                "powdrr",
                &powdrr_baseline,
                "elasticsearch",
                &es_first,
                &case.name,
                &case.plan,
            )?;
            let (_, es_latency) = measure_runner(config.warmup, config.iterations, || {
                run_es_case(&http_client, es_url, index_name, &case.plan).boxed()
            })
            .await?;
            print_summary(&case.name, "elasticsearch", &es_latency);
        }

        if let Some((_, _, collection)) = mongo_target.as_ref() {
            let mongo_first = run_mongo_case(collection, &case.plan).await?;
            assert_same_rows(
                "powdrr",
                &powdrr_baseline,
                "mongodb",
                &mongo_first,
                &case.name,
                &case.plan,
            )?;
            let (_, mongo_latency) = measure_runner(config.warmup, config.iterations, || {
                run_mongo_case(collection, &case.plan).boxed()
            })
            .await?;
            print_summary(&case.name, "mongodb", &mongo_latency);
        }

        assert_eq!(
            normalize_rows_for_plan(&powdrr_baseline.rows, &case.plan),
            normalize_rows_for_plan(&powdrr_result.rows, &case.plan)
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

fn infer_workload(
    dataset: &ParquetDocumentSet,
    requested_limit: usize,
    forced_sort_field: Option<&str>,
) -> Result<WorkloadShape> {
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
    let sort_field = match forced_sort_field {
        Some(field) => {
            let Some(schema_field) = schema_map.get(field) else {
                bail!(
                    "configured sort field {} is not present in the dataset schema",
                    field
                );
            };
            match schema_field.data_type {
                PowdrrDataType::Integer | PowdrrDataType::Float | PowdrrDataType::String => {
                    field.to_string()
                }
                _ => bail!(
                    "configured sort field {} is not a supported scalar serving field",
                    field
                ),
            }
        }
        None => infer_sort_field(
            &dataset.rows,
            &string_fields,
            &numeric_fields,
            requested_limit,
        )
        .ok_or_else(|| anyhow!("expected at least one sortable field"))?,
    };

    let range_field = numeric_fields.first().cloned();
    let (range_lower_bound, range_upper_bound) = range_field
        .as_ref()
        .and_then(|field| {
            let field_type = schema_map.get(field).map(|value| value.data_type.clone())?;
            Some(infer_numeric_bounds(&dataset.rows, field, &field_type))
        })
        .unwrap_or((None, None));

    let mut projection = vec![sort_field.clone()];
    if let Some(eq_field) = eq_field.as_ref() {
        projection.push(eq_field.clone());
    }
    for field in dataset
        .schema
        .fields()
        .iter()
        .map(|field| field.name.clone())
    {
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
        range_upper_bound,
    })
}

fn infer_sort_field(
    rows: &[Value],
    string_fields: &[String],
    numeric_fields: &[String],
    requested_limit: usize,
) -> Option<String> {
    let candidates = string_fields
        .iter()
        .map(|field| (field, 0usize))
        .chain(numeric_fields.iter().map(|field| (field, 1usize)))
        .filter(|(field, _)| is_benchmark_sort_field_candidate(field))
        .collect::<Vec<_>>();

    candidates
        .into_iter()
        .max_by(|(left_field, left_kind), (right_field, right_kind)| {
            let left_safe_limit =
                safe_ordered_limit_for_field(rows, left_field, requested_limit).unwrap_or(0);
            let right_safe_limit =
                safe_ordered_limit_for_field(rows, right_field, requested_limit).unwrap_or(0);
            left_safe_limit
                .cmp(&right_safe_limit)
                .then_with(|| {
                    score_sort_field(rows, left_field).cmp(&score_sort_field(rows, right_field))
                })
                .then_with(|| right_kind.cmp(left_kind))
                .then_with(|| left_field.cmp(right_field))
        })
        .map(|(field, _)| field.clone())
}

fn is_benchmark_sort_field_candidate(field_name: &str) -> bool {
    !field_name.starts_with('_') && field_name != "index_col"
}

fn score_sort_field(rows: &[Value], field_name: &str) -> usize {
    rows.iter()
        .filter_map(|row| row.get(field_name))
        .map(|value| serde_json::to_string(&canonical_value(value)).unwrap())
        .collect::<HashSet<_>>()
        .len()
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

fn infer_numeric_bounds(
    rows: &[Value],
    field_name: &str,
    field_type: &PowdrrDataType,
) -> (Option<Value>, Option<Value>) {
    match field_type {
        PowdrrDataType::Integer => {
            let mut values = rows
                .iter()
                .filter_map(|row| row.get(field_name).and_then(Value::as_i64))
                .collect::<Vec<_>>();
            if values.is_empty() {
                return (None, None);
            }
            values.sort();
            (
                Some(json!(values[values.len() / 3])),
                Some(json!(values[(values.len() * 2) / 3])),
            )
        }
        PowdrrDataType::Float => {
            let mut values = rows
                .iter()
                .filter_map(|row| row.get(field_name).and_then(Value::as_f64))
                .collect::<Vec<_>>();
            if values.is_empty() {
                return (None, None);
            }
            values.sort_by(|left, right| left.partial_cmp(right).unwrap());
            (
                Some(json!(values[values.len() / 3])),
                Some(json!(values[(values.len() * 2) / 3])),
            )
        }
        _ => (None, None),
    }
}

fn build_benchmark_cases(
    workload: &WorkloadShape,
    rows: &[Value],
    requested_limit: usize,
) -> Vec<BenchmarkCase> {
    let mut cases = vec![];

    push_benchmark_case(
        &mut cases,
        rows,
        requested_limit,
        "top_n",
        benchmark_plan(workload, vec![], false, requested_limit),
    );
    push_benchmark_case(
        &mut cases,
        rows,
        requested_limit,
        "top_n_desc",
        benchmark_plan(workload, vec![], true, requested_limit),
    );

    if let Some(eq_field) = workload.eq_field.as_ref() {
        if let Some(eq_value) = workload.eq_values.first() {
            let eq_filter = ServingPredicate {
                field: eq_field.clone(),
                eq: Some(eq_value.clone()),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            };
            push_benchmark_case(
                &mut cases,
                rows,
                requested_limit,
                "eq_top_n",
                benchmark_plan(workload, vec![eq_filter.clone()], false, requested_limit),
            );
            push_benchmark_case(
                &mut cases,
                rows,
                requested_limit,
                "eq_top_n_desc",
                benchmark_plan(workload, vec![eq_filter.clone()], true, requested_limit),
            );

            if let (Some(range_field), Some(range_lower_bound)) = (
                workload.range_field.as_ref(),
                workload.range_lower_bound.as_ref(),
            ) {
                let mut filters = vec![eq_filter.clone()];
                filters.push(ServingPredicate {
                    field: range_field.clone(),
                    eq: None,
                    in_values: None,
                    gt: None,
                    gte: Some(range_lower_bound.clone()),
                    lt: None,
                    lte: None,
                });
                push_benchmark_case(
                    &mut cases,
                    rows,
                    requested_limit,
                    "eq_range_top_n",
                    benchmark_plan(workload, filters.clone(), false, requested_limit),
                );
                push_benchmark_case(
                    &mut cases,
                    rows,
                    requested_limit,
                    "eq_range_top_n_desc",
                    benchmark_plan(workload, filters, true, requested_limit),
                );
            }

            if let (Some(range_field), Some(range_lower_bound), Some(range_upper_bound)) = (
                workload.range_field.as_ref(),
                workload.range_lower_bound.as_ref(),
                workload.range_upper_bound.as_ref(),
            ) {
                if compare_json_values(Some(range_lower_bound), Some(range_upper_bound))
                    == Ordering::Less
                {
                    let mut filters = vec![eq_filter.clone()];
                    filters.push(ServingPredicate {
                        field: range_field.clone(),
                        eq: None,
                        in_values: None,
                        gt: None,
                        gte: Some(range_lower_bound.clone()),
                        lt: Some(range_upper_bound.clone()),
                        lte: None,
                    });
                    push_benchmark_case(
                        &mut cases,
                        rows,
                        requested_limit,
                        "eq_window_top_n",
                        benchmark_plan(workload, filters.clone(), false, requested_limit),
                    );
                    push_benchmark_case(
                        &mut cases,
                        rows,
                        requested_limit,
                        "eq_window_top_n_desc",
                        benchmark_plan(workload, filters, true, requested_limit),
                    );
                }
            }
        }

        if workload.eq_values.len() >= 2 {
            let in_filter = ServingPredicate {
                field: eq_field.clone(),
                eq: None,
                in_values: Some(workload.eq_values.clone()),
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            };
            push_benchmark_case(
                &mut cases,
                rows,
                requested_limit,
                "in_top_n",
                benchmark_plan(workload, vec![in_filter.clone()], false, requested_limit),
            );
            push_benchmark_case(
                &mut cases,
                rows,
                requested_limit,
                "in_top_n_desc",
                benchmark_plan(workload, vec![in_filter.clone()], true, requested_limit),
            );

            if let (Some(range_field), Some(range_lower_bound)) = (
                workload.range_field.as_ref(),
                workload.range_lower_bound.as_ref(),
            ) {
                let mut filters = vec![in_filter.clone()];
                filters.push(ServingPredicate {
                    field: range_field.clone(),
                    eq: None,
                    in_values: None,
                    gt: None,
                    gte: Some(range_lower_bound.clone()),
                    lt: None,
                    lte: None,
                });
                push_benchmark_case(
                    &mut cases,
                    rows,
                    requested_limit,
                    "in_range_top_n",
                    benchmark_plan(workload, filters.clone(), false, requested_limit),
                );
                push_benchmark_case(
                    &mut cases,
                    rows,
                    requested_limit,
                    "in_range_top_n_desc",
                    benchmark_plan(workload, filters, true, requested_limit),
                );
            }
        }
    }

    if let (Some(range_field), Some(range_lower_bound)) = (
        workload.range_field.as_ref(),
        workload.range_lower_bound.as_ref(),
    ) {
        let range_filter = ServingPredicate {
            field: range_field.clone(),
            eq: None,
            in_values: None,
            gt: None,
            gte: Some(range_lower_bound.clone()),
            lt: None,
            lte: None,
        };
        push_benchmark_case(
            &mut cases,
            rows,
            requested_limit,
            "range_top_n",
            benchmark_plan(workload, vec![range_filter.clone()], false, requested_limit),
        );
        push_benchmark_case(
            &mut cases,
            rows,
            requested_limit,
            "range_top_n_desc",
            benchmark_plan(workload, vec![range_filter], true, requested_limit),
        );
    }

    if let (Some(range_field), Some(range_upper_bound)) = (
        workload.range_field.as_ref(),
        workload.range_upper_bound.as_ref(),
    ) {
        let upper_range_filter = ServingPredicate {
            field: range_field.clone(),
            eq: None,
            in_values: None,
            gt: None,
            gte: None,
            lt: Some(range_upper_bound.clone()),
            lte: None,
        };
        push_benchmark_case(
            &mut cases,
            rows,
            requested_limit,
            "range_lt_top_n",
            benchmark_plan(
                workload,
                vec![upper_range_filter.clone()],
                false,
                requested_limit,
            ),
        );
        push_benchmark_case(
            &mut cases,
            rows,
            requested_limit,
            "range_lt_top_n_desc",
            benchmark_plan(workload, vec![upper_range_filter], true, requested_limit),
        );
    }

    if let (Some(range_field), Some(range_lower_bound), Some(range_upper_bound)) = (
        workload.range_field.as_ref(),
        workload.range_lower_bound.as_ref(),
        workload.range_upper_bound.as_ref(),
    ) {
        if compare_json_values(Some(range_lower_bound), Some(range_upper_bound)) == Ordering::Less {
            let window_filter = ServingPredicate {
                field: range_field.clone(),
                eq: None,
                in_values: None,
                gt: None,
                gte: Some(range_lower_bound.clone()),
                lt: Some(range_upper_bound.clone()),
                lte: None,
            };
            push_benchmark_case(
                &mut cases,
                rows,
                requested_limit,
                "range_window_top_n",
                benchmark_plan(
                    workload,
                    vec![window_filter.clone()],
                    false,
                    requested_limit,
                ),
            );
            push_benchmark_case(
                &mut cases,
                rows,
                requested_limit,
                "range_window_top_n_desc",
                benchmark_plan(workload, vec![window_filter], true, requested_limit),
            );
        }
    }

    cases
}

fn benchmark_plan(
    workload: &WorkloadShape,
    filters: Vec<ServingPredicate>,
    descending: bool,
    requested_limit: usize,
) -> ServingRequestPlan {
    ServingRequestPlan {
        select: Some(workload.projection.clone()),
        filters,
        order_by: vec![ServingSort {
            field: workload.sort_field.clone(),
            descending,
        }],
        limit: Some(requested_limit),
        allow_slow_path: true,
        explain: false,
    }
}

fn push_benchmark_case(
    cases: &mut Vec<BenchmarkCase>,
    rows: &[Value],
    requested_limit: usize,
    name: &str,
    mut plan: ServingRequestPlan,
) {
    let Some(comparable_limit) = infer_comparable_limit(rows, &plan, requested_limit) else {
        return;
    };
    plan.limit = Some(comparable_limit);
    cases.push(BenchmarkCase {
        name: name.to_string(),
        plan,
        comparable_limit,
    });
}

fn infer_comparable_limit(
    rows: &[Value],
    plan: &ServingRequestPlan,
    requested_limit: usize,
) -> Option<usize> {
    let mut matching_rows = rows
        .iter()
        .filter(|row| row_matches_request(row, plan))
        .collect::<Vec<_>>();
    if matching_rows.is_empty() {
        return None;
    }

    let available_limit = requested_limit.min(matching_rows.len());
    if available_limit == 0 {
        return None;
    }

    let Some(sort) = plan.order_by.first() else {
        return Some(available_limit);
    };

    matching_rows.sort_by(|left, right| compare_rows_for_sort(left, right, sort));
    if available_limit == matching_rows.len() {
        return Some(available_limit);
    }

    let boundary_value = matching_rows[available_limit - 1].get(&sort.field);
    let next_value = matching_rows[available_limit].get(&sort.field);
    if !json_values_equal(boundary_value, next_value) {
        return Some(available_limit);
    }

    let mut comparable_limit = available_limit;
    while comparable_limit > 0
        && json_values_equal(
            matching_rows[comparable_limit - 1].get(&sort.field),
            boundary_value,
        )
    {
        comparable_limit -= 1;
    }

    (comparable_limit > 0).then_some(comparable_limit)
}

fn safe_ordered_limit_for_field(
    rows: &[Value],
    field_name: &str,
    requested_limit: usize,
) -> Option<usize> {
    let asc_plan = ServingRequestPlan {
        select: None,
        filters: vec![],
        order_by: vec![ServingSort {
            field: field_name.to_string(),
            descending: false,
        }],
        limit: Some(requested_limit),
        allow_slow_path: true,
        explain: false,
    };
    let desc_plan = ServingRequestPlan {
        select: None,
        filters: vec![],
        order_by: vec![ServingSort {
            field: field_name.to_string(),
            descending: true,
        }],
        limit: Some(requested_limit),
        allow_slow_path: true,
        explain: false,
    };

    Some(
        infer_comparable_limit(rows, &asc_plan, requested_limit)?.min(infer_comparable_limit(
            rows,
            &desc_plan,
            requested_limit,
        )?),
    )
}

fn row_matches_request(row: &Value, plan: &ServingRequestPlan) -> bool {
    plan.filters
        .iter()
        .all(|predicate| row_matches_predicate(row, predicate))
}

fn row_matches_predicate(row: &Value, predicate: &ServingPredicate) -> bool {
    let Some(field_value) = row.get(&predicate.field) else {
        return false;
    };

    if let Some(eq) = predicate.eq.as_ref() {
        return canonical_value(field_value) == canonical_value(eq);
    }
    if let Some(in_values) = predicate.in_values.as_ref() {
        return in_values
            .iter()
            .any(|candidate| canonical_value(candidate) == canonical_value(field_value));
    }

    if let Some(gt) = predicate.gt.as_ref() {
        if compare_json_values(Some(field_value), Some(gt)) != Ordering::Greater {
            return false;
        }
    }
    if let Some(gte) = predicate.gte.as_ref() {
        let ordering = compare_json_values(Some(field_value), Some(gte));
        if ordering != Ordering::Greater && ordering != Ordering::Equal {
            return false;
        }
    }
    if let Some(lt) = predicate.lt.as_ref() {
        if compare_json_values(Some(field_value), Some(lt)) != Ordering::Less {
            return false;
        }
    }
    if let Some(lte) = predicate.lte.as_ref() {
        let ordering = compare_json_values(Some(field_value), Some(lte));
        if ordering != Ordering::Less && ordering != Ordering::Equal {
            return false;
        }
    }

    true
}

fn compare_rows_for_sort(left: &Value, right: &Value, sort: &ServingSort) -> Ordering {
    let ordering = compare_json_values(left.get(&sort.field), right.get(&sort.field));
    if sort.descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_json_values(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    match (left, right) {
        (Some(Value::String(left)), Some(Value::String(right))) => left.cmp(right),
        (Some(Value::Bool(left)), Some(Value::Bool(right))) => left.cmp(right),
        (Some(Value::Number(left)), Some(Value::Number(right))) => {
            match (left.as_i64(), right.as_i64()) {
                (Some(left), Some(right)) => left.cmp(&right),
                _ => left
                    .as_f64()
                    .partial_cmp(&right.as_f64())
                    .unwrap_or(Ordering::Equal),
            }
        }
        (Some(Value::Null), Some(Value::Null)) | (None, None) => Ordering::Equal,
        (Some(Value::Null), _) | (None, _) => Ordering::Greater,
        (_, Some(Value::Null)) | (_, None) => Ordering::Less,
        (Some(left), Some(right)) => canonical_value(left)
            .to_string()
            .cmp(&canonical_value(right).to_string()),
    }
}

fn json_values_equal(left: Option<&Value>, right: Option<&Value>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => canonical_value(left) == canonical_value(right),
        (None, None) => true,
        _ => false,
    }
}

async fn setup_powdrr(
    powdrr_target: &PowdrrTarget,
    http_client: &HttpClient,
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

    send_powdrr_json(
        powdrr_target,
        http_client,
        HttpMethod::PUT,
        "/_test/v1/_testing_and_processing_mode",
        &mode,
    )
    .await
    .context("failed to set Powdrr benchmark mode")?;

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
            column_names: schema
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
        schema: schema.clone(),
    };

    send_powdrr_json(
        powdrr_target,
        http_client,
        HttpMethod::POST,
        "/_test/v1/_add_checkpoint",
        &checkpoint,
    )
    .await
    .context("failed to add Powdrr checkpoint")?;

    send_powdrr_json(
        powdrr_target,
        http_client,
        HttpMethod::PUT,
        &format!("/{}/_serve/config", table_name),
        &json!({
            "patterns": cases
                .iter()
                .map(|case| pattern_from_case(&case.name, &case.plan))
                .collect::<Vec<_>>()
        }),
    )
    .await
    .context("failed to set Powdrr serving config")?;

    Ok(())
}

fn pattern_from_case(name: &str, plan: &ServingRequestPlan) -> ServingPattern {
    let mut eq_fields = vec![];
    let mut range_field = None;
    for filter in plan.filters.iter() {
        if filter.eq.is_some() || filter.in_values.is_some() {
            eq_fields.push(filter.field.clone());
        }
        if filter.gt.is_some()
            || filter.gte.is_some()
            || filter.lt.is_some()
            || filter.lte.is_some()
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
        bail!(
            "failed to create Elasticsearch index {}",
            create_response.text().await?
        );
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
    powdrr_target: &PowdrrTarget,
    http_client: &HttpClient,
    table_name: &str,
    plan: &ServingRequestPlan,
) -> Result<BackendQueryResult> {
    let response_body = send_powdrr_json(
        powdrr_target,
        http_client,
        HttpMethod::POST,
        &format!("/{}/_serve", table_name),
        plan,
    )
    .await
    .context("Powdrr query failed")?;

    let parsed: ServingQueryResponse = serde_json::from_str(&response_body)?;
    Ok(BackendQueryResult {
        rows: parsed.rows,
        classification: Some(parsed.classification),
    })
}

async fn send_powdrr_json<T: Serialize>(
    powdrr_target: &PowdrrTarget,
    http_client: &HttpClient,
    method: HttpMethod,
    path: &str,
    body: &T,
) -> Result<String> {
    match powdrr_target {
        PowdrrTarget::InProcess(test_server) => {
            let response = match method {
                HttpMethod::POST => {
                    test_server
                        .client()
                        .post(format!("{}{}", BENCH_BASE_URL, path))
                        .mime(mime::APPLICATION_JSON)
                        .body(serde_json::to_string(body)?)
                        .perform()
                        .await?
                }
                HttpMethod::PUT => {
                    test_server
                        .client()
                        .put(format!("{}{}", BENCH_BASE_URL, path))
                        .mime(mime::APPLICATION_JSON)
                        .body(serde_json::to_string(body)?)
                        .perform()
                        .await?
                }
                unsupported => {
                    bail!("unsupported in-process Powdrr HTTP method {}", unsupported);
                }
            };
            let status = response.status();
            let response_body = response.read_utf8_body().await?;
            if status != 200 {
                bail!("{}", response_body);
            }
            Ok(response_body)
        }
        PowdrrTarget::External(base_url) => {
            let response = http_client
                .request(
                    method,
                    format!("{}{}", base_url.trim_end_matches('/'), path),
                )
                .header("content-type", "application/json")
                .body(serde_json::to_vec(body)?)
                .send()
                .await?;
            let status = response.status();
            let response_body = response.text().await?;
            if !status.is_success() {
                bail!("{}", response_body);
            }
            Ok(response_body)
        }
    }
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

fn normalize_rows_for_plan(rows: &[Value], plan: &ServingRequestPlan) -> Vec<String> {
    let mut keyed_rows = rows
        .iter()
        .map(|row| (serde_json::to_string(&canonical_value(row)).unwrap(), row))
        .collect::<Vec<_>>();

    if let Some(sort) = plan.order_by.first() {
        keyed_rows.sort_by(|(left_str, left_row), (right_str, right_row)| {
            compare_rows_for_sort(left_row, right_row, sort).then_with(|| left_str.cmp(right_str))
        });
    } else {
        keyed_rows.sort_by(|(left, _), (right, _)| left.cmp(right));
    }

    keyed_rows
        .into_iter()
        .map(|(row_string, _)| row_string)
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
    plan: &ServingRequestPlan,
) -> Result<()> {
    let left_rows = normalize_rows_for_plan(&left_result.rows, plan);
    let right_rows = normalize_rows_for_plan(&right_result.rows, plan);
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

#[cfg(test)]
mod tests {
    use super::{
        BenchmarkCase, WorkloadShape, build_benchmark_cases, infer_comparable_limit,
        infer_sort_field,
    };
    use crate::ServingPredicate;
    use powdrr_lib::serving_plan::{ServingRequestPlan, ServingSort};
    use serde_json::{Value, json};
    use std::collections::HashSet;

    fn sort_plan(field: &str, descending: bool, limit: usize) -> ServingRequestPlan {
        ServingRequestPlan {
            select: None,
            filters: vec![],
            order_by: vec![ServingSort {
                field: field.to_string(),
                descending,
            }],
            limit: Some(limit),
            allow_slow_path: true,
            explain: false,
        }
    }

    fn case_names(cases: &[BenchmarkCase]) -> HashSet<String> {
        cases.iter().map(|case| case.name.clone()).collect()
    }

    #[test]
    fn comparable_limit_moves_before_a_tied_cutoff() {
        let rows = vec![
            json!({"title": "alpha", "tenant": "acme", "score": 10}),
            json!({"title": "bravo", "tenant": "acme", "score": 9}),
            json!({"title": "bravo", "tenant": "omega", "score": 8}),
            json!({"title": "charlie", "tenant": "omega", "score": 7}),
        ];

        assert_eq!(
            infer_comparable_limit(&rows, &sort_plan("title", false, 2), 2),
            Some(1)
        );
    }

    #[test]
    fn infer_sort_field_prefers_a_field_with_a_stable_prefix() {
        let rows = vec![
            json!({"rank": 1, "title": "alpha"}),
            json!({"rank": 1, "title": "bravo"}),
            json!({"rank": 1, "title": "charlie"}),
            json!({"rank": 2, "title": "delta"}),
        ];
        let string_fields = vec!["title".to_string()];
        let numeric_fields = vec!["rank".to_string()];

        assert_eq!(
            infer_sort_field(&rows, &string_fields, &numeric_fields, 3),
            Some("title".to_string())
        );
    }

    #[test]
    fn build_benchmark_cases_adds_desc_and_combined_cases() {
        let rows: Vec<Value> = vec![
            json!({"title": "alpha", "tenant": "acme", "score": 10}),
            json!({"title": "bravo", "tenant": "acme", "score": 20}),
            json!({"title": "charlie", "tenant": "beta", "score": 30}),
            json!({"title": "delta", "tenant": "beta", "score": 40}),
            json!({"title": "echo", "tenant": "beta", "score": 50}),
            json!({"title": "foxtrot", "tenant": "gamma", "score": 60}),
        ];
        let workload = WorkloadShape {
            projection: vec![
                "score".to_string(),
                "tenant".to_string(),
                "title".to_string(),
            ],
            sort_field: "title".to_string(),
            eq_field: Some("tenant".to_string()),
            eq_values: vec![json!("beta"), json!("acme"), json!("gamma")],
            range_field: Some("score".to_string()),
            range_lower_bound: Some(json!(30)),
            range_upper_bound: Some(json!(50)),
        };

        let names = case_names(&build_benchmark_cases(&workload, &rows, 3));

        assert!(names.contains("top_n_desc"));
        assert!(names.contains("eq_top_n_desc"));
        assert!(names.contains("in_top_n_desc"));
        assert!(names.contains("range_top_n_desc"));
        assert!(names.contains("range_lt_top_n"));
        assert!(names.contains("range_lt_top_n_desc"));
        assert!(names.contains("range_window_top_n"));
        assert!(names.contains("range_window_top_n_desc"));
        assert!(names.contains("eq_range_top_n"));
        assert!(names.contains("eq_range_top_n_desc"));
        assert!(names.contains("eq_window_top_n"));
        assert!(names.contains("eq_window_top_n_desc"));
        assert!(names.contains("in_range_top_n"));
        assert!(names.contains("in_range_top_n_desc"));
    }

    #[test]
    fn comparable_limit_respects_filters_before_sorting() {
        let rows = vec![
            json!({"title": "alpha", "tenant": "acme"}),
            json!({"title": "bravo", "tenant": "omega"}),
            json!({"title": "bravo", "tenant": "omega"}),
            json!({"title": "charlie", "tenant": "omega"}),
        ];
        let plan = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("omega")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            order_by: vec![ServingSort {
                field: "title".to_string(),
                descending: false,
            }],
            limit: Some(2),
            allow_slow_path: true,
            explain: false,
        };

        assert_eq!(infer_comparable_limit(&rows, &plan, 2), Some(2));
    }
}
