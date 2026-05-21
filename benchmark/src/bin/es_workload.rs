use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use gotham::mime;
use gotham::test::TestServer;
use powdrr_query_server::router::router;
use reqwest::blocking::Client;
use serde::Deserialize;

const CASES_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../main_lib/tests/data/es_compat_cases.json"
));
const DEFAULT_CASE_IDS: &str = "logs_wildcard_query_string_returns_expected_hits,logs_wildcard_search_after_returns_expected_hits,logs_wildcard_date_histogram_with_bounds_returns_expected_buckets,logs_wildcard_terms_order_and_missing_return_expected_buckets,logs_wildcard_terms_subaggregation_returns_expected_buckets";

#[derive(Debug, Deserialize)]
struct CaseFile {
    cases: Vec<CompatibilityCase>,
}

#[derive(Debug, Deserialize, Clone)]
struct CompatibilityCase {
    id: String,
    #[serde(rename = "description")]
    description: String,
    differential_enabled: bool,
    setup_steps: Vec<TargetedStep>,
    request: RequestSpec,
}

#[derive(Debug, Deserialize, Clone)]
struct TargetedStep {
    targets: Vec<String>,
    request: RequestSpec,
}

#[derive(Debug, Deserialize, Clone)]
struct RequestSpec {
    method: String,
    path: String,
    body: String,
    mime: String,
}

#[derive(Clone)]
struct ResolvedRequest {
    method: String,
    path: String,
    body: String,
    mime: String,
}

#[derive(Debug)]
struct ResponseRecord {
    status: u16,
    body: String,
}

trait Target {
    fn label(&self) -> &'static str;
    fn execute(&self, request: &ResolvedRequest) -> ResponseRecord;
}

struct LocalTarget {
    server: TestServer,
}

impl LocalTarget {
    fn new() -> Result<Self> {
        Ok(Self {
            server: TestServer::new(router(true))
                .map_err(|error| anyhow!("failed to start local benchmark server: {error}"))?,
        })
    }
}

impl Target for LocalTarget {
    fn label(&self) -> &'static str {
        "local"
    }

    fn execute(&self, request: &ResolvedRequest) -> ResponseRecord {
        let url = format!("http://localhost{}", request.path);
        let mime_value = mime_from_label(&request.mime);
        let response = match request.method.as_str() {
            "GET" => self.server.client().get(&url).perform().unwrap(),
            "HEAD" => self.server.client().head(&url).perform().unwrap(),
            "DELETE" => self.server.client().delete(&url).perform().unwrap(),
            "POST" => self
                .server
                .client()
                .post(&url, request.body.clone(), mime_value)
                .perform()
                .unwrap(),
            "PUT" => self
                .server
                .client()
                .put(&url, request.body.clone(), mime_value)
                .perform()
                .unwrap(),
            other => panic!("unsupported local method: {}", other),
        };
        let status = response.status().as_u16();
        let body = String::from_utf8(response.read_body().unwrap_or_default()).unwrap_or_default();
        ResponseRecord { status, body }
    }
}

struct ExternalTarget {
    client: Client,
    base_url: String,
}

impl ExternalTarget {
    fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }
}

impl Target for ExternalTarget {
    fn label(&self) -> &'static str {
        "external"
    }

    fn execute(&self, request: &ResolvedRequest) -> ResponseRecord {
        let url = format!("{}{}", self.base_url, request.path);
        let mut builder = match request.method.as_str() {
            "GET" => self.client.get(&url),
            "HEAD" => self.client.head(&url),
            "DELETE" => self.client.delete(&url),
            "POST" => self.client.post(&url),
            "PUT" => self.client.put(&url),
            other => panic!("unsupported external method: {}", other),
        };
        if !request.body.is_empty() {
            builder = builder
                .header("content-type", request.mime.as_str())
                .body(request.body.clone());
        }
        let response = builder.send().unwrap();
        let status = response.status().as_u16();
        let body = response.text().unwrap_or_default();
        ResponseRecord { status, body }
    }
}

#[derive(Clone, Debug)]
struct BenchmarkConfig {
    es_url: String,
    case_ids: Vec<String>,
    warmup: usize,
    iterations: usize,
}

impl BenchmarkConfig {
    fn from_env() -> Self {
        Self {
            es_url: env::var("POWDRR_ES_WORKLOAD_BENCH_ES_URL")
                .unwrap_or_else(|_| "http://localhost:9200".to_string()),
            case_ids: env::var("POWDRR_ES_WORKLOAD_BENCH_CASE_IDS")
                .unwrap_or_else(|_| DEFAULT_CASE_IDS.to_string())
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string())
                .collect(),
            warmup: env::var("POWDRR_ES_WORKLOAD_BENCH_WARMUP")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(3),
            iterations: env::var("POWDRR_ES_WORKLOAD_BENCH_ITERATIONS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(10),
        }
    }
}

#[derive(Clone, Debug)]
struct LatencySummary {
    avg_ms: f64,
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
}

fn main() -> Result<()> {
    let config = BenchmarkConfig::from_env();
    let case_map = load_cases()
        .into_iter()
        .map(|case| (case.id.clone(), case))
        .collect::<HashMap<_, _>>();
    let selected_cases = config
        .case_ids
        .iter()
        .map(|case_id| {
            case_map
                .get(case_id)
                .cloned()
                .with_context(|| format!("missing benchmark fixture case {}", case_id))
        })
        .collect::<Result<Vec<_>>>()?;

    let local_target = LocalTarget::new()?;
    let external_target = ExternalTarget::new(config.es_url.clone());

    println!("ES workload benchmark");
    println!("Elasticsearch URL: {}", config.es_url);
    println!(
        "Warmup: {}, iterations: {}",
        config.warmup, config.iterations
    );
    println!(
        "{:<52} {:<10} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "case", "backend", "avg", "p50", "p95", "min", "max"
    );

    for case in selected_cases.iter() {
        if !case.differential_enabled {
            bail!("benchmark case {} must be differential-enabled", case.id);
        }
        let vars = case_vars(&case.id);
        execute_setup(&local_target, case, &vars)?;
        execute_setup(&external_target, case, &vars)?;
        let request = resolve_request(&case.request, &vars);

        let local_first = local_target.execute(&request);
        let external_first = external_target.execute(&request);
        ensure_success(case, local_target.label(), &local_first)?;
        ensure_success(case, external_target.label(), &external_first)?;

        let local = measure_latency(config.warmup, config.iterations, || {
            let response = local_target.execute(&request);
            if response.status >= 400 {
                Err(anyhow!(
                    "case {} failed on {} during benchmark: status={}, body={}",
                    case.id,
                    local_target.label(),
                    response.status,
                    response.body
                ))
            } else {
                Ok(())
            }
        })?;
        let external = measure_latency(config.warmup, config.iterations, || {
            let response = external_target.execute(&request);
            if response.status >= 400 {
                Err(anyhow!(
                    "case {} failed on {} during benchmark: status={}, body={}",
                    case.id,
                    external_target.label(),
                    response.status,
                    response.body
                ))
            } else {
                Ok(())
            }
        })?;

        print_summary(case, local_target.label(), &local);
        print_summary(case, external_target.label(), &external);
    }

    Ok(())
}

fn load_cases() -> Vec<CompatibilityCase> {
    serde_json::from_str::<CaseFile>(CASES_JSON).unwrap().cases
}

fn case_vars(case_id: &str) -> HashMap<String, String> {
    let index_name = unique_index_name(case_id);
    let mut vars = HashMap::new();
    vars.insert("index".to_string(), index_name.clone());
    vars.insert("other_index".to_string(), format!("{index_name}_other"));
    vars.insert("alias".to_string(), format!("{index_name}_alias"));
    vars.insert(
        "secondary_alias".to_string(),
        format!("{index_name}_alias_secondary"),
    );
    vars.insert("template".to_string(), format!("{index_name}_template"));
    vars.insert(
        "component_template".to_string(),
        format!("{index_name}_component_template"),
    );
    vars.insert("pipeline".to_string(), format!("{index_name}_pipeline"));
    vars.insert("policy".to_string(), format!("{index_name}_policy"));
    vars
}

fn unique_index_name(case_id: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("bench_{}_{}", case_id, timestamp)
}

fn execute_setup(
    target: &dyn Target,
    case: &CompatibilityCase,
    vars: &HashMap<String, String>,
) -> Result<()> {
    for step in case.setup_steps.iter().filter(|step| {
        step.targets
            .iter()
            .any(|value| value == target.label() || value == "all")
    }) {
        let resolved = resolve_request(&step.request, vars);
        let response = target.execute(&resolved);
        if response.status >= 400 {
            bail!(
                "setup for case {} failed on {}: {} {} status={} body={}",
                case.id,
                target.label(),
                resolved.method,
                resolved.path,
                response.status,
                response.body
            );
        }
    }
    Ok(())
}

fn resolve_request(request: &RequestSpec, vars: &HashMap<String, String>) -> ResolvedRequest {
    let path = render_template(&request.path, vars);
    let mut body = render_template(&request.body, vars);
    if path == "/_bulk" && !body.is_empty() && !body.ends_with('\n') {
        body.push('\n');
    }
    ResolvedRequest {
        method: request.method.clone(),
        path,
        body,
        mime: request.mime.clone(),
    }
}

fn render_template(raw: &str, vars: &HashMap<String, String>) -> String {
    let mut rendered = raw.to_string();
    for (key, value) in vars.iter() {
        rendered = rendered.replace(&format!("{{{{{}}}}}", key), value);
    }
    rendered
}

fn ensure_success(
    case: &CompatibilityCase,
    target_label: &str,
    response: &ResponseRecord,
) -> Result<()> {
    if response.status >= 400 {
        bail!(
            "case {} ({}) failed on {}: status={} body={}",
            case.id,
            case.description,
            target_label,
            response.status,
            response.body
        );
    }
    Ok(())
}

fn measure_latency<F>(warmup: usize, iterations: usize, mut runner: F) -> Result<LatencySummary>
where
    F: FnMut() -> Result<()>,
{
    for _ in 0..warmup {
        runner()?;
    }

    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        runner()?;
        samples.push(start.elapsed());
    }
    summarize_samples(samples)
}

fn summarize_samples(samples: Vec<Duration>) -> Result<LatencySummary> {
    if samples.is_empty() {
        bail!("no latency samples were collected");
    }
    let mut millis = samples
        .into_iter()
        .map(|sample| sample.as_secs_f64() * 1000.0)
        .collect::<Vec<_>>();
    millis.sort_by(|left, right| left.partial_cmp(right).unwrap());
    let sum = millis.iter().sum::<f64>();
    let len = millis.len();
    Ok(LatencySummary {
        avg_ms: sum / len as f64,
        min_ms: millis[0],
        p50_ms: millis[len / 2],
        p95_ms: millis[((len - 1) * 95) / 100],
        max_ms: *millis.last().unwrap(),
    })
}

fn print_summary(case: &CompatibilityCase, backend: &str, summary: &LatencySummary) {
    println!(
        "{:<52} {:<10} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2}",
        case.id,
        backend,
        summary.avg_ms,
        summary.p50_ms,
        summary.p95_ms,
        summary.min_ms,
        summary.max_ms
    );
}

fn mime_from_label(label: &str) -> mime::Mime {
    match label {
        "application/json" => mime::APPLICATION_JSON,
        "text/plain" => mime::TEXT_PLAIN,
        other => other.parse().unwrap(),
    }
}
