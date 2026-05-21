use std::collections::{BTreeSet, HashMap};
use std::env;
use std::net::{SocketAddr, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gotham::mime;
use gotham::test::TestServer;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::Value;

use powdrr_query_server::router::router;

const CASES_JSON: &str = include_str!("../../testdata/es_compat_cases.json");
const API_MANIFEST_JSON: &str = include_str!("../../testdata/es_api_coverage_manifest.json");
static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, Deserialize)]
struct CaseFile {
    cases: Vec<CompatibilityCase>,
}

#[derive(Debug, Deserialize)]
struct CompatibilityCase {
    id: String,
    #[serde(rename = "description")]
    _description: String,
    differential_enabled: bool,
    setup_steps: Vec<TargetedStep>,
    request: RequestSpec,
    assertions: Vec<Assertion>,
}

#[derive(Debug, Deserialize)]
struct ApiCoverageManifest {
    handlers: Vec<ApiCoverageEntry>,
}

#[derive(Debug, Deserialize)]
struct ApiCoverageEntry {
    handler: String,
    coverage: CoverageLevel,
    fixtures: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum CoverageLevel {
    Differential,
    LocalOnly,
    Unsupported,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Assertion {
    Status {
        value: u16,
    },
    BodyContains {
        value: String,
    },
    JsonPathBool {
        path: String,
        value: bool,
    },
    JsonPathBoolList {
        path: String,
        values: Vec<bool>,
    },
    JsonPathExists {
        path: String,
    },
    JsonPathFloat {
        path: String,
        value: f64,
        tolerance: Option<f64>,
    },
    JsonPathFloatList {
        path: String,
        values: Vec<f64>,
        tolerance: Option<f64>,
    },
    JsonPathMissing {
        path: String,
    },
    JsonPathNumberList {
        path: String,
        values: Vec<i64>,
    },
    JsonPathNumber {
        path: String,
        value: i64,
    },
    JsonPathNumberSet {
        path: String,
        values: Vec<i64>,
    },
    JsonPathStringList {
        path: String,
        values: Vec<String>,
    },
    JsonPathString {
        path: String,
        value: String,
    },
    JsonPathStringSet {
        path: String,
        values: Vec<String>,
    },
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
    fn new() -> Self {
        Self {
            server: TestServer::with_timeout(router(true), 1000).unwrap(),
        }
    }
}

impl Target for LocalTarget {
    fn label(&self) -> &'static str {
        "local"
    }

    fn execute(&self, request: &ResolvedRequest) -> ResponseRecord {
        let url = format!("http://localhost{}", request.path);
        let mime_value = mime_from_label(&request.mime);
        match request.method.as_str() {
            "HEAD" => {
                let response = self.server.client().head(&url).perform().unwrap();
                response_record_from_test_response(response)
            }
            "GET" => {
                let response = self.server.client().get(&url).perform().unwrap();
                response_record_from_test_response(response)
            }
            "DELETE" => {
                let response = self.server.client().delete(&url).perform().unwrap();
                response_record_from_test_response(response)
            }
            "POST" => {
                let response = self
                    .server
                    .client()
                    .post(&url, request.body.clone(), mime_value)
                    .perform()
                    .unwrap();
                response_record_from_test_response(response)
            }
            "PUT" => {
                let response = self
                    .server
                    .client()
                    .put(&url, request.body.clone(), mime_value)
                    .perform()
                    .unwrap();
                response_record_from_test_response(response)
            }
            other => panic!("unsupported local method: {}", other),
        }
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
            "HEAD" => self.client.head(&url),
            "GET" => self.client.get(&url),
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

#[derive(Clone)]
struct ResolvedRequest {
    method: String,
    path: String,
    body: String,
    mime: String,
}

#[test]
fn compatibility_matrix_local_current_engine() {
    let _guard = lock_test_environment();
    if env::var("POWDRR_ES_COMPAT_URL").is_ok() {
        eprintln!(
            "Skipping local ES compatibility run; unset POWDRR_ES_COMPAT_URL to run the local suite in this process."
        );
        return;
    }

    let Err(reason) = ensure_local_engine_dependencies() else {
        let target = LocalTarget::new();
        let cases = load_cases();
        let mut failures = Vec::new();
        for case in cases.iter() {
            eprintln!("local case: {}", case.id);
            if let Err(message) = run_case_with_capture(&case.id, || {
                let vars = case_vars(case);
                let response = execute_case(&target, case, &vars);
                validate_case(&response, &vars, case, target.label());
            }) {
                failures.push(message);
            }
        }

        assert_failures_empty("local", &failures);
        return;
    };

    eprintln!("Skipping local ES compatibility run; {}", reason);
}

#[test]
fn compatibility_matrix_differential_when_external_es_is_configured() {
    let _guard = lock_test_environment();
    let Some(base_url) = env::var("POWDRR_ES_COMPAT_URL").ok() else {
        eprintln!("Skipping external ES compatibility run; set POWDRR_ES_COMPAT_URL to enable it.");
        return;
    };

    let Err(reason) = ensure_local_engine_dependencies() else {
        let local_target = LocalTarget::new();
        let external_target = ExternalTarget::new(base_url);
        let cases = load_cases();
        let mut failures = Vec::new();
        for case in cases.iter().filter(|case| case.differential_enabled) {
            eprintln!("differential case: {}", case.id);
            if let Err(message) = run_case_with_capture(&case.id, || {
                let vars = case_vars(case);
                let local_response = execute_case(&local_target, case, &vars);
                let external_response = execute_case(&external_target, case, &vars);

                validate_case(&local_response, &vars, case, local_target.label());
                validate_case(&external_response, &vars, case, external_target.label());
                compare_case_results(case, &vars, &local_response, &external_response);
            }) {
                failures.push(message);
            }
        }

        assert_failures_empty("differential", &failures);
        return;
    };

    eprintln!("Skipping differential ES compatibility run; {}", reason);
}

#[test]
fn compatibility_matrix_case_file_parses_and_ids_are_unique() {
    let _guard = lock_test_environment();
    let cases = load_cases();
    assert!(
        !cases.is_empty(),
        "expected at least one compatibility case"
    );

    let mut ids = BTreeSet::new();
    for case in cases {
        assert!(
            ids.insert(case.id.clone()),
            "duplicate compatibility case id '{}'",
            case.id
        );
    }
}

#[test]
fn es_api_coverage_manifest_covers_all_router_handlers() {
    let _guard = lock_test_environment();
    let cases = load_cases();
    let case_ids = cases
        .iter()
        .map(|case| (case.id.as_str(), case))
        .collect::<HashMap<_, _>>();
    let manifest = load_api_manifest();

    let manifest_handlers = manifest
        .handlers
        .iter()
        .map(|entry| entry.handler.clone())
        .collect::<BTreeSet<_>>();
    let router_handlers = router_handler_inventory();

    assert_eq!(
        manifest_handlers, router_handlers,
        "ES API manifest must classify every routed ES handler"
    );

    for entry in manifest.handlers {
        assert!(
            !entry.fixtures.is_empty(),
            "handler '{}' must reference at least one compatibility fixture",
            entry.handler
        );

        for fixture_id in entry.fixtures {
            let case = case_ids.get(fixture_id.as_str()).unwrap_or_else(|| {
                panic!(
                    "handler '{}' references unknown compatibility fixture '{}'",
                    entry.handler, fixture_id
                )
            });
            match entry.coverage {
                CoverageLevel::Differential => assert!(
                    case.differential_enabled,
                    "handler '{}' references non-differential fixture '{}'",
                    entry.handler, fixture_id
                ),
                CoverageLevel::LocalOnly | CoverageLevel::Unsupported => assert!(
                    !case.differential_enabled,
                    "handler '{}' references differential fixture '{}' but is classified {:?}",
                    entry.handler, fixture_id, entry.coverage
                ),
            }
        }
    }
}

fn ensure_local_engine_dependencies() -> Result<(), String> {
    require_local_service("LocalStack/DynamoDB", "127.0.0.1:4566")?;
    require_local_service("Redis", "127.0.0.1:6379")?;
    require_local_service("MinIO", "127.0.0.1:9000")?;
    require_local_service("Iceberg REST catalog", "127.0.0.1:8181")?;
    Ok(())
}

fn lock_test_environment() -> MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn run_case_with_capture<F>(case_id: &str, f: F) -> Result<(), String>
where
    F: FnOnce(),
{
    catch_unwind(AssertUnwindSafe(f))
        .map_err(|payload| format!("case '{}': {}", case_id, panic_payload_to_string(payload)))
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }

    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }

    "panic with non-string payload".to_string()
}

fn assert_failures_empty(label: &str, failures: &[String]) {
    assert!(
        failures.is_empty(),
        "{} compatibility failures ({}):\n\n{}",
        label,
        failures.len(),
        failures.join("\n\n")
    );
}

fn require_local_service(name: &str, address: &str) -> Result<(), String> {
    let socket_address: SocketAddr = address.parse().unwrap();
    TcpStream::connect_timeout(&socket_address, Duration::from_millis(200))
        .map(|_| ())
        .map_err(|err| format!("requires {} at {} ({})", name, address, err))
}

fn case_vars(case: &CompatibilityCase) -> HashMap<String, String> {
    let index_name = unique_index_name(&case.id);
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

fn execute_case(
    target: &dyn Target,
    case: &CompatibilityCase,
    vars: &HashMap<String, String>,
) -> ResponseRecord {
    for setup_step in case.setup_steps.iter().filter(|step| {
        step.targets
            .iter()
            .any(|v| v == target.label() || v == "all")
    }) {
        let resolved = resolve_request(&setup_step.request, vars);
        eprintln!(
            "  {} setup: {} {}",
            target.label(),
            resolved.method,
            resolved.path
        );
        let response = target.execute(&resolved);
        assert_setup_step_succeeded(&response, case, target.label(), &resolved);
    }

    let resolved = resolve_request(&case.request, vars);
    eprintln!(
        "  {} request: {} {}",
        target.label(),
        resolved.method,
        resolved.path
    );
    target.execute(&resolved)
}

fn validate_case(
    response: &ResponseRecord,
    vars: &HashMap<String, String>,
    case: &CompatibilityCase,
    target_label: &str,
) {
    for assertion in case.assertions.iter() {
        evaluate_assertion(assertion, response, vars, case, target_label);
    }
}

fn compare_case_results(
    case: &CompatibilityCase,
    vars: &HashMap<String, String>,
    local_response: &ResponseRecord,
    external_response: &ResponseRecord,
) {
    for assertion in case.assertions.iter() {
        compare_assertion_projection(assertion, vars, case, local_response, external_response);
    }
}

fn evaluate_assertion(
    assertion: &Assertion,
    response: &ResponseRecord,
    vars: &HashMap<String, String>,
    case: &CompatibilityCase,
    target_label: &str,
) {
    match assertion {
        Assertion::Status { value } => {
            assert_eq!(
                response.status, *value,
                "case '{}' failed on target '{}': expected status {}, body={}",
                case.id, target_label, value, response.body
            );
        }
        Assertion::BodyContains { value } => {
            let expected = render_template(value, vars);
            assert!(
                response.body.contains(&expected),
                "case '{}' failed on target '{}': expected body to contain '{}', body={}",
                case.id,
                target_label,
                expected,
                response.body
            );
        }
        Assertion::JsonPathBool { path, value } => {
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual = extract_values(&json, &rendered_path)
                .into_iter()
                .next()
                .and_then(|v| v.as_bool())
                .unwrap_or_else(|| {
                    panic!(
                        "missing bool path '{}' in case '{}'",
                        rendered_path, case.id
                    )
                });
            assert_eq!(
                actual, *value,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, rendered_path
            );
        }
        Assertion::JsonPathBoolList { path, values } => {
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual = bool_list(&json, &rendered_path, case);
            assert_eq!(
                actual, *values,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, rendered_path
            );
        }
        Assertion::JsonPathExists { path } => {
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            assert!(
                path_exists(&json, &rendered_path),
                "case '{}' failed on target '{}': missing path '{}'",
                case.id,
                target_label,
                rendered_path
            );
        }
        Assertion::JsonPathFloat {
            path,
            value,
            tolerance,
        } => {
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual = extract_values(&json, &rendered_path)
                .into_iter()
                .next()
                .and_then(|v| v.as_f64())
                .unwrap_or_else(|| {
                    panic!(
                        "missing float path '{}' in case '{}'",
                        rendered_path, case.id
                    )
                });
            let allowed_delta = tolerance.unwrap_or(1e-9);
            assert!(
                (actual - *value).abs() <= allowed_delta,
                "case '{}' failed on target '{}' for path '{}': expected {}, got {}, tolerance {}",
                case.id,
                target_label,
                rendered_path,
                value,
                actual,
                allowed_delta
            );
        }
        Assertion::JsonPathFloatList {
            path,
            values,
            tolerance,
        } => {
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual = float_list(&json, &rendered_path, case);
            let allowed_delta = tolerance.unwrap_or(1e-9);
            assert_eq!(
                actual.len(),
                values.len(),
                "case '{}' failed on target '{}' for path '{}': expected {} float values, got {}",
                case.id,
                target_label,
                rendered_path,
                values.len(),
                actual.len()
            );
            for (index, (actual, expected)) in actual.iter().zip(values.iter()).enumerate() {
                assert!(
                    (*actual - *expected).abs() <= allowed_delta,
                    "case '{}' failed on target '{}' for path '{}' at index {}: expected {}, got {}, tolerance {}",
                    case.id,
                    target_label,
                    rendered_path,
                    index,
                    expected,
                    actual,
                    allowed_delta
                );
            }
        }
        Assertion::JsonPathMissing { path } => {
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            assert!(
                !path_exists(&json, &rendered_path),
                "case '{}' failed on target '{}': expected path '{}' to be absent",
                case.id,
                target_label,
                rendered_path
            );
        }
        Assertion::JsonPathNumber { path, value } => {
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual = extract_values(&json, &rendered_path)
                .into_iter()
                .next()
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| {
                    panic!(
                        "missing number path '{}' in case '{}'",
                        rendered_path, case.id
                    )
                });
            assert_eq!(
                actual, *value,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, rendered_path
            );
        }
        Assertion::JsonPathNumberList { path, values } => {
            let expected = values.clone();
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual = number_list(&json, &rendered_path, case);
            assert_eq!(
                actual, expected,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, rendered_path
            );
        }
        Assertion::JsonPathNumberSet { path, values } => {
            let expected: BTreeSet<i64> = values.iter().copied().collect();
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual: BTreeSet<i64> = extract_values(&json, &rendered_path)
                .into_iter()
                .map(|v| {
                    v.as_i64().unwrap_or_else(|| {
                        panic!(
                            "non-integer value for path '{}' in case '{}'",
                            rendered_path, case.id
                        )
                    })
                })
                .collect();
            assert_eq!(
                actual, expected,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, rendered_path
            );
        }
        Assertion::JsonPathString { path, value } => {
            let expected = render_template(value, vars);
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual = extract_values(&json, &rendered_path)
                .into_iter()
                .next()
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| {
                    panic!(
                        "missing string path '{}' in case '{}'",
                        rendered_path, case.id
                    )
                })
                .to_string();
            assert_eq!(
                actual, expected,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, rendered_path
            );
        }
        Assertion::JsonPathStringList { path, values } => {
            let expected: Vec<String> = values
                .iter()
                .map(|value| render_template(value, vars))
                .collect();
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual = string_list(&json, &rendered_path, case);
            assert_eq!(
                actual, expected,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, rendered_path
            );
        }
        Assertion::JsonPathStringSet { path, values } => {
            let expected: BTreeSet<String> = values
                .iter()
                .map(|value| render_template(value, vars))
                .collect();
            let rendered_path = render_template(path, vars);
            let json = parse_json_body(response, case, target_label);
            let actual: BTreeSet<String> = extract_values(&json, &rendered_path)
                .into_iter()
                .map(|v| {
                    v.as_str()
                        .unwrap_or_else(|| {
                            panic!(
                                "non-string value for path '{}' in case '{}'",
                                rendered_path, case.id
                            )
                        })
                        .to_string()
                })
                .collect();
            assert_eq!(
                actual, expected,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, rendered_path
            );
        }
    }
}

fn compare_assertion_projection(
    assertion: &Assertion,
    vars: &HashMap<String, String>,
    case: &CompatibilityCase,
    local_response: &ResponseRecord,
    external_response: &ResponseRecord,
) {
    match assertion {
        Assertion::Status { .. } => {
            assert_eq!(
                local_response.status,
                external_response.status,
                "case '{}' produced different statuses between local and external: local={}, external={}, local_body={}, external_body={}",
                case.id,
                local_response.status,
                external_response.status,
                local_response.body,
                external_response.body
            );
        }
        Assertion::BodyContains { .. } => {}
        Assertion::JsonPathBool { path, .. } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = first_bool(&local_json, &rendered_path, case);
            let external = first_bool(&external_json, &rendered_path, case);
            assert_eq!(
                local, external,
                "case '{}' produced different bool values for path '{}': local={}, external={}",
                case.id, rendered_path, local, external
            );
        }
        Assertion::JsonPathBoolList { path, .. } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = bool_list(&local_json, &rendered_path, case);
            let external = bool_list(&external_json, &rendered_path, case);
            assert_eq!(
                local, external,
                "case '{}' produced different bool lists for path '{}': local={:?}, external={:?}",
                case.id, rendered_path, local, external
            );
        }
        Assertion::JsonPathExists { path } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = path_exists(&local_json, &rendered_path);
            let external = path_exists(&external_json, &rendered_path);
            assert_eq!(
                local, external,
                "case '{}' produced different path existence for '{}': local={}, external={}",
                case.id, rendered_path, local, external
            );
        }
        Assertion::JsonPathFloat {
            path, tolerance, ..
        } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = first_float(&local_json, &rendered_path, case);
            let external = first_float(&external_json, &rendered_path, case);
            let allowed_delta = tolerance.unwrap_or(1e-9);
            assert!(
                (local - external).abs() <= allowed_delta,
                "case '{}' produced different float values for path '{}': local={}, external={}, tolerance={}",
                case.id,
                rendered_path,
                local,
                external,
                allowed_delta
            );
        }
        Assertion::JsonPathFloatList {
            path, tolerance, ..
        } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = float_list(&local_json, &rendered_path, case);
            let external = float_list(&external_json, &rendered_path, case);
            let allowed_delta = tolerance.unwrap_or(1e-9);
            assert_eq!(
                local.len(),
                external.len(),
                "case '{}' produced different float list lengths for path '{}': local={:?}, external={:?}",
                case.id,
                rendered_path,
                local,
                external
            );
            for (index, (local_value, external_value)) in
                local.iter().zip(external.iter()).enumerate()
            {
                assert!(
                    (*local_value - *external_value).abs() <= allowed_delta,
                    "case '{}' produced different float values for path '{}' at index {}: local={}, external={}, tolerance={}",
                    case.id,
                    rendered_path,
                    index,
                    local_value,
                    external_value,
                    allowed_delta
                );
            }
        }
        Assertion::JsonPathMissing { path } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = path_exists(&local_json, &rendered_path);
            let external = path_exists(&external_json, &rendered_path);
            assert_eq!(
                local, external,
                "case '{}' produced different path presence for '{}': local={}, external={}",
                case.id, rendered_path, local, external
            );
        }
        Assertion::JsonPathNumber { path, .. } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = first_number(&local_json, &rendered_path, case);
            let external = first_number(&external_json, &rendered_path, case);
            assert_eq!(
                local, external,
                "case '{}' produced different numeric values for path '{}': local={}, external={}",
                case.id, rendered_path, local, external
            );
        }
        Assertion::JsonPathNumberList { path, .. } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = number_list(&local_json, &rendered_path, case);
            let external = number_list(&external_json, &rendered_path, case);
            assert_eq!(
                local, external,
                "case '{}' produced different numeric lists for path '{}': local={:?}, external={:?}",
                case.id, rendered_path, local, external
            );
        }
        Assertion::JsonPathNumberSet { path, .. } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = number_set(&local_json, &rendered_path, case);
            let external = number_set(&external_json, &rendered_path, case);
            assert_eq!(
                local, external,
                "case '{}' produced different numeric sets for path '{}': local={:?}, external={:?}",
                case.id, rendered_path, local, external
            );
        }
        Assertion::JsonPathString { path, .. } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = first_string(&local_json, &rendered_path, case);
            let external = first_string(&external_json, &rendered_path, case);
            assert_eq!(
                local, external,
                "case '{}' produced different string values for path '{}': local={}, external={}",
                case.id, rendered_path, local, external
            );
        }
        Assertion::JsonPathStringList { path, .. } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = string_list(&local_json, &rendered_path, case);
            let external = string_list(&external_json, &rendered_path, case);
            assert_eq!(
                local, external,
                "case '{}' produced different string lists for path '{}': local={:?}, external={:?}",
                case.id, rendered_path, local, external
            );
        }
        Assertion::JsonPathStringSet { path, .. } => {
            let rendered_path = render_template(path, vars);
            let local_json = parse_json_body(local_response, case, "local");
            let external_json = parse_json_body(external_response, case, "external");
            let local = string_set(&local_json, &rendered_path, vars, case);
            let external = string_set(&external_json, &rendered_path, vars, case);
            assert_eq!(
                local, external,
                "case '{}' produced different string sets for path '{}': local={:?}, external={:?}",
                case.id, rendered_path, local, external
            );
        }
    }
}

fn assert_setup_step_succeeded(
    response: &ResponseRecord,
    case: &CompatibilityCase,
    target_label: &str,
    request: &ResolvedRequest,
) {
    assert!(
        response.status < 400,
        "setup for case '{}' failed on target '{}': {} {} returned status {}, body={}",
        case.id,
        target_label,
        request.method,
        request.path,
        response.status,
        response.body
    );
}

fn parse_json_body<'a>(
    response: &'a ResponseRecord,
    case: &CompatibilityCase,
    target_label: &str,
) -> Value {
    serde_json::from_str::<Value>(&response.body).unwrap_or_else(|err| {
        panic!(
            "case '{}' failed on target '{}': expected JSON body, err={}, body={}",
            case.id, target_label, err, response.body
        )
    })
}

fn first_bool(value: &Value, path: &str, case: &CompatibilityCase) -> bool {
    extract_values(value, path)
        .into_iter()
        .next()
        .and_then(|v| v.as_bool())
        .unwrap_or_else(|| panic!("missing bool path '{}' in case '{}'", path, case.id))
}

fn first_float(value: &Value, path: &str, case: &CompatibilityCase) -> f64 {
    extract_values(value, path)
        .into_iter()
        .next()
        .and_then(|v| v.as_f64())
        .unwrap_or_else(|| panic!("missing float path '{}' in case '{}'", path, case.id))
}

fn bool_list(value: &Value, path: &str, case: &CompatibilityCase) -> Vec<bool> {
    extract_values(value, path)
        .into_iter()
        .map(|v| {
            v.as_bool().unwrap_or_else(|| {
                panic!("non-bool value for path '{}' in case '{}'", path, case.id)
            })
        })
        .collect()
}

fn float_list(value: &Value, path: &str, case: &CompatibilityCase) -> Vec<f64> {
    extract_values(value, path)
        .into_iter()
        .map(|v| {
            v.as_f64().unwrap_or_else(|| {
                panic!("non-float value for path '{}' in case '{}'", path, case.id)
            })
        })
        .collect()
}

fn first_number(value: &Value, path: &str, case: &CompatibilityCase) -> i64 {
    extract_values(value, path)
        .into_iter()
        .next()
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| panic!("missing number path '{}' in case '{}'", path, case.id))
}

fn first_string(value: &Value, path: &str, case: &CompatibilityCase) -> String {
    extract_values(value, path)
        .into_iter()
        .next()
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("missing string path '{}' in case '{}'", path, case.id))
        .to_string()
}

fn number_set(value: &Value, path: &str, case: &CompatibilityCase) -> BTreeSet<i64> {
    extract_values(value, path)
        .into_iter()
        .map(|v| {
            v.as_i64().unwrap_or_else(|| {
                panic!(
                    "non-integer value for path '{}' in case '{}'",
                    path, case.id
                )
            })
        })
        .collect()
}

fn number_list(value: &Value, path: &str, case: &CompatibilityCase) -> Vec<i64> {
    extract_values(value, path)
        .into_iter()
        .map(|v| {
            v.as_i64().unwrap_or_else(|| {
                panic!(
                    "non-integer value for path '{}' in case '{}'",
                    path, case.id
                )
            })
        })
        .collect()
}

fn string_set(
    value: &Value,
    path: &str,
    _vars: &HashMap<String, String>,
    case: &CompatibilityCase,
) -> BTreeSet<String> {
    extract_values(value, path)
        .into_iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| {
                    panic!("non-string value for path '{}' in case '{}'", path, case.id)
                })
                .to_string()
        })
        .collect()
}

fn string_list(value: &Value, path: &str, case: &CompatibilityCase) -> Vec<String> {
    extract_values(value, path)
        .into_iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| {
                    panic!("non-string value for path '{}' in case '{}'", path, case.id)
                })
                .to_string()
        })
        .collect()
}

fn path_exists(value: &Value, path: &str) -> bool {
    !extract_values(value, path).is_empty()
}

fn extract_values<'a>(value: &'a Value, path: &str) -> Vec<&'a Value> {
    let mut current = vec![value];
    for token in path.split('.') {
        if let Some(field) = token.strip_suffix("[*]") {
            current = current
                .into_iter()
                .flat_map(|v| match v.get(field) {
                    Some(Value::Array(values)) => values.iter().collect::<Vec<&Value>>(),
                    _ => Vec::new(),
                })
                .collect();
        } else {
            current = current.into_iter().filter_map(|v| v.get(token)).collect();
        }
    }
    current
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

fn unique_index_name(case_id: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("compat_{}_{}", case_id, timestamp)
}

fn load_cases() -> Vec<CompatibilityCase> {
    serde_json::from_str::<CaseFile>(CASES_JSON).unwrap().cases
}

fn load_api_manifest() -> ApiCoverageManifest {
    serde_json::from_str::<ApiCoverageManifest>(API_MANIFEST_JSON).unwrap()
}

fn router_handler_inventory() -> BTreeSet<String> {
    let source = include_str!("../../query_server/src/router.rs");
    let mut handlers = BTreeSet::new();
    handlers.extend(extract_router_handlers(
        source,
        "elastic_search_endpoints::es_",
    ));
    handlers.extend(extract_router_handlers(
        source,
        "elastic_search_lifetime_policy::es_",
    ));
    handlers
}

fn extract_router_handlers(source: &str, prefix: &str) -> BTreeSet<String> {
    let mut handlers = BTreeSet::new();
    let mut remainder = source;

    while let Some(start) = remainder.find(prefix) {
        let candidate = &remainder[start..];
        let end = candidate
            .find(|character: char| {
                !(character.is_ascii_alphanumeric() || character == ':' || character == '_')
            })
            .unwrap_or(candidate.len());
        handlers.insert(candidate[..end].to_string());
        remainder = &candidate[end..];
    }

    handlers
}

fn mime_from_label(label: &str) -> mime::Mime {
    match label {
        "application/json" => mime::APPLICATION_JSON,
        "text/plain" => mime::TEXT_PLAIN,
        other => other.parse().unwrap(),
    }
}

fn response_record_from_test_response(response: gotham::test::TestResponse) -> ResponseRecord {
    let status = response.status().as_u16();
    let body = read_test_body(response);
    ResponseRecord { status, body }
}

fn read_test_body(response: gotham::test::TestResponse) -> String {
    let body = response.read_body().unwrap_or_default();
    String::from_utf8(body).unwrap_or_default()
}
