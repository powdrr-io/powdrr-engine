use std::collections::{BTreeSet, HashMap};
use std::env;
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gotham::mime;
use gotham::test::TestServer;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::Value;

use powdrr_lib::router::router;

const CASES_JSON: &str = include_str!("data/es_compat_cases.json");

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
    Status { value: u16 },
    JsonPathBool { path: String, value: bool },
    JsonPathNumber { path: String, value: i64 },
    JsonPathString { path: String, value: String },
    JsonPathStringSet { path: String, values: Vec<String> },
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
    let Err(reason) = ensure_local_engine_dependencies() else {
        let target = LocalTarget::new();
        let cases = load_cases();
        for case in cases.iter() {
            run_case(&target, case);
        }
        return;
    };

    eprintln!("Skipping local ES compatibility run; {}", reason);
}

#[test]
fn compatibility_matrix_differential_when_external_es_is_configured() {
    let Some(base_url) = env::var("POWDRR_ES_COMPAT_URL").ok() else {
        eprintln!("Skipping external ES compatibility run; set POWDRR_ES_COMPAT_URL to enable it.");
        return;
    };

    let Err(reason) = ensure_local_engine_dependencies() else {
        let local_target = LocalTarget::new();
        let external_target = ExternalTarget::new(base_url);
        let cases = load_cases();
        for case in cases.iter().filter(|case| case.differential_enabled) {
            run_case(&local_target, case);
            run_case(&external_target, case);
        }
        return;
    };

    eprintln!("Skipping differential ES compatibility run; {}", reason);
}

fn ensure_local_engine_dependencies() -> Result<(), String> {
    require_local_service("LocalStack/DynamoDB", "127.0.0.1:4566")?;
    require_local_service("Redis", "127.0.0.1:6379")?;
    Ok(())
}

fn require_local_service(name: &str, address: &str) -> Result<(), String> {
    let socket_address: SocketAddr = address.parse().unwrap();
    TcpStream::connect_timeout(&socket_address, Duration::from_millis(200))
        .map(|_| ())
        .map_err(|err| format!("requires {} at {} ({})", name, address, err))
}

fn run_case(target: &dyn Target, case: &CompatibilityCase) {
    let index_name = unique_index_name(&case.id);
    let mut vars = HashMap::new();
    vars.insert("index".to_string(), index_name.clone());

    for setup_step in case.setup_steps.iter().filter(|step| step.targets.iter().any(|v| v == target.label() || v == "all")) {
        let resolved = resolve_request(&setup_step.request, &vars);
        let _ = target.execute(&resolved);
    }

    let response = target.execute(&resolve_request(&case.request, &vars));
    for assertion in case.assertions.iter() {
        evaluate_assertion(assertion, &response, &vars, case, target.label());
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
        Assertion::JsonPathBool { path, value } => {
            let json = parse_json_body(response, case, target_label);
            let actual = extract_values(&json, path)
                .into_iter()
                .next()
                .and_then(|v| v.as_bool())
                .unwrap_or_else(|| panic!("missing bool path '{}' in case '{}'", path, case.id));
            assert_eq!(
                actual, *value,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, path
            );
        }
        Assertion::JsonPathNumber { path, value } => {
            let json = parse_json_body(response, case, target_label);
            let actual = extract_values(&json, path)
                .into_iter()
                .next()
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| panic!("missing number path '{}' in case '{}'", path, case.id));
            assert_eq!(
                actual, *value,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, path
            );
        }
        Assertion::JsonPathString { path, value } => {
            let expected = render_template(value, vars);
            let json = parse_json_body(response, case, target_label);
            let actual = extract_values(&json, path)
                .into_iter()
                .next()
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("missing string path '{}' in case '{}'", path, case.id))
                .to_string();
            assert_eq!(
                actual, expected,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, path
            );
        }
        Assertion::JsonPathStringSet { path, values } => {
            let expected: BTreeSet<String> = values
                .iter()
                .map(|value| render_template(value, vars))
                .collect();
            let json = parse_json_body(response, case, target_label);
            let actual: BTreeSet<String> = extract_values(&json, path)
                .into_iter()
                .map(|v| {
                    v.as_str()
                        .unwrap_or_else(|| panic!("non-string value for path '{}' in case '{}'", path, case.id))
                        .to_string()
                })
                .collect();
            assert_eq!(
                actual, expected,
                "case '{}' failed on target '{}' for path '{}'",
                case.id, target_label, path
            );
        }
    }
}

fn parse_json_body<'a>(response: &'a ResponseRecord, case: &CompatibilityCase, target_label: &str) -> Value {
    serde_json::from_str::<Value>(&response.body).unwrap_or_else(|err| {
        panic!(
            "case '{}' failed on target '{}': expected JSON body, err={}, body={}",
            case.id, target_label, err, response.body
        )
    })
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
            current = current
                .into_iter()
                .filter_map(|v| v.get(token))
                .collect();
        }
    }
    current
}

fn resolve_request(request: &RequestSpec, vars: &HashMap<String, String>) -> ResolvedRequest {
    ResolvedRequest {
        method: request.method.clone(),
        path: render_template(&request.path, vars),
        body: render_template(&request.body, vars),
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
