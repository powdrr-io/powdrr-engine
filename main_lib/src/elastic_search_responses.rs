use std::cmp::Ordering;
use std::collections::HashMap;

use gotham::{hyper::StatusCode, mime};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::elastic_search_common::ElasticSearchResponse;

#[derive(Serialize, Clone)]
pub struct Shards {
    pub total: u32,
    pub successful: u32,
    pub failed: u32,
}

#[derive(Serialize, Clone)]
pub struct OperationResult {
    pub _index: String,
    pub _id: String,
    pub _version: u64,
    pub result: String,
    pub _shards: Shards,
    pub _seq_no: u64,
    pub _primary_term: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get: Option<QueryResultHit>,
}

#[derive(Serialize)]
pub struct BulkResult {
    pub errors: bool,
    pub took: u32,
    pub items: Vec<HashMap<String, OperationResult>>,
}

impl BulkResult {
    pub fn success(took: u32, created: Vec<OperationResult>) -> Self {
        BulkResult {
            errors: false,
            took: took,
            items: created
                .iter()
                .map(|x| HashMap::from([("created".to_string(), x.clone())]))
                .collect(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct QueryResultShards {
    pub total: u32,
    pub successful: u32,
    pub skipped: u32,
    pub failed: u32,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct QueryResultTotalComplex {
    pub value: u64,
    pub relation: String,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum QueryResultTotal {
    Simple(u64),
    Complex(QueryResultTotalComplex),
}

#[derive(Deserialize, Serialize, Clone)]
pub struct QueryResultHit {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _index: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _id: Option<String>,
    pub _version: u64,
    pub _seq_no: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _primary_term: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub found: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<Value>>,
    pub _source: Value,
}

impl QueryResultHit {
    pub fn from_record(index: &Option<String>, value: &Value, found: Option<bool>) -> Self {
        Self::from_record_with_sort(index, value, found, None)
    }

    pub fn from_record_with_sort(
        index: &Option<String>,
        value: &Value,
        found: Option<bool>,
        sort: Option<Vec<Value>>,
    ) -> Self {
        let value_map = value.as_object().unwrap().clone();
        let score = value_map
            .get("score")
            .and_then(|f| f.as_f64())
            .or_else(|| bm25_fallback_score(&value_map));
        let id = value_map.get("_id").unwrap().as_str().unwrap().to_string();
        let version = value_map.get("_version").unwrap().as_u64().unwrap();
        let seq_no = value_map.get("_seq_no").unwrap().as_u64().unwrap();
        let source = value_map.get("_source").unwrap().as_str().unwrap();
        // TODO: we are parsing the string into a value just to put it an object
        // that will get serialized out again. That is lame. If we can get the serializer
        // to look at a string but put it in like it is a Value, that would be better.
        let source_value = serde_json::from_str(source).unwrap();
        QueryResultHit {
            _index: index.clone(),
            _id: Some(id),
            _version: version,
            _seq_no: seq_no,
            _score: score,
            _primary_term: Some(1),
            found,
            sort,
            _source: source_value,
        }
    }
}

fn bm25_fallback_score(value_map: &serde_json::Map<String, Value>) -> Option<f64> {
    let term_cnt = value_map.get("term_cnt")?.as_f64()?;
    let word_cnt = value_map.get("word_cnt")?.as_f64()?;
    let constant_k = 1.2;
    let constant_b = 0.75;
    let avgdl = 5.6;
    Some(
        (term_cnt * (constant_k + 1.0))
            / (term_cnt + constant_k * (1.0 - constant_b + (constant_b * word_cnt / avgdl))),
    )
}

pub(crate) fn compare_query_result_hits_desc(
    left: &QueryResultHit,
    right: &QueryResultHit,
) -> Ordering {
    match (left._score, right._score) {
        (Some(left_score), Some(right_score)) => right_score
            .partial_cmp(&left_score)
            .unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
    .then_with(|| right._seq_no.cmp(&left._seq_no))
    .then_with(|| left._id.cmp(&right._id))
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct QueryResultHits {
    pub total: QueryResultTotal,
    pub max_score: Option<f64>,
    pub hits: Vec<QueryResultHit>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct TermAggregationBucket {
    pub key: String,
    pub doc_count: u64,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct TermAggregationResult {
    pub doc_count_error_upper_bound: u64,
    pub sum_other_doc_count: u64,
    pub buckets: Vec<TermAggregationBucket>,
    #[serde(flatten)]
    pub aggs: HashMap<String, AggregationResult>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct AverageAggregationResult {
    pub value: f64,
    #[serde(flatten)]
    pub aggs: HashMap<String, AggregationResult>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct FilterAggregationResult {
    pub doc_count: u64,
    #[serde(flatten)]
    pub aggs: HashMap<String, AggregationResult>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct CardinalityAggregationResult {
    pub value: u64,
    #[serde(flatten)]
    pub aggs: HashMap<String, AggregationResult>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct RangeAggregationBucket {
    pub key: String,
    pub from: u64,
    pub from_as_string: String,
    pub to: u64,
    pub to_as_string: String,
    pub doc_count: u64,
    #[serde(flatten)]
    pub aggs: HashMap<String, AggregationResult>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct HistogramAggregationBucket {
    // TODO: this may not be correct
    pub key: String,
    pub from: u64,
    pub from_as_string: String,
    pub to: u64,
    pub to_as_string: String,
    pub doc_count: u64,
    #[serde(flatten)]
    pub aggs: HashMap<String, AggregationResult>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct HistogramAggregationResult {
    pub(crate) buckets: Vec<HistogramAggregationBucket>,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct RangeAggregationResult {
    pub buckets: Vec<RangeAggregationBucket>,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub(crate) enum AggregationResult {
    Average(AverageAggregationResult),
    Cardinality(CardinalityAggregationResult),
    Filter(FilterAggregationResult),
    Histogram(HistogramAggregationResult),
    Range(RangeAggregationResult),
    Terms(TermAggregationResult),
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct QueryResults {
    pub took: u32,
    pub timed_out: bool,
    pub _shards: QueryResultShards,
    pub hits: QueryResultHits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<HashMap<String, AggregationResult>>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct UpdateByQueryResultsRetries {
    pub bulk: i64,
    pub search: i64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct UpdateByQueryResults {
    pub took: u64,
    pub timed_out: bool,
    pub total: u64,
    pub updated: u64,
    pub deleted: u64,
    pub batches: u64,
    pub version_conflicts: u64,
    pub noops: u64,
    pub retries: UpdateByQueryResultsRetries,
    pub throttled_millis: u64,
    pub requests_per_second: i64,
    pub throttled_until_millis: u64,
    pub failures: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug: Option<Vec<Value>>,
}

#[derive(Serialize, Clone)]
pub(crate) struct QueryResultsNotFound {
    pub _index: String,
    pub _id: String,
    pub found: bool,
}

impl QueryResultsNotFound {
    pub(crate) fn to_response(&self) -> ElasticSearchResponse {
        ElasticSearchResponse {
            status: StatusCode::NOT_FOUND,
            mime: mime::APPLICATION_JSON,
            body: serde_json::to_string(self).unwrap(),
            headers: vec![],
        }
    }
}

impl QueryResultTotalComplex {
    fn new(num: usize) -> Self {
        QueryResultTotalComplex {
            value: num as u64,
            relation: "eq".to_string(),
        }
    }
}

pub(crate) fn transient_error(message: &String) -> ElasticSearchResponse {
    // TODO: probably pass in the error here, put traceback in debug logs
    // TODO: come up with some kind of code to allow correlating back to the logs
    tracing::error!("Transient error: {}", message);
    ElasticSearchResponse {
        status: StatusCode::SERVICE_UNAVAILABLE,
        mime: mime::TEXT_PLAIN,
        body: "An error occurred".to_string(),
        headers: vec![],
    }
}

impl QueryResults {
    pub fn empty(
        took: u32,
        num_shards: u32,
        aggregations: Option<HashMap<String, AggregationResult>>,
        total_hits_complex: bool,
    ) -> Self {
        let total_hits = match total_hits_complex {
            true => QueryResultTotal::Complex(QueryResultTotalComplex::new(0)),
            false => QueryResultTotal::Simple(0),
        };
        QueryResults {
            took: took,
            timed_out: false,
            _shards: QueryResultShards {
                total: num_shards,
                successful: num_shards,
                skipped: 0,
                failed: 0,
            },
            hits: QueryResultHits {
                total: total_hits,
                max_score: None,
                hits: vec![],
            },
            aggregations: aggregations,
        }
    }

    #[allow(dead_code)]
    pub fn timed_out(took: u32, num_shards: u32, total_hits_complex: bool) -> Self {
        let total_hits = match total_hits_complex {
            true => QueryResultTotal::Complex(QueryResultTotalComplex::new(0)),
            false => QueryResultTotal::Simple(0),
        };
        QueryResults {
            took: took,
            timed_out: true,
            _shards: QueryResultShards {
                total: num_shards,
                successful: num_shards,
                skipped: 0,
                failed: 0,
            },
            hits: QueryResultHits {
                total: total_hits,
                max_score: None,
                hits: vec![],
            },
            aggregations: None,
        }
    }

    pub fn success(
        took: u32,
        num_shards: u32,
        total_hits: usize,
        max_score: Option<f64>,
        hits: Vec<QueryResultHit>,
        aggregations: Option<HashMap<String, AggregationResult>>,
        total_hits_complex: bool,
    ) -> Self {
        let total_hits = match total_hits_complex {
            true => QueryResultTotal::Complex(QueryResultTotalComplex::new(total_hits)),
            false => QueryResultTotal::Simple(total_hits as u64),
        };

        QueryResults {
            took: took,
            timed_out: false,
            _shards: QueryResultShards {
                total: num_shards,
                successful: num_shards,
                skipped: 0,
                failed: 0,
            },
            hits: QueryResultHits {
                total: total_hits,
                max_score: max_score,
                hits: hits,
            },
            aggregations: aggregations,
        }
    }

    pub(crate) fn to_response(&self) -> ElasticSearchResponse {
        ElasticSearchResponse {
            status: StatusCode::OK,
            mime: mime::APPLICATION_JSON,
            body: serde_json::to_string(self).unwrap(),
            headers: vec![],
        }
    }
}

pub(crate) struct QueryFailure {
    pub message: String,
}

impl QueryFailure {
    pub(crate) fn to_response(&self) -> ElasticSearchResponse {
        assert!(self.message.len() > 0);
        ElasticSearchResponse {
            status: StatusCode::BAD_REQUEST,
            mime: mime::TEXT_PLAIN,
            body: self.message.clone(),
            headers: vec![],
        }
    }
}

pub(crate) struct UpdateByQuerySuccess {
    pub result: UpdateByQueryResults,
}

impl UpdateByQuerySuccess {
    pub(crate) fn to_response(&self) -> ElasticSearchResponse {
        ElasticSearchResponse {
            status: StatusCode::OK,
            mime: mime::APPLICATION_JSON,
            body: serde_json::to_string(&self.result).unwrap(),
            headers: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{compare_query_result_hits_desc, QueryResultHit};
    use serde_json::json;

    #[test]
    fn test_query_result_hit_uses_bm25_fallback_score() {
        let value = json!({
            "_id": "doc-1",
            "_version": 1,
            "_seq_no": 9,
            "_source": "{\"message\":\"hello\"}",
            "term_cnt": 2.0,
            "word_cnt": 4.0
        });

        let hit = QueryResultHit::from_record(&Some("logs".to_string()), &value, None);

        assert!(hit._score.is_some());
        assert!(hit._score.unwrap() > 0.0);
    }

    #[test]
    fn test_compare_query_result_hits_desc_prefers_score_then_seq_no() {
        let higher_score = QueryResultHit {
            _index: Some("logs".to_string()),
            _id: Some("doc-1".to_string()),
            _version: 1,
            _seq_no: 1,
            _score: Some(2.0),
            _primary_term: Some(1),
            found: None,
            sort: None,
            _source: json!({"message": "higher"}),
        };
        let lower_score = QueryResultHit {
            _index: Some("logs".to_string()),
            _id: Some("doc-2".to_string()),
            _version: 1,
            _seq_no: 5,
            _score: Some(1.0),
            _primary_term: Some(1),
            found: None,
            sort: None,
            _source: json!({"message": "lower"}),
        };
        let newer_seq_no = QueryResultHit {
            _index: Some("logs".to_string()),
            _id: Some("doc-3".to_string()),
            _version: 1,
            _seq_no: 11,
            _score: Some(1.0),
            _primary_term: Some(1),
            found: None,
            sort: None,
            _source: json!({"message": "newer"}),
        };

        assert!(compare_query_result_hits_desc(&higher_score, &lower_score).is_lt());
        assert!(compare_query_result_hits_desc(&newer_seq_no, &lower_score).is_lt());
    }
}

#[derive(Serialize)]
#[allow(dead_code)]
pub(crate) struct SingleDocResult {
    _id: String,
    _index: String,
    _primary_term: String,
    result: String,
    _seq_no: u64,
    _shards: Shards,
    _version: u64,
    forced_refresh: bool,
}

impl SingleDocResult {
    #[allow(dead_code)]
    pub(crate) fn to_response(&self) -> ElasticSearchResponse {
        ElasticSearchResponse {
            status: StatusCode::OK,
            mime: mime::APPLICATION_JSON,
            body: serde_json::to_string(self).unwrap(),
            headers: vec![],
        }
    }
}

#[derive(Serialize)]
pub(crate) struct SingleDocCreateFailedResult {
    pub error: ErrorDetails,
    pub status: u32,
}

#[derive(Serialize)]
pub(crate) struct ErrorDetails {
    root_cause: Option<Vec<ErrorDetails>>,
    #[serde(rename = "type")]
    _type: String,
    reason: String,
    index_uuid: Option<String>,
    shard: Option<String>,
    index: Option<String>,
}

impl ErrorDetails {
    pub(crate) fn single_cause(
        _type: &String,
        reason: &String,
        index_uuid: Option<String>,
        shard: Option<String>,
        index: Option<String>,
    ) -> Self {
        ErrorDetails {
            root_cause: Some(vec![ErrorDetails {
                root_cause: None,
                _type: _type.clone(),
                reason: reason.clone(),
                index_uuid: index_uuid.clone(),
                shard: shard.clone(),
                index: index.clone(),
            }]),
            _type: _type.clone(),
            reason: reason.clone(),
            index_uuid: index_uuid.clone(),
            shard: shard.clone(),
            index: index.clone(),
        }
    }
}

impl ErrorDetails {
    #[allow(dead_code)]
    pub(crate) fn to_response(&self) -> ElasticSearchResponse {
        ElasticSearchResponse {
            status: StatusCode::OK,
            mime: mime::APPLICATION_JSON,
            body: serde_json::to_string(self).unwrap(),
            headers: vec![],
        }
    }
}
