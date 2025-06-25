use std::collections::HashMap;

use gotham::{hyper::StatusCode, mime, state::State};
use gotham::helpers::http::response::create_response;
use serde::Serialize;
use serde_json::Value;

use crate::elastic_search_common::CommandResponse;


#[derive(Serialize, Clone)]
pub(crate) struct Shards {
    pub total: u32,
    pub successful: u32,
    pub failed: u32,
}


#[derive(Serialize, Clone)]
pub(crate) struct OperationResult {
    pub _index: String,
    pub _id: String,
    pub _version: u32,
    pub result: String,
    pub _shards: Shards,
    pub _seq_no: i64,
    pub _primary_term: u32,
    pub status: Option<u32>,
}


#[derive(Serialize)]
pub(crate) struct BulkResult {
    pub errors: bool,
    pub took: u32,
    pub items: Vec<HashMap<String, OperationResult>>
}


impl BulkResult {
    pub fn success(took: u32, created: Vec<OperationResult>) -> Self {
        BulkResult { 
            errors: false, 
            took: took,
            items: created.iter().map(|x|HashMap::from([("created".to_string(), x.clone())])).collect()
        }
    }
}

#[derive(Serialize, Clone)]
pub(crate) struct QueryResultShards {
    total: u32,
    successful: u32,
    skipped: u32,
    failed: u32,
}

#[derive(Serialize, Clone)]
pub(crate) struct QueryResultTotal {
    value: u64,
    relation: String,
}

#[derive(Serialize, Clone)]
pub(crate) struct QueryResultHit {
    _index: String,
    _id: String,
    _version: i64,
    _seq_no: i64,
    _score: f64,
    _source: Value,
}


#[derive(Serialize, Clone)]
pub(crate) struct QueryResultHits {
    total: QueryResultTotal,
    max_score: f64,
    hits: Vec<QueryResultHit>
}

#[derive(Serialize, Clone)]
pub(crate) struct AggregationBucket {
    key: String,
    doc_count: u64,
}

#[derive(Serialize, Clone)]
pub(crate) struct AggregationResult {
    pub doc_count_error_upper_bound: u64,
    pub sum_other_doc_count: u64,
    pub buckets: Vec<AggregationBucket>
}


#[derive(Serialize, Clone)]
pub(crate) struct QueryResults {
    took: u32,
    timed_out: bool,
    _shards: QueryResultShards,
    hits: QueryResultHits,
    aggregations: Option<HashMap<String, AggregationResult>>,
}


#[derive(Serialize, Clone)]
pub(crate) struct QueryResultsNotFound {
    pub _index: String,
    pub _id: String,
    pub found: bool,
}

impl CommandResponse for QueryResultsNotFound {
    fn generate_response(&self, state: &State) -> gotham::hyper::Response<gotham::hyper::Body> {
        create_response(state, StatusCode::NOT_FOUND, mime::APPLICATION_JSON, serde_json::to_string(self).unwrap())
    }
}

impl QueryResultHit {
    pub fn new(index: &String, id: &String, version: i64, seq_no: i64, score: f64, source: Value) -> Self {
        QueryResultHit {
            _index: index.clone(),
            _id: id.clone(),
            _version: version,
            _seq_no: seq_no,
            _score: score,
            _source: source.clone()
        }
    }

    pub fn score(&self) -> f64 {
        self._score
    }
}

impl QueryResultTotal {
    fn new(num: usize) -> Self {
        QueryResultTotal { value: num as u64, relation: "eq".to_string() }
    }
}


impl QueryResults {
    pub fn empty(took: u32, num_shards: u32) -> Self {
        QueryResults { 
            took: took, 
            timed_out: false, 
            _shards: QueryResultShards { total: num_shards, successful: num_shards, skipped: 0, failed: 0 }, 
            hits: QueryResultHits { total: QueryResultTotal::new(0), max_score: 0.0, hits: vec!() },
            aggregations: None,
        }
    }

    #[allow(dead_code)]
    pub fn timed_out(took: u32, num_shards: u32) -> Self {
        QueryResults { 
            took: took, 
            timed_out: true, 
            _shards: QueryResultShards { total: num_shards, successful: num_shards, skipped: 0, failed: 0 }, 
            hits: QueryResultHits { total: QueryResultTotal::new(0), max_score: 0.0, hits: vec!() },
            aggregations: None,
        }        
    }

    pub fn success(took: u32, num_shards: u32, total_hits: usize, max_score: f64, hits: Vec<QueryResultHit>, aggregations: Option<HashMap<String, AggregationResult>>) -> Self {
        QueryResults { 
            took: took, 
            timed_out: false, 
            _shards: QueryResultShards { total: num_shards, successful: num_shards, skipped: 0, failed: 0 }, 
            hits: QueryResultHits { total: QueryResultTotal::new(total_hits), max_score: max_score, hits: hits },
            aggregations: aggregations,
        }  
    }
}



impl CommandResponse for QueryResults {
    fn generate_response(&self, state: &State) -> gotham::hyper::Response<gotham::hyper::Body> {
        create_response(state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(self).unwrap())
    }
}


pub(crate) struct QueryFailure {
    pub message: String
}

impl CommandResponse for QueryFailure {
    fn generate_response(&self, state: &State) -> gotham::hyper::Response<gotham::hyper::Body> {
        create_response(state, StatusCode::BAD_REQUEST, mime::TEXT_PLAIN, self.message.clone())
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
    forced_refresh: bool
}


impl CommandResponse for SingleDocResult {
    fn generate_response(&self, state: &State) -> gotham::hyper::Response<gotham::hyper::Body> {
        create_response(state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(self).unwrap())
    }
}


#[derive(Serialize)]
pub(crate) struct SingleDocCreateFailedResult {
    pub error: ErrorDetails,
    pub status: u32
}


#[derive(Serialize)]
pub(crate) struct ErrorDetails {
    root_cause: Option<Vec<ErrorDetails>>,
    #[serde(rename="type")]
    _type: String,
    reason: String,
    index_uuid: Option<String>,
    shard: Option<String>,
    index: Option<String>,
}


impl ErrorDetails {
    pub(crate) fn single_cause(_type: &String, reason: &String, index_uuid: Option<String>, shard: Option<String>, index: Option<String>) -> Self {
        ErrorDetails { 
            root_cause: Some(vec!(ErrorDetails{ 
                root_cause: None, 
                _type: _type.clone(), 
                reason: reason.clone(), 
                index_uuid: index_uuid.clone(), 
                shard: shard.clone(), 
                index: index.clone(),
            })),
            _type: _type.clone(), 
            reason: reason.clone(), 
            index_uuid: index_uuid.clone(), 
            shard: shard.clone(), 
            index: index.clone(),
        }
    }
}



impl CommandResponse for ErrorDetails {
    fn generate_response(&self, state: &State) -> gotham::hyper::Response<gotham::hyper::Body> {
        create_response(state, StatusCode::OK, mime::APPLICATION_JSON, serde_json::to_string(self).unwrap())
    }
}

