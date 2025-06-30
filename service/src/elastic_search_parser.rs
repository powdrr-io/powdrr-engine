
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::data_access::execute_sql;
use crate::elastic_search_commands::{to_serde_value, SqlCommand, UpdateByQueryCommand};
use crate::elastic_search_common::{Command, ParseError};
use crate::elastic_search_endpoints::QueryStringSearch;
use crate::elastic_search_responses::{AggregationResult, AverageAggregationResult, CardinalityAggregationResult, FilterAggregationResult, HistogramAggregationResult, RangeAggregationBucket, RangeAggregationResult, TermAggregationBucket, TermAggregationResult};



#[derive(Clone)]
pub(crate) struct TermAggProcessor {
    sql: String,
}

impl TermAggProcessor {
    fn create_aggregation_bucket(value: &Value) -> TermAggregationBucket {
        let value_map = value.as_object().unwrap();
        let mut value_map_iter = value_map.iter();
        let first_pair = value_map_iter.next().unwrap();
        let second_pair = value_map_iter.next().unwrap();

        TermAggregationBucket {
            key: second_pair.1.to_string(),
            doc_count: first_pair.1.as_u64().unwrap()
        }
    }

    async fn create_buckets(table_name: &String, query: &String) -> Vec<TermAggregationBucket> {
        let final_sql = query.replace("{target_table}", table_name);
        let data_frame = match execute_sql(&final_sql).await {
            Ok(df) => df,
            Err(_) => panic!("nope")
        };

        assert_eq!(data_frame.schema().columns().len(), 2);

        let serde_values = to_serde_value(&data_frame).await;

        serde_values.iter().map(|v| TermAggProcessor::create_aggregation_bucket(v)).collect::<Vec<TermAggregationBucket>>()
    }

    async fn process(&self, table_name: Option<String>, subaggregations: Option<Vec<Aggregation>>) -> AggregationResult {
        let child_aggs = process_aggregations(subaggregations, table_name.clone()).await;

        let buckets = match &table_name {
            Some(t) => TermAggProcessor::create_buckets(t, &self.sql).await,
            None => vec!()
        };

        AggregationResult::Terms(TermAggregationResult{
            doc_count_error_upper_bound: 0,
            sum_other_doc_count: 0,
            buckets: buckets,
            aggs: child_aggs
        })
    }
}

#[derive(Clone)]
pub(crate) struct RangeAggBucket {
    sql: String,
    key: String,
    from: u64,
    from_as_string: String,
    to: u64,
    to_as_string: String,
    subaggregations: Option<Vec<Aggregation>>
}

#[derive(Clone)]
pub(crate) struct RangeAggProcessor {
    buckets: Vec<RangeAggBucket>
}

impl RangeAggProcessor {
    async fn create_aggregation_bucket(bucket_spec: &RangeAggBucket, table_name: Option<String>) -> RangeAggregationBucket {
        let child_aggs = process_aggregations(bucket_spec.subaggregations.clone(), table_name.clone()).await;

        let doc_count = match &table_name {
            Some(t) => {
                let final_sql = bucket_spec.sql.replace("{target_table}", t);
                let data_frame = match execute_sql(&final_sql).await {
                    Ok(df) => df,
                    Err(_) => panic!("nope")
                };

                assert_eq!(data_frame.schema().columns().len(), 1);

                let serde_values = to_serde_value(&data_frame).await;

                serde_values.get(0).unwrap().as_object().unwrap().get("cnt").unwrap().as_u64().unwrap()
            },
            None => 0
        };

        RangeAggregationBucket {
            key: bucket_spec.key.clone(),
            from: bucket_spec.from,
            from_as_string: bucket_spec.from_as_string.clone(),
            to: bucket_spec.to,
            to_as_string: bucket_spec.to_as_string.clone(),
            doc_count: doc_count,
            aggs: child_aggs,
        }
    }

    async fn create_buckets(&self, table_name: Option<String>) -> Vec<RangeAggregationBucket> {
        let mut buckets = vec!();
        for bucket_spec in self.buckets.iter() {
            buckets.push(RangeAggProcessor::create_aggregation_bucket(&bucket_spec, table_name.clone()).await)
        }
        buckets
    }

    async fn process(&self, table_name: Option<String>, subaggregations: Option<Vec<Aggregation>>) -> AggregationResult {
        // The subaggregations should get passed into each bucket
        assert!(subaggregations.is_none());

        let buckets = self.create_buckets(table_name).await;

        AggregationResult::Range(RangeAggregationResult{
            buckets: buckets,
        })
    }
}




#[derive(Clone)]
pub(crate) struct AverageAggProcessor {
    sql: String,
}

impl AverageAggProcessor {
    async fn calculate_average(table_name: &String, query: &String) -> f64 {
        let final_sql = query.replace("{target_table}", table_name);
        let data_frame = match execute_sql(&final_sql).await {
            Ok(df) => df,
            Err(_) => panic!("nope")
        };

        assert_eq!(data_frame.schema().columns().len(), 1);

        let serde_values = to_serde_value(&data_frame).await;

        serde_values.get(0).unwrap().as_object().unwrap().get("avg").unwrap().as_f64().unwrap()
    }

    async fn process(&self, table_name: Option<String>, subaggregations: Option<Vec<Aggregation>>) -> AggregationResult {
        let child_aggs = process_aggregations(subaggregations, table_name.clone()).await;

        let avg = match &table_name {
            Some(t) => AverageAggProcessor::calculate_average(t, &self.sql).await,
            None => 0.0
        };

        AggregationResult::Average(AverageAggregationResult{
            value: avg,
            aggs: child_aggs,
        })
    }
}

#[derive(Clone)]
pub(crate) struct CardinalityAggProcessor {
    sql: String,
}

impl CardinalityAggProcessor {
    async fn calculate_cardinality(table_name: &String, query: &String) -> u64 {
        let final_sql = query.replace("{target_table}", table_name);
        let data_frame = match execute_sql(&final_sql).await {
            Ok(df) => df,
            Err(_) => panic!("nope")
        };

        assert_eq!(data_frame.schema().columns().len(), 1);

        let serde_values = to_serde_value(&data_frame).await;

        serde_values.get(0).unwrap().as_object().unwrap().get("type_count").unwrap().as_u64().unwrap()
    }

    async fn process(&self, table_name: Option<String>, subaggregations: Option<Vec<Aggregation>>) -> AggregationResult {
        let child_aggs = process_aggregations(subaggregations, table_name.clone()).await;

        let type_count = match &table_name {
            Some(t) => CardinalityAggProcessor::calculate_cardinality(t, &self.sql).await,
            None => 0
        };
        AggregationResult::Cardinality(CardinalityAggregationResult{
            type_count: type_count,
            aggs: child_aggs,
        })
    }
}

#[derive(Clone)]
pub(crate) struct DateHistogramAggBucket {
    #[allow(dead_code)]
    subaggregations: Option<Vec<Aggregation>>
}

#[derive(Clone)]
pub(crate) struct DateHistogramAggProcessor {
    #[allow(dead_code)]
    buckets: Vec<DateHistogramAggBucket>
}

impl DateHistogramAggProcessor {
    async fn process(&self, _table_name: Option<String>, subaggregations: Option<Vec<Aggregation>>) -> AggregationResult {
        assert!(subaggregations.is_none());
        AggregationResult::Histogram(HistogramAggregationResult{
            buckets: vec!()
        })
    }
}

#[derive(Clone)]
pub(crate) struct FilterAggProcessor {
    sql: String,
}

impl FilterAggProcessor {
    async fn process(&self, table_name: Option<String>, subaggregations: Option<Vec<Aggregation>>) -> AggregationResult {
        let doc_count = match &table_name {
            Some(t) => {
                let final_sql = self.sql.replace("{target_table}", t);
                let data_frame = execute_sql(&final_sql).await.unwrap();
                assert_eq!(data_frame.schema().columns().len(), 1);
                let serde_values = to_serde_value(&data_frame).await;
                serde_values.get(0).unwrap().as_object().unwrap().get("cnt").unwrap().as_u64().unwrap()
            },
            None => 0
        };
        let child_aggs = process_aggregations(subaggregations, table_name.clone()).await;

        AggregationResult::Filter(FilterAggregationResult {
            doc_count: doc_count,
            aggs: child_aggs
        })
    }
}

#[derive(Clone)]
pub(crate) struct MissingAggProcessor {
}

impl MissingAggProcessor {
    async fn process(&self, table_name: Option<String>, subaggregations: Option<Vec<Aggregation>>) -> AggregationResult {
        let child_aggs = process_aggregations(subaggregations, table_name).await;

        // TODO: we need to find doc that are actually missing values
        AggregationResult::Filter(FilterAggregationResult {
            doc_count: 0,
            aggs: child_aggs
        })
    }
}


#[derive(Clone)]
pub(crate) enum AggProcessor {
    Average(AverageAggProcessor),
    Cardinality(CardinalityAggProcessor),
    DateHistogram(DateHistogramAggProcessor),
    Filter(FilterAggProcessor),
    Missing(MissingAggProcessor),
    Range(RangeAggProcessor),
    Term(TermAggProcessor),
}

#[derive(Clone)]
pub(crate) struct Aggregation {
    pub name: String,
    pub processor: AggProcessor,
    pub subaggregations: Option<Vec<Aggregation>>
}

pub(crate) async fn process_aggregation(aggregation: &Aggregation, table_name: Option<String>) -> AggregationResult {
    match &aggregation.processor {
        AggProcessor::Average(average) => {
            average.process(table_name, aggregation.subaggregations.clone()).await
        },
        AggProcessor::Cardinality(cardinality) => {
            cardinality.process(table_name, aggregation.subaggregations.clone()).await
        },
        AggProcessor::DateHistogram(date_histogram) => {
            date_histogram.process(table_name, aggregation.subaggregations.clone()).await
        },
        AggProcessor::Filter(filter) => {
            filter.process(table_name, aggregation.subaggregations.clone()).await
        },
        AggProcessor::Missing(missing) => {
            missing.process(table_name, aggregation.subaggregations.clone()).await
        },
        AggProcessor::Range(range) => {
            range.process(table_name, aggregation.subaggregations.clone()).await
        },
        AggProcessor::Term(term) => {
            term.process(table_name, aggregation.subaggregations.clone()).await
        },
    }
}

pub(crate) async fn process_aggregations(aggregations: Option<Vec<Aggregation>>, table_name: Option<String>) -> HashMap<String, AggregationResult> {
    let mut results = HashMap::new();
    if aggregations.is_some() {
        for aggregation in aggregations.unwrap() {
            results.insert(aggregation.name.clone(), Box::pin(process_aggregation(&aggregation, table_name.clone())).await);
        }
    }
    results
}


pub fn parse(table: Option<String>, val: &String, query: &QueryStringSearch) -> Result<Arc<dyn Command>, ParseError> {
    let body: SearchBody = serde_json::from_str(val.as_str()).map_err(|e|ParseError{ message: format!("{}", e)})?;
    let command = to_command(table, &body, query)?;
    Ok(Arc::new(command))
}

pub fn parse_update_by_query(table: Option<String>, val: &String) -> Result<Arc<dyn Command>, ParseError> {
    let body: UpdateByQueryBody = match serde_json::from_str::<UpdateByQueryBody>(val.as_str()) {
      Ok(b) => b,
      Err(e) => {
          let error = format!("{}", e);
          println!("{}", error);
          return Err(ParseError{ message: error })
      }
    };
    let command = to_command_update_by_query(table, &body)?;
    Ok(Arc::new(command))
}


#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum SortType {
    Bare(String),
    Parameterized(HashMap<String, SortBody>),
}

fn default_as_true() -> bool {
    true
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggTerms {
    field: String,
    size: Option<u32>,
    #[serde(default = "default_as_true")]
    show_term_doc_count_error: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecTermsBody {
    field: String,
    size: Option<u32>,
    #[serde(default = "default_as_true")]
    show_term_doc_count_error: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecTerms {
    terms: AggSpecTermsBody,
    aggs: Option<HashMap<String, AggSpec>>,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecMissingBody {
    field: String,
    size: Option<u32>,
    #[serde(default = "default_as_true")]
    show_term_doc_count_error: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecMissing {
    missing: AggSpecMissingBody,
    aggs: Option<HashMap<String, AggSpec>>
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecFilterTerm {
    term: HashMap<String, String>
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecFilterRangeSpan {
    from: String,
    to: String
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecFilterRangeStructured {
    field: String,
    ranges: Vec<AggSpecFilterRangeSpan>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum AggSpecFilterRangeBody {
    Structured(AggSpecFilterRangeStructured),
    Raw(HashMap<String, HashMap<String, String>>),
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecFilterRange {
    range: AggSpecFilterRangeBody,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum AggSpecFilterBody {
    Term(AggSpecFilterTerm),
    Range(AggSpecFilterRange),
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecFilter {
    filter: AggSpecFilterBody,
    aggs: Option<HashMap<String, AggSpec>>
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecDateHistogramBody {
    field: String,
    fixed_interval: String,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecDateHistogram {
    date_histogram: AggSpecDateHistogramBody,
    aggs: Option<HashMap<String, AggSpec>>
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecCardinalityBody {
    field: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecCardinality {
    cardinality: AggSpecCardinalityBody,
    aggs: Option<HashMap<String, AggSpec>>
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecRange {
    range: AggSpecFilterRangeBody,
    aggs: Option<HashMap<String, AggSpec>>
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecAverageBody {
    field: String
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecAverage {
    avg: AggSpecAverageBody,
    aggs: Option<HashMap<String, AggSpec>>
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum AggSpec {
    Terms(AggSpecTerms),
    Missing(AggSpecMissing),
    Filter(AggSpecFilter),
    DateHistogram(AggSpecDateHistogram),
    Cardinality(AggSpecCardinality),
    Range(AggSpecRange),
    Average(AggSpecAverage),
}


#[derive(Serialize, Deserialize, Clone)]
struct SearchBody {
    pit: Option<PitInfo>,
    size: Option<u32>,
    from: Option<u32>,
    seq_no_primary_term: Option<bool>,
    query: Option<Query>,
    aggs: Option<HashMap<String, AggSpec>>,
    sort: Option<Vec<SortType>>
}


#[derive(Serialize, Deserialize, Clone)]
struct PitInfo {
    id: String,
    keep_alive: String
}


#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum Query {
    Match(Match),
    Bool(Bool),
    Term(Term),
    Exists(Exists),
    SimpleQueryString(SimpleQueryString),
    Range(Range),
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Match {
    #[serde(rename = "match")]
    _match: HashMap<String, FieldMatch>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum FieldMatch {
    String(String),
    Struct(FieldMatchBody)
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct FieldMatchBody {
    query: String
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Bool {
    #[serde(rename = "bool")]
    _bool: BoolBody
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum SingleOrVec {
    Vec(Vec<Query>),
    Single(Box<Query>),
}

impl SingleOrVec {
    fn as_vec(&self) -> Vec<Query> {
        match self {
            SingleOrVec::Single(s) => {
                vec!(*s.clone())
            },
            SingleOrVec::Vec(v) => {
                v.clone()
            }
        }
    }
}

fn default_single_or_vec() -> SingleOrVec {
    SingleOrVec::Vec(vec!())
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct BoolBody {
    #[serde(default = "default_single_or_vec")]
    filter: SingleOrVec,
    #[serde(default = "default_single_or_vec")]
    should: SingleOrVec,
    #[serde(default = "default_single_or_vec")]
    must: SingleOrVec,
    #[serde(default = "default_single_or_vec")]
    must_not: SingleOrVec,
    minimum_should_match: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Term {
    term: HashMap<String, Value>,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Exists {
    exists: ExistsBody,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ExistsBody {
    field: String
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RangeSpec {
    gt: Option<String>,
    gte: Option<String>,
    lt: Option<String>,
    lte: Option<String>,
    format: Option<String>,
    relation: Option<String>,
    time_zone: Option<String>,
    boost: Option<f64>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Range {
    range: HashMap<String, RangeSpec>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SortBody {
    #[serde(rename = "type")]
    _type: Option<String>,
    order: Option<String>,
    unmapped_type: Option<String>,
    script: Option<ScriptBlock>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SimpleQueryString {
    simple_query_string: SimpleQueryStringBody,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SimpleQueryStringBody {
    query: String,
    fields: Vec<String>,
    default_operator: String,
}

#[derive(Clone)]
pub(crate) enum FilterExpression {
    And(Vec<FilterExpression>),
    Or(Vec<FilterExpression>),
    Not(Box<FilterExpression>),
    Expr(String),
}

impl FilterExpression {
    pub fn and(exprs: Vec<FilterExpression>) -> Option<Self> {
        if exprs.len() == 0 {
            None
        } else if exprs.len() == 1 {
            Some(exprs.get(0).unwrap().clone())
        } else {
            Some(FilterExpression::And(exprs))
        }
    }

    pub fn or(exprs: Vec<FilterExpression>) -> Option<Self> {
        if exprs.len() == 0 {
            None
        } else if exprs.len() == 1 {
            Some(exprs.get(0).unwrap().clone())
        } else {
            Some(FilterExpression::Or(exprs))
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct UpdateByQueryBody {
    query: Query,
    script: ScriptBlock,
    sort: Option<Vec<SortType>>,
    max_docs: Option<usize>,
    conflicts: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct UpdateBody {
    pub scripted_upsert: Option<bool>,
    pub script: Option<ScriptBlock>,
    pub doc: Option<Value>,
    pub upsert: Option<Value>,
    pub doc_as_upsert: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ScriptBlock {
    pub source: String,
    pub lang: String,
    #[serde(default)]
    pub params: HashMap<String, Value>,
}

#[derive(Clone)]
struct SqlBuilder {
    fields: Vec<String>,
    joins: Vec<String>,
    filter_stack: RefCell<Vec<Vec<FilterExpression>>>,
    limit: Option<u64>,
    calculate_score: bool,
    order_by: Vec<String>,
    group_by: Vec<String>,
    top_level: bool,
}

impl SqlBuilder {
    fn for_query() -> Self {
        SqlBuilder { 
            fields: vec!(), 
            joins: vec!(), 
            filter_stack: RefCell::new(vec!(vec!())),
            limit: None, 
            calculate_score: false,
            order_by: vec!(),
            group_by: vec!(),
            top_level: true,
        }
    }

    fn for_agg() -> Self {
        SqlBuilder {
            fields: vec!(),
            joins: vec!(),
            filter_stack: RefCell::new(vec!(vec!())),
            limit: None,
            calculate_score: false,
            order_by: vec!(),
            group_by: vec!(),
            top_level: false,
        }
    }

    fn push_filter_context(&mut self) -> &mut Self {
        self.filter_stack.get_mut().push(vec!());
        self
    }

    fn pop_filter_context(&mut self, is_and: bool) -> &mut Self {
        self.pop_and_maybe_not_filter_context(is_and, false)
    }

    fn pop_and_not_filter_context(&mut self, is_and: bool) -> &mut Self {
        self.pop_and_maybe_not_filter_context(is_and, true)
    }

    fn pop_and_maybe_not_filter_context(&mut self, is_and: bool, is_not: bool) -> &mut Self {
        let local_filter_stack = self.filter_stack.get_mut();
        assert!(local_filter_stack.len() > 0);

        let filter = match is_and {
            true => FilterExpression::and(local_filter_stack.pop().unwrap()),
            false => FilterExpression::or(local_filter_stack.pop().unwrap()),
        };

        if filter.is_some() {
            let local_last = local_filter_stack.last_mut().unwrap();
            if is_not {
                local_last.push(FilterExpression::Not(Box::new(filter.unwrap())));
            } else {
                local_last.push(filter.unwrap());
            }
        }
        self
    }

    fn filter(&mut self, filter: String) -> &mut Self {
        let local_filter_stack = self.filter_stack.get_mut();
        local_filter_stack.last_mut().unwrap().push(FilterExpression::Expr(filter));
        self
    }

    fn _fields(&self) -> String {
        if self.fields.len() == 0 {
            "*".to_string()
        } else {
            self.fields.join(", ")
        }
    }

    fn _deletes(&self) -> String {
        if self.top_level {
            " LEFT JOIN {deletes_table} dt ON dt._id = t._id".to_string()
        } else {
            "".to_string()
        }
    }

    fn _joins(&self) -> String {
        if self.joins.len() == 0 {
            self._deletes()
        } else {
            format!(" {}{}", self.joins.join(" "), self._deletes())
        }
    }

    fn _latest(&self) -> String {
        if self.top_level {
            "(dt._seq_no is null or t._seq_no > dt._seq_no)".to_string()
        } else {
            "".to_string()
        }
    }

    fn _filters(&self) -> String {
        let mut local_filter_stack = self.filter_stack.borrow().clone();
        assert_eq!(local_filter_stack.len(), 1);
        let top = local_filter_stack.pop().unwrap();
        if top.len() == 0 && !self.top_level {
            "".to_string()
        } else if top.len() == 0 {
            format!(" WHERE {}", self._latest())
        } else if self.top_level{
            format!(" WHERE {} AND {}", self._latest(), SqlBuilder::format_filter(&FilterExpression::And(top)))
        } else {
            format!(" WHERE {}", SqlBuilder::format_filter(&FilterExpression::And(top)))
        }
    }

    fn format_filter(expr: &FilterExpression) -> String {
        match expr {
            FilterExpression::And(exprs) => {
                assert!(exprs.len() > 0);
                if exprs.len() == 1 {
                    return SqlBuilder::format_filter(exprs.get(0).unwrap());
                } else {
                    format!("({})", exprs.iter().map(|x|SqlBuilder::format_filter(x)).collect::<Vec<String>>().join(" AND "))
                }
            },
            FilterExpression::Or(exprs) => {
                if exprs.len() == 1 {
                    return SqlBuilder::format_filter(exprs.get(0).unwrap());
                } else {
                    format!("({})", exprs.iter().map(|x|SqlBuilder::format_filter(x)).collect::<Vec<String>>().join(" OR "))
                }                
            },
            FilterExpression::Not(inner_expr) => {
                format!("NOT({})", SqlBuilder::format_filter(&inner_expr))
            },
            FilterExpression::Expr(val) => {
                val.clone()
            }
        }
    }

    fn _limit(&self) -> String {
        if self.limit.is_none() {
            "".to_string()
        } else {
            format!(" LIMIT {}", self.limit.unwrap())
        }
    }

    fn _order_by(&self) -> String {
        if self.order_by.len() == 0 {
            "".to_string()
        } else {
            format!(" ORDER BY {}", self.group_by.join(", "))
        }
    }

    fn _group_by(&self) -> String {
        if self.group_by.len() == 0 {
            "".to_string()
        } else {
            format!(" GROUP BY {}", self.group_by.join(", "))
        }
    }

    fn score(&self) -> bool {
        self.calculate_score
    }

    fn build(&self) -> String {
        let filter_str = self._filters();
        format!(
            "select {} from {} {}{}{}{}{}",
            self._fields(),
            "{target_table} t",
            self._joins(),
            filter_str,
            self._order_by(),
            self._group_by(),
            self._limit()
        )
    }
}

fn create_aggregation_filters(filter: &AggSpecFilterBody) -> Vec<String> {
    match filter {
        AggSpecFilterBody::Term(term) => {
            assert_eq!(term.term.len(), 1);
            let (name, value) = term.term.iter().next().unwrap();
            vec!(format!("{name} = '{value}'"))
        },
        AggSpecFilterBody::Range(range) => {
            create_aggregation_range_filters(&range.range)
        }
    }
}

fn create_aggregation_range_filters(range: &AggSpecFilterRangeBody) -> Vec<String> {
    match range {
        AggSpecFilterRangeBody::Raw(raw) => {
            assert_eq!(raw.len(), 1);
            let (name, value_and_op) = raw.iter().next().unwrap();
            assert_eq!(value_and_op.len(), 1);
            let (op, value) = value_and_op.iter().next().unwrap();
            // TODO: need to convert the op to the appropriate SQL operator
            // TODO: need to convert the value to the appropriate SQL type
            let converted_op = op;
            let converted_value = value;
            vec!(format!("{name} {converted_op} {converted_value}"))
        },
        AggSpecFilterRangeBody::Structured(structured) => {
            let mut retval = vec!();
            let name = &structured.field;
            for range in structured.ranges.iter() {
                // TODO: need to convert both these values
                let converted_from_value = &range.from;
                let converted_to_value = &range.to;
                retval.push(format!("{name} >= {converted_from_value}"));
                retval.push(format!("{name} < {converted_to_value}"));
            }
            retval
        }
    }
}

fn create_aggregation_processor(input_builder: &SqlBuilder, spec: &AggSpec) -> (AggProcessor, Option<Vec<Aggregation>>) {
    match spec {
        AggSpec::Terms(terms) => {
            let field_name = terms.terms.field.clone();
            let mut builder = input_builder.clone();
            builder.group_by.push(format!("{field_name}"));
            let aggregations = aggs_to_sql(Some(builder.clone()), terms.aggs.clone());
            builder.fields.push(format!("{field_name} as field_name"));
            builder.fields.push("count(1) as cnt".to_string());
            let sql = builder.build();
            let processor = AggProcessor::Term(TermAggProcessor{ sql: sql });
            (processor, aggregations)
        },
        AggSpec::Filter(filter) => {
            let mut builder = input_builder.clone();
            for filter in create_aggregation_filters(&filter.filter) {
                builder.filter(filter);
            }
            let mut query_builder = builder.clone();
            query_builder.fields.push("count(1) as cnt".to_string());
            let sql = query_builder.build();
            let processor = AggProcessor::Filter(FilterAggProcessor{ sql: sql });
            let aggregations = aggs_to_sql(Some(builder), filter.aggs.clone());
            (processor, aggregations)
        },
        AggSpec::Missing(missing) => {
            let processor = AggProcessor::Missing(MissingAggProcessor{ });
            let aggregations = aggs_to_sql(Some(input_builder.clone()), missing.aggs.clone());
            (processor, aggregations)
        },
        AggSpec::DateHistogram(hist) => {
            // TODO: this is a mess. Need to figure out how histograms work
            let mut builder = input_builder.clone();
            let field_name = hist.date_histogram.field.clone();
            builder.fields.push(format!("{field_name} as field_value"));
            builder.fields.push("count(1) as doc_count".to_string());
            // TODO: get offset and interval from the spec
            // TODO: convert the field as necessary (aka datetime to millis since epoch)
            let offset = 0;
            let interval = 5;
            builder.group_by.push(format!("floor(({field_name} - {offset}) / {interval}) * {interval} + {offset}"));
            let _sql = builder.build();
            let processor = AggProcessor::DateHistogram(DateHistogramAggProcessor{ buckets: vec!() });
            (processor, None)
        },
        AggSpec::Cardinality(cardinality) => {
            let mut builder = input_builder.clone();
            let field_name = &cardinality.cardinality.field;
            builder.fields.push(format!("count(distinct {field_name}) as type_count"));
            let sql = builder.build();
            let processor = AggProcessor::Cardinality(CardinalityAggProcessor{ sql: sql });
            let aggregations = aggs_to_sql(Some(input_builder.clone()), cardinality.aggs.clone());
            (processor, aggregations)
        } ,
        AggSpec::Range(range) => {
            // TODO: this is a mess. Need to figure out the full range of options here
            // and figure out how to target the multibucket range case
            let mut builder = input_builder.clone();
            for filter in create_aggregation_range_filters(&range.range) {
                builder.filter(filter);
            }
            let mut query_builder = builder.clone();
            query_builder.fields.push("count(1) as cnt".to_string());
            let sql = query_builder.build();
            let aggregations = aggs_to_sql(Some(builder), range.aggs.clone());
            let processor = AggProcessor::Range(RangeAggProcessor {
                buckets: vec!(
                    RangeAggBucket {
                        sql,
                        key: "2025-06-27T20:18:59.356Z-2025-06-27T20:20:59.356Z".to_string(),
                        from: 1751055539356,
                        from_as_string: "2025-06-27T20:18:59.356Z".to_string(),
                        to: 1751055659356,
                        to_as_string: "2025-06-27T20:20:59.356Z".to_string(),
                        subaggregations: aggregations,
                    }
                )
            });

            (processor, None)
        }
        AggSpec::Average(average) => {
            let mut builder = input_builder.clone();
            let field_name = &average.avg.field;
            builder.fields.push(format!("avg({field_name}) as avg"));
            let sql = builder.build();
            let processor = AggProcessor::Average(AverageAggProcessor{ sql: sql });
            let aggregations = aggs_to_sql(Some(input_builder.clone()), average.aggs.clone());
            (processor, aggregations)
        }
    }
}

fn create_aggregation(input_builder: Option<SqlBuilder>, name: &String, spec: &AggSpec) -> Aggregation {
    let builder = input_builder.unwrap_or_else(|| SqlBuilder::for_agg());
    let (processor, subaggregations) = create_aggregation_processor(&builder, spec);
    Aggregation {
        name: name.clone(),
        processor: processor,
        subaggregations: subaggregations,
    }
}

fn aggs_to_sql(input_builder: Option<SqlBuilder>, aggs: Option<HashMap<String, AggSpec>>) -> Option<Vec<Aggregation>> {
    if aggs.is_none() {
        return None;
    }

    Some(aggs.unwrap().iter().map(|x| create_aggregation(input_builder.clone(), x.0, x.1)).collect())
}

fn to_command(table: Option<String>, body: &SearchBody, query: &QueryStringSearch) -> Result<SqlCommand, ParseError> {
    let mut builder = SqlBuilder::for_query();

    if body.from.is_some() {
        if body.from.unwrap() != 0 {
            panic!("Not implemented");
        }
    }

    if body.query.is_some() {
        to_command_worker(&mut builder, &body.query.clone().unwrap())?;
    } else {
        builder.fields.push("*".to_string());
    }

    let table_name = match table {
        Some(t) => t,
        None => match &body.pit {
            // TODO: parse the pit to get the table name
            Some(p) => p.id.clone(),
            None => panic!("Didn't find a table")
        }
    };

    let sql = builder.build();
    let aggs = aggs_to_sql(None, body.aggs.clone());

    Ok(SqlCommand{
        sql: sql,
        table: table_name,
        calculate_score: builder.score(),
        aggs: aggs,
        query_params: query.clone()
    })
}

fn to_command_update_by_query(table: Option<String>, body: &UpdateByQueryBody) -> Result<UpdateByQueryCommand, ParseError> {
    let mut builder = SqlBuilder::for_query();

    to_command_worker(&mut builder, &body.query)?;

    let table_name = match table {
        Some(t) => t,
        None => panic!("Didn't find a table")
    };

    Ok(UpdateByQueryCommand{
        query_command: SqlCommand{
            sql: builder.build(),
            table: table_name,
            calculate_score: builder.score(),
            aggs: None,
            query_params: QueryStringSearch {
                allow_partial_search_results: None,
                sort: None,
                rest_total_hits_as_int: None,
            }
        },
        script_block: body.script.clone(),
    })
}

fn to_command_worker(builder: &mut SqlBuilder, query: &Query) -> Result<(), ParseError> {
    match query {
        Query::Match(m) => to_sql_match(builder, &m),
        Query::Bool(b) => to_sql_bool(builder, &b),
        Query::Term(t) => to_sql_term(builder, &t),
        Query::Exists(e) => to_sql_exists(builder, &e),
        Query::Range(r) => to_sql_range(builder, &r),
        Query::SimpleQueryString(s) => to_sql_simple_query(builder, &s),
    }
}

fn to_field_term(body: &FieldMatch) -> String {
    match body {
        FieldMatch::String(s) => s.clone(),
        FieldMatch::Struct(s) => s.query.clone()
    }
}

fn to_sql_match(builder: &mut SqlBuilder, match_obj: &Match) -> Result<(), ParseError> {
    if match_obj._match.len() != 1 {
        panic!("Not implemented")
    }

    builder.calculate_score = true;
    if builder.joins.len() == 0 {
        builder.joins.push("INNER JOIN {target_table}_search_index si on si.doc_id = t._id".to_string());
    }

    for pair in match_obj._match.iter() {
        builder.filter(format!("(si.field_name = '{}' AND si.field_term = '{}')", pair.0, to_field_term(pair.1)));
    }
    Ok(())
}

fn to_sql_bool(builder: &mut SqlBuilder, bool_obj: &Bool) -> Result<(), ParseError> {
    builder.push_filter_context();
    if bool_obj._bool.must.as_vec().len() > 0 {
        builder.push_filter_context();

        bool_obj._bool.must.as_vec().iter().map(|x|to_command_worker(builder, x)).collect::<Result<Vec<()>, ParseError>>()?;

        builder.pop_filter_context(true);
    }
    if bool_obj._bool.should.as_vec().len() > 0 {
        builder.push_filter_context();

        bool_obj._bool.should.as_vec().iter().map(|x|to_command_worker(builder, x)).collect::<Result<Vec<()>, ParseError>>()?;

        builder.pop_filter_context(false);        
    }
    if bool_obj._bool.must_not.as_vec().len() > 0 {
        // Must not is an AND of NOTS which we rewrite into a NOT of ORS to simplify the codegen logic a bit here.
        builder.push_filter_context();

        bool_obj._bool.must_not.as_vec().iter().map(|x|to_command_worker(builder, x)).collect::<Result<Vec<()>, ParseError>>()?;

        builder.pop_and_not_filter_context(false);
    } 
    if bool_obj._bool.filter.as_vec().len() > 0 {
        builder.push_filter_context();

        bool_obj._bool.filter.as_vec().iter().map(|x|to_command_worker(builder, x)).collect::<Result<Vec<()>, ParseError>>()?;

        builder.pop_filter_context(true);
    }
    builder.pop_filter_context(true);
    Ok(())
}

fn to_sql_term(builder: &mut SqlBuilder, term_obj: &Term) -> Result<(), ParseError> {
    for pair in term_obj.term.iter() {
        if pair.1.is_string() {
            builder.filter(format!("t.{} = '{}'", pair.0, pair.1.as_str().unwrap()));
        } else {
            builder.filter(format!("t.{} = {}", pair.0, pair.1));
        }
    }
    Ok(())
}

fn to_sql_exists(_builder: &mut SqlBuilder, _exists_obj: &Exists) -> Result<(), ParseError> {
    // TODO: need to figure out how to query the schema
    // builder.filter(format!("t.{} is not null", exists_obj.exists.field));
    Ok(())
}

fn to_sql_range(builder: &mut SqlBuilder, range_obj: &Range) -> Result<(), ParseError> {
    if range_obj.range.len() != 1 {
        panic!("Not implemented")
    }
    
    let pair = range_obj.range.iter().next().unwrap();
    let field_name = pair.0;
    let spec = pair.1;
    
    if spec.format.is_some() || spec.relation.is_some() || spec.time_zone.is_some() || spec.boost.is_some() {
        panic!("Not implemented")
    }
    
    match &spec.gt {
        Some(val) => {
            builder.filter(format!("{field_name} > {val}"));
        },
        None => ()
    };

    match &spec.gte {
        Some(val) => {
            builder.filter(format!("{field_name} >= {val}"));
        },
        None => ()
    };

    match &spec.lt {
        Some(val) => {
            builder.filter(format!("{field_name} < {val}"));
        },
        None => ()
    };

    match &spec.lte {
        Some(val) => {
            builder.filter(format!("{field_name} <= {val}"));
        },
        None => ()
    };
    
    Ok(())
}

fn to_sql_simple_query(builder: &mut SqlBuilder, query_obj: &SimpleQueryString) -> Result<(), ParseError> {
    if query_obj.simple_query_string.fields.len() == 0 {
        panic!("Not implemented")
    }

    builder.calculate_score = true;
    if builder.joins.len() == 0 {
        builder.joins.push("INNER JOIN {target_table}_search_index si on si.doc_id = t._id".to_string());
    }

    // TODO: need to really parse the query string
    let split_query = query_obj.simple_query_string.query.split(" ");
    for field_term in split_query {
        for field_name in query_obj.simple_query_string.fields.iter() {
            builder.filter(format!("(si.field_name = '{}' AND si.field_term = '{}')", field_name, field_term));
        }
    }
    Ok(())    
}


#[cfg(test)]
mod tests {
    use crate::elastic_search_endpoints::QueryStringSearch;
    use crate::elastic_search_parser::{parse, UpdateByQueryBody};

    use super::{to_command, SearchBody};

    #[test]
    fn test_parse_match() {
        let parse_result = parse(
            Some("foo".to_string()),
            &r#"
{
   "query": {
     "match": {
       "message": {
         "query": "this is a test"
       }
     }
   }
}"#.to_string(),
            &QueryStringSearch::new(),
        );

        match parse_result {
            Ok(pr) => {
                assert_eq!("SqlCommand", pr.get_name());
                ()
            }
            _ => panic!("Parsing error")
        }
    }


    #[test]
    fn test_parse_match_new() {
        let parse_result: SearchBody = match serde_json::from_str(
            r#"
{
   "query": {
     "match": {
       "message": {
         "query": "this is a test"
       }
     }
   }
}"#
        ) {
            Ok(r) => r,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let command = to_command(Some("testtime".to_string()), &parse_result, &QueryStringSearch::new()).unwrap();

        println!("{}", command.sql);
    }

    #[test]
    fn test_parse_bool() {
        let test_val = r#"{
  "size": 1,
  "seq_no_primary_term": true,
  "from": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "should": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "type": "canvas-workpad-template"
                      }
                    }
                  ],
                  "must_not": [
                    {
                      "exists": {
                        "field": "namespace"
                      }
                    },
                    {
                      "range": {
                        "task.runAt": {
                          "lte": "now"
                        }
                      }
                    }
                  ]
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  }
}"#;

        let parse_result: SearchBody = match serde_json::from_str(test_val) {
            Ok(pr) => pr,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let command = to_command(Some("testtime".to_string()), &parse_result, &QueryStringSearch::new()).unwrap();

        println!("{}", command.sql);

        let test_val = r#"{
  "size": 20,
  "seq_no_primary_term": true,
  "from": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "should": [
              {
                "match": {
                  "ingest-package-policies.package.name": "apm"
                }
              }
            ],
            "minimum_should_match": 1
          }
        },
        {
          "bool": {
            "should": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "type": "ingest-package-policies"
                      }
                    }
                  ],
                  "must_not": [
                    {
                      "exists": {
                        "field": "namespace"
                      }
                    },
                    {
                      "exists": {
                        "field": "namespaces"
                      }
                    }
                  ]
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  },
  "sort": [
    {
      "ingest-package-policies.updated_at": {
        "order": "desc",
        "unmapped_type": "date"
      }
    }
  ]
}"#;

        let _parse_result: SearchBody = match serde_json::from_str(test_val) {
            Ok(pr) => pr,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let test_val = r#"{
  "size": 20,
  "seq_no_primary_term": true,
  "from": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "should": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "type": "upgrade-assistant-reindex-operation"
                      }
                    }
                  ],
                  "must_not": [
                    {
                      "exists": {
                        "field": "namespace"
                      }
                    },
                    {
                      "exists": {
                        "field": "namespaces"
                      }
                    }
                  ]
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ],
      "must": [
        {
          "simple_query_string": {
            "query": "0",
            "fields": [
              "upgrade-assistant-reindex-operation.status"
            ],
            "default_operator": "OR"
          }
        }
      ]
    }
  }
}"#;

        let parse_result: SearchBody = match serde_json::from_str(test_val) {
            Ok(pr) => pr,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let command = to_command(Some("testtime".to_string()), &parse_result, &QueryStringSearch::new()).unwrap();

        println!("{}", command.sql);

        let test_val = r#"{
  "size": 1000,
  "seq_no_primary_term": true,
  "from": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "should": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "type": "space"
                      }
                    }
                  ],
                  "must_not": [
                    {
                      "exists": {
                        "field": "namespace"
                      }
                    },
                    {
                      "exists": {
                        "field": "namespaces"
                      }
                    }
                  ]
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  },
  "sort": [
    {
      "space.name.keyword": {
        "unmapped_type": "keyword"
      }
    }
  ]
}"#;

        let _parse_result: SearchBody = match serde_json::from_str(test_val) {
            Ok(pr) => pr,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let test_val = r#"{"size":1,"seq_no_primary_term":true,"from":0,"query":{"bool":{"filter":[{"bool":{"should":[{"bool":{"must":[{"term":{"type":"canvas-workpad-template"}}],"must_not":[{"exists":{"field":"namespace"}},{"exists":{"field":"namespaces"}}]}}],"minimum_should_match":1}}]}}}"#;

        let parse_result: SearchBody = match serde_json::from_str(test_val) {
            Ok(pr) => pr,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let _command = to_command(Some("fake_name".to_string()), &parse_result, &QueryStringSearch::new());



    }

    #[test]
    fn test_parse_update_by_query() {
        let test_val = include_str!("../tests/data/search_query_1.json");

        let _parse_result: SearchBody = match serde_json::from_str(test_val) {
            Ok(pr) => pr,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let test_val = include_str!("../tests/data/update_by_query_1.json");

        let _parse_result: UpdateByQueryBody = match serde_json::from_str(test_val) {
            Ok(pr) => {
                pr
            },
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };
    }

    #[test]
    fn test_parse_query_aggs() {
        let test_val = r#"{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "type": "task"
          }
        }
      ]
    }
  },
  "aggs": {
    "taskType": {
      "terms": {
        "size": 100,
        "field": "task.taskType"
      },
      "aggs": {
        "status": {
          "terms": {
            "field": "task.status"
          }
        }
      }
    },
    "schedule": {
      "terms": {
        "field": "task.schedule.interval",
        "size": 100
      }
    },
    "nonRecurringTasks": {
      "missing": {
        "field": "task.schedule"
      }
    },
    "ownerIds": {
      "filter": {
        "range": {
          "task.startedAt": {
            "gte": "now-1w/w"
          }
        }
      },
      "aggs": {
        "ownerIds": {
          "cardinality": {
            "field": "task.ownerId"
          }
        }
      }
    },
    "idleTasks": {
      "filter": {
        "term": {
          "task.status": "idle"
        }
      },
      "aggs": {
        "scheduleDensity": {
          "range": {
            "field": "task.runAt",
            "ranges": [
              {
                "from": "now",
                "to": "now+2m"
              }
            ]
          },
          "aggs": {
            "histogram": {
              "date_histogram": {
                "field": "task.runAt",
                "fixed_interval": "3s"
              },
              "aggs": {
                "interval": {
                  "terms": {
                    "field": "task.schedule.interval"
                  }
                }
              }
            }
          }
        },
        "overdue": {
          "filter": {
            "range": {
              "task.runAt": {
                "lt": "now"
              }
            }
          },
          "aggs": {
            "nonRecurring": {
              "missing": {
                "field": "task.schedule"
              }
            }
          }
        }
      }
    }
  },
  "size": 0,
  "track_total_hits": true
}"#;

        let _parse_result: SearchBody = match serde_json::from_str(test_val) {
            Ok(pr) => pr,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        //let _command = to_command(Some("foobar".to_string()), &parse_result);

        let test_val  = r#"
        {
           "query": {
             "match": {
               "message": {
                 "query": "Login"
               }
             }
           },
           "aggs": {
             "messageType": {
               "terms": {
                 "field": "message"
               }          
            }
           }
        }"#;

        let parse_result: SearchBody = match serde_json::from_str(test_val) {
            Ok(pr) => pr,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let _command = to_command(Some("foobar".to_string()), &parse_result, &QueryStringSearch::new());
    }
}
