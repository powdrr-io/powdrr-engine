use serde_json::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct SearchPlan {
    pub target: SearchTarget,
    pub from: u32,
    pub size: Option<u32>,
    pub search_after: Option<Vec<Value>>,
    pub seq_no_primary_term: Option<bool>,
    pub query: Option<QueryPlan>,
    pub aggregations: Vec<AggregationPlan>,
    pub sort: Vec<SortPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SearchTarget {
    Table(String),
    Pit(PitTarget),
}

#[derive(Clone, Debug, PartialEq)]
pub struct PitTarget {
    pub id: String,
    pub keep_alive: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortPlan {
    Bare(String),
    Field {
        field: String,
        order: Option<String>,
        unmapped_type: Option<String>,
        script: Option<ScriptPlan>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryPlan {
    Match(MatchPlan),
    Bool(BoolPlan),
    Term(TermPlan),
    Exists(ExistsPlan),
    Range(RangePlan),
    SimpleQueryString(SimpleQueryStringPlan),
}

#[derive(Clone, Debug, PartialEq)]
pub struct MatchPlan {
    pub clauses: Vec<MatchClausePlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MatchClausePlan {
    pub field: String,
    pub query: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoolPlan {
    pub filter: Vec<QueryPlan>,
    pub should: Vec<QueryPlan>,
    pub must: Vec<QueryPlan>,
    pub must_not: Vec<QueryPlan>,
    pub minimum_should_match: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TermPlan {
    pub clauses: Vec<TermClausePlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TermClausePlan {
    pub field: String,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExistsPlan {
    pub field: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RangePlan {
    pub clauses: Vec<RangeClausePlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RangeClausePlan {
    pub field: String,
    pub operator: RangeOperatorPlan,
    pub format: Option<String>,
    pub relation: Option<String>,
    pub time_zone: Option<String>,
    pub boost: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RangeOperatorPlan {
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SimpleQueryStringPlan {
    pub query: String,
    pub fields: Vec<String>,
    pub default_operator: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregationPlan {
    pub name: String,
    pub spec: AggregationPlanSpec,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggregationPlanSpec {
    Terms(TermsAggregationPlan),
    Missing(MissingAggregationPlan),
    Filter(FilterAggregationPlan),
    DateHistogram(DateHistogramAggregationPlan),
    Cardinality(CardinalityAggregationPlan),
    Range(RangeAggregationPlan),
    Average(AverageAggregationPlan),
}

#[derive(Clone, Debug, PartialEq)]
pub struct TermsAggregationPlan {
    pub field: String,
    pub size: Option<u32>,
    pub show_term_doc_count_error: bool,
    pub sub_aggregations: Vec<AggregationPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MissingAggregationPlan {
    pub field: String,
    pub size: Option<u32>,
    pub show_term_doc_count_error: bool,
    pub sub_aggregations: Vec<AggregationPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FilterAggregationPlan {
    pub filter: AggregationFilterPlan,
    pub sub_aggregations: Vec<AggregationPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggregationFilterPlan {
    Term { field: String, value: String },
    Range(AggregationRangeBoundsPlan),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DateHistogramAggregationPlan {
    pub field: String,
    pub fixed_interval: String,
    pub sub_aggregations: Vec<AggregationPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CardinalityAggregationPlan {
    pub field: String,
    pub sub_aggregations: Vec<AggregationPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RangeAggregationPlan {
    pub range: AggregationRangeBoundsPlan,
    pub sub_aggregations: Vec<AggregationPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AverageAggregationPlan {
    pub field: String,
    pub sub_aggregations: Vec<AggregationPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggregationRangeBoundsPlan {
    Raw {
        field: String,
        operator: RangeOperatorPlan,
    },
    Structured {
        field: String,
        ranges: Vec<AggregationWindowPlan>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregationWindowPlan {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateByQueryPlan {
    pub table: String,
    pub query: QueryPlan,
    pub script: ScriptPlan,
    pub sort: Vec<SortPlan>,
    pub max_docs: Option<usize>,
    pub conflicts: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScriptPlan {
    pub source: String,
    pub lang: String,
    pub params: Value,
}
