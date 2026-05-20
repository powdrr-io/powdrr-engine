use crate::elastic_search_commands::UpdateByQueryCommand;
use crate::elastic_search_common::ParseError;
use crate::elastic_search_endpoints::QueryStringSearch;
use crate::search_executor::{
    SearchCommand, search_plan_to_command, update_by_query_plan_to_command,
};
use crate::search_plan;
use crate::search_runtime::ScriptBlock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub fn parse(
    table: Option<String>,
    val: &String,
    query: &QueryStringSearch,
) -> Result<SearchCommand, ParseError> {
    let plan = parse_search_plan(table, val)?;
    search_plan_to_command(plan, query)
}

pub fn parse_search_plan(
    table: Option<String>,
    val: &String,
) -> Result<search_plan::SearchPlan, ParseError> {
    let body: SearchBody = serde_json::from_str(val.as_str()).map_err(|e| ParseError {
        message: format!("{}", e),
    })?;
    to_search_plan(table, &body)
}

pub fn parse_update_by_query(
    table: Option<String>,
    val: &String,
) -> Result<UpdateByQueryCommand, ParseError> {
    let plan = parse_update_by_query_plan(table, val)?;
    update_by_query_plan_to_command(plan)
}

pub fn parse_update_by_query_plan(
    table: Option<String>,
    val: &String,
) -> Result<search_plan::UpdateByQueryPlan, ParseError> {
    let body: UpdateByQueryBody = match serde_json::from_str::<UpdateByQueryBody>(val.as_str()) {
        Ok(b) => b,
        Err(e) => {
            let error = format!("{}", e);
            println!("{}", error);
            return Err(ParseError { message: error });
        }
    };
    to_update_by_query_plan(table, &body)
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum SortSection {
    Single(SortType),
    Multiple(Vec<SortType>),
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
    order: Option<HashMap<String, String>>,
    missing: Option<Value>,
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
    aggs: Option<HashMap<String, AggSpec>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecFilterTerm {
    term: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecFilterRangeSpan {
    from: String,
    to: String,
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
    Raw(HashMap<String, RangeSpecOperator>),
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
    aggs: Option<HashMap<String, AggSpec>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecDateHistogramBody {
    field: String,
    fixed_interval: String,
    min_doc_count: Option<u64>,
    extended_bounds: Option<AggSpecDateHistogramExtendedBoundsBody>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecDateHistogramExtendedBoundsBody {
    min: Value,
    max: Value,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecDateHistogram {
    date_histogram: AggSpecDateHistogramBody,
    aggs: Option<HashMap<String, AggSpec>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecCardinalityBody {
    field: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecCardinality {
    cardinality: AggSpecCardinalityBody,
    aggs: Option<HashMap<String, AggSpec>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecRange {
    range: AggSpecFilterRangeBody,
    aggs: Option<HashMap<String, AggSpec>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecAverageBody {
    field: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecAverage {
    avg: AggSpecAverageBody,
    aggs: Option<HashMap<String, AggSpec>>,
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
    search_after: Option<Vec<Value>>,
    seq_no_primary_term: Option<bool>,
    query: Option<Query>,
    aggs: Option<HashMap<String, AggSpec>>,
    sort: Option<SortSection>,
}

#[derive(Serialize, Deserialize, Clone)]
struct PitInfo {
    id: String,
    keep_alive: String,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum Query {
    Match(Match),
    MultiMatch(MultiMatch),
    Bool(Bool),
    Term(Term),
    Terms(Terms),
    Ids(Ids),
    Exists(Exists),
    QueryString(QueryString),
    SimpleQueryString(SimpleQueryString),
    Range(Range),
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Match {
    #[serde(rename = "match")]
    _match: HashMap<String, FieldMatch>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct MultiMatch {
    multi_match: MultiMatchBody,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct MultiMatchBody {
    query: String,
    fields: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum FieldMatch {
    String(String),
    Struct(FieldMatchBody),
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct FieldMatchBody {
    query: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Bool {
    #[serde(rename = "bool")]
    _bool: BoolBody,
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
                vec![*s.clone()]
            }
            SingleOrVec::Vec(v) => v.clone(),
        }
    }
}

fn default_single_or_vec() -> SingleOrVec {
    SingleOrVec::Vec(vec![])
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
pub(crate) struct Terms {
    terms: HashMap<String, Vec<Value>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Ids {
    ids: IdsBody,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct IdsBody {
    values: Vec<Value>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Exists {
    exists: ExistsBody,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ExistsBody {
    field: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RangeSpecGt {
    gt: Value,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RangeSpecGte {
    gte: Value,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RangeSpecLt {
    lt: Value,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RangeSpecLte {
    lte: Value,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum RangeSpecOperator {
    GT(RangeSpecGt),
    GTE(RangeSpecGte),
    LT(RangeSpecLt),
    LTE(RangeSpecLte),
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RangeSpec {
    #[serde(flatten)]
    op: RangeSpecOperator,
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

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct QueryString {
    query_string: QueryStringBody,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct QueryStringBody {
    query: String,
    fields: Option<Vec<String>>,
    default_operator: Option<String>,
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

fn to_search_plan(
    table: Option<String>,
    body: &SearchBody,
) -> Result<search_plan::SearchPlan, ParseError> {
    let target = match table {
        Some(table_name) => search_plan::SearchTarget::Table(table_name),
        None => match &body.pit {
            Some(pit) => search_plan::SearchTarget::Pit(search_plan::PitTarget {
                id: pit.id.clone(),
                keep_alive: pit.keep_alive.clone(),
            }),
            None => {
                return Err(ParseError {
                    message: "Didn't find a table".to_string(),
                });
            }
        },
    };

    Ok(search_plan::SearchPlan {
        target,
        from: body.from.unwrap_or(0),
        size: body.size,
        search_after: body.search_after.clone(),
        seq_no_primary_term: body.seq_no_primary_term,
        query: body.query.as_ref().map(to_query_plan).transpose()?,
        aggregations: aggs_to_plan(&body.aggs)?,
        sort: sort_section_to_plans(&body.sort),
    })
}

fn to_update_by_query_plan(
    table: Option<String>,
    body: &UpdateByQueryBody,
) -> Result<search_plan::UpdateByQueryPlan, ParseError> {
    let table_name = match table {
        Some(table_name) => table_name,
        None => {
            return Err(ParseError {
                message: "Didn't find a table".to_string(),
            });
        }
    };

    Ok(search_plan::UpdateByQueryPlan {
        table: table_name,
        query: to_query_plan(&body.query)?,
        script: to_script_plan(&body.script),
        sort: sort_vec_to_plans(&body.sort),
        max_docs: body.max_docs,
        conflicts: body.conflicts.clone(),
    })
}

fn to_script_plan(script: &ScriptBlock) -> search_plan::ScriptPlan {
    search_plan::ScriptPlan {
        source: script.source.clone(),
        lang: script.lang.clone(),
        params: script.params.clone(),
    }
}

fn to_query_plan(query: &Query) -> Result<search_plan::QueryPlan, ParseError> {
    match query {
        Query::Match(match_obj) => {
            let mut clauses = match_obj
                ._match
                .iter()
                .map(|(field, value)| search_plan::MatchClausePlan {
                    field: field.clone(),
                    query: to_field_term(value),
                })
                .collect::<Vec<_>>();
            clauses.sort_by(|left, right| left.field.cmp(&right.field));
            Ok(search_plan::QueryPlan::Match(search_plan::MatchPlan {
                clauses,
            }))
        }
        Query::MultiMatch(multi_match) => {
            if multi_match.multi_match.fields.is_empty() {
                return Err(ParseError {
                    message: "`multi_match` query requires at least one field".to_string(),
                });
            }

            let mut should = multi_match
                .multi_match
                .fields
                .iter()
                .map(|field| {
                    search_plan::QueryPlan::Match(search_plan::MatchPlan {
                        clauses: vec![search_plan::MatchClausePlan {
                            field: field.clone(),
                            query: multi_match.multi_match.query.clone(),
                        }],
                    })
                })
                .collect::<Vec<_>>();
            should.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));

            if should.len() == 1 {
                Ok(should.pop().unwrap())
            } else {
                Ok(search_plan::QueryPlan::Bool(search_plan::BoolPlan {
                    filter: vec![],
                    should,
                    must: vec![],
                    must_not: vec![],
                    minimum_should_match: Some(1),
                }))
            }
        }
        Query::Bool(bool_obj) => Ok(search_plan::QueryPlan::Bool(search_plan::BoolPlan {
            filter: bool_obj
                ._bool
                .filter
                .as_vec()
                .iter()
                .map(to_query_plan)
                .collect::<Result<Vec<_>, _>>()?,
            should: bool_obj
                ._bool
                .should
                .as_vec()
                .iter()
                .map(to_query_plan)
                .collect::<Result<Vec<_>, _>>()?,
            must: bool_obj
                ._bool
                .must
                .as_vec()
                .iter()
                .map(to_query_plan)
                .collect::<Result<Vec<_>, _>>()?,
            must_not: bool_obj
                ._bool
                .must_not
                .as_vec()
                .iter()
                .map(to_query_plan)
                .collect::<Result<Vec<_>, _>>()?,
            minimum_should_match: bool_obj._bool.minimum_should_match,
        })),
        Query::Term(term_obj) => {
            let mut clauses = term_obj
                .term
                .iter()
                .map(|(field, value)| search_plan::TermClausePlan {
                    field: field.clone(),
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            clauses.sort_by(|left, right| left.field.cmp(&right.field));
            Ok(search_plan::QueryPlan::Term(search_plan::TermPlan {
                clauses,
            }))
        }
        Query::Terms(terms_obj) => {
            let (field, values) = terms_obj.terms.iter().next().ok_or_else(|| ParseError {
                message: "`terms` query requires exactly one field".to_string(),
            })?;
            if terms_obj.terms.len() != 1 {
                return Err(ParseError {
                    message: "`terms` query requires exactly one field".to_string(),
                });
            }
            term_values_to_query_plan(
                field.clone(),
                values,
                "`terms` query requires at least one value",
            )
        }
        Query::Ids(ids_obj) => term_values_to_query_plan(
            "_id".to_string(),
            &ids_obj.ids.values,
            "`ids` query requires at least one value",
        ),
        Query::Exists(exists_obj) => Ok(search_plan::QueryPlan::Exists(search_plan::ExistsPlan {
            field: exists_obj.exists.field.clone(),
        })),
        Query::QueryString(query_string) => query_string_to_plan(&query_string.query_string),
        Query::Range(range_obj) => {
            let mut clauses = range_obj
                .range
                .iter()
                .map(|(field, spec)| search_plan::RangeClausePlan {
                    field: field.clone(),
                    operator: range_operator_to_plan(&spec.op),
                    format: spec.format.clone(),
                    relation: spec.relation.clone(),
                    time_zone: spec.time_zone.clone(),
                    boost: spec.boost,
                })
                .collect::<Vec<_>>();
            clauses.sort_by(|left, right| left.field.cmp(&right.field));
            Ok(search_plan::QueryPlan::Range(search_plan::RangePlan {
                clauses,
            }))
        }
        Query::SimpleQueryString(simple_query) => Ok(search_plan::QueryPlan::SimpleQueryString(
            search_plan::SimpleQueryStringPlan {
                query: simple_query.simple_query_string.query.clone(),
                fields: simple_query.simple_query_string.fields.clone(),
                default_operator: simple_query.simple_query_string.default_operator.clone(),
            },
        )),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueryStringToken {
    LParen,
    RParen,
    And,
    Or,
    Word(String),
    Quoted(String),
}

fn query_string_to_plan(body: &QueryStringBody) -> Result<search_plan::QueryPlan, ParseError> {
    let default_operator = body
        .default_operator
        .clone()
        .unwrap_or_else(|| "OR".to_string())
        .to_ascii_uppercase();
    if default_operator != "AND" && default_operator != "OR" {
        return Err(ParseError {
            message: "`query_string.default_operator` only supports `AND` or `OR`".to_string(),
        });
    }

    let tokens = tokenize_query_string(&body.query)?;
    if tokens.is_empty() {
        return Err(ParseError {
            message: "`query_string.query` cannot be empty".to_string(),
        });
    }
    let tokens = insert_implicit_query_string_operators(&tokens, &default_operator);
    let mut index = 0;
    let plan = parse_query_string_or(&tokens, &mut index, body.fields.as_ref())?;
    if index != tokens.len() {
        return Err(ParseError {
            message: "unsupported trailing token in `query_string`".to_string(),
        });
    }
    Ok(plan)
}

fn tokenize_query_string(query: &str) -> Result<Vec<QueryStringToken>, ParseError> {
    let mut tokens = Vec::new();
    let chars = query.chars().collect::<Vec<_>>();
    let mut index = 0;
    while index < chars.len() {
        let ch = chars[index];
        if ch.is_whitespace() {
            index += 1;
            continue;
        }
        if ch == '(' {
            tokens.push(QueryStringToken::LParen);
            index += 1;
            continue;
        }
        if ch == ')' {
            tokens.push(QueryStringToken::RParen);
            index += 1;
            continue;
        }
        if ch == '"' {
            index += 1;
            let mut value = String::new();
            let mut terminated = false;
            while index < chars.len() {
                let current = chars[index];
                if current == '\\' && index + 1 < chars.len() {
                    value.push(chars[index + 1]);
                    index += 2;
                    continue;
                }
                if current == '"' {
                    terminated = true;
                    index += 1;
                    break;
                }
                value.push(current);
                index += 1;
            }
            if !terminated {
                return Err(ParseError {
                    message: "unterminated quoted string in `query_string`".to_string(),
                });
            }
            tokens.push(QueryStringToken::Quoted(value));
            continue;
        }

        let start = index;
        while index < chars.len()
            && !chars[index].is_whitespace()
            && chars[index] != '('
            && chars[index] != ')'
        {
            index += 1;
        }
        let raw = chars[start..index].iter().collect::<String>();
        let token = match raw.to_ascii_uppercase().as_str() {
            "AND" => QueryStringToken::And,
            "OR" => QueryStringToken::Or,
            _ => QueryStringToken::Word(raw),
        };
        tokens.push(token);
    }
    Ok(tokens)
}

fn insert_implicit_query_string_operators(
    tokens: &[QueryStringToken],
    default_operator: &str,
) -> Vec<QueryStringToken> {
    let implicit = if default_operator == "AND" {
        QueryStringToken::And
    } else {
        QueryStringToken::Or
    };

    let mut expanded = Vec::with_capacity(tokens.len() * 2);
    for token in tokens.iter() {
        if let Some(previous) = expanded.last() {
            if query_string_token_can_end_primary(previous)
                && query_string_token_can_start_primary(token)
            {
                expanded.push(implicit.clone());
            }
        }
        expanded.push(token.clone());
    }
    expanded
}

fn query_string_token_can_end_primary(token: &QueryStringToken) -> bool {
    matches!(
        token,
        QueryStringToken::Quoted(_) | QueryStringToken::RParen
    ) || matches!(token, QueryStringToken::Word(word) if !word.ends_with(':'))
}

fn query_string_token_can_start_primary(token: &QueryStringToken) -> bool {
    matches!(
        token,
        QueryStringToken::Word(_) | QueryStringToken::Quoted(_) | QueryStringToken::LParen
    )
}

fn parse_query_string_or(
    tokens: &[QueryStringToken],
    index: &mut usize,
    fields: Option<&Vec<String>>,
) -> Result<search_plan::QueryPlan, ParseError> {
    let mut should = vec![parse_query_string_and(tokens, index, fields)?];
    while *index < tokens.len() && matches!(tokens[*index], QueryStringToken::Or) {
        *index += 1;
        should.push(parse_query_string_and(tokens, index, fields)?);
    }
    if should.len() == 1 {
        Ok(should.pop().unwrap())
    } else {
        Ok(search_plan::QueryPlan::Bool(search_plan::BoolPlan {
            filter: vec![],
            should,
            must: vec![],
            must_not: vec![],
            minimum_should_match: Some(1),
        }))
    }
}

fn parse_query_string_and(
    tokens: &[QueryStringToken],
    index: &mut usize,
    fields: Option<&Vec<String>>,
) -> Result<search_plan::QueryPlan, ParseError> {
    let mut must = vec![parse_query_string_primary(tokens, index, fields)?];
    while *index < tokens.len() && matches!(tokens[*index], QueryStringToken::And) {
        *index += 1;
        must.push(parse_query_string_primary(tokens, index, fields)?);
    }
    if must.len() == 1 {
        Ok(must.pop().unwrap())
    } else {
        Ok(search_plan::QueryPlan::Bool(search_plan::BoolPlan {
            filter: vec![],
            should: vec![],
            must,
            must_not: vec![],
            minimum_should_match: None,
        }))
    }
}

fn parse_query_string_primary(
    tokens: &[QueryStringToken],
    index: &mut usize,
    fields: Option<&Vec<String>>,
) -> Result<search_plan::QueryPlan, ParseError> {
    let Some(token) = tokens.get(*index) else {
        return Err(ParseError {
            message: "unexpected end of `query_string` expression".to_string(),
        });
    };

    match token {
        QueryStringToken::LParen => {
            *index += 1;
            let plan = parse_query_string_or(tokens, index, fields)?;
            match tokens.get(*index) {
                Some(QueryStringToken::RParen) => {
                    *index += 1;
                    Ok(plan)
                }
                _ => Err(ParseError {
                    message: "unclosed parenthesis in `query_string`".to_string(),
                }),
            }
        }
        QueryStringToken::Word(word) => {
            *index += 1;
            query_string_word_to_plan(word, tokens, index, fields)
        }
        QueryStringToken::Quoted(value) => {
            *index += 1;
            lower_bare_query_string_term(value, fields)
        }
        QueryStringToken::And | QueryStringToken::Or | QueryStringToken::RParen => {
            Err(ParseError {
                message: "unsupported operator placement in `query_string`".to_string(),
            })
        }
    }
}

fn query_string_word_to_plan(
    word: &str,
    tokens: &[QueryStringToken],
    index: &mut usize,
    fields: Option<&Vec<String>>,
) -> Result<search_plan::QueryPlan, ParseError> {
    if word.eq_ignore_ascii_case("NOT") || word.starts_with('-') || word.starts_with('+') {
        return Err(ParseError {
            message:
                "restricted `query_string` does not support NOT, required, or prohibited clauses"
                    .to_string(),
        });
    }
    reject_unsupported_query_string_fragment(word)?;

    if let Some(field) = word.strip_suffix(':') {
        let Some(value_token) = tokens.get(*index) else {
            return Err(ParseError {
                message: "fielded `query_string` clause is missing a value".to_string(),
            });
        };
        *index += 1;
        let value = query_string_value_token(value_token)?;
        return lower_fielded_query_string_clause(field, &value);
    }

    if let Some((field, value)) = word.split_once(':') {
        return lower_fielded_query_string_clause(field, value);
    }

    lower_bare_query_string_term(word, fields)
}

fn query_string_value_token(token: &QueryStringToken) -> Result<String, ParseError> {
    match token {
        QueryStringToken::Word(word) => {
            reject_unsupported_query_string_fragment(word)?;
            Ok(word.clone())
        }
        QueryStringToken::Quoted(value) => Ok(value.clone()),
        _ => Err(ParseError {
            message: "fielded `query_string` clause requires a literal value".to_string(),
        }),
    }
}

fn reject_unsupported_query_string_fragment(fragment: &str) -> Result<(), ParseError> {
    if fragment.contains('*')
        || fragment.contains('?')
        || fragment.contains('~')
        || fragment.contains('^')
    {
        return Err(ParseError {
            message: "restricted `query_string` does not support wildcards, fuzziness, or boosts"
                .to_string(),
        });
    }
    Ok(())
}

fn lower_bare_query_string_term(
    value: &str,
    fields: Option<&Vec<String>>,
) -> Result<search_plan::QueryPlan, ParseError> {
    let Some(fields) = fields else {
        return Err(ParseError {
            message: "restricted `query_string` requires `fields` for bare terms".to_string(),
        });
    };
    if fields.is_empty() {
        return Err(ParseError {
            message: "restricted `query_string` requires at least one field".to_string(),
        });
    }

    let mut should = fields
        .iter()
        .map(|field| lower_fielded_query_string_clause(field, value))
        .collect::<Result<Vec<_>, _>>()?;
    should.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));

    if should.len() == 1 {
        Ok(should.pop().unwrap())
    } else {
        Ok(search_plan::QueryPlan::Bool(search_plan::BoolPlan {
            filter: vec![],
            should,
            must: vec![],
            must_not: vec![],
            minimum_should_match: Some(1),
        }))
    }
}

fn lower_fielded_query_string_clause(
    field: &str,
    value: &str,
) -> Result<search_plan::QueryPlan, ParseError> {
    if field.is_empty() {
        return Err(ParseError {
            message: "restricted `query_string` requires a non-empty field".to_string(),
        });
    }
    reject_unsupported_query_string_fragment(field)?;
    if let Some(term_value) = query_string_value_to_term_value(field, value) {
        return Ok(search_plan::QueryPlan::Term(search_plan::TermPlan {
            clauses: vec![search_plan::TermClausePlan {
                field: field.to_string(),
                value: term_value,
            }],
        }));
    }

    Ok(search_plan::QueryPlan::Match(search_plan::MatchPlan {
        clauses: vec![search_plan::MatchClausePlan {
            field: field.to_string(),
            query: value.to_string(),
        }],
    }))
}

fn query_string_value_to_term_value(field: &str, value: &str) -> Option<Value> {
    if value.eq_ignore_ascii_case("true") {
        return Some(Value::Bool(true));
    }
    if value.eq_ignore_ascii_case("false") {
        return Some(Value::Bool(false));
    }
    if field_uses_exact_match_for_strings(field) {
        return Some(Value::String(value.to_string()));
    }
    if value.starts_with('"') || value.contains(' ') {
        return None;
    }
    if let Ok(parsed) = value.parse::<i64>() {
        return Some(Value::from(parsed));
    }
    if let Ok(parsed) = value.parse::<f64>() {
        if parsed.is_finite() {
            return serde_json::Number::from_f64(parsed).map(Value::Number);
        }
    }
    Some(Value::String(value.to_string()))
}

fn field_uses_exact_match_for_strings(value: &str) -> bool {
    value == "_id" || value.ends_with(".keyword")
}

fn terms_order_to_plan(
    order: Option<&HashMap<String, String>>,
) -> Result<Option<search_plan::TermsOrderPlan>, ParseError> {
    let Some(order) = order else {
        return Ok(None);
    };
    if order.len() != 1 {
        return Err(ParseError {
            message: "`terms.order` only supports a single `_count` or `_key` entry".to_string(),
        });
    }
    let (field, direction) = order.iter().next().unwrap();
    let direction = direction.to_ascii_lowercase();
    let plan = match (field.as_str(), direction.as_str()) {
        ("_count", "asc") => search_plan::TermsOrderPlan::CountAsc,
        ("_count", "desc") => search_plan::TermsOrderPlan::CountDesc,
        ("_key", "asc") => search_plan::TermsOrderPlan::KeyAsc,
        ("_key", "desc") => search_plan::TermsOrderPlan::KeyDesc,
        _ => {
            return Err(ParseError {
                message: "`terms.order` only supports `_count` or `_key` with `asc` or `desc`"
                    .to_string(),
            });
        }
    };
    Ok(Some(plan))
}

fn term_values_to_query_plan(
    field: String,
    values: &[Value],
    empty_message: &str,
) -> Result<search_plan::QueryPlan, ParseError> {
    let mut should = values
        .iter()
        .map(|value| {
            search_plan::QueryPlan::Term(search_plan::TermPlan {
                clauses: vec![search_plan::TermClausePlan {
                    field: field.clone(),
                    value: value.clone(),
                }],
            })
        })
        .collect::<Vec<_>>();

    match should.len() {
        0 => Err(ParseError {
            message: empty_message.to_string(),
        }),
        1 => Ok(should.pop().unwrap()),
        _ => Ok(search_plan::QueryPlan::Bool(search_plan::BoolPlan {
            filter: vec![],
            should,
            must: vec![],
            must_not: vec![],
            minimum_should_match: Some(1),
        })),
    }
}

fn range_operator_to_plan(operator: &RangeSpecOperator) -> search_plan::RangeOperatorPlan {
    match operator {
        RangeSpecOperator::GT(value) => search_plan::RangeOperatorPlan::Gt(value.gt.clone()),
        RangeSpecOperator::GTE(value) => search_plan::RangeOperatorPlan::Gte(value.gte.clone()),
        RangeSpecOperator::LT(value) => search_plan::RangeOperatorPlan::Lt(value.lt.clone()),
        RangeSpecOperator::LTE(value) => search_plan::RangeOperatorPlan::Lte(value.lte.clone()),
    }
}

fn sort_section_to_plans(sort: &Option<SortSection>) -> Vec<search_plan::SortPlan> {
    match sort {
        Some(SortSection::Single(sort)) => vec![sort_type_to_plan(sort)],
        Some(SortSection::Multiple(sort)) => sort.iter().map(sort_type_to_plan).collect(),
        None => vec![],
    }
}

fn sort_vec_to_plans(sort: &Option<Vec<SortType>>) -> Vec<search_plan::SortPlan> {
    sort.as_ref()
        .map(|values| values.iter().map(sort_type_to_plan).collect())
        .unwrap_or_default()
}

fn sort_type_to_plan(sort: &SortType) -> search_plan::SortPlan {
    match sort {
        SortType::Bare(field) => search_plan::SortPlan::Bare(field.clone()),
        SortType::Parameterized(parameters) => {
            assert_eq!(parameters.len(), 1);
            let (field, body) = parameters.iter().next().unwrap();
            search_plan::SortPlan::Field {
                field: field.clone(),
                order: body.order.clone(),
                unmapped_type: body.unmapped_type.clone(),
                script: body.script.as_ref().map(to_script_plan),
            }
        }
    }
}

fn aggs_to_plan(
    aggs: &Option<HashMap<String, AggSpec>>,
) -> Result<Vec<search_plan::AggregationPlan>, ParseError> {
    let Some(aggs) = aggs else {
        return Ok(vec![]);
    };

    let mut plans = aggs
        .iter()
        .map(|(name, spec)| agg_spec_to_plan(name, spec))
        .collect::<Result<Vec<_>, _>>()?;
    plans.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(plans)
}

fn agg_spec_to_plan(
    name: &str,
    spec: &AggSpec,
) -> Result<search_plan::AggregationPlan, ParseError> {
    let spec =
        match spec {
            AggSpec::Terms(terms) => {
                search_plan::AggregationPlanSpec::Terms(search_plan::TermsAggregationPlan {
                    field: terms.terms.field.clone(),
                    size: terms.terms.size,
                    order: terms_order_to_plan(terms.terms.order.as_ref())?,
                    missing: terms.terms.missing.clone(),
                    show_term_doc_count_error: terms.terms.show_term_doc_count_error,
                    sub_aggregations: aggs_to_plan(&terms.aggs)?,
                })
            }
            AggSpec::Missing(missing) => {
                search_plan::AggregationPlanSpec::Missing(search_plan::MissingAggregationPlan {
                    field: missing.missing.field.clone(),
                    size: missing.missing.size,
                    show_term_doc_count_error: missing.missing.show_term_doc_count_error,
                    sub_aggregations: aggs_to_plan(&missing.aggs)?,
                })
            }
            AggSpec::Filter(filter) => {
                search_plan::AggregationPlanSpec::Filter(search_plan::FilterAggregationPlan {
                    filter: aggregation_filter_to_plan(&filter.filter),
                    sub_aggregations: aggs_to_plan(&filter.aggs)?,
                })
            }
            AggSpec::DateHistogram(histogram) => search_plan::AggregationPlanSpec::DateHistogram(
                search_plan::DateHistogramAggregationPlan {
                    field: histogram.date_histogram.field.clone(),
                    fixed_interval: histogram.date_histogram.fixed_interval.clone(),
                    min_doc_count: histogram.date_histogram.min_doc_count,
                    extended_bounds: histogram.date_histogram.extended_bounds.as_ref().map(
                        |bounds| search_plan::DateHistogramExtendedBoundsPlan {
                            min: bounds.min.clone(),
                            max: bounds.max.clone(),
                        },
                    ),
                    sub_aggregations: aggs_to_plan(&histogram.aggs)?,
                },
            ),
            AggSpec::Cardinality(cardinality) => search_plan::AggregationPlanSpec::Cardinality(
                search_plan::CardinalityAggregationPlan {
                    field: cardinality.cardinality.field.clone(),
                    sub_aggregations: aggs_to_plan(&cardinality.aggs)?,
                },
            ),
            AggSpec::Range(range) => {
                search_plan::AggregationPlanSpec::Range(search_plan::RangeAggregationPlan {
                    range: aggregation_range_to_plan(&range.range),
                    sub_aggregations: aggs_to_plan(&range.aggs)?,
                })
            }
            AggSpec::Average(average) => {
                search_plan::AggregationPlanSpec::Average(search_plan::AverageAggregationPlan {
                    field: average.avg.field.clone(),
                    sub_aggregations: aggs_to_plan(&average.aggs)?,
                })
            }
        };

    Ok(search_plan::AggregationPlan {
        name: name.to_string(),
        spec,
    })
}

fn aggregation_filter_to_plan(filter: &AggSpecFilterBody) -> search_plan::AggregationFilterPlan {
    match filter {
        AggSpecFilterBody::Term(term) => {
            assert_eq!(term.term.len(), 1);
            let (field, value) = term.term.iter().next().unwrap();
            search_plan::AggregationFilterPlan::Term {
                field: field.clone(),
                value: value.clone(),
            }
        }
        AggSpecFilterBody::Range(range) => {
            search_plan::AggregationFilterPlan::Range(aggregation_range_to_plan(&range.range))
        }
    }
}

fn aggregation_range_to_plan(
    range: &AggSpecFilterRangeBody,
) -> search_plan::AggregationRangeBoundsPlan {
    match range {
        AggSpecFilterRangeBody::Raw(raw) => {
            assert_eq!(raw.len(), 1);
            let (field, operator) = raw.iter().next().unwrap();
            search_plan::AggregationRangeBoundsPlan::Raw {
                field: field.clone(),
                operator: range_operator_to_plan(operator),
            }
        }
        AggSpecFilterRangeBody::Structured(structured) => {
            search_plan::AggregationRangeBoundsPlan::Structured {
                field: structured.field.clone(),
                ranges: structured
                    .ranges
                    .iter()
                    .map(|range| search_plan::AggregationWindowPlan {
                        from: range.from.clone(),
                        to: range.to.clone(),
                    })
                    .collect(),
            }
        }
    }
}

#[cfg(test)]
fn to_command(
    table: Option<String>,
    body: &SearchBody,
    query: &QueryStringSearch,
) -> Result<SearchCommand, ParseError> {
    let plan = to_search_plan(table, body)?;
    search_plan_to_command(plan, query)
}

fn to_field_term(body: &FieldMatch) -> String {
    match body {
        FieldMatch::String(s) => s.clone(),
        FieldMatch::Struct(s) => s.query.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{SearchBody, to_command};
    use crate::elastic_search_endpoints::QueryStringSearch;
    use crate::elastic_search_parser::{
        UpdateByQueryBody, parse, parse_search_plan, parse_update_by_query_plan,
    };
    use crate::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema};
    use crate::search_plan;

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
}"#
            .to_string(),
            &QueryStringSearch::new(),
        );

        match parse_result {
            Ok(pr) => {
                let sql = pr.legacy_sql_command().unwrap().sql.build_debug();
                assert!(sql.contains("this is a test"))
            }
            _ => panic!("Parsing error"),
        }
    }

    #[test]
    fn test_parse_search_plan_match() {
        let plan = parse_search_plan(
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
}"#
            .to_string(),
        )
        .unwrap();

        assert_eq!(
            plan.target,
            search_plan::SearchTarget::Table("foo".to_string())
        );

        match plan.query.unwrap() {
            search_plan::QueryPlan::Match(match_plan) => {
                assert_eq!(match_plan.clauses.len(), 1);
                assert_eq!(match_plan.clauses[0].field, "message");
                assert_eq!(match_plan.clauses[0].query, "this is a test");
            }
            _ => panic!("Expected match plan"),
        }
    }

    #[test]
    fn test_parse_search_plan_terms() {
        let plan = parse_search_plan(
            Some("foo".to_string()),
            &r#"
{
   "query": {
     "terms": {
       "index_col": [2, 5]
     }
   }
}"#
            .to_string(),
        )
        .unwrap();

        match plan.query.unwrap() {
            search_plan::QueryPlan::Bool(bool_plan) => {
                assert_eq!(bool_plan.should.len(), 2);
                assert_eq!(bool_plan.minimum_should_match, Some(1));
            }
            _ => panic!("Expected bool plan"),
        }
    }

    #[test]
    fn test_parse_search_plan_multi_match() {
        let plan = parse_search_plan(
            Some("foo".to_string()),
            &r#"
{
   "query": {
     "multi_match": {
       "query": "login",
       "fields": ["message", "message.keyword"]
     }
   }
}"#
            .to_string(),
        )
        .unwrap();

        match plan.query.unwrap() {
            search_plan::QueryPlan::Bool(bool_plan) => {
                assert_eq!(bool_plan.should.len(), 2);
                assert_eq!(bool_plan.minimum_should_match, Some(1));
            }
            _ => panic!("Expected bool plan"),
        }
    }

    #[test]
    fn test_parse_search_plan_query_string() {
        let plan = parse_search_plan(
            Some("foo".to_string()),
            &r#"
{
   "query": {
     "query_string": {
       "query": "service:auth AND Login",
       "fields": ["message"],
       "default_operator": "OR"
     }
   }
}"#
            .to_string(),
        )
        .unwrap();

        match plan.query.unwrap() {
            search_plan::QueryPlan::Bool(bool_plan) => {
                assert_eq!(bool_plan.must.len(), 2);
            }
            _ => panic!("Expected bool plan"),
        }
    }

    #[test]
    fn test_parse_search_plan_ids() {
        let plan = parse_search_plan(
            Some("foo".to_string()),
            &r#"
{
   "query": {
     "ids": {
       "values": ["2", "5"]
     }
   }
}"#
            .to_string(),
        )
        .unwrap();

        match plan.query.unwrap() {
            search_plan::QueryPlan::Bool(bool_plan) => {
                assert_eq!(bool_plan.should.len(), 2);
                assert_eq!(bool_plan.minimum_should_match, Some(1));
            }
            _ => panic!("Expected bool plan"),
        }
    }

    #[test]
    fn test_parse_search_plan_query_string_rejects_wildcards() {
        let error = parse_search_plan(
            Some("foo".to_string()),
            &r#"
{
   "query": {
     "query_string": {
       "query": "serv*",
       "fields": ["message"]
     }
   }
}"#
            .to_string(),
        )
        .unwrap_err();

        assert!(error.message.contains("does not support wildcards"));
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
}"#,
        ) {
            Ok(r) => r,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let command = to_command(
            Some("testtime".to_string()),
            &parse_result,
            &QueryStringSearch::new(),
        )
        .unwrap();
        assert_eq!(command.execution_plan().shards.len(), 1);
        assert_eq!(command.execution_plan().shards[0].segments.len(), 1);

        let schema = PowdrrSchema::from(&vec![
            PowdrrField {
                name: "message".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "_seq_no".to_string(),
                data_type: PowdrrDataType::Integer,
            },
        ]);

        let sql = command
            .legacy_sql_command()
            .unwrap()
            .sql
            .build_same(&schema);

        assert!(sql.contains("t.\"message\""));
        assert!(sql.contains("si.\"field_name\" = 'message'"));
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

        let command = to_command(
            Some("testtime".to_string()),
            &parse_result,
            &QueryStringSearch::new(),
        )
        .unwrap();

        let schema = PowdrrSchema::from(&vec![
            PowdrrField {
                name: "type".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "ingest-package-policies_package_name".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "task_runAt".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "_seq_no".to_string(),
                data_type: PowdrrDataType::Integer,
            },
        ]);

        println!(
            "{}",
            command
                .legacy_sql_command()
                .unwrap()
                .sql
                .build_same(&schema)
        );

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

        let command = to_command(
            Some("testtime".to_string()),
            &parse_result,
            &QueryStringSearch::new(),
        )
        .unwrap();

        let schema = PowdrrSchema::from(&vec![
            PowdrrField {
                name: "type".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "ingest-package-policies_package_name".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "_seq_no".to_string(),
                data_type: PowdrrDataType::Integer,
            },
        ]);

        println!(
            "{}",
            command
                .legacy_sql_command()
                .unwrap()
                .sql
                .build_same(&schema)
        );

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

        let _command = to_command(
            Some("fake_name".to_string()),
            &parse_result,
            &QueryStringSearch::new(),
        );
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
            Ok(pr) => pr,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("ERROR");
            }
        };

        let plan =
            parse_update_by_query_plan(Some("foobar".to_string()), &test_val.to_string()).unwrap();

        assert_eq!(plan.table, "foobar");
        assert_eq!(plan.script.lang, "painless");
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

        let histogram_plan = parse_search_plan(
            Some("logs".to_string()),
            &r#"{
  "aggs": {
    "per_day": {
      "date_histogram": {
        "field": "@timestamp",
        "fixed_interval": "1d",
        "min_doc_count": 0,
        "extended_bounds": {
          "min": "2099-03-07T00:00:00.000Z",
          "max": "2099-03-10T00:00:00.000Z"
        }
      }
    }
  }
}"#
            .to_string(),
        )
        .unwrap();

        match &histogram_plan.aggregations[0].spec {
            search_plan::AggregationPlanSpec::DateHistogram(histogram) => {
                assert_eq!(histogram.min_doc_count, Some(0));
                assert!(histogram.extended_bounds.is_some());
            }
            _ => panic!("Expected date histogram plan"),
        }

        let terms_plan = parse_search_plan(
            Some("logs".to_string()),
            &r#"{
  "aggs": {
    "by_service": {
      "terms": {
        "field": "service",
        "size": 10,
        "order": {
          "_key": "asc"
        },
        "missing": "unknown"
      }
    }
  }
}"#
            .to_string(),
        )
        .unwrap();

        match &terms_plan.aggregations[0].spec {
            search_plan::AggregationPlanSpec::Terms(terms) => {
                assert_eq!(terms.order, Some(search_plan::TermsOrderPlan::KeyAsc));
                assert_eq!(
                    terms.missing,
                    Some(serde_json::Value::String("unknown".to_string()))
                );
            }
            _ => panic!("Expected terms plan"),
        }

        let test_val = r#"
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

        let _command = to_command(
            Some("foobar".to_string()),
            &parse_result,
            &QueryStringSearch::new(),
        );
    }
}
