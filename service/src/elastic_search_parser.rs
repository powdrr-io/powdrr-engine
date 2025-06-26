
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::elastic_search_commands::{SqlCommand, UpdateByQueryCommand};
use crate::elastic_search_common::{Command, ParseError};


// Example:
// {
//   "query": {
//     "match": {
//       "message": {
//         "query": "this is a test"
//       }
//     }
//   }
// }


pub fn parse(table: Option<String>, val: &String) -> Result<Arc<dyn Command>, ParseError> {
    let body: SearchBody = serde_json::from_str(val.as_str()).map_err(|e|ParseError{ message: format!("{}", e)})?;
    let command = to_command(table, &body)?;
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
pub(crate) struct AggSpecCardinality {
    cardinality: HashMap<String, String>,
    aggs: Option<HashMap<String, AggSpec>>
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AggSpecRange {
    range: AggSpecFilterRangeBody,
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
}


#[derive(Serialize, Deserialize, Clone)]
struct SearchBody {
    pit: Option<PitInfo>,
    size: Option<u32>,
    from: Option<u32>,
    seq_no_primary_term: Option<bool>,
    query: Query,
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

pub(crate) enum FilterExpression {
    And(Vec<FilterExpression>),
    Or(Vec<FilterExpression>),
    Not(Box<FilterExpression>),
    Expr(String),
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
pub(crate) struct ScriptBlock {
    pub source: String,
    pub lang: String,
    #[serde(default)]
    pub params: HashMap<String, Value>,
}

struct SqlBuilder {
    fields: Vec<String>,
    joins: Vec<String>,
    filter_stack: Cell<Vec<Vec<FilterExpression>>>,
    limit: Option<u64>,
    calculate_score: bool,
    order_by: Vec<String>,
}

impl SqlBuilder {
    fn new() -> Self {
        SqlBuilder { 
            fields: vec!(), 
            joins: vec!(), 
            filter_stack: Cell::new(vec!(vec!())),
            limit: None, 
            calculate_score: false,
            order_by: vec!(),
        }
    }

    fn push_filter_context(&mut self) -> &mut Self {
        self.filter_stack.get_mut().push(vec!());
        self
    }

    fn pop_filter_context(&mut self, is_and: bool) -> &mut Self {
        let local_filter_stack = self.filter_stack.get_mut();
        assert!(local_filter_stack.len() > 0);

        let filter = match is_and {
            true => FilterExpression::And(local_filter_stack.pop().unwrap()),
            false => FilterExpression::Or(local_filter_stack.pop().unwrap()),
        };

        let local_last = local_filter_stack.last_mut().unwrap();
        local_last.push(filter);
        self
    }

    fn filter(&mut self, filter: String) -> &mut Self {
        let local_filter_stack = self.filter_stack.get_mut();
        local_filter_stack.last_mut().unwrap().push(FilterExpression::Expr(filter));
        self
    }

    fn not_last_filter(&mut self) -> &mut Self {
        let local_filter_stack = self.filter_stack.get_mut();
        let local_last = local_filter_stack.last_mut().unwrap();
        assert!(local_last.len() > 0);
        let not_filter = FilterExpression::Not(Box::new(local_last.pop().unwrap()));
        local_last.push(not_filter);
        self
    }

    fn _fields(&self) -> String {
        if self.fields.len() == 0 {
            "*".to_string()
        } else {
            self.fields.join(", ")
        }
    }

    fn _joins(&self) -> String {
        if self.joins.len() == 0 {
            " LEFT JOIN {deletes_table} dt on t._id = dt._id".to_string()
        } else {
            format!(" {} LEFT JOIN {{deletes_table}} dt on t._id = dt._id", self.joins.join(" "))
        }
    }

    fn _filters(&mut self) -> String {
        let local_filter_stack = self.filter_stack.get_mut();
        assert!(local_filter_stack.len() == 1);
        let top = local_filter_stack.pop().unwrap();
        if top.len() == 0 {
            " WHERE (dt._seq_no is null or t._seq_no > dt._seq_no)".to_string()
        } else {
            format!(" WHERE (dt._seq_no is null or t._seq_no > dt._seq_no) AND {}", SqlBuilder::format_filter(&FilterExpression::And(top)))
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
            // TODO
            "".to_string()
        }
    }

    fn score(&self) -> bool {
        self.calculate_score
    }

    fn build(&mut self) -> String {
        let filter_str = self._filters();
        format!(
            "select {} from {} {}{}{}{}",
            self._fields(),
            "{target_table} t",
            self._joins(),
            filter_str,
            self._order_by(),
            self._limit()
        )
    }
}

fn agg_to_sql(spec: &AggSpec) -> String {
    match spec {
        AggSpec::Terms(terms) => {
            let field_name = terms.terms.field.clone();
            format!("select {field_name}, count(1) from {{target_table}} t group by {field_name}")
        },
        AggSpec::Filter(_filter) => {
            todo!()
        },
        AggSpec::Missing(_missing) => {
            todo!()
        },
        AggSpec::DateHistogram(_hist) => {
            todo!()
        },
        AggSpec::Cardinality(_cardinality) => {
            todo!()
        } ,
        AggSpec::Range(_range) => {
            todo!()
        }
    }
}

fn aggs_to_sql(aggs: Option<HashMap<String, AggSpec>>) -> Option<HashMap<String, String>> {
    if aggs.is_none() {
        return None;
    }

    Some(aggs.unwrap().iter().map(|x| (x.0.clone(), agg_to_sql(x.1))).collect())
}

fn to_command(table: Option<String>, body: &SearchBody) -> Result<SqlCommand, ParseError> {
    let mut builder = SqlBuilder::new();

    if body.from.is_some() {
        if body.from.unwrap() != 0 {
            panic!("Not implemented");
        }
    }

    to_command_worker(&mut builder, &body.query)?;

    let table_name = match table {
        Some(t) => t,
        None => match &body.pit {
            // TODO: parse the pit to get the table name
            Some(p) => p.id.clone(),
            None => panic!("Didn't find a table")
        }
    };

    let aggs = aggs_to_sql(body.aggs.clone());

    Ok(SqlCommand{ sql: builder.build(), table: table_name, calculate_score: builder.score(), aggs: aggs })
}

fn to_command_update_by_query(table: Option<String>, body: &UpdateByQueryBody) -> Result<UpdateByQueryCommand, ParseError> {
    let mut builder = SqlBuilder::new();

    to_command_worker(&mut builder, &body.query)?;

    let table_name = match table {
        Some(t) => t,
        None => panic!("Didn't find a table")
    };

    Ok(UpdateByQueryCommand{
        query_command: SqlCommand{ sql: builder.build(), table: table_name, calculate_score: builder.score(), aggs: None },
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
        Query::SimpleQueryString(s) => to_sql_simple_query(builder, s),
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
        builder.joins.push("inner join {target_table}_search_index si on si.doc_id = t.index_col".to_string());
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

        builder.pop_filter_context(false);        
        builder.not_last_filter();
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
        builder.filter(format!("t.{} = '{}'", pair.0, pair.1));
    }
    Ok(())
}

fn to_sql_exists(builder: &mut SqlBuilder, exists_obj: &Exists) -> Result<(), ParseError> {
    builder.filter(format!("t.{} is not None", exists_obj.exists.field));
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
        builder.joins.push("inner join {target_table}_search_index si on si.doc_id = t.index_col".to_string());
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
}"#.to_string()
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

        let command = to_command(Some("testtime".to_string()), &parse_result).unwrap();

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

        let command = to_command(Some("testtime".to_string()), &parse_result).unwrap();

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

        let command = to_command(Some("testtime".to_string()), &parse_result).unwrap();

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

        let _command = to_command(Some("fake_name".to_string()), &parse_result);



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

        let _command = to_command(Some("foobar".to_string()), &parse_result);        
    }
}
