
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use phf::phf_map;

use crate::elastic_search_commands::{MatchBuilder, SqlCommand, UpdateByQueryCommand};
use crate::elastic_search_common::{Command, ParseError};


fn check_single_entry_dict(value: &Value) -> Result<(String, Value), ParseError> {
    let val_keys: Vec<&String> = match value.as_object() {
        Some(obj) => obj.keys().collect(),
        None => return Err(ParseError{message: "Unexpected format".to_string()})
    };

    if val_keys.len() != 1 {
        return Err(ParseError{message: "Only handling single val just now".to_string()})
    }

    let key = val_keys.get(0).unwrap(); 
    let inner_value = &value[key];
    Ok((key.to_string(), inner_value.clone()))
}


type CommandParserFnPtr = fn(&String, &Value) -> Result<Arc<dyn Command>, ParseError>;


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
fn match_command_parser(table: &String, value: &Value) ->  Result<Arc<dyn Command>, ParseError> {
    let match_values = match check_single_entry_dict(&value) {
        Ok(mv) => mv,
        Err(e) => return Err(e),
    };

    let field = match_values.0;
    let mut match_builder = MatchBuilder::new(table, &field);
    match match_values.1.as_object() {
        Some(v) => {
            for entry in v.iter() {
                if entry.0 == "query" {
                    match entry.1.as_str() {
                        Some(v) => match_builder.query = Some(v.to_string()),
                        None => return Err(ParseError { message: "Expected string".to_string() }),
                    };
                } else {
                    return Err(ParseError { message: "Need to add in more options".to_string() });
                }
            }
        },
        None => return Err(ParseError { message: "Expected values".to_string() }),
    };
    match match_builder.build() {
        Ok(m) => Ok(Arc::new(m)),
        Err(e) => Err(e)
    }
}


static COMMANDS: phf::Map<&'static str, CommandParserFnPtr> = phf_map! {
    "match" => match_command_parser,
};


pub fn old_parse(table: &String, val: &String) -> Result<Arc<dyn Command>, ParseError> {
    let parsed_val: Value = match serde_json::from_str(val) {
        Ok(v) => v,
        Err(_) => return Err(ParseError{message: "Invalid JSON".to_string()})
    };

    let query_val = match parsed_val.get("query") {
        Some(q) => q,
        None => return Err(ParseError{message: "Query must contain 'query'".to_string()})
    };

    let query_val_keys: Vec<&String> = match query_val.as_object() {
        Some(obj) => obj.keys().collect(),
        None => return Err(ParseError{message: "Unexpected format".to_string()})
    };

    if query_val_keys.len() != 1 {
        return Err(ParseError{message: "Only handling single val just now".to_string()})
    }

    let command_name = query_val_keys.get(0).unwrap();
    match COMMANDS.get(&command_name) {
        Some(cp) => cp(table, &query_val[command_name]),
        None => Err(ParseError{message: "Unknown command".to_string()})
    }
}


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
struct SearchBody {
    pit: Option<PitInfo>,
    size: Option<u32>,
    from: Option<u32>,
    seq_no_primary_term: Option<bool>,
    query: Query,
    sort: Option<Vec<HashMap<String, SortBody>>>
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
pub(crate) struct BoolBody {
    #[serde(default)]
    filter: Vec<Query>,
    #[serde(default)]
    should: Vec<Query>,
    #[serde(default)]
    must: Vec<Query>,
    #[serde(default)]
    must_not: Vec<Query>,
    minimum_should_match: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Term {
    term: HashMap<String, String>,
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
pub(crate) struct SortBody {
    order: String,
    unmapped_type: String
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
    sort: Option<Vec<HashMap<String, SortBody>>>,
    max_docs: Option<usize>,
    conflicts: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ScriptBlock {
    pub script: String,
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
            "select {} from {} t{}{}{}{}",
            self._fields(),
            "{target_table} t",
            self._joins(),
            filter_str,
            self._order_by(),
            self._limit()
        )
    }
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

    Ok(SqlCommand{ sql: builder.build(), table: table_name, calculate_score: builder.score() })
}

fn to_command_update_by_query(table: Option<String>, body: &UpdateByQueryBody) -> Result<UpdateByQueryCommand, ParseError> {
    let mut builder = SqlBuilder::new();

    to_command_worker(&mut builder, &body.query)?;

    let table_name = match table {
        Some(t) => t,
        None => panic!("Didn't find a table")
    };

    Ok(UpdateByQueryCommand{ 
        query_command: SqlCommand{ sql: builder.build(), table: table_name, calculate_score: builder.score() },
        script_block: body.script.clone(),
    })
}

fn to_command_worker(builder: &mut SqlBuilder, query: &Query) -> Result<(), ParseError> {
    match query {
        Query::Match(m) => to_sql_match(builder, &m),
        Query::Bool(b) => to_sql_bool(builder, &b),
        Query::Term(t) => to_sql_term(builder, &t),
        Query::Exists(e) => to_sql_exists(builder, &e),
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
    if bool_obj._bool.must.len() > 0 {
        builder.push_filter_context();

        bool_obj._bool.must.iter().map(|x|to_command_worker(builder, x)).collect::<Result<Vec<()>, ParseError>>()?;

        builder.pop_filter_context(true);
    }
    if bool_obj._bool.should.len() > 0 {
        builder.push_filter_context();

        bool_obj._bool.should.iter().map(|x|to_command_worker(builder, x)).collect::<Result<Vec<()>, ParseError>>()?;

        builder.pop_filter_context(false);        
    }
    if bool_obj._bool.must_not.len() > 0 {
        // Must not is an AND of NOTS which we rewrite into a NOT of ORS to simplify the codegen logic a bit here.
        builder.push_filter_context();

        bool_obj._bool.must_not.iter().map(|x|to_command_worker(builder, x)).collect::<Result<Vec<()>, ParseError>>()?;

        builder.pop_filter_context(false);        
        builder.not_last_filter();
    } 
    if bool_obj._bool.filter.len() > 0 {
        builder.push_filter_context();

        bool_obj._bool.filter.iter().map(|x|to_command_worker(builder, x)).collect::<Result<Vec<()>, ParseError>>()?;

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
    use crate::elastic_search_parser::parse;

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

    }

}
