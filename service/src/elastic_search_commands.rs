
use std::{collections::HashMap, pin::Pin, sync::Arc};
use std::sync::LazyLock;
use async_trait::async_trait;
use datafusion::{arrow::array::RecordBatch, prelude::DataFrame};
use futures::FutureExt;
use serde_json::{json, Value};

use crate::{data_access::{self, execute_sql}, distributed_cache, elastic_search_common::{Command, CommandResponse, ParseError, ResultGeneratorFuture, SqlBuilder}, elastic_search_ingest::{self, WriteBuffer}, elastic_search_parser::ScriptBlock, elastic_search_responses::{QueryFailure, QueryResultHit, QueryResults}, expression_evaluator, painless_parser, state_hosted_service::API_SERVICE_CLIENT, state_peers::SnapshotDescriptor};
use crate::elastic_search_responses::{AggregationBucket, AggregationResult, QueryResultsNotFound};

fn empty_result() -> Arc<dyn CommandResponse> {
    // TODO: need to record and feed through the requested number of shards from index creation
    Arc::new(QueryResults::empty(0, 2))
}


fn to_hit(index: &String, value: &Value) -> QueryResultHit {
    let mut value_map = value.as_object().unwrap().clone();
    let score = value_map.get("score").map_or_else(|| 0.0, |f|f.as_f64().unwrap());
    let id = value_map.get("_id").unwrap().as_str().unwrap().to_string();
    let version = value_map.get("_version").unwrap().as_i64().unwrap();
    let seq_no = value_map.get("_seq_no").unwrap().as_i64().unwrap();
    value_map.remove("score");
    value_map.remove("_id");
    value_map.remove("_version");
    value_map.remove("_seq_no");
    QueryResultHit::new(
        index,
        &id,
        version,
        seq_no,
        score,
        json!(value_map)
    )
}

pub(crate) async fn to_serde_value(data_frame: &DataFrame) -> Vec<Value> {
    let record_batches: Vec<RecordBatch> = match data_frame.clone().collect().await {
        Ok(b) => b,
        Err(_e) => panic!("nope")
    };        
    
    let record_batch_references: Vec<&RecordBatch> = record_batches.iter().map(|r| r).collect();

    let buf = Vec::new();
    let mut writer = arrow_json::LineDelimitedWriter::new(buf);
    writer.write_batches(record_batch_references.as_slice()).unwrap();
    writer.finish().unwrap();
    
    // Get the underlying buffer back,
    let buf = writer.into_inner();    
    let reader = String::from_utf8(buf).unwrap(); 

    let parsed_json: Vec<Value> = reader.lines().map(|x|serde_json::from_str(x).unwrap()).collect();

    parsed_json    
}

async fn to_hits(index: &String, data_frame: &DataFrame) -> Vec<QueryResultHit> {
    let parsed_json = to_serde_value(data_frame).await;

    let hits = parsed_json.iter().map(|x|to_hit(index, &x)).collect();
    hits
}

#[allow(dead_code)]

pub(crate) struct Match {
    pub table: String,
    pub field: String,
    pub query: String,
    pub operator: Option<String>,
    pub minimum_should_match: Option<u32>,
}


#[async_trait]
impl Command for Match {
    fn generate_sql(&self) -> String {
        let field = &self.field;
        let query = &self.query;
        SqlBuilder {
            columns: vec!("*".to_string()),
            table: Some("{target_table}_search_index si inner join {target_table} t on si.doc_id = t.index_col".to_string()),
            filters: vec!(format!("field_name = '{field}'").to_string(), format!("field_term = '{query}'").to_string()),
            order_by: vec!(),
        }.build()
    }  

    fn get_name(&self) -> String {
        "Match".to_string()
    }

    fn get_tables(&self) -> Vec<String> {
        vec!(self.table.clone())
    }

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>> {
        let table = self.table.clone();
        async move {
            let table_name = match result_table_name {
                Some(t) => t,
                None => return Ok(empty_result())
            };

            let initial_data_frame = match execute_sql(&format!("select 1 from {table_name}")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };

            let num_records_with_term = match initial_data_frame.clone().count().await {
                Ok(tr) => tr,
                Err(_) => panic!("nope"),
            };            

            // TODO: need to get more of the metadata tracking system working to get total_records and avgdl for real
            let total_records: f64 = match distributed_cache::get_approx_num_records(&table) {
                Ok(t) => t as f64,
                Err(_) => panic!("nope")
            };
            let records_with_term = num_records_with_term as f64;
            let constant_k = 1.2;
            let constant_b = 0.75;
            let avgdl = 5.6;

            let bm25_sql = format!("SELECT *, ln(({total_records} - {records_with_term} + 0.5)/({records_with_term} + 0.5) + 1) * (term_cnt * ({constant_k} + 1)) / (term_cnt + {constant_k} * (1 - {constant_b} + ({constant_b} * word_cnt / {avgdl}))) as score FROM {table_name} order by score desc");
            let sql_data_frame = match execute_sql(&bm25_sql).await {
                Ok(df) => df,
                Err(_) => panic!("nope"),
            };
            let data_frame = match sql_data_frame.drop_columns(&["term_cnt", "word_cnt", "field_term", "field_name", "doc_id"]) {
                Ok(df) => df,
                Err(_) => panic!("nope"),
            };

            let first_10_rows = match data_frame.clone().limit(0, Some(10)) {
                Ok(ftr) => ftr,
                Err(_) => panic!("nope"),
            };      
                  
            let hits = to_hits(&table, &first_10_rows).await;
            let final_result = QueryResults::success(
                10,
                2,
                num_records_with_term,
                hits.get(0).unwrap().score(),
                hits,
                None,
            );    

            Ok(Arc::new(final_result))
        }.boxed()
    }

    fn generate_filters(&self) -> Vec<&crate::state_common::FileFilter> {
        vec!()
    }

    fn required_extensions(&self) -> Vec<String> {
        vec!("es".to_string())
    }

    async fn _current_target_snapshots(&self) -> Vec<SnapshotDescriptor> {
        let checkpoint_id = API_SERVICE_CLIENT.get_latest_checkpoint(&self.table, Some(&"es".to_string())).await.unwrap();
        match checkpoint_id {
            Some(c) => vec!(SnapshotDescriptor{ table_name: self.table.clone(), snapshot_id: c }),
            None => vec!(),
        }
    }    
}

#[allow(dead_code)]
pub(crate) struct MatchBuilder {
    pub table: String,
    pub field: String,
    pub query: Option<String>,
    // TODO: add other fields
}

unsafe impl Send for MatchBuilder {}
unsafe impl Sync for MatchBuilder {}


impl MatchBuilder {
    #[allow(dead_code)]
    pub fn new(table: &String, field: &String) -> Self {
        MatchBuilder { table: table.clone(), field: field.clone(), query: None }
    }

    #[allow(dead_code)]
    pub fn build(self) -> Result<Match, ParseError> {
        match self.query {
            None => return Err(ParseError { message: "Match must include query".to_string() }),
            _ => ()
        };
        Ok(Match {
            table: self.table,
            field: self.field,
            query: self.query.unwrap(),
            operator: None,
            minimum_should_match: None
        })
    }
}


pub(crate) struct LookupById {
    pub table: String,
    pub ids: Vec<String>,
}

impl LookupById {
    async fn to_dataframe(result_table_name: Option<String>) -> Option<DataFrame> {
        match result_table_name {
            Some(rtn) => {
                match data_access::execute_sql(&format!("select * from {}", rtn)).await {
                    Ok(df) => Some(df),
                    Err(_) => panic!("nope"),
                }
            },
            None => None
        }
    }
}


#[async_trait]
impl Command for LookupById {
    fn generate_sql(&self) -> String {
        let id_list = self.ids.iter().map(|id|format!("'{}'", id)).collect::<Vec<String>>().join(",");
        SqlBuilder {
            columns: vec!("*".to_string()),
            table: Some("{target_table} t left join {deletes_table} dt on t._id = dt._id".to_string()),
            filters: vec!(format!("t._id in ({id_list})"), "(dt._seq_no is null or t._seq_no > dt._seq_no)".to_string()),
            order_by: vec!(),
        }.build()
    }   

    fn get_name(&self) -> String {
        "LookupById".to_string()
    }

    fn get_tables(&self) -> Vec<String> {
        vec!(self.table.clone())
    }

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>> {
        let table = self.table.clone();
        let ids = self.ids.clone();
        async move {
            let result = match LookupById::to_dataframe(result_table_name).await {
                Some(df) => {
                    let hits = to_hits(&table, &df).await;
                    let inner_result: Arc<dyn CommandResponse> = if hits.len() == 0 {
                        Arc::new(QueryResultsNotFound { _index: table, _id: ids.get(0).unwrap().clone(), found: false })
                    } else {
                        Arc::new(QueryResults::success(10, 2, hits.len(), 1.0, hits, None))
                    };
                    inner_result
                },
                None => {
                    Arc::new(QueryResultsNotFound { _index: table, _id: ids.get(0).unwrap().clone(), found: false })
                }
            };
            Ok(result)
        }.boxed()
    }

    fn generate_filters(&self) -> Vec<&crate::state_common::FileFilter> {
        vec!()
    }

    fn required_extensions(&self) -> Vec<String> {
        vec!()
    }

    async fn _current_target_snapshots(&self) -> Vec<SnapshotDescriptor> {
        let checkpoint_id = API_SERVICE_CLIENT.get_latest_checkpoint(&self.table, None).await.unwrap();
        match checkpoint_id {
            Some(c) => vec!(SnapshotDescriptor{ table_name: self.table.clone(), snapshot_id: c }),
            None => vec!(),
        }
    }    
}


pub(crate) struct SqlCommand {
    pub sql: String,
    pub table: String,
    pub calculate_score: bool,
    pub aggs: Option<HashMap<String, String>>,
}

static SEARCH_COLUMNS: LazyLock<Vec<String>> = LazyLock::new(|| vec!(
    "\"term_cnt\"".to_string(),
    "\"word_cnt\"".to_string(),
    "\"field_term\"".to_string(),
    "\"field_name\"".to_string(),
    "\"@timestamp\"".to_string(),
    //"\"user\"".to_string(),
));

impl SqlCommand {
    async fn get_final_table_name(public_table_name: &String, temp_table_name: &String, calculate_score: bool) -> String {
        if calculate_score {
            let initial_data_frame = match execute_sql(&format!("select * from {temp_table_name}")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };

            let num_records_with_term = match initial_data_frame.clone().count().await {
                Ok(tr) => tr,
                Err(_) => panic!("nope"),
            };

            let mut column_names = initial_data_frame.schema().columns().iter().map(|c|format!("\"{}\"", c.name()).to_string()).collect::<Vec<String>>();
            column_names.retain(|c| !SEARCH_COLUMNS.contains(c));
            let column_names_joined = column_names.join(", ");

            // TODO: need to get more of the metadata tracking system working to get total_records and avgdl for real
            let total_records: f64 = match distributed_cache::get_approx_num_records(public_table_name) {
                Ok(t) => t as f64,
                Err(_) => panic!("nope")
            };
            let records_with_term = num_records_with_term as f64;
            let constant_k = 1.2;
            let constant_b = 0.75;
            let avgdl = 5.6;

            let final_table_name = format!("{temp_table_name}_final");
            let bm25_sql = format!("CREATE TABLE {final_table_name} AS SELECT {column_names_joined}, ln(({total_records} - {records_with_term} + 0.5)/({records_with_term} + 0.5) + 1) * (term_cnt * ({constant_k} + 1)) / (term_cnt + {constant_k} * (1 - {constant_b} + ({constant_b} * word_cnt / {avgdl}))) as score FROM {temp_table_name} order by score desc");
            let _sql_data_frame = match execute_sql(&bm25_sql).await {
                Ok(df) => df,
                Err(_) => panic!("nope"),
            };
            final_table_name.clone()
        } else {
            temp_table_name.clone()
        }
    }
    
    fn to_aggregation_bucket(value: &Value) -> AggregationBucket {
        let value_map = value.as_object().unwrap();
        let mut value_map_iter = value_map.iter();
        let first_pair = value_map_iter.next().unwrap();
        let second_pair = value_map_iter.next().unwrap();
        
        AggregationBucket {
            key: second_pair.1.to_string(),
            doc_count: first_pair.1.as_u64().unwrap()
        }
    }

    async fn to_buckets(table_name: &String, query: &String) -> Vec<AggregationBucket> {
        let final_sql = query.replace("{target_table}", table_name);
        let data_frame = match execute_sql(&final_sql).await {
            Ok(df) => df,
            Err(_) => panic!("nope")
        };
        
        assert_eq!(data_frame.schema().columns().len(), 2);
        
        let serde_values = to_serde_value(&data_frame).await;
        
        serde_values.iter().map(|v| SqlCommand::to_aggregation_bucket(v)).collect::<Vec<AggregationBucket>>()
    }
    
    async fn generate_aggregations(table_name: &String, aggs: Option<HashMap<String, String>>) -> Option<HashMap<String, AggregationResult>> {
        if aggs.is_none() {
            return None
        }
        
        let mut results = HashMap::new();
        for (name, query) in aggs.unwrap().iter() {
            let buckets = SqlCommand::to_buckets(table_name, query).await;
            results.insert(
                name.clone(),
                AggregationResult {
                    doc_count_error_upper_bound: 0,
                    sum_other_doc_count: 0,
                    buckets: buckets,
                }
            );
        }
        Some(results)
    }
}


#[async_trait]
impl Command for SqlCommand {
    fn generate_sql(&self) -> String {
        self.sql.clone()
    }  

    fn get_name(&self) -> String {
        "SqlCommand".to_string()
    }

    fn get_tables(&self) -> Vec<String> {
        vec!(self.table.clone())
    }

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>> {
        let table = self.table.clone();
        let calculate_score = self.calculate_score;
        let aggs = self.aggs.clone();
        async move {
            let table_name = match result_table_name {
                Some(t) => t,
                None => return Ok(empty_result())
            };
            let final_table_name = SqlCommand::get_final_table_name(&table, &table_name, calculate_score).await;
            let data_frame = match execute_sql(&format!("select * from {final_table_name}")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };
            let num_records = match data_frame.clone().count().await {
                Ok(tr) => tr,
                Err(_) => panic!("nope"),
            };            

            let first_10_rows = match data_frame.clone().limit(0, Some(10)) {
                Ok(ftr) => ftr,
                Err(_) => panic!("nope"),
            };

            let hits = to_hits(&table, &first_10_rows).await;
            
            let aggregations = SqlCommand::generate_aggregations(&final_table_name, aggs).await;
            let final_result = QueryResults::success(
                10,
                2,
                num_records,
                hits.get(0).unwrap().score(),
                hits,
                aggregations,
            );    

            Ok(Arc::new(final_result))
        }.boxed()
    }

    fn generate_filters(&self) -> Vec<&crate::state_common::FileFilter> {
        vec!()
    }

    fn required_extensions(&self) -> Vec<String> {
        vec!("es".to_string())
    }    

    async fn _current_target_snapshots(&self) -> Vec<SnapshotDescriptor> {
        let checkpoint_id = API_SERVICE_CLIENT.get_latest_checkpoint(&self.table, Some(&"es".to_string())).await.unwrap();
        match checkpoint_id {
            Some(c) => vec!(SnapshotDescriptor{ table_name: self.table.clone(), snapshot_id: c }),
            None => vec!(),
        }
    }    
}

pub(crate) struct UpdateByQueryCommand {
    pub query_command: SqlCommand,
    pub script_block: ScriptBlock,
}

impl UpdateByQueryCommand {
    fn evaluate(script: &ScriptBlock, value: &Value) -> Value {
        // TODO: run script
        let translated_script = match painless_parser::translate(&script.source) {
            Ok(t) => t,
            Err(_) => panic!("Need to make an error path")
        };
        let (_, mut output_val) = expression_evaluator::eval_template(&translated_script, value, HashMap::new(), minijinja::Value::from_serialize(script.params.clone()));
        let value_map = output_val.as_object_mut().unwrap();
        let version = value_map.get("_version").unwrap().as_number().unwrap();
        value_map.insert("_version".to_string(), Value::from(version.as_u64().unwrap() + 1));
        output_val
    }
}


#[async_trait]
impl Command for UpdateByQueryCommand {
    fn generate_sql(&self) -> String {
        self.query_command.sql.clone()
    }  

    fn get_name(&self) -> String {
        "UpdateByQueryCommand".to_string()
    }

    fn get_tables(&self) -> Vec<String> {
        vec!(self.query_command.table.clone())
    }

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>> {
        let table = self.query_command.table.clone();
        let calculate_score = self.query_command.calculate_score;
        let script_block = self.script_block.clone();
        async move {
            let table_name = match result_table_name {
                Some(t) => t,
                None => return Ok(empty_result())
            };
            let final_table_name = SqlCommand::get_final_table_name(&table, &table_name, calculate_score).await;
            let data_frame = match execute_sql(&format!("select * from {final_table_name}")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };

            let result_values = to_serde_value(&data_frame).await;

            let final_result_values: Vec<Value> = result_values.iter().map(|x|UpdateByQueryCommand::evaluate(&script_block, x)).collect();

            let mut buffer = WriteBuffer::new();
            final_result_values.iter().for_each(|f|buffer.lines.push(serde_json::to_string(f).unwrap()));
            match elastic_search_ingest::commit(&buffer, &table).await {
                Ok(_) => (),
                Err(_) => panic!("nope"),
            };
            Ok(Arc::new(QueryFailure{ message: "I am trying!".to_string() }))
        }.boxed()
    }

    fn generate_filters(&self) -> Vec<&crate::state_common::FileFilter> {
        vec!()
    }

    fn required_extensions(&self) -> Vec<String> {
        vec!("es".to_string())
    }    

    async fn _current_target_snapshots(&self) -> Vec<SnapshotDescriptor> {
        let checkpoint_id = API_SERVICE_CLIENT.get_latest_checkpoint(&self.query_command.table, Some(&"es".to_string())).await.unwrap();
        match checkpoint_id {
            Some(c) => vec!(SnapshotDescriptor{ table_name: self.query_command.table.clone(), snapshot_id: c }),
            None => vec!(),
        }
    }    
}


