
use std::{collections::HashMap, pin::Pin, sync::Arc};
use std::sync::LazyLock;
use arrow_json::writer::LineDelimited;
use arrow_json::WriterBuilder;
use async_trait::async_trait;
use datafusion::{arrow::array::RecordBatch, prelude::DataFrame};
use futures::FutureExt;
use gotham::mime;
use http::StatusCode;
use serde_json::Value;

use crate::{data_access::{self, execute_sql}, distributed_cache, elastic_search_common::{Command, CommandResponse, ResultGeneratorFuture}, elastic_search_ingest::{self, WriteBuffer}, elastic_search_parser::ScriptBlock, elastic_search_responses::{QueryResultHit, QueryResults}, expression_evaluator, painless_parser, state_hosted_service::API_SERVICE_CLIENT, state_peers::SnapshotDescriptor};
use crate::elastic_search_common::ElasticSearchResponse;
use crate::elastic_search_endpoints::QueryStringSearch;
use crate::elastic_search_parser::{process_aggregations, Aggregation};
use crate::elastic_search_responses::{AggregationResult, QueryResultsNotFound, UpdateByQueryResults, UpdateByQueryResultsRetries, UpdateByQuerySuccess};
use crate::elastic_search_storage_schema::{RecordInput, WriteBufferBuilder};
use crate::schema_massager::{to_powdrr_schema, PowdrrSchema, SqlBuilder, SqlExpression, SqlQuery};

async fn empty_result(aggs: Option<Vec<Aggregation>>, total_hits_complex: bool) -> Arc<dyn CommandResponse> {
    // TODO: need to record and feed through the requested number of shards from index creation
    let aggregation_results = SqlCommand::generate_aggregations(None, aggs, None).await;
    Arc::new(QueryResults::empty(50, 1, aggregation_results, total_hits_complex))
}


fn to_hit(index: &String, value: &Value) -> QueryResultHit {
    let value_map = value.as_object().unwrap().clone();
    let score = value_map.get("score").map_or_else(|| 0.0, |f|f.as_f64().unwrap());
    let id = value_map.get("_id").unwrap().as_str().unwrap().to_string();
    let version = value_map.get("_version").unwrap().as_u64().unwrap();
    let seq_no = value_map.get("_seq_no").unwrap().as_i64().unwrap();
    let source = value_map.get("_source").unwrap().as_str().unwrap();
    // TODO: we are parsing the string into a value just to put it an object
    // that will get serialized out again. That is lame. If we can get the serializer
    // to look at a string but put it in like it is a Value, that would be better.
    let source_value = serde_json::from_str(source).unwrap();
    QueryResultHit {
        _index: Some(index.clone()),
        _id: Some(id),
        _version: version,
        _seq_no: seq_no,
        _score: Some(score),
        _primary_term: Some(1),
        found: None,
        _source: source_value,
    }
}

pub(crate) async fn to_serde_value(data_frame: &DataFrame) -> (Vec<Value>, Option<PowdrrSchema>) {
    let record_batches: Vec<RecordBatch> = match data_frame.clone().collect().await {
        Ok(b) => b,
        Err(e) => {
            let error = format!("{:?}", e);
            println!("{}", error);
            panic!("nope");
        }
    };

    let schema = match record_batches.len() {
        0 => None,
        _ => Some(to_powdrr_schema(&record_batches.get(0).unwrap().schema())),
    };
    
    let record_batch_references: Vec<&RecordBatch> = record_batches.iter().map(|r| r).collect();

    let buf = Vec::new();
    let builder = WriterBuilder::new().with_explicit_nulls(true);
    let mut writer = builder.build::<_, LineDelimited>(buf);
    //let mut writer = arrow_json::LineDelimitedWriter::new(buf);
    writer.write_batches(record_batch_references.as_slice()).unwrap();
    writer.finish().unwrap();
    
    // Get the underlying buffer back,
    let buf = writer.into_inner();    
    let reader = String::from_utf8(buf).unwrap(); 

    let parsed_json: Vec<Value> = reader.lines().map(|x|serde_json::from_str(x).unwrap()).collect();

    (parsed_json, schema)
}

async fn to_hits(index: &String, data_frame: &DataFrame) -> (Vec<QueryResultHit>, Option<PowdrrSchema>) {
    let (parsed_json, schema) = to_serde_value(data_frame).await;

    let hits = parsed_json.iter().map(|x|to_hit(index, &x)).collect();
    (hits, schema)
}


pub(crate) struct LookupById {
    table: String,
    ids: Vec<String>,
    sql: SqlQuery,
}

impl LookupById {
    pub fn new(table: &String, ids: &Vec<String>) -> Self {
        let mut sql_builder = SqlBuilder::for_query(true);
        sql_builder.filter(SqlExpression::In(
            Box::new(SqlExpression::FieldRef("t".to_string(), "_id".to_string())),
            ids.iter().map(|i|SqlExpression::LiteralString(i.clone())).collect()
        ));
        LookupById {
            table: table.clone(),
            ids: ids.clone(),
            sql: sql_builder.build(),
        }
    }
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
                    let (hits, _) = to_hits(&table, &df).await;
                    let inner_result: Arc<dyn CommandResponse> = if hits.len() == 0 {
                        Arc::new(QueryResultsNotFound { _index: table, _id: ids.get(0).unwrap().clone(), found: false })
                    } else {
                        assert_eq!(hits.len(), 1);
                        let response = ElasticSearchResponse {
                            status: StatusCode::OK,
                            mime: mime::APPLICATION_JSON,
                            body: serde_json::to_string(hits.get(0).unwrap()).unwrap(),
                            headers: vec![],
                        };
                        Arc::new(response)
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

    fn generate_sql(&self) -> SqlQuery {
        self.sql.clone()
    }

    fn generate_filters(&self) -> Vec<&crate::state_common::FileFilter> {
        vec!()
    }

    fn required_extensions(&self) -> Vec<String> {
        vec!()
    }

    async fn current_target_snapshots(&self) -> Vec<SnapshotDescriptor> {
        let checkpoint_id = API_SERVICE_CLIENT.get_latest_checkpoint(&self.table, None).await.unwrap();
        match checkpoint_id {
            Some(c) => vec!(SnapshotDescriptor{ table_name: self.table.clone(), snapshot_id: c }),
            None => vec!(),
        }
    }    
}


pub(crate) struct SqlCommand {
    pub sql: SqlQuery,
    pub table: String,
    pub aggs: Option<Vec<Aggregation>>,
    pub query_params: QueryStringSearch,
    pub calculate_score: bool,
}

static SEARCH_COLUMNS: LazyLock<Vec<String>> = LazyLock::new(|| vec!(
    "\"term_cnt\"".to_string(),
    "\"word_cnt\"".to_string(),
    "\"field_term\"".to_string(),
    "\"field_name\"".to_string(),
    // TODO: figure out how to get @ character into SQL properly
    "\"@timestamp\"".to_string(),
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
    
    async fn generate_aggregations(schema: Option<PowdrrSchema>, aggs: Option<Vec<Aggregation>>, table_name: Option<String>) -> Option<HashMap<String, AggregationResult>> {
        if aggs.is_none() {
            return None
        }
        
        Some(process_aggregations(schema, aggs, table_name).await)
    }
}


#[async_trait]
impl Command for SqlCommand {
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
        let query_params = self.query_params.clone();
        async move {
            let table_name = match result_table_name {
                Some(t) => t,
                None => return Ok(empty_result(aggs, !query_params.rest_total_hits_as_int.unwrap_or_else(|| false)).await)
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

            let (hits, schema) = to_hits(&table, &first_10_rows).await;

            let aggregations = SqlCommand::generate_aggregations(schema, aggs, Some(final_table_name)).await;
            // TODO: need to calculate the actual max score here
            let max_score = hits.get(0).unwrap()._score.unwrap();
            let final_result = QueryResults::success(
                50,
                1,
                num_records,
                max_score,
                hits,
                aggregations,
                !query_params.rest_total_hits_as_int.unwrap_or_else(|| false),
            );

            Ok(Arc::new(final_result))
        }.boxed()
    }

    fn generate_sql(&self) -> SqlQuery {
        self.sql.clone()
    }

    fn generate_filters(&self) -> Vec<&crate::state_common::FileFilter> {
        vec!()
    }

    fn required_extensions(&self) -> Vec<String> {
        if self.calculate_score {
            vec!("es".to_string())
        } else {
            vec!()
        }
    }

    async fn current_target_snapshots(&self) -> Vec<SnapshotDescriptor> {
        let extension = match self.calculate_score {
            true => Some("es".to_string()),
            false => None
        };
        let checkpoint_id = API_SERVICE_CLIENT.get_latest_checkpoint(&self.table, extension).await.unwrap();
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


enum EvalResult {
    Update(RecordInput),
    Noop,
    #[allow(dead_code)]
    Delete((String, u64)),
}

struct UpdateByQueryResult {
    table: String,
    update_buffer: WriteBufferBuilder,
    #[allow(dead_code)]
    delete_buffer: WriteBuffer,
    update_count: u64,
    delete_count: u64,
    noop_count: u64,
}

impl UpdateByQueryResult {
    fn total_count(&self) -> u64 {
        self.update_count + self.delete_count + self.noop_count
    }
    
    async fn commit(&mut self) -> Result<Arc<dyn CommandResponse>, String> {
        // TODO: ideally this would write all the files once and do a single commit to the service.
        match elastic_search_ingest::commit_general(&self.update_buffer.build(), &self.table, &"commit".to_string()).await {
            Ok(_) => (),
            Err(_) => panic!("nope"),
        };
        match elastic_search_ingest::commit_general(&self.delete_buffer, &self.table, &"delete".to_string()).await {
            Ok(_) => (),
            Err(_) => panic!("nope"),
        };
        Ok(UpdateByQueryCommand::success(
            self.total_count(),
            self.update_count,
            self.delete_count,
            self.noop_count,
            1
        ))
    }
}


impl UpdateByQueryCommand {
    fn evaluate(script: &ScriptBlock, value: &QueryResultHit) -> EvalResult {
        // TODO: run script
        let translated_script = match painless_parser::translate(&script.source) {
            Ok(t) => t,
            Err(_) => panic!("Need to make an error path")
        };
        let output = expression_evaluator::eval_template(
            &translated_script,
            &value._source,
            HashMap::from([
                ("op".to_string(), minijinja::Value::from("update"))
            ]),
            minijinja::Value::from_serialize(&script.params)
        );

        let op = output.other_context.get("op").map_or_else(|| "noop", |v|v.as_str().unwrap());
        
        match op {
            "update" => {
                EvalResult::Update(RecordInput {
                    _id: value._id.as_ref().unwrap().clone(),
                    _seq_no: value._seq_no,
                    _version: value._version + 1,
                    existing_normalized: None,
                    source: output.source,
                })
            },
            "noop" => {
                EvalResult::Noop
            },
            "delete" => {
                todo!("Need to implement delete")
            },
            _ => {
                panic!("Unknown operation")
            }
        }
    }

    fn empty_result() -> Arc<dyn CommandResponse> {
        UpdateByQueryCommand::success(0, 0, 0, 0, 0)
    }

    fn success(total: u64, updated: u64, deleted: u64, noops: u64, batches: u64) -> Arc<dyn CommandResponse> {
        Arc::new(UpdateByQuerySuccess{ result: UpdateByQueryResults{
            took: 0,
            timed_out: false,
            total: total,
            updated: updated,
            deleted: deleted,
            batches: batches,
            version_conflicts: 0,
            noops: noops,
            retries: UpdateByQueryResultsRetries {
                bulk: 0,
                search: 0,
            },
            throttled_millis: 0,
            requests_per_second: -1,
            throttled_until_millis: 0,
            failures: vec![],
        }})
    }

    async fn create_final_result(table: &String, final_values: Vec<EvalResult>) -> UpdateByQueryResult {
        let mut update_buffer = WriteBufferBuilder::new();
        let mut delete_buffer = WriteBuffer::new();
        
        let mut update_count: u64 = 0;
        let mut delete_count: u64 = 0;
        let mut noop_count: u64 = 0;
        
        for result in final_values {
            match result {
                EvalResult::Noop => {
                    noop_count += 1;
                },
                EvalResult::Delete((doc_id, seq_no)) => {
                    delete_buffer.lines.push(serde_json::to_string(&elastic_search_ingest::create_delete(&doc_id, seq_no)).unwrap());
                    delete_count += 1;
                },
                EvalResult::Update(value) => {
                    update_buffer.records.push(value);
                    update_count += 1;
                }
            }
        }
        
        UpdateByQueryResult {
            table: table.clone(),
            update_buffer: update_buffer,
            delete_buffer: delete_buffer,
            update_count: update_count,
            delete_count: delete_count,
            noop_count: noop_count,
        }
    }
}


#[async_trait]
impl Command for UpdateByQueryCommand {
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
                None => return Ok(UpdateByQueryCommand::empty_result())
            };
            let final_table_name = SqlCommand::get_final_table_name(&table, &table_name, calculate_score).await;
            let data_frame = match execute_sql(&format!("select * from {final_table_name}")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };

            let (result_values, _) = to_hits(&table, &data_frame).await;

            let final_result_values: Vec<EvalResult> = result_values.iter().map(|x|UpdateByQueryCommand::evaluate(&script_block, x)).collect();

            let mut result_info = UpdateByQueryCommand::create_final_result(&table, final_result_values).await;
            result_info.commit().await
        }.boxed()
    }

    fn generate_sql(&self) -> SqlQuery {
        self.query_command.generate_sql()
    }

    fn generate_filters(&self) -> Vec<&crate::state_common::FileFilter> {
        self.query_command.generate_filters()
    }

    fn required_extensions(&self) -> Vec<String> {
        self.query_command.required_extensions()
    }    

    async fn current_target_snapshots(&self) -> Vec<SnapshotDescriptor> {
        self.query_command.current_target_snapshots().await
    }    
}


