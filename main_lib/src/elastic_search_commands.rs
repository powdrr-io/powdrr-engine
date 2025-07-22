
use std::{collections::HashMap, pin::Pin};
use std::sync::LazyLock;
use arrow_json::writer::LineDelimited;
use arrow_json::WriterBuilder;
use async_trait::async_trait;
use datafusion::{arrow::array::RecordBatch, prelude::DataFrame};
use futures::FutureExt;
use gotham::mime;
use http::StatusCode;
use serde_json::Value;

use crate::{data_access::{self, execute_sql}, distributed_cache, elastic_search_common::{Command, ResultGeneratorFuture}, elastic_search_parser::ScriptBlock, elastic_search_responses::{QueryResultHit, QueryResults}, expression_evaluator, painless_parser, state_hosted_service::API_SERVICE_CLIENT, state_peers::SnapshotDescriptor};
use crate::elastic_search_common::ElasticSearchResponse;
use crate::elastic_search_endpoints::QueryStringSearch;
use crate::elastic_search_ingest::IngestError;
use crate::elastic_search_parser::{process_aggregations, Aggregation};
use crate::elastic_search_responses::{AggregationResult, QueryResultsNotFound, UpdateByQueryResults, UpdateByQueryResultsRetries, UpdateByQuerySuccess};
use crate::elastic_search_storage_schema::{FullRecord, RecordDelete, RecordInput, SpeedboatCommitBuilder};
use crate::schema_massager::{to_powdrr_schema, PowdrrSchema, SqlBuilder, SqlExpression, SqlQuery};
use crate::state_peers::{PrivateInvocation, PrivateSqlInvocation};

async fn empty_result(aggs: Option<Vec<Aggregation>>, total_hits_complex: bool) -> ElasticSearchResponse {
    // TODO: need to record and feed through the requested number of shards from index creation
    let aggregation_results = SqlCommand::generate_aggregations(None, aggs, None).await;
    QueryResults::empty(50, 1, aggregation_results, total_hits_complex).to_response()
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

async fn to_full_records(data_frame: &DataFrame) -> (Vec<FullRecord>, Option<PowdrrSchema>) {
    let (parsed_json, schema) = to_serde_value(data_frame).await;

    let full_records = parsed_json.iter().map(|x| FullRecord::from_record(&x)).collect();
    (full_records, schema)
}

/*
async fn to_record_inputs(data_frame: &DataFrame) -> (Vec<RecordInput>, Option<PowdrrSchema>) {
    let (parsed_json, schema) = to_serde_value(data_frame).await;

    let record_inputs = parsed_json.iter().map(|x| RecordInput::from_record(&x)).collect();
    (record_inputs, schema)
}
 */

async fn to_hits(index: &String, data_frame: &DataFrame, found: Option<bool>) -> (Vec<QueryResultHit>, Option<PowdrrSchema>) {
    let (parsed_json, schema) = to_serde_value(data_frame).await;

    let hits = parsed_json.iter().map(|x|QueryResultHit::from_record(&Some(index.clone()), &x, found)).collect();
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

    async fn current_target_snapshots(&self) -> Vec<SnapshotDescriptor> {
        let checkpoint_id = API_SERVICE_CLIENT.get_latest_checkpoint(&self.table, None).await.unwrap();
        match checkpoint_id {
            Some(c) => vec!(SnapshotDescriptor{ table_name: self.table.clone(), snapshot_id: c }),
            None => vec!(),
        }
    }
}

#[async_trait]
impl Command for LookupById {
    async fn get_private_invocation(&self) -> PrivateInvocation {
        PrivateInvocation::Sql(PrivateSqlInvocation {
            sql: self.sql.clone(),
            required_extensions: vec![],
            file_filter: vec![],
            snapshots: self.current_target_snapshots().await,
        })
    }

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>> {
        let table = self.table.clone();
        let ids = self.ids.clone();
        async move {
            let result = match LookupById::to_dataframe(result_table_name).await {
                Some(df) => {
                    let (hits, _) = to_hits(&table, &df, Some(true)).await;
                    let inner_result: ElasticSearchResponse = if hits.len() == 0 {
                        (QueryResultsNotFound { _index: table, _id: ids.get(0).unwrap().clone(), found: false }).to_response()
                    } else {
                        assert_eq!(hits.len(), 1);
                        ElasticSearchResponse {
                            status: StatusCode::OK,
                            mime: mime::APPLICATION_JSON,
                            body: serde_json::to_string(hits.get(0).unwrap()).unwrap(),
                            headers: vec![],
                        }
                    };
                    inner_result
                },
                None => {
                    (QueryResultsNotFound { _index: table, _id: ids.get(0).unwrap().clone(), found: false }).to_response()
                }
            };
            Ok(result)
        }.boxed()
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
        let final_table_name = format!("{temp_table_name}_final");
        if calculate_score {
            let initial_data_frame = match execute_sql(&format!("select * from {temp_table_name}")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };

            let num_records_with_term = match initial_data_frame.clone().count().await {
                Ok(tr) => tr,
                Err(_) => panic!("nope"),
            };

            let mut column_names = initial_data_frame.schema().columns().iter().map(|c| format!("\"{}\"", c.name()).to_string()).collect::<Vec<String>>();
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

            match data_access::create_table(&final_table_name, &format!("SELECT {column_names_joined}, ln(({total_records} - {records_with_term} + 0.5)/({records_with_term} + 0.5) + 1) * (term_cnt * ({constant_k} + 1)) / (term_cnt + {constant_k} * (1 - {constant_b} + ({constant_b} * word_cnt / {avgdl}))) as score FROM {temp_table_name} order by score desc")).await {
                Ok(_) => (),
                Err(_) => panic!("nope"),
            };
        } else {
            match data_access::create_table(&final_table_name, &format!("SELECT * from {temp_table_name};")).await {
                Ok(_) => (),
                Err(_) => panic!("nope"),
            };
        }
        final_table_name.clone()
    }
    
    async fn generate_aggregations(schema: Option<PowdrrSchema>, aggs: Option<Vec<Aggregation>>, table_name: Option<String>) -> Option<HashMap<String, AggregationResult>> {
        if aggs.is_none() {
            return None
        }
        
        Some(process_aggregations(schema, aggs, table_name).await)
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


#[async_trait]
impl Command for SqlCommand {
    async fn get_private_invocation(&self) -> PrivateInvocation {
        PrivateInvocation::Sql(PrivateSqlInvocation{
            sql: self.sql.clone(),
            required_extensions: self.required_extensions(),
            file_filter: vec![],
            snapshots: self.current_target_snapshots().await,
        })
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

            // TODO: figure out the real size to return. There is default and then size can
            // be passed in.
            let first_n_rows = match data_frame.clone().limit(0, Some(100)) {
                Ok(ftr) => ftr,
                Err(_) => panic!("nope"),
            };

            let (hits, schema) = to_hits(&table, &first_n_rows, None).await;

            let aggregations = SqlCommand::generate_aggregations(schema, aggs, Some(final_table_name.clone())).await;
            // TODO: need to calculate the actual max score here
            let max_score = hits.get(0).unwrap()._score;
            let final_result = QueryResults::success(
                50,
                1,
                num_records,
                max_score,
                hits,
                aggregations,
                !query_params.rest_total_hits_as_int.unwrap_or_else(|| false),
            ).to_response();
            data_access::drop(&final_table_name).await;
            Ok(final_result)
        }.boxed()
    }
}

pub(crate) struct UpdateByQueryCommand {
    pub query_command: SqlCommand,
    pub script_block: ScriptBlock,
}


enum EvalResult {
    Update(RecordInput, u64),
    Noop,
    #[allow(dead_code)]
    Delete(String, u64),
}

struct UpdateByQueryResult {
    buffer: SpeedboatCommitBuilder,
    noop_count: usize,
    debug: Option<Vec<Value>>,
}

impl UpdateByQueryResult {
    fn total_count(&self) -> usize {
        self.buffer.num_inserts() + self.buffer.num_updates() + self.buffer.num_deletes() + self.noop_count
    }
    
    async fn commit(&mut self) -> Result<ElasticSearchResponse, IngestError> {
        self.buffer.commit().await?;
        Ok(UpdateByQueryCommand::success(
            self.total_count() as u64,
            self.buffer.num_updates() as u64,
            self.buffer.num_deletes() as u64,
            self.noop_count as u64,
            1,
            self.debug.clone()
        ).to_response())
    }
}


impl UpdateByQueryCommand {
    fn evaluate( script: &ScriptBlock, value: &FullRecord) -> EvalResult {
        let translated_script = match painless_parser::translate(&script.source) {
            Ok(t) => t,
            Err(_) => panic!("Need to make an error path")
        };
        let output = expression_evaluator::eval_template(
            &translated_script,
            &value.record_input.source().unwrap(),
            HashMap::from([
                ("op".to_string(), minijinja::Value::from("update"))
            ]),
            minijinja::Value::from_serialize(&script.params)
        );

        let op = output.other_context.get("op").map_or_else(|| "noop", |v|v.as_str().unwrap());

        match op {
            "update" => {
                EvalResult::Update(
                    RecordInput::new(
                        value.record_input.id().clone(),
                        value.record_input.version() + 1,
                        &output.source,
                        None,
                    ),
                    value.seq_no,
                )
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

    fn empty_result() -> UpdateByQuerySuccess {
        UpdateByQueryCommand::success(0, 0, 0, 0, 0, None)
    }

    fn success(total: u64, updated: u64, deleted: u64, noops: u64, batches: u64, debug_data: Option<Vec<Value>>) -> UpdateByQuerySuccess {
       UpdateByQuerySuccess{ result: UpdateByQueryResults{
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
            debug: debug_data,
        }}
    }

    async fn create_final_result(table: &String, final_values: Vec<EvalResult>) -> UpdateByQueryResult {
        let mut buffer = SpeedboatCommitBuilder::new(table);

        let mut noop_count: usize = 0;
        
        for result in final_values {
            match result {
                EvalResult::Noop => {
                    noop_count += 1;
                },
                EvalResult::Delete(doc_id, seq_no) => {
                    buffer.delete(&RecordDelete::new(&doc_id, seq_no));
                },
                EvalResult::Update(value, seq_no) => {
                    buffer.delete(&RecordDelete::new(value.id(), seq_no));
                    buffer.update(&value);
                }
            }
        }
        
        UpdateByQueryResult {
            buffer: buffer.clone(),
            noop_count: noop_count,
            // UNCOMMENT FOR DEBUGGING: debug: Some(update_buffer.records.iter().map(|x|x.source().unwrap().clone()).collect()),
            debug: None,
        }
    }
}


#[async_trait]
impl Command for UpdateByQueryCommand {
    async fn get_private_invocation(&self) -> PrivateInvocation {
        self.query_command.get_private_invocation().await
    }
    
    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>> {
        let table = self.query_command.table.clone();
        let calculate_score = self.query_command.calculate_score;
        let script_block = self.script_block.clone();
        async move {
            let table_name = match result_table_name {
                Some(t) => t,
                None => return Ok(UpdateByQueryCommand::empty_result().to_response())
            };
            let final_table_name = SqlCommand::get_final_table_name(&table, &table_name, calculate_score).await;
            let data_frame = match execute_sql(&format!("select * from {final_table_name}")).await {
                Ok(df) => df,
                Err(_) => panic!("nope")
            };

            let (mut result_values, _) = to_full_records(&data_frame).await;
            result_values.iter_mut().for_each(|x|x.record_input.ensure_source());
            let final_result_values: Vec<EvalResult> = result_values.iter().map(|x|UpdateByQueryCommand::evaluate(&script_block, x)).collect();

            let mut result_info = UpdateByQueryCommand::create_final_result(&table, final_result_values).await;
            data_access::drop(&final_table_name).await;
            match result_info.commit().await {
                Ok(r) => Ok(r),
                Err(e) => Err(e.to_string())
            }
        }.boxed()
    }
}
