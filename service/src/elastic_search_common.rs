
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use arrow_json::reader::infer_json_schema;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::error::ArrowError;
use futures::future::try_join_all;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{Body, Response};
use gotham::mime::Mime;
use gotham::state::State;
use http::{HeaderName, StatusCode};

use crate::data_access::{execute_sql, load_memtable};
use crate::elastic_search_responses::QueryFailure;
use crate::state_peers::{self, PeerClient, PeerClientError, PrivateSqlInvocation, SnapshotDescriptor};
use crate::state_common::FileFilter;
use crate::util::log_err;


pub(crate) const MIME_ES_JSON: LazyLock<Mime> = LazyLock::new(|| "application/vnd.elasticsearch+json;compatible-with=8".parse().unwrap());


pub(crate) struct CommandContext {

}

#[derive(Debug)]
pub(crate) struct ParseError {
    pub message: String,
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message.as_str())?;
        Ok(())
    }
}

impl Error for ParseError {}


pub(crate) trait CommandResponse {
    fn generate_response(&self, state: &State) -> Response<Body>;
}


pub(crate) struct ElasticSearchResponse {
    pub status: StatusCode, 
    pub mime: Mime, 
    pub body: String,
    pub headers: Vec<(HeaderName, String)>,
}

unsafe impl Send for ElasticSearchResponse {}
unsafe impl Sync for ElasticSearchResponse {}


impl CommandResponse for ElasticSearchResponse {
    fn generate_response(&self, state: &State) -> gotham::hyper::Response<gotham::hyper::Body> {
        let mut response = create_response(state, self.status.clone(), self.mime.clone(), self.body.clone());
        if self.headers.len() != 0 {
            let response_headers = response.headers_mut();
            for (k, v) in self.headers.iter() {
                response_headers.insert(k, v.parse().unwrap());
            }
        }
        response
    }
}


pub(crate) struct SqlBuilder {
    pub columns: Vec<String>,
    pub table: Option<String>,
    pub filters: Vec<String>,
    pub order_by: Vec<String>,
}


impl SqlBuilder {
    #[allow(dead_code)]
    fn new() -> Self {
        SqlBuilder {
            columns: Vec::new(),
            table: None,
            filters: Vec::new(),
            order_by: Vec::new(),
        }
    }

    #[allow(dead_code)]
    fn get_table(&self) -> &Option<String> {
        &self.table
    }

    #[allow(dead_code)]
    fn set_table(&mut self, val: String) -> &mut SqlBuilder {
        self.table = Some(val);
        self
    }

    #[allow(dead_code)]
    fn add_column(&mut self, column: String) -> &mut SqlBuilder {
        self.columns.push(column);
        self
    }

    #[allow(dead_code)]
    fn add_filter(&mut self, filter: String) -> &mut SqlBuilder {
        self.filters.push(filter);
        self
    }

    #[allow(dead_code)]
    fn add_order_by(&mut self, order_by: String) -> &mut SqlBuilder {
        self.order_by.push(order_by);
        self
    }

    fn _format_filters(&self) -> String {
        if self.filters.len() == 0 {
            "".to_string()
        } else {
            format!(" WHERE {}", self.filters.join(" AND "))
        }
    }

    fn _format_order_by(&self) -> String {
        if self.order_by.len() == 0 {
            "".to_string()
        } else {
            format!(" ORDER BY {}", self.order_by.join(", "))
        }
    }

    #[allow(dead_code)]
    pub (crate) fn build(&self) -> String {
        let columns = self.columns.join(", ");
        let filters = self._format_filters();
        let order_by: String = self._format_order_by();
        match &self.table {
            Some(t) => format!("SELECT {columns} FROM {t}{filters}{order_by}"),
            None => panic!()
        }
        
    }

    #[allow(dead_code)]
    fn merge(_builders: Vec<&SqlBuilder>) -> SqlBuilder {
        todo!()
    }
}


pub type ResultGeneratorFuture = dyn Future<Output = Result<Arc<dyn CommandResponse>, String>> + Send;

#[async_trait]
pub(crate) trait Command: Send + Sync {
    #[allow(dead_code)]
    fn get_name(&self) -> String;

    #[allow(dead_code)]
    fn get_tables(&self) -> Vec<String>;

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>>;

    fn generate_sql(&self) -> String;

    #[allow(dead_code)]
    fn generate_filters(&self) -> Vec<&FileFilter>;

    fn required_extensions(&self) -> Vec<String>;

    async fn _current_target_snapshots(&self) -> Vec<SnapshotDescriptor>;
}

pub(crate) async fn call_private_sql(
    peer_client: &dyn PeerClient,
    target_sql: &String, 
    required_extensions: &Vec<String>,
    target_snapshots: &Vec<SnapshotDescriptor>,
    index: u64,
    num: u64,
) -> Result<String, PeerClientError> {
    let invocation = PrivateSqlInvocation {
        sql: target_sql.clone(),
        required_extensions: required_extensions.clone(),
        file_filter: vec!(),
        snapshots: target_snapshots.clone(),
        index: index,
        num: num,
    };

    peer_client.private_sql(&invocation).await
}


pub(crate) async fn call_private_sql_and_load(
    peer_client: &dyn PeerClient,
    target_sql: &String, 
    required_extensions: &Vec<String>,
    target_snapshots: &Vec<SnapshotDescriptor>,
    index: u64,
    num: u64,
) -> Result<Option<Vec<RecordBatch>>, PeerClientError> {
    let result = match call_private_sql(peer_client, target_sql, required_extensions, target_snapshots, index, num).await {
        Ok(r) => r,
        Err(e) => return Err(e),
    };
    
    let inferred_schema = infer_json_schema(result.as_bytes(), None).unwrap();
    let json_reader = match arrow_json::ReaderBuilder::new(Arc::new(inferred_schema.0)).build(result.as_bytes()) {
        Ok(d) => d,
        Err(_) => panic!("Private API returned result that does not match schema")
    };

    let record_batches: Result<Vec<RecordBatch>, ArrowError> = json_reader.collect();
    match record_batches {
        Ok(rb) => Ok(Some(rb)),
        Err(_) => log_err(PeerClientError{ message: "Arrow error".to_string() })
    }
}


async fn call_peers_and_load_results(
    required_extensions: &Vec<String>,
    target_snapshots: &Vec<SnapshotDescriptor>, 
    sql: &String
) -> Result<Option<String>, PeerClientError> {
    if target_snapshots.len() == 0 {
        return Ok(None)
    }

    let peer_clients: Vec<Box<dyn PeerClient>> = state_peers::get_peer_clients();

    let all_calls = peer_clients.iter().enumerate().map(
        |tuple| call_private_sql_and_load(tuple.1.as_ref(), sql, required_extensions, target_snapshots, tuple.0 as u64, peer_clients.len() as u64));
    let all_records: Vec<RecordBatch> = match try_join_all(all_calls).await {
        Ok(ar) => ar.iter().filter(|x| x.is_some()).map(|x| x.clone().unwrap()).flatten().collect(),
        Err(e) => {
            let error = format!("{}", e.message);
            println!("{}", error);
            panic!("dude")
        },
    };
    if all_records.len() == 0 {
        return Ok(None)
    }

    let table_name = match load_memtable(&all_records).await {
        Ok(name) => name,
        Err(e) => return Err(PeerClientError{ message: e.message().to_string() })
    };

    let final_name = format!("{table_name}_dedup");

    match execute_sql(&format!("create table {final_name} as select distinct on (_id) * from {table_name} order by _id, _version desc")).await {
        Ok(_) => Ok(Some(final_name)),
        Err(e) => Err(PeerClientError{ message: e.message().to_string() })
    }
}


pub async fn load_command_raw_result(_context: CommandContext, command: Arc<dyn Command>) -> Result<Option<String>, PeerClientError> {
    let target_snapshots = command._current_target_snapshots().await;
    let target_sql = command.generate_sql();
    let required_extensions = command.required_extensions();
    call_peers_and_load_results(&required_extensions, &target_snapshots, &target_sql).await
}


pub async fn execute_command(_context: CommandContext, command: Arc<dyn Command>) -> Arc<dyn CommandResponse> {
    let result_table_name = match load_command_raw_result(_context, command.clone()).await {
        Ok(t) => t,
        Err(_) => return Arc::new(QueryFailure{ message: "Failed".to_string() }),
    };         
    let response = match command.result_generator(result_table_name).await {
        Ok(d) => d,
        Err(_e) => return Arc::new(QueryFailure{ message: "Failed".to_string() }),
    };
    response
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_builder_test1() {
        let mut sql_builder = SqlBuilder::new();
        sql_builder.set_table("foo_bar".to_string());
        sql_builder.add_column("_id".to_string()).add_column("*".to_string());
        sql_builder.add_filter("baz > 0".to_string()).add_filter("quux = 'what up'".to_string());
        sql_builder.add_order_by("dudical".to_string()).add_order_by("my_man".to_string());
        assert_eq!(sql_builder.build(), "SELECT _id, * FROM foo_bar WHERE baz > 0 AND quux = 'what up' ORDER BY dudical, my_man");
    }
}
