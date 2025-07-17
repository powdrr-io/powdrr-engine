
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use futures::future::try_join_all;
use futures_util::{stream, StreamExt};
use gotham::helpers::http::response::create_response;
use gotham::mime::Mime;
use gotham::state::State;
use http::{HeaderName, StatusCode};
use prost::Message;
use serde_json::{Map, Value};
use crate::data_access;
use crate::data_access::load_memtable;
use crate::elastic_search_responses::QueryFailure;
use crate::schema_massager::SqlQuery;
use crate::state_peers::{self, PeerClient, PeerClientError, PrivateSqlInvocation, SnapshotDescriptor};
use crate::state_common::FileFilter;


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

pub(crate) struct ElasticSearchResponse {
    pub status: StatusCode, 
    pub mime: Mime, 
    pub body: String,
    pub headers: Vec<(HeaderName, String)>,
}

unsafe impl Send for ElasticSearchResponse {}
unsafe impl Sync for ElasticSearchResponse {}


impl ElasticSearchResponse {
    pub(crate) fn generate_response(&self, state: &State) -> gotham::hyper::Response<gotham::hyper::Body> {
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


pub type ResultGeneratorFuture = dyn Future<Output = Result<ElasticSearchResponse, String>> + Send;

#[async_trait]
pub(crate) trait Command: Send + Sync {
    #[allow(dead_code)]
    fn get_name(&self) -> String;

    #[allow(dead_code)]
    fn get_tables(&self) -> Vec<String>;

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>>;

    fn generate_sql(&self) -> SqlQuery;

    #[allow(dead_code)]
    fn generate_filters(&self) -> Vec<&FileFilter>;

    fn required_extensions(&self) -> Vec<String>;

    async fn current_target_snapshots(&self) -> Vec<SnapshotDescriptor>;
}

pub(crate) async fn call_private_sql(
    peer_client: &dyn PeerClient,
    target_sql: &SqlQuery,
    required_extensions: &Vec<String>,
    target_snapshots: &Vec<SnapshotDescriptor>,
    index: u64,
    num: u64,
) -> Result<Vec<RecordBatch>, PeerClientError> {
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
    target_sql: &SqlQuery,
    required_extensions: &Vec<String>,
    target_snapshots: &Vec<SnapshotDescriptor>,
    index: u64,
    num: u64,
) -> Result<Option<Vec<RecordBatch>>, PeerClientError> {
    match call_private_sql(peer_client, target_sql, required_extensions, target_snapshots, index, num).await {
        Ok(r) => Ok(Some(r)),
        Err(e) => Err(e),
    }
}


async fn call_peers_and_load_results(
    required_extensions: &Vec<String>,
    target_snapshots: &Vec<SnapshotDescriptor>, 
    sql: &SqlQuery
) -> Result<Option<String>, PeerClientError> {
    if target_snapshots.len() == 0 {
        return Ok(None)
    }

    let peer_clients: Vec<Box<dyn PeerClient>> = state_peers::get_peer_clients();

    let all_calls = peer_clients.iter().enumerate().map(
        |(index, client)| call_private_sql_and_load(client.as_ref(), sql, required_extensions, target_snapshots, index as u64, peer_clients.len() as u64));
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

    Ok(Some(table_name))
}


pub async fn load_command_raw_result(_context: CommandContext, command: Arc<dyn Command>) -> Result<Option<String>, PeerClientError> {
    let target_snapshots = command.current_target_snapshots().await;
    let required_extensions = command.required_extensions();
    let target_sql = command.generate_sql();
    call_peers_and_load_results(&required_extensions, &target_snapshots, &target_sql).await
}


pub async fn execute_command(_context: CommandContext, command: Arc<dyn Command>) -> ElasticSearchResponse {
    let result_table_name = match load_command_raw_result(_context, command.clone()).await {
        Ok(t) => t,
        Err(_) => return QueryFailure{ message: "Failed".to_string() }.to_response(),
    };         
    let response = command.result_generator(result_table_name.clone()).await.unwrap_or_else(|_e| {
        QueryFailure { message: "Failed".to_string() }.to_response()
    });
    if result_table_name.is_some() {
        data_access::drop(result_table_name.as_ref().unwrap()).await;
    }
    response
}


// We are using a columnar format. Structs are not as efficient in columnar formats.
// Therefore we "denormalize" the data from structs into individual fields. For example:
//
// {
//     "A": {
//         "B": null,
//         "C": "not null"
//     }
// }
//
// becomes
//
// {
//     "A_B": null,
//     "A_C": "not null"
// }
//
fn create_denormalized_value_worker(target_map: &mut Map<String, Value>, prefix: &String, value: &Value) -> () {
    assert!(value.is_object());
    for (map_key, map_value) in value.as_object().unwrap().iter() {
        match map_value {
            Value::Object(_) => {
                create_denormalized_value_worker(target_map, &format!("{}{}_", prefix, map_key), &map_value);
            },
            _ => {
                target_map.insert(format!("{}{}", prefix, map_key), map_value.clone());
            }
        }
    }
}

pub fn create_denormalized_value(value: &Value) -> Value {
    let mut new_map = serde_json::Map::new();

    create_denormalized_value_worker(&mut new_map, &"".to_string(), value);

    Value::from(new_map)
}


pub(crate) async fn result_to_record_batch(result: Vec<Vec<u8>>) -> Vec<RecordBatch> {
    let mut retval = Vec::new();
    let flight_data = result.iter().map(|x|Ok(FlightData::decode(&x[..]).unwrap())).collect::<Vec<Result<FlightData, FlightError>>>();
    let mut record_batch_stream = FlightRecordBatchStream::new_from_flight_data(stream::iter(flight_data));
    while let Some(batch) = record_batch_stream.next().await {
        match batch {
            Ok(batch) => retval.push(batch),
            Err(e) => {
                let error = format!("Error: {}", e);
                panic!("{}", error);
            }
        };
    }
    retval
}


#[cfg(test)]
mod tests {
    use crate::elastic_search_common::create_denormalized_value;

    #[test]
    fn test_denormalized() {
        let test_val = r#"{"A":{"B":null,"C":"NOT NULL"}}"#;

        let parsed_val = serde_json::from_str::<serde_json::Value>(test_val).unwrap();

        let test_val_again = serde_json::to_string(&parsed_val).unwrap();
        assert_eq!(test_val, test_val_again);

        let denormalized_val = create_denormalized_value(&parsed_val);
        assert_eq!(denormalized_val.as_object().unwrap().len(), 2);
        assert_eq!(denormalized_val.as_object().unwrap().get("A_B").unwrap(), &serde_json::Value::Null);
        assert_eq!(denormalized_val.as_object().unwrap().get("A_C").unwrap(), &serde_json::Value::String("NOT NULL".to_string()));
        let denormalized_val_str = serde_json::to_string(&denormalized_val).unwrap();
        assert!(denormalized_val_str.contains("A_B"));
        assert!(denormalized_val_str.contains("A_C"));
    }
}
