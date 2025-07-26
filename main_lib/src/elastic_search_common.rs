
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
use crate::state_peers::{self, PeerClient, PeerClientError, PrivateInvocation, PrivateInvocationResult};


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

pub struct CommandError {
    pub message: String,
}

unsafe impl Send for CommandError {}
unsafe impl Sync for CommandError {}


pub type ResultGeneratorFuture = dyn Future<Output = Result<ElasticSearchResponse, CommandError>> + Send;


#[async_trait]
pub(crate) trait Command: Send + Sync {
    async fn get_private_invocation(&self) -> PrivateInvocation;

    fn result_generator(&self, result_table_name: Option<String>) -> Pin<Box<ResultGeneratorFuture>>;
}


pub(crate) async fn call_private_sql(
    peer_client: &dyn PeerClient,
    invocation: &PrivateInvocation,
    index: u64,
    num: u64,
) -> Result<PrivateInvocationResult, PeerClientError> {
    match invocation {
        PrivateInvocation::Sql(sql_invocation) => {
            match peer_client.private_sql(sql_invocation, index, num).await {
                Ok(data) => Ok(PrivateInvocationResult::Data(data)),
                Err(e) => Err(e),
            }
        },
        PrivateInvocation::Compaction(compaction_invocation) => {
            match peer_client.private_compaction(compaction_invocation, index, num).await {
                Ok(data) => Ok(PrivateInvocationResult::Data(data)),
                Err(e) => Err(e),
            }
        },
        PrivateInvocation::Extension(extension_invocation) => {
            match peer_client.private_extension(extension_invocation, index, num).await {
                Ok(result) => Ok(PrivateInvocationResult::Extension(result)),
                Err(e) => Err(e),
            }
        }
    }
}


pub async fn call_peers(
    invocation: &PrivateInvocation
) -> Result<Vec<PrivateInvocationResult>, PeerClientError> {
    let peer_clients: Vec<Box<dyn PeerClient>> = state_peers::get_peer_clients();

    let all_calls = peer_clients.iter().enumerate().map(
        |(index, client)| call_private_sql(client.as_ref(), invocation, index as u64, peer_clients.len() as u64));
    try_join_all(all_calls).await
}


pub async fn load_command_raw_result(_context: CommandContext, command: Arc<dyn Command>) -> Result<Option<String>, PeerClientError> {
    let invocation = command.get_private_invocation().await;
    let all_results = match call_peers(&invocation).await {
        Ok(results) => results,
        Err(e) => return Err(e)
    };

    if all_results.len() == 0 {
        return Ok(None)
    }

    let all_records = all_results.iter().map(|x| {
        match x {
            PrivateInvocationResult::Extension(_) => panic!("Unexpected"),
            PrivateInvocationResult::Data(data) => data.clone(),
        }
    }).flatten().collect::<Vec<RecordBatch>>();

    if all_records.len() != 0 {
        let table_name = match load_memtable(&all_records).await {
            Ok(name) => name,
            Err(e) => return Err(PeerClientError { message: e.message().to_string() })
        };

        Ok(Some(table_name))
    } else {
        Ok(None)
    }
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
            Value::Array(_) => {
                // We just skip all arrays for now.
            },
            Value::Null => {
                // We just skip all nulls for now.
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
    use serde_json::json;
    use crate::elastic_search_common::create_denormalized_value;

    #[test]
    fn test_denormalized() {
        let test_val = r#"{"A":{"A":null,"B":4,"C":"NOT NULL","D":[1,2,3]}}"#;

        let parsed_val = serde_json::from_str::<serde_json::Value>(test_val).unwrap();

        let test_val_again = serde_json::to_string(&parsed_val).unwrap();
        assert_eq!(test_val, test_val_again);

        let denormalized_val = create_denormalized_value(&parsed_val);
        assert_eq!(denormalized_val.as_object().unwrap().len(), 2);
        assert_eq!(denormalized_val.as_object().unwrap().get("A_B").unwrap(), &json!(4));
        assert_eq!(denormalized_val.as_object().unwrap().get("A_C").unwrap(), &serde_json::Value::String("NOT NULL".to_string()));
        let denormalized_val_str = serde_json::to_string(&denormalized_val).unwrap();
        assert!(denormalized_val_str.contains("A_B"));
        assert!(denormalized_val_str.contains("A_C"));
    }
}
