use std::error::Error;
use gotham::mime;
use gotham::state::State;
use http::StatusCode;
use crate::data_contract::TableDescription;
use crate::elastic_search_common::ElasticSearchResponse;
use crate::state_provider::{ServiceApiError, STATE_PROVIDER};

pub(crate) fn add_file_suffix(base_file_path: &String, suffix: &String, extension: Option<&String>) -> String {
    if !base_file_path.ends_with(".json") && !base_file_path.ends_with(".arrow") && !base_file_path.ends_with(".parquet") {
        return match extension {
            None => format!("{}_{}", base_file_path, suffix).to_string(),
            Some(e) => format!("{}_{}{}", base_file_path, suffix, e).to_string(),
        }
    }

    let index = base_file_path.rfind(".");
    match index {
        Some(i) => {
            match extension {
                None => format!("{}_{}{}", base_file_path[..i].to_string(), suffix, base_file_path[i..].to_string()).to_string(),
                Some(e) => format!("{}_{}{}", base_file_path[..i].to_string(), suffix, e).to_string(),
            }
        },
        None => {
            match extension {
                None => format!("{}_{}", base_file_path, suffix).to_string(),
                Some(e) => format!("{}_{}{}", base_file_path, suffix, e).to_string(),
            }
        }
    }
}


pub(crate) fn log_err<SuccessType, ErrorType: Error>(error: ErrorType) -> Result<SuccessType, ErrorType> {
    let error_str = format!("{}", error);
    println!("{}", error_str);
    tracing::info!("{}", error);
    Err(error)
}


pub(crate) fn log_service_err(error: ServiceApiError) -> ElasticSearchResponse {
    let error_str = format!("{}", error);
    println!("{}", error_str);
    tracing::info!("{}", error);
    ElasticSearchResponse {
        status: StatusCode::SERVICE_UNAVAILABLE,
        mime: mime::TEXT_PLAIN_UTF_8,
        body: "Service unavailable".to_string(),
        headers: vec![],
    }
}

pub(crate) fn log_service_err_response(error: ServiceApiError, state: State) -> (State, gotham::hyper::Response<gotham::hyper::Body>) {
    let res = log_service_err(error).generate_response(&state);
    (state, res)
}

pub(crate) async fn describe_table_log_error_then_none(table_name: &String) -> Option<TableDescription> {
    STATE_PROVIDER.describe_table(table_name).await.unwrap_or_else(|e|{
        log_service_err(e);
        None
    })
}

