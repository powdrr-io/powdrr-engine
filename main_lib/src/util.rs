use std::error::Error;


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
