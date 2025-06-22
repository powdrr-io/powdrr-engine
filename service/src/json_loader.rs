use std::{error::Error, fmt::Display, fs::File, io::{self, BufRead}, path::Path, sync::Arc};

use arrow_json::reader::infer_json_schema;
use arrow_select::concat;
use datafusion::arrow::{array::RecordBatch, datatypes::Schema, error::ArrowError};

use crate::util::log_err;



#[derive(Debug, Clone)]
struct JsonLoaderError {
    message: String,
}

impl Display for JsonLoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message);
        Ok(())
    }
}

impl Error for JsonLoaderError {}


fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}


fn one_line_to_record_batch(line: Result<String, io::Error>) -> Result<RecordBatch, JsonLoaderError> {
    let line_str  = match line {
        Ok(l) => l.clone(),
        Err(_) => return log_err(JsonLoaderError { message: "IO Error".to_string() })
    };
    let inferred_schema = infer_json_schema(line_str.as_bytes(), None).unwrap();

    let json_reader = match arrow_json::ReaderBuilder::new(Arc::new(inferred_schema.0)).build(line_str.as_bytes()) {
        Ok(d) => d,
        Err(_) => panic!("Private API returned result that does not match schema")
    };

    let record_batches: Result<Vec<RecordBatch>, ArrowError> = json_reader.collect();
    match record_batches {
        Ok(rb) => Ok(rb.get(0).unwrap().clone()),
        Err(_) => log_err(JsonLoaderError{ message: "Arrow error".to_string() })
    }
}


fn replace_schema(record_batch: &RecordBatch, schema: &Schema) -> Option<RecordBatch> {
    if *record_batch.schema() == *schema {
        return None
    }
    // First reorder the fields if needed
    panic!("Need to figure this out")
}


pub(crate) fn load_json(file_path: &String) -> Result<Vec<RecordBatch>, JsonLoaderError> {
    let line_iterator = match read_lines(file_path) {
        Ok(i) => i,
        Err(_e) => return log_err(JsonLoaderError{ message: "File opening error".to_string() }),
    };
    /*
    let record_batch_result: Result<Vec<RecordBatch>, JsonLoaderError> = line_iterator.map(one_line_to_record_batch).collect();
    match record_batch_result {
        Ok(rb) => {
            let final_schema = match Schema::try_merge(rb.iter().map(|r|*r.schema().as_ref())) {
                Ok(mb) => mb,
                Err(e) => return log_err(JsonLoaderError { message: "Schema merge failed".to_string() })
            };
            let normalized_record_batches = rb.iter().map(|x|{
                match replace_schema(x, &final_schema) {
                    Some(rb) => rb,
                    None => panic!("what happened?")
                }
            });
            match concat::concat_batches(&Arc::new(final_schema.clone()), normalized_record_batches.map(|x|&x)) {
                Ok(rb) => Ok(vec!(rb)),
                Err(e) => log_err(JsonLoaderError { message: "Error during concat".to_string() })
            }
        },
        Err(e) => return Err(e),
    }
    */
    Ok(vec!())
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_loading() {
    }    
}