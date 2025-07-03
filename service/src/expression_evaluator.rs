use std::{collections::HashMap, fmt, sync::{Arc, Mutex}};

use chrono::{DateTime, FixedOffset};
use minijinja::{value::{Object, ValueKind}, Environment, Error, State, Value};
use crate::elastic_search_common::create_normalized_name;

#[derive(Debug)]
struct Outputs {
    outputs: Arc<Mutex<HashMap<String, Value>>>
}

impl Outputs {
    fn new() -> (Self, Arc<Mutex<HashMap<String, Value>>>) {
        let outputs_map = Arc::new(Mutex::new(HashMap::new()));

        (
            Outputs { outputs: outputs_map.clone() },
            outputs_map
        )
    }
}

impl fmt::Display for Outputs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<Query table={:?}>", self.outputs)
    }
}

impl Object for Outputs {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        if name == "assign" {
            self.outputs.lock().unwrap().insert(args[0].as_str().unwrap().to_string(), args[1].clone());
            Ok(Value::from(""))
        } else {
            panic!("Method does not exist in Outputs")
        }
    }
}

#[derive(Debug)]
struct Types {
    types: HashMap<String, Value>,
}

impl Types {
    fn new() -> Self {
        Types { types: HashMap::from([
            ("ZonedDateTime".to_string(), Value::from_object(ZonedDateTimeClass{})),
        ])

        }
    }
}

impl Object for Types { 
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        let key_str = key.as_str().unwrap();
        let retval = self.types.get(key_str);
        match retval {
            Some(r) => Some(r.clone()),
            None => None
        }
    }     
}

#[derive(Debug)]
struct ZonedDateTimeClass {
}


impl Object for ZonedDateTimeClass {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        if name == "parse" {
            if args[0].is_undefined() {
                Ok(Value::from_object(ZonedDateTimeObject{ value: DateTime::from_timestamp_millis(0).unwrap().fixed_offset() }))
            } else {
                let arg_str = args[0].as_str().unwrap().to_string();
                Ok(Value::from_object(ZonedDateTimeObject{ value: DateTime::parse_from_rfc3339(&arg_str).unwrap() }))
            }
        } else {
            panic!("Method does not exist in Outputs")
        }
    }      
}

#[derive(Debug)]
struct ZonedDateTimeObject {
    value: chrono::DateTime<FixedOffset>,
}

impl Object for ZonedDateTimeObject {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        _args: &[Value],
    ) -> Result<Value, Error> {
        if name == create_normalized_name(&"toInstant".to_string()) {
            Ok(Value::from_object(Instant{ value: self.value.clone() }))
        } else {
            panic!("Method does not exist in Outputs")
        }
    }      
}

#[derive(Debug)]
struct Instant {
    value: chrono::DateTime<FixedOffset>,
}

impl Object for Instant {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        _args: &[Value],
    ) -> Result<Value, Error> {
        if name == create_normalized_name(&"toEpochMilli".to_string()) {
            // TODO: convert datetime str in 'value' to millis from the epoch
            Ok(Value::from(self.value.timestamp_millis()))
        } else {
            panic!("Method does not exist in Outputs")
        }
    }      
}


pub(crate) struct EvalOutput {
    pub result: String,
    pub source: serde_json::Value,
    pub other_context: HashMap<String, Value>,
}


pub(crate) fn eval_template(expr_str: &str, source: &serde_json::Value, other_context: HashMap<String, Value>, params: Value) -> EvalOutput {
    let mut env = Environment::new();
    env.add_template("foo", expr_str).unwrap();
    let (outputs_ctx, outputs_map) = Outputs::new();
    env.add_global("__private_impl", Value::from_object(outputs_ctx));
    env.add_global("__types", Value::from_object(Types::new()));
    let mut full_context = other_context.clone();
    // Note: Value::from_serialize() converts a serde_json::Value to a minijinja::Value
    full_context.insert("_source".to_string(), Value::from_serialize(source));
    env.add_global("ctx", full_context);
    env.add_global("params", params);    
    let template = env.get_template("foo").unwrap();
    let template_output = match template.render(()) {
        Ok(t) => t,
        Err(e) => {
            let error = format!("{}", e);
            println!("{}", error);
            panic!("nope");
        }
    };
    let output_map_final = outputs_map.lock().unwrap();
    let mut output_value = source.clone();
    let other_context_output = apply(&other_context, &mut output_value, &output_map_final);
    EvalOutput {
        result: template_output,
        source: output_value,
        other_context: other_context_output
    }
}


fn get_container<'a>(document: &'a mut serde_json::Value, path_split: Vec<&str>) -> (&'a mut serde_json::Map<String, serde_json::Value>, String) {
    if path_split.len() <= 2 {
        panic!("Should always we ctx._source.<stuff>")
    }

    assert_eq!(*path_split.get(0).unwrap(), "ctx");
    assert_eq!(*path_split.get(1).unwrap(), "_source");

    // current_obj starts out representing _source
    let mut current_obj = document.as_object_mut().unwrap();
    let mut current_index: usize = 2;
    while current_index < path_split.len() - 1 {
        let key = path_split.get(current_index).unwrap().to_string();
        match current_obj.get(&key) {
            Some(_) => (),
            None => {
                current_obj.insert(key.clone(), serde_json::Value::from(serde_json::Map::new()));
            }
        };
        let value = current_obj.get_mut(&key).unwrap();
        current_obj = match value.as_object_mut() {
            Some(o) => o,
            None => panic!("Oops, need an error path here")
        };
        current_index += 1;
    }
    (current_obj, path_split.get(current_index).unwrap().to_string())
}

fn convert(value: &Value) -> serde_json::Value {
    match value.kind() {
        ValueKind::String => serde_json::Value::String(value.as_str().unwrap().to_string()),
        _ => panic!("Unable to convert type from Jinja to Serde")
    }
}

pub(crate) fn apply(
        other_context: &HashMap<String, Value>, 
        document: &mut serde_json::Value,
        updates: &HashMap<String, Value>
) -> HashMap<String, Value> {
    let mut retval = other_context.clone();
    for (path, value) in updates {
        let path_split: Vec<&str> = path.split(".").collect();
        if path_split.len() == 2 {
            retval.insert(path_split[1].to_string(), value.clone());
        } else if path_split.len() > 2 {
            assert_eq!(path_split.get(1).unwrap().to_string(), "_source".to_string());
            let (container, key) = get_container(document, path_split);
            container.insert(key, convert(&value));
        } else {
            panic!("Path length of 1 is not supported: {}", path)
        };
    }
    retval
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use minijinja::Value;

    use crate::{expression_evaluator::eval_template, painless_parser::translate};
    use crate::elastic_search_common::create_normalized_value;

    #[test]
    fn test_assignment_script() {
        let test_val = r#"
        if(ctx._source.task.retryAt != null) {
            ctx._source.task.scheduledAt=ctx._source.task.retryAt;
        } else {
            ctx._source.task.scheduledAt=ctx._source.task.runAt;
        }"#;    

        let source = r#"{
                "task": {
                    "attempts": 1,
                    "retryAt": null,
                    "runAt": "2025-05-26T12:12:12Z",
                    "taskType": "foobar"
                }
        }"#;
        let source_val: serde_json::Value = create_normalized_value(&serde_json::from_str(&source).unwrap());

        let params = r#"{
            "claimableTaskTypes": ["foobar"],
            "taskMaxAttempts": {
                "foobar": 1
            },
            "now": 99999999999999999999
        }"#;
        let params_val: serde_json::Value = create_normalized_value(&serde_json::from_str(&params).unwrap());

        let translated = translate(&test_val.to_string()).unwrap();

        let eval_result = eval_template(translated.as_str(), &source_val, HashMap::new(), Value::from_serialize(params_val));

        let final_doc_str = serde_json::to_string(&eval_result.source).unwrap();
        assert!(final_doc_str.contains("\"scheduledat\":\"2025-05-26T12:12:12Z\""));
    }
        
    #[test]
    fn test_translate_and_execute_datetime_method() {
        let test_val = "ZonedDateTime.parse(\"2025-05-31T12:34:56Z\").toInstant().toEpochMilli()";
        let translated = translate(&test_val.to_string()).unwrap();

        let eval_result = eval_template(translated.as_str(), &serde_json::from_str("{}").unwrap(), HashMap::new(), Value::from_safe_string("nope".to_string()));

        // TODO: actually verify the result here       
        println!("{}", eval_result.result)
    }    
}