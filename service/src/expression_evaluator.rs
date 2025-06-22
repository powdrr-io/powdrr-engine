use std::{collections::HashMap, fmt, sync::{Arc, Mutex}};

use chrono::{DateTime, FixedOffset};
use minijinja::{value::{Object, ValueKind}, Environment, Error, State, Value};


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
            let arg_str = args[0].as_str().unwrap().to_string();
            Ok(Value::from_object(ZonedDateTimeObject{ value: DateTime::parse_from_rfc3339(&arg_str).unwrap() }))
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
        if name == "toInstant" {
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
        if name == "toEpochMilli" {
            // TODO: convert datetime str in 'value' to millis from the epoch
            Ok(Value::from(self.value.timestamp_millis()))
        } else {
            panic!("Method does not exist in Outputs")
        }
    }      
}


pub(crate) fn eval_template(expr_str: &str, source: &serde_json::Value, other_context: HashMap<String, Value>, params: Value) -> (String, serde_json::Value) {
    let mut env = Environment::new();
    env.add_template("foo", expr_str).unwrap();
    let (outputs_ctx, outputs_map) = Outputs::new();
    env.add_global("__private_impl", Value::from_object(outputs_ctx));
    env.add_global("__types", Value::from_object(Types::new()));
    let mut full_context = other_context.clone();
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
    apply(&mut output_value, &output_map_final);
    (template_output, output_value)
}


fn get_container<'a>(document: &'a mut serde_json::Value, path: &String) -> (&'a mut serde_json::Map<String, serde_json::Value>, String) {
    let path_split: Vec<&str> = path.split(".").collect();
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

pub(crate) fn apply(document: &mut serde_json::Value, updates: &HashMap<String, Value>) -> () {
    for pair in updates {
        let (container, key) = get_container(document, pair.0);
        container.insert(key, convert(&pair.1));
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use minijinja::Value;

    use crate::{expression_evaluator::eval_template, painless_parser::translate};

    #[test]
    fn test_assignment_script() {
        /*
        let test_val = r#"
    if (params.claimableTaskTypes.contains(ctx._source.task.taskType)) {
      if (ctx._source.task.schedule != null || ctx._source.task.attempts < params.taskMaxAttempts[ctx._source.task.taskType]) {
        if(ctx._source.task.retryAt != null && ZonedDateTime.parse(ctx._source.task.retryAt).toInstant().toEpochMilli() < params.now) {
          ctx._source.task.scheduledAt=ctx._source.task.retryAt;
        } else {
          ctx._source.task.scheduledAt=ctx._source.task.runAt;
        }
        ctx._source.task.status = "claiming"; ctx._source.task.ownerId=params.fieldUpdates.ownerId; ctx._source.task.retryAt=params.fieldUpdates.retryAt;
      } else {
        ctx._source.task.status = "failed";
      }
    } else if (params.unusedTaskTypes.contains(ctx._source.task.taskType)) {
      ctx._source.task.status = "unrecognized";
    } else {
      ctx.op = "noop";
    }"#;
        */
        let test_val = r#"
        if (params.claimableTaskTypes.contains(ctx._source.task.taskType)) {
            ctx._source.task.scheduledAt=ctx._source.task.retryAt;
        } else {
            ctx._source.task.scheduledAt=ctx._source.task.runAt;
        }"#;    

        let source = r#"{
                "task": {
                    "attempts": 1,
                    "retryAt": "2025-05-25T00:00:00Z",
                    "runAt": "2025-05-26T12:12:12Z",
                    "taskType": "foobar"
                }
        }"#;
        let source_val: serde_json::Value = serde_json::from_str(&source).unwrap();

        let params = r#"{
            "claimableTaskTypes": ["foobar"],
            "taskMaxAttempts": {
                "foobar": 1
            },
            "now": 999999999999
        }"#;
        let params_val: serde_json::Value = serde_json::from_str(&params).unwrap();

        let translated = translate(&test_val.to_string()).unwrap();

        let (_, _) = eval_template(translated.as_str(), &source_val, HashMap::new(), Value::from_serialize(params_val));

        let final_doc_str = serde_json::to_string(&source_val).unwrap();
        println!("{}", final_doc_str);


    }

    #[test]
    fn test_translate_and_execute_datetime_method() {
        let test_val = "ZonedDateTime.parse(\"2025-05-31T12:34:56Z\").toInstant().toEpochMilli()";
        let translated = translate(&test_val.to_string()).unwrap();

        let (retval, _) = eval_template(translated.as_str(), &serde_json::from_str("{}").unwrap(), HashMap::new(), Value::from_safe_string("nope".to_string()));

        println!("{}", retval)
    }    
}