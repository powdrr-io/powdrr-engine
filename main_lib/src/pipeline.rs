use std::{collections::HashMap, error::Error, fmt::Display};

use minijinja::context;
use serde::{ser::SerializeMap, Deserialize, Serialize};
use serde_json::Value;

use crate::expression_evaluator;


fn is_false(b: &bool) -> bool { !b }


#[derive(Debug)]
pub struct PipelineError {}

impl Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = f.write_str("PipelineError");
        Ok(())
    }
}


impl Error for PipelineError {}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SetProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _if: Option<String>,
    pub field: String,
    pub value_formula: String,
    #[serde(skip_serializing_if = "is_false")]
    #[serde(default)]
    pub ignore_empty_value: bool,    
    pub metadata: HashMap<String, String>,
}

impl SetProcessorBody {
    pub fn new(description: &Option<String>, _if: &Option<String>, field: &String, value_formula: &String, ignore_empty_value: bool, metadata: &HashMap<String, String>) -> Self {
        SetProcessorBody { 
            description: description.clone(), 
            _if: _if.clone(), 
            field: field.clone(), 
            value_formula: value_formula.clone(), 
            ignore_empty_value: ignore_empty_value,
            metadata: metadata.clone()
        }
    }

    fn apply(&self, value: &mut Value) -> Result<(), PipelineError> {
        match &self._if {
            Some(if_val) => {
                // TODO: need to revisit this, what is that context?
                let if_result = expression_evaluator::eval_template(if_val, value, HashMap::new(), context!{ a => "a" });
                // TODO: does Painless have a "true-ish" semantic like Python?
                if if_result.result.to_string() != "true" {
                    return Ok(())
                }
            },
            None => ()
        };

        // TODO: need to revisit this, context is weird, what about fields not in the root of the source?
        let eval_result = expression_evaluator::eval_template(&self.value_formula, value, HashMap::new(), context!{ a => "a" });
        match value {
            Value::Object(inner_val) => {
                match inner_val.get_mut("_source") {
                    Some(source_val) => {
                        match source_val {
                            Value::Object(source_map) => {
                                source_map.insert(self.field.clone(), Value::String(eval_result.result.as_str().to_string()));
                            },
                            _ => return Err(PipelineError {  })
                        }
                    },
                    None => return Err(PipelineError {  })
                }
            },
            _ => return Err(PipelineError {  })
        }

        Ok(())
    }    
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppendProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _if: Option<String>,
    pub field: String,
    pub value_formula: String,
    #[serde(skip_serializing_if = "is_false")]
    #[serde(default)]
    pub ignore_empty_value: bool,    
    pub metadata: HashMap<String, String>,
}

impl AppendProcessorBody {
    pub fn new(description: &Option<String>, _if: &Option<String>, field: &String, value_formula: &String, ignore_empty_value: bool, metadata: &HashMap<String, String>) -> Self {
        AppendProcessorBody { 
            description: description.clone(), 
            _if: _if.clone(), 
            field: field.clone(), 
            value_formula: value_formula.clone(), 
            ignore_empty_value: ignore_empty_value,
            metadata: metadata.clone()
        }
    }

    fn apply(&self, _value: &mut Value) -> Result<(), PipelineError> {
        todo!()
    }    
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RemoveProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _if: Option<String>,
    pub field: String,
    pub metadata: HashMap<String, String>,
}

impl RemoveProcessorBody {
    pub fn new(description: &Option<String>, _if: &Option<String>, field: &String, metadata: &HashMap<String, String>) -> Self {
        RemoveProcessorBody { 
            description: description.clone(), 
            _if: _if.clone(), 
            field: field.clone(), 
            metadata: metadata.clone()
        }
    }

    fn apply(&self, _value: &mut Value) -> Result<(), PipelineError> {
        todo!()
    }    
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PipelineProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _if: Option<String>,
    pub name: String,
    pub metadata: HashMap<String, String>,
}

impl PipelineProcessorBody {
    pub fn new(description: &Option<String>, _if: &Option<String>, name: &String, metadata: &HashMap<String, String>) -> Self {
        PipelineProcessorBody { 
            description: description.clone(), 
            _if: _if.clone(), 
            name: name.clone(),
            metadata: metadata.clone(),
        }
    }

    fn apply(&self, _value: &mut Value) -> Result<(), PipelineError> {
        todo!()
    }    
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RenameProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _if: Option<String>,
    pub field: String,
    pub target_field: String,
    pub metadata: HashMap<String, String>,
}

impl RenameProcessorBody {
    pub fn new(description: &Option<String>, _if: &Option<String>, field: &String, target_field: &String, metadata: &HashMap<String, String>) -> Self {
        RenameProcessorBody { 
            description: description.clone(), 
            _if: _if.clone(), 
            field: field.clone(),
            target_field: target_field.clone(),
            metadata: metadata.clone(),
        }
    }

    fn apply(&self, _value: &mut Value) -> Result<(), PipelineError> {
        todo!()
    }    
}


#[derive(Debug, Clone)]
pub enum ProcessorBodies {
    Set(SetProcessorBody),
    Remove(RemoveProcessorBody),
    Pipeline(PipelineProcessorBody),
    Rename(RenameProcessorBody),
    Append(AppendProcessorBody),
}


impl Serialize for ProcessorBodies {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        let mut map_serializer = serializer.serialize_map(None)?;
        match self {
            ProcessorBodies::Set(body) => {
                map_serializer.serialize_entry("set", body)?;
            },
            ProcessorBodies::Pipeline(body) => {
                map_serializer.serialize_entry("pipeline", body)?;
            },
            ProcessorBodies::Remove(body) => {
                map_serializer.serialize_entry("remove", body)?;
            },
            ProcessorBodies::Rename(body) => {
                map_serializer.serialize_entry("rename", body)?;
            },  
            ProcessorBodies::Append(body) => {
                map_serializer.serialize_entry("append", body)?;
            },                                                 
        }
        map_serializer.end()
    }
}

impl<'de> Deserialize<'de> for ProcessorBodies {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de> {
        let map_value: HashMap<String, Value> = HashMap::deserialize(deserializer)?;
        if map_value.len() != 1 {
            panic!("How do I error here?")
        }
        let only_pair = map_value.iter().next().unwrap();
        match only_pair.0.as_str() {
            "set" => Ok(ProcessorBodies::Set(serde_json::from_value::<SetProcessorBody>(only_pair.1.clone()).unwrap())),
            "rename" => Ok(ProcessorBodies::Rename(serde_json::from_value::<RenameProcessorBody>(only_pair.1.clone()).unwrap())),
            "pipeline" => Ok(ProcessorBodies::Pipeline(serde_json::from_value::<PipelineProcessorBody>(only_pair.1.clone()).unwrap())),
            "remove" => Ok(ProcessorBodies::Remove(serde_json::from_value::<RemoveProcessorBody>(only_pair.1.clone()).unwrap())),
            "append" => Ok(ProcessorBodies::Append(serde_json::from_value::<AppendProcessorBody>(only_pair.1.clone()).unwrap())),
            _ => panic!("How do I error here?")
        }
    }
}


pub(crate) fn apply_definition(pipeline: &PipelineDefinition, doc: &mut Value) -> () {
    let mut has_error = false;
    for processor in &pipeline.processors {
        match apply_processor(processor, doc) {
            Ok(_) => (),
            Err(_) => {
                has_error = true;
                break;
            }
        }
    }

    if has_error {
        for processor in &pipeline.on_failure {
            match apply_processor(processor, doc) {
                Ok(_) => (),
                Err(_) => {
                    break;
                }
            }
        }        
    }
}


fn apply_processor(processor: &ProcessorBodies, value: &mut Value) -> Result<(), PipelineError> {
    match processor {
        ProcessorBodies::Set(body) => {
            body.apply(value)
        },
        ProcessorBodies::Rename(body) => {
            body.apply(value)
        },
        ProcessorBodies::Pipeline(body) => {
            body.apply(value)
        },
        ProcessorBodies::Remove(body) => {
            body.apply(value)
        },     
        ProcessorBodies::Append(body) => {
            body.apply(value)
        },                     
    }
}


#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PipelineDefinition {
    pub description: Option<String>,
    pub processors: Vec<ProcessorBodies>,
    pub on_failure: Vec<ProcessorBodies>,
}


impl PipelineDefinition {
    #[allow(dead_code)]
    pub fn new(description: &Option<String>, processors: &Vec<ProcessorBodies>, on_failure: &Vec<ProcessorBodies>) -> Self {
        PipelineDefinition { 
            description: description.clone(), 
            processors: processors.clone(),
            on_failure: on_failure.clone(),
        }
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::pipeline::{PipelineDefinition, ProcessorBodies, SetProcessorBody};

    #[test]
    fn test_processor_parser() {  
        let definition = PipelineDefinition{
            description: Some("dude fresh".to_string()),
            processors: vec!(ProcessorBodies::Set(SetProcessorBody{
                description: Some("hotness".to_string()),
                _if: Some("you know it".to_string()),
                value_formula: "the best".to_string(),
                field: "or nothing at all".to_string(),
                ignore_empty_value: true,
                metadata: HashMap::from([("key".to_string(), "value".to_string())])
            })),
            on_failure: vec!(),
        };

        let serialized = serde_json::to_string(&definition).unwrap();

        let deserialied= serde_json::from_str::<PipelineDefinition>(serialized.as_str()).unwrap();
        assert_eq!(deserialied.processors.len(), 1);
    }
}