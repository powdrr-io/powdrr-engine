use std::collections::HashMap;
use chrono::prelude::*;

use serde::{ser::SerializeMap, Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{painless_parser, pipeline::{self, AppendProcessorBody, PipelineDefinition, PipelineError, PipelineProcessorBody, ProcessorBodies, RemoveProcessorBody, RenameProcessorBody, SetProcessorBody}, state_hosted_service::API_SERVICE_CLIENT};
use crate::util::log_service_err;

fn is_false(b: &bool) -> bool { !b }


pub(crate) struct ESPipelineError {
    pub message: String
}

#[derive(Serialize)]
pub(crate) struct CreatePipelineResult {
    acknowledged: bool
}

impl CreatePipelineResult {
    fn new() -> Self {
        CreatePipelineResult { acknowledged: true }
    }
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SetElasticSearchProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    _if: Option<String>,
    field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    copy_from: Option<String>,
    #[serde(skip_serializing_if = "is_false")]
    #[serde(default)]
    ignore_empty_value: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct PipelineElasticSearchProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    _if: Option<String>,
    name: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct AppendElasticSearchProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    _if: Option<String>,
    field: String,
    value: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RemoveElasticSearchProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    _if: Option<String>,
    field: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RenameElasticSearchProcessorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(rename = "if")]
    #[serde(skip_serializing_if = "Option::is_none")]
    _if: Option<String>,
    field: String,
    target_field: String,
}


#[derive(Clone)]
pub(crate) enum ElasticSearchProcessorBodies {
    Set(SetElasticSearchProcessorBody),
    Pipeline(PipelineElasticSearchProcessorBody),
    Remove(RemoveElasticSearchProcessorBody),
    Append(AppendElasticSearchProcessorBody),
    Rename(RenameElasticSearchProcessorBody),
}


impl Serialize for ElasticSearchProcessorBodies {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        let mut map_serializer = serializer.serialize_map(None)?;
        match self {
            ElasticSearchProcessorBodies::Set(body) => {
                map_serializer.serialize_entry("set", body)?;
            },
            ElasticSearchProcessorBodies::Pipeline(body) => {
                map_serializer.serialize_entry("pipeline", body)?;
            },
            ElasticSearchProcessorBodies::Remove(body) => {
                map_serializer.serialize_entry("remove", body)?;
            },
            ElasticSearchProcessorBodies::Append(body) => {
                map_serializer.serialize_entry("append", body)?;
            },
            ElasticSearchProcessorBodies::Rename(body) => {
                map_serializer.serialize_entry("rename", body)?;
            },                                    
        }
        map_serializer.end()
    }
}

impl<'de> Deserialize<'de> for ElasticSearchProcessorBodies {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de> {
        let map_value: HashMap<String, Value> = HashMap::deserialize(deserializer)?;
        if map_value.len() != 1 {
            panic!("How do I error here?")
        }
        let only_pair = map_value.iter().next().unwrap();
        let error = format!("{}", only_pair.1);
        println!("{}", error);
        match only_pair.0.as_str() {
            "set" => Ok(ElasticSearchProcessorBodies::Set(serde_json::from_value::<SetElasticSearchProcessorBody>(only_pair.1.clone()).unwrap())),
            "pipeline" => Ok(ElasticSearchProcessorBodies::Pipeline(serde_json::from_value::<PipelineElasticSearchProcessorBody>(only_pair.1.clone()).unwrap())),
            "remove" => Ok(ElasticSearchProcessorBodies::Remove(serde_json::from_value::<RemoveElasticSearchProcessorBody>(only_pair.1.clone()).unwrap())),
            "append" => Ok(ElasticSearchProcessorBodies::Append(serde_json::from_value::<AppendElasticSearchProcessorBody>(only_pair.1.clone()).unwrap())),
            "rename" => Ok(ElasticSearchProcessorBodies::Rename(serde_json::from_value::<RenameElasticSearchProcessorBody>(only_pair.1.clone()).unwrap())),
            _ => panic!("How do I error here?")
        }
    }
}

fn is_empty(the_list: &Vec<ElasticSearchProcessorBodies>) -> bool {
    the_list.len() == 0
}


#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ElasticSearchPipelineDefinition {
    pub description: Option<String>,
    pub processors: Vec<ElasticSearchProcessorBodies>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_empty")]
    pub on_failure: Vec<ElasticSearchProcessorBodies>,
}


impl ElasticSearchPipelineDefinition {
    fn convert_if(if_stmt: &Option<String>, original_name: &String, originals: &mut HashMap<String, String>) -> Result<Option<String>, PipelineError> {
        match if_stmt {
            Some(if_str) => {
                originals.insert(original_name.clone(), if_str.clone());
                Ok(Some(painless_parser::translate(if_str).map_err(|_|PipelineError{})?))
            },
            None => {
                Ok(None)
            },
        }    
    }

    #[allow(dead_code)]
    fn from_process_definition(definition: &PipelineDefinition) -> Self {
        let processors = definition.processors.iter().map(|x|Self::convert_from_processor(x)).collect();
        let on_failure = definition.on_failure.iter().map(|x|Self::convert_from_processor(x)).collect();
        ElasticSearchPipelineDefinition { 
            description: definition.description.clone(), 
            processors: processors,
            on_failure: on_failure,
        }
    }

    #[allow(dead_code)]
    fn convert_from_processor(processor: &ProcessorBodies) -> ElasticSearchProcessorBodies {
        match processor {
            ProcessorBodies::Set(body) => {
                ElasticSearchProcessorBodies::Set(SetElasticSearchProcessorBody{
                    description: body.description.clone(),
                    _if: body.metadata.get("original_if").cloned(),
                    field: body.field.clone(),
                    value: body.metadata.get("original_value").cloned(),
                    copy_from: body.metadata.get("original_copy_from").cloned(),
                    ignore_empty_value: body.ignore_empty_value,
                })
            },
            ProcessorBodies::Rename(body) => {
                ElasticSearchProcessorBodies::Rename(RenameElasticSearchProcessorBody{
                    description: body.description.clone(),
                    _if: body.metadata.get("original_if").cloned(),
                    field: body.field.clone(),
                    target_field: body.target_field.clone(),
                })
            },
            ProcessorBodies::Pipeline(body) => {
                ElasticSearchProcessorBodies::Pipeline(PipelineElasticSearchProcessorBody{
                    description: body.description.clone(),
                    _if: body.metadata.get("original_if").cloned(),
                    name: body.name.clone(),
                })
            },
            ProcessorBodies::Remove(body) => {
                ElasticSearchProcessorBodies::Remove(RemoveElasticSearchProcessorBody{
                    description: body.description.clone(),
                    _if: body.metadata.get("original_if").cloned(),
                    field: body.field.clone(),
                })
            }, 
            ProcessorBodies::Append(body) => {
                ElasticSearchProcessorBodies::Append(AppendElasticSearchProcessorBody{
                    description: body.description.clone(),
                    _if: body.metadata.get("original_if").cloned(),
                    field: body.field.clone(),
                    value: body.value_formula.clone(),
                })
            },                                                  
        }
    }

    fn convert_to_processor(processor: &ElasticSearchProcessorBodies) -> Result<ProcessorBodies, PipelineError> {
        match processor {
            ElasticSearchProcessorBodies::Set(body) => {
                let mut originals = HashMap::new();
                let if_translated = ElasticSearchPipelineDefinition::convert_if(&body._if, &"original_if".to_string(), &mut originals)?;
                let value_formula = match &body.copy_from {
                    Some(cf) => {
                        originals.insert("original_copy_from".to_string(), cf.clone());
                        match &body.value {
                            Some(_) => return Err(PipelineError {  }),
                            None => {
                                format!("{} _message.{} {}", "{{", cf, "}}")
                            }
                        }
                    },
                    None => {
                        match &body.value {
                            Some(v) => {
                                originals.insert("original_value".to_string(), v.clone());
                                v.clone()
                            },
                            None => return Err(PipelineError {  }),
                        }
                    }
                };
                Ok(ProcessorBodies::Set(SetProcessorBody::new(
                    &body.description,
                    &if_translated,
                    &body.field,
                    &value_formula,
                    body.ignore_empty_value,
                    &originals,
                )))
            },
            ElasticSearchProcessorBodies::Pipeline(body) => {
                let mut originals = HashMap::new();
                let if_translated = ElasticSearchPipelineDefinition::convert_if(&body._if, &"original_if".to_string(), &mut originals)?;
                Ok(ProcessorBodies::Pipeline(PipelineProcessorBody::new(
                    &body.description,
                    &if_translated,
                    &body.name,
                    &originals
                )))
            },
            ElasticSearchProcessorBodies::Remove(body) => {
                let mut originals = HashMap::new();
                let if_translated = ElasticSearchPipelineDefinition::convert_if(&body._if, &"original_if".to_string(), &mut originals)?;
                Ok(ProcessorBodies::Remove(RemoveProcessorBody::new(
                    &body.description,
                    &if_translated,
                    &body.field,
                    &originals
                )))
            },
            ElasticSearchProcessorBodies::Append(body) => {
                let mut originals = HashMap::new();
                let if_translated = ElasticSearchPipelineDefinition::convert_if(&body._if, &"original_if".to_string(), &mut originals)?;
                Ok(ProcessorBodies::Append(AppendProcessorBody::new(
                    &body.description,
                    &if_translated,
                    &body.field,
                    &body.value,
                    false,
                    &originals
                )))
            },
            ElasticSearchProcessorBodies::Rename(body) => {
                let mut originals = HashMap::new();
                let if_translated = ElasticSearchPipelineDefinition::convert_if(&body._if, &"original_if".to_string(), &mut originals)?;
                Ok(ProcessorBodies::Rename(RenameProcessorBody::new(
                    &body.description,
                    &if_translated,
                    &body.field,
                    &body.target_field,
                    &originals
                )))
            }
        }
    }

    fn to_pipeline_definition(&self) -> Result<PipelineDefinition, PipelineError> {
        let processors = self.processors.iter().map(|x|Self::convert_to_processor(x)).collect::<Result<Vec<ProcessorBodies>, PipelineError>>()?;
        let on_failure = self.on_failure.iter().map(|x|Self::convert_to_processor(x)).collect::<Result<Vec<ProcessorBodies>, PipelineError>>()?;
        Ok(PipelineDefinition { 
            description: self.description.clone(), 
            processors: processors,
            on_failure: on_failure,
        })
    }

}


#[allow(dead_code)]
pub(crate) fn parse_named_definition(value: &String) -> Result<(String, ElasticSearchPipelineDefinition), PipelineError> {
    let named_definition = serde_json::from_str::<HashMap<String, ElasticSearchPipelineDefinition>>(value).unwrap();

    if named_definition.len() != 1 {
        return Err(PipelineError {  });
    }

    let only_pair = named_definition.iter().next().unwrap();

    Ok((only_pair.0.clone(), only_pair.1.clone()))
}

pub(crate) async fn create_pipeline(name: &String, definition_str: &String) -> Result<CreatePipelineResult, ESPipelineError> {
    let pipeline_definition = serde_json::from_str(definition_str).map_err(|_|ESPipelineError{ message: "dunno".to_string() })?;

    API_SERVICE_CLIENT.create_pipeline(name, &pipeline_definition).await.map_err(|e|ESPipelineError{ message: format!("{}", e) })?;

    Ok(CreatePipelineResult::new())
}


#[derive(Serialize)]
pub(crate) struct SimulatePipelineResult {
    docs: Vec<HashMap<String, Value>>
}

#[derive(Deserialize)]
struct SimulatePipelineRequest {
    pipeline: Option<ElasticSearchPipelineDefinition>,
    docs: Vec<Value>,
}


fn apply_ingest(value: &mut Value) -> () {
    match value {
        Value::Object(inner_map) => {
            inner_map.insert("_version".to_string(), Value::String("-3".to_string()));
            let mut m = Map::new();
            m.insert("timestamp".to_owned(),  Value::String(Utc::now().to_string()));
            inner_map.insert("_ingest".to_string(), Value::Object(m));
        },
        _ => panic!("Value is malformed")
    }
}


pub(crate) async fn simulate_pipeline(name: &Option<String>, definition_str: &String) -> Result<SimulatePipelineResult, ESPipelineError> {
    let mut request: SimulatePipelineRequest = match serde_json::from_str(definition_str) {
        Ok(r) => r,
        Err(e) => {
            let error = format!("{}", e);
            println!("{}", error);
            return Err(ESPipelineError{ message: error })
        }
    };

    let pipeline = match request.pipeline {
        Some(pipeline) => pipeline.to_pipeline_definition().map_err(|_|ESPipelineError{ message: "dunno".to_string() })?,
        None => {
            match name {
                Some(name) => match API_SERVICE_CLIENT.describe_pipeline(name).await {
                    Ok(pd) => match pd {
                        Some(pd) => pd,
                        None => return Err(ESPipelineError { message: "No pipeline".to_string() })
                    },
                    Err(e) => {
                        log_service_err(e);
                        return Err(ESPipelineError { message: "No pipeline".to_string() })
                    }
                },
                None => return Err(ESPipelineError{ message: "No pipeline".to_string() })
            }
        }
    };

    request.docs.iter_mut().for_each(|d|apply_ingest(d));
    request.docs.iter_mut().for_each(|d|pipeline::apply_definition(&pipeline, d));
    let result_docs = request.docs.iter().map(|x|HashMap::from([("doc".to_string(), x.clone())])).collect();

    Ok(SimulatePipelineResult{ docs: result_docs })
}


#[cfg(test)]
mod tests {
    use crate::{elastic_search_pipeline::{parse_named_definition, ElasticSearchPipelineDefinition}, pipeline::PipelineDefinition};

    fn double_roundtrip(original: &str) {
        let definition_pair = parse_named_definition(&original.to_string()).unwrap();
        let _name = definition_pair.0;
        let definition = definition_pair.1;

        let es_serialized = serde_json::to_string(&definition).unwrap();

        let es_reparsed_def = serde_json::from_str::<ElasticSearchPipelineDefinition>(es_serialized.as_str()).unwrap();

        let converted = es_reparsed_def.to_pipeline_definition().unwrap();

        let serialized = serde_json::to_string(&converted).unwrap();

        let reparsed_def = serde_json::from_str::<PipelineDefinition>(&serialized).unwrap();

        let es_converted = ElasticSearchPipelineDefinition::from_process_definition(&reparsed_def);

        let es_converted_serialized = serde_json::to_string(&es_converted).unwrap();

        assert_eq!(definition.processors.len(), es_converted.processors.len());
        assert_eq!(definition.description, es_converted.description);
        assert_eq!(definition.on_failure.len(), es_converted.on_failure.len());
        assert_eq!(es_serialized, es_converted_serialized);
    }

    #[test]
    fn test_es_processor_parser() {  
        let test_val = r#"{"foo_bar": {
  "description" : "My optional pipeline description",
  "processors" : [
    {
      "set" : {
        "description" : "My optional processor description",
        "field": "my-keyword-field",
        "value": "foo"
      }
    }
  ]
    }}"#;

        double_roundtrip(test_val);
    }

    #[test]
    fn test_es_filebeat_kibana_audit_pipeline() {
        let test_val = r#"{
  "filebeat-8.7.1-kibana-audit-pipeline": {
    "description": "Pipeline for parsing Kibana audit logs",
    "processors": [
      {
        "set": {
          "value": "{{_ingest.timestamp}}",
          "field": "event.ingested"
        }
      },
      {
        "set": {
          "copy_from": "@timestamp",
          "field": "event.created"
        }
      },
      {
        "pipeline": {
          "name": "filebeat-8.7.1-kibana-audit-pipeline-json"
        }
      },
      {
        "set": {
          "field": "event.kind",
          "value": "event"
        }
      },
      {
        "append": {
          "value": "{{user.name}}",
          "if": "ctx?.user?.name != null",
          "field": "related.user"
        }
      }
    ],
    "on_failure": [
      {
        "set": {
          "value": "{{ _ingest.on_failure_message }}",
          "field": "error.message"
        }
      }
    ]
  }
}"#;

        double_roundtrip(test_val);
    }

}
