use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum PowdrrDataType {
    Array(Box<PowdrrDataType>),
    Boolean,
    Float,
    Integer,
    Null,
    Object(Box<PowdrrSchema>),
    String,
}

impl PowdrrDataType {
    pub fn is_null(&self) -> bool {
        matches!(self, PowdrrDataType::Null)
    }

    pub fn is_object(&self) -> bool {
        matches!(self, PowdrrDataType::Object(_))
    }

    pub fn as_object_schema(&self) -> Option<PowdrrSchema> {
        match self {
            PowdrrDataType::Object(schema) => Some(schema.deref().clone()),
            _ => None,
        }
    }

    pub fn is_array(&self) -> bool {
        matches!(self, PowdrrDataType::Array(_))
    }

    pub fn array_element_type(&self) -> PowdrrDataType {
        match self {
            PowdrrDataType::Array(element) => element.deref().clone(),
            _ => panic!("Check to see that it is an array first."),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PowdrrField {
    pub name: String,
    pub data_type: PowdrrDataType,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PowdrrSchema {
    fields: Vec<PowdrrField>,
}

impl PowdrrSchema {
    pub fn minimal() -> Self {
        PowdrrSchema { fields: vec![] }
    }

    pub fn deletes() -> Self {
        PowdrrSchema {
            fields: vec![PowdrrField {
                name: "_id_seq_no".to_string(),
                data_type: PowdrrDataType::String,
            }],
        }
    }

    pub fn from(fields: &Vec<PowdrrField>) -> Self {
        let mut fields_clone = fields.clone();
        fields_clone.sort_by(|a, b| a.name.partial_cmp(&b.name).unwrap());
        PowdrrSchema {
            fields: fields_clone,
        }
    }

    pub fn fields(&self) -> &Vec<PowdrrField> {
        &self.fields
    }

    pub fn to_map(&self) -> HashMap<String, PowdrrField> {
        self.fields
            .iter()
            .map(|x| (x.name.clone(), x.clone()))
            .collect::<HashMap<String, PowdrrField>>()
    }

    fn merge_field(self_field: &PowdrrField, other_field: &PowdrrField) -> Option<PowdrrField> {
        if self_field.data_type.is_null() && !other_field.data_type.is_null() {
            Some(other_field.clone())
        } else if self_field.data_type.is_object() && other_field.data_type.is_object() {
            let mut self_schema = self_field.data_type.as_object_schema().unwrap();
            let other_schema = other_field.data_type.as_object_schema().unwrap();
            self_schema.merge_from(&other_schema);
            Some(PowdrrField {
                name: other_field.name.clone(),
                data_type: PowdrrDataType::Object(Box::new(self_schema)),
            })
        } else if self_field.data_type.is_array() && other_field.data_type.is_array() {
            let self_element_type = self_field.data_type.array_element_type();
            let other_element_type = other_field.data_type.array_element_type();
            if other_element_type.is_null() {
                None
            } else if self_element_type.is_null() && !other_element_type.is_null() {
                Some(other_field.clone())
            } else if other_element_type.is_object() && self_element_type.is_object() {
                let mut self_field_schema = self_element_type.as_object_schema().unwrap();
                let other_field_schema = other_element_type.as_object_schema().unwrap();
                self_field_schema.merge_from(&other_field_schema);
                Some(PowdrrField {
                    name: other_field.name.clone(),
                    data_type: PowdrrDataType::Array(Box::new(PowdrrDataType::Object(Box::new(
                        self_field_schema,
                    )))),
                })
            } else if self_element_type != other_element_type {
                panic!("Array element types are changing, it is bad")
            } else {
                None
            }
        } else if self_field.data_type != other_field.data_type {
            panic!("Data types are changing, it is bad")
        } else {
            None
        }
    }

    pub fn merge_from(&mut self, other: &PowdrrSchema) {
        let self_map = self.to_map();

        for other_field in other.fields.iter() {
            match self_map.get(&other_field.name) {
                Some(self_field) => match Self::merge_field(self_field, other_field) {
                    Some(merged_field) => {
                        let position = self
                            .fields
                            .iter()
                            .position(|f| f.name == other_field.name)
                            .unwrap();
                        self.fields[position] = merged_field;
                    }
                    None => (),
                },
                None => {
                    self.fields.push(other_field.clone());
                }
            }
        }
    }
}
