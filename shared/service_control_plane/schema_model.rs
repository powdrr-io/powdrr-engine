#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq)]
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
            PowdrrDataType::Object(schema) => Some(std::ops::Deref::deref(schema).clone()),
            _ => None,
        }
    }

    pub fn is_array(&self) -> bool {
        matches!(self, PowdrrDataType::Array(_))
    }

    pub fn array_element_type(&self) -> PowdrrDataType {
        match self {
            PowdrrDataType::Array(element) => std::ops::Deref::deref(element).clone(),
            _ => panic!("Check to see that it is an array first."),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq)]
pub struct PowdrrField {
    pub name: String,
    pub data_type: PowdrrDataType,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq)]
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

    pub fn to_map(&self) -> std::collections::HashMap<String, PowdrrField> {
        self.fields
            .iter()
            .map(|x| (x.name.clone(), x.clone()))
            .collect::<std::collections::HashMap<String, PowdrrField>>()
    }
}
