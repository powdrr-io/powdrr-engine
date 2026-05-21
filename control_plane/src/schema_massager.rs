use serde_json::Value;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[cfg(feature = "arrow-schema")]
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
#[cfg(feature = "arrow-schema")]
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
#[cfg(feature = "iceberg-schema")]
use iceberg::spec::{PrimitiveType, Type};
#[cfg(feature = "arrow-schema")]
use std::sync::Arc;

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

    #[cfg(feature = "arrow-schema")]
    pub fn to_sql_type(&self) -> String {
        match self {
            PowdrrDataType::Array(_) => todo!(),
            PowdrrDataType::Boolean => "BOOLEAN".to_string(),
            PowdrrDataType::Float => "DOUBLE".to_string(),
            PowdrrDataType::Integer => "BIGINT".to_string(),
            PowdrrDataType::Object(_) => todo!(),
            PowdrrDataType::Null => panic!("Cannot convert null to SQL type"),
            PowdrrDataType::String => "STRING".to_string(),
        }
    }

    #[cfg(feature = "arrow-schema")]
    pub fn to_arrow_type(&self, index: usize) -> (DataType, usize) {
        match self {
            PowdrrDataType::Array(element_type) => {
                let (element_arrow_type, next_index) = element_type.to_arrow_type(index);
                let element_arrow_field = Field::new("value".to_string(), element_arrow_type, true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        next_index.to_string(),
                    )]));
                let arrow_type = DataType::List(Arc::new(element_arrow_field));
                (arrow_type, next_index + 1)
            }
            PowdrrDataType::Boolean => (DataType::Boolean, index),
            PowdrrDataType::Float => (DataType::Float64, index),
            PowdrrDataType::Integer => (DataType::Int64, index),
            PowdrrDataType::Object(schema) => {
                let (arrow_fields, next_index) = schema.to_arrow_fields_internal(index);
                let arrow_type = DataType::Struct(Fields::from(arrow_fields));
                (arrow_type, next_index)
            }
            PowdrrDataType::Null => (DataType::Utf8, index),
            PowdrrDataType::String => (DataType::Utf8, index),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq)]
pub struct PowdrrField {
    pub name: String,
    pub data_type: PowdrrDataType,
}

impl Display for PowdrrField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{}: {:?}", &self.name, &self.data_type).as_str())
    }
}

impl PowdrrField {
    #[cfg(feature = "arrow-schema")]
    fn to_arrow_field(&self, index: usize) -> (Field, usize) {
        assert!(index > 0, "These need to be 1-indexed, not 0-indexed");
        let (arrow_data_type, next_index) = self.data_type.to_arrow_type(index);
        let arrow_field =
            Field::new(self.name.clone(), arrow_data_type, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                next_index.to_string(),
            )]));
        (arrow_field, next_index + 1)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq)]
pub struct PowdrrSchema {
    pub fields: Vec<PowdrrField>,
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

    pub fn merge_all(schemas: Vec<Self>) -> Self {
        assert!(!schemas.is_empty());

        let mut iter = schemas.iter();
        let mut merged_schema = iter.next().unwrap().clone();

        for schema in iter {
            merged_schema.merge_from(schema);
        }
        merged_schema
            .fields
            .sort_by(|a, b| a.name.partial_cmp(&b.name).unwrap());
        merged_schema
    }

    fn merge_field(self_field: &PowdrrField, other_field: &PowdrrField) -> Option<PowdrrField> {
        if other_field.data_type.is_null() {
            None
        } else if self_field.data_type.is_null() && !other_field.data_type.is_null() {
            Some(other_field.clone())
        } else if other_field.data_type.is_object() && self_field.data_type.is_object() {
            let mut self_field_schema = self_field.data_type.as_object_schema().unwrap();
            let other_field_schema = other_field.data_type.as_object_schema().unwrap();
            self_field_schema.merge_from(&other_field_schema);
            Some(PowdrrField {
                name: other_field.name.clone(),
                data_type: PowdrrDataType::Object(Box::new(self_field_schema)),
            })
        } else if other_field.data_type.is_array() && self_field.data_type.is_array() {
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

    pub fn coerce_value_option(&self, value: &mut Option<Value>) {
        if value.is_none() {
            return;
        }

        self.coerce_value(value.as_mut().unwrap());
    }

    pub fn coerce_value(&self, value: &mut Value) {
        assert!(value.is_object());

        let value_map = value.as_object_mut().unwrap();
        for field in self.fields.iter() {
            match value_map.get_mut(&field.name) {
                Some(field_value) => match &field.data_type {
                    PowdrrDataType::Object(field_value_schema) => {
                        field_value_schema.coerce_value(field_value);
                    }
                    _ => (),
                },
                None => {
                    value_map.insert(
                        field.name.clone(),
                        Self::default_serde_value(&field.data_type),
                    );
                }
            }
        }
    }

    fn default_serde_value(data_type: &PowdrrDataType) -> Value {
        match data_type {
            PowdrrDataType::Object(schema) => {
                let mut value_fields = serde_json::Map::new();
                for field in schema.fields.iter() {
                    value_fields.insert(
                        field.name.clone(),
                        Self::default_serde_value(&field.data_type),
                    );
                }
                Value::Object(value_fields)
            }
            _ => Value::Null,
        }
    }

    #[cfg(feature = "iceberg-schema")]
    pub fn from_iceberg(
        table_iceberg_schema: &Arc<iceberg::spec::Schema>,
        file_iceberg_schema: &Arc<iceberg::spec::Schema>,
    ) -> Self {
        if file_iceberg_schema.as_struct().fields().is_empty() {
            Self::convert_from_iceberg(table_iceberg_schema)
        } else {
            Self::convert_from_iceberg(file_iceberg_schema)
        }
    }

    #[cfg(feature = "iceberg-schema")]
    fn convert_from_iceberg(iceberg_schema: &Arc<iceberg::spec::Schema>) -> Self {
        let mut fields = vec![];
        for field in iceberg_schema.as_struct().fields().iter() {
            match *field.field_type.clone() {
                Type::Primitive(primitive_type) => match primitive_type {
                    PrimitiveType::Boolean => fields.push(PowdrrField {
                        name: field.name.clone(),
                        data_type: PowdrrDataType::Boolean,
                    }),
                    PrimitiveType::Int | PrimitiveType::Long => fields.push(PowdrrField {
                        name: field.name.clone(),
                        data_type: PowdrrDataType::Integer,
                    }),
                    PrimitiveType::Float | PrimitiveType::Double => fields.push(PowdrrField {
                        name: field.name.clone(),
                        data_type: PowdrrDataType::Float,
                    }),
                    PrimitiveType::String => fields.push(PowdrrField {
                        name: field.name.clone(),
                        data_type: PowdrrDataType::String,
                    }),
                    PrimitiveType::Decimal { .. }
                    | PrimitiveType::Date
                    | PrimitiveType::Time
                    | PrimitiveType::Timestamp
                    | PrimitiveType::Timestamptz
                    | PrimitiveType::TimestampNs
                    | PrimitiveType::TimestamptzNs
                    | PrimitiveType::Uuid
                    | PrimitiveType::Fixed(_)
                    | PrimitiveType::Binary => todo!(),
                },
                Type::Struct(_) | Type::List(_) | Type::Map(_) => todo!(),
            }
        }
        PowdrrSchema { fields }
    }

    #[cfg(feature = "arrow-schema")]
    fn to_arrow_fields(&self) -> Vec<Field> {
        let (fields, _) = self.to_arrow_fields_internal(1);
        fields
    }

    #[cfg(feature = "arrow-schema")]
    fn to_arrow_fields_internal(&self, index: usize) -> (Vec<Field>, usize) {
        let mut fields = vec![];
        let mut next_field_id = index;
        for field in self.fields.iter() {
            let (arrow_field, returned_next_field_id) = field.to_arrow_field(next_field_id);
            assert!(returned_next_field_id > next_field_id);
            next_field_id = returned_next_field_id;
            fields.push(arrow_field);
        }
        fields.sort_by(|a, b| a.name().partial_cmp(b.name()).unwrap());
        (fields, next_field_id)
    }

    #[cfg(feature = "arrow-schema")]
    pub fn to_arrow_schema(&self) -> Schema {
        Schema::new(self.to_arrow_fields())
    }
}
