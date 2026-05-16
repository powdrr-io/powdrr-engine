use arrow_json::reader::infer_json_schema;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use iceberg::spec::{PrimitiveType, Type};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;

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
        match self {
            PowdrrDataType::Null => true,
            _ => false,
        }
    }

    pub fn is_object(&self) -> bool {
        match self {
            PowdrrDataType::Object(_) => true,
            _ => false,
        }
    }

    pub fn as_object_schema(&self) -> Option<PowdrrSchema> {
        match self {
            PowdrrDataType::Object(schema) => Some(schema.deref().clone()),
            _ => None,
        }
    }

    pub fn is_array(&self) -> bool {
        match self {
            PowdrrDataType::Array(_) => true,
            _ => false,
        }
    }

    pub fn array_element_type(&self) -> PowdrrDataType {
        match self {
            PowdrrDataType::Array(element) => element.deref().clone(),
            _ => panic!("Check to see that it is an array first."),
        }
    }

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

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PowdrrField {
    pub name: String,
    pub data_type: PowdrrDataType,
}

impl Display for PowdrrField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{}: {:?}", &self.name, &self.data_type).as_str())?;
        Ok(())
    }
}

impl PowdrrField {
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

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PowdrrSchema {
    fields: Vec<PowdrrField>,
}

impl PowdrrSchema {
    pub fn minimal() -> Self {
        PowdrrSchema {
            fields: vec!(
                //PowdrrField { name: "_id".to_string(), data_type: PowdrrDataType::String },
                //PowdrrField { name: "_id_seq_no".to_string(), data_type: PowdrrDataType::String },
                //PowdrrField { name: "_seq_no".to_string(), data_type: PowdrrDataType::Integer },
            ),
        }
    }

    pub fn deletes() -> Self {
        PowdrrSchema {
            fields: vec![PowdrrField {
                name: "_id_seq_no".to_string(),
                data_type: PowdrrDataType::String,
            }],
        }
    }

    pub fn from_iceberg(
        table_iceberg_schema: &Arc<iceberg::spec::Schema>,
        file_iceberg_schema: &Arc<iceberg::spec::Schema>,
    ) -> Self {
        if file_iceberg_schema.as_struct().fields().len() == 0 {
            Self::convert_from_iceberg(table_iceberg_schema)
        } else {
            Self::convert_from_iceberg(file_iceberg_schema)
        }
    }

    fn convert_from_iceberg(iceberg_schema: &Arc<iceberg::spec::Schema>) -> Self {
        let mut fields = vec![];
        for field in iceberg_schema.as_struct().fields().iter() {
            match *field.field_type.clone() {
                Type::Primitive(primitive_type) => match primitive_type {
                    PrimitiveType::Boolean => {
                        fields.push(PowdrrField {
                            name: field.name.clone(),
                            data_type: PowdrrDataType::Boolean,
                        });
                    }
                    PrimitiveType::Int => {
                        fields.push(PowdrrField {
                            name: field.name.clone(),
                            data_type: PowdrrDataType::Integer,
                        });
                    }
                    PrimitiveType::Long => {
                        fields.push(PowdrrField {
                            name: field.name.clone(),
                            data_type: PowdrrDataType::Integer,
                        });
                    }
                    PrimitiveType::Float => {
                        fields.push(PowdrrField {
                            name: field.name.clone(),
                            data_type: PowdrrDataType::Float,
                        });
                    }
                    PrimitiveType::Double => {
                        fields.push(PowdrrField {
                            name: field.name.clone(),
                            data_type: PowdrrDataType::Float,
                        });
                    }
                    PrimitiveType::Decimal { .. } => todo!(),
                    PrimitiveType::Date => todo!(),
                    PrimitiveType::Time => todo!(),
                    PrimitiveType::Timestamp => todo!(),
                    PrimitiveType::Timestamptz => todo!(),
                    PrimitiveType::TimestampNs => todo!(),
                    PrimitiveType::TimestamptzNs => todo!(),
                    PrimitiveType::String => {
                        fields.push(PowdrrField {
                            name: field.name.clone(),
                            data_type: PowdrrDataType::String,
                        });
                    }
                    PrimitiveType::Uuid => todo!(),
                    PrimitiveType::Fixed(_) => todo!(),
                    PrimitiveType::Binary => todo!(),
                },
                Type::Struct(_) => todo!(),
                Type::List(_) => todo!(),
                Type::Map(_) => todo!(),
            }
        }
        PowdrrSchema { fields }
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

    fn to_arrow_fields(&self) -> Vec<Field> {
        let (fields, _) = self.to_arrow_fields_internal(1);
        fields
    }

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
        for field in fields.iter() {
            if field.name() == "dude" {
                assert_eq!(field.metadata().len(), 1);
            }
        }
        (fields, next_field_id)
    }

    pub fn to_arrow_schema(&self) -> Schema {
        Schema::new(self.to_arrow_fields())
    }

    pub(crate) fn merge_all(schemas: Vec<Self>) -> Self {
        assert!(schemas.len() > 0);

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
            // If our existing field is null, we can just replace it with the other field
            Some(other_field.clone())
        } else if other_field.data_type.is_object() && self_field.data_type.is_object() {
            // Both are objects so merge the schema in the field itself (recursive call)
            let mut self_field_schema = self_field.data_type.as_object_schema().unwrap();
            let other_field_schema = other_field.data_type.as_object_schema().unwrap();
            self_field_schema.merge_from(&other_field_schema);
            let merged_field = PowdrrField {
                name: other_field.name.clone(),
                data_type: PowdrrDataType::Object(Box::new(self_field_schema)),
            };
            Some(merged_field)
        } else if other_field.data_type.is_array() && self_field.data_type.is_array() {
            // Both are arrays so merge the schema in the field itself (recursive call)
            let self_element_type = self_field.data_type.array_element_type();
            let other_element_type = other_field.data_type.array_element_type();
            if other_element_type.is_null() {
                None
            } else if self_element_type.is_null() && !other_element_type.is_null() {
                // If our existing field is null, we can just replace it with the other field
                Some(other_field.clone())
            } else if other_element_type.is_object() && self_element_type.is_object() {
                let mut self_field_schema = self_element_type.as_object_schema().unwrap();
                let other_field_schema = other_element_type.as_object_schema().unwrap();
                self_field_schema.merge_from(&other_field_schema);
                let merged_field = PowdrrField {
                    name: other_field.name.clone(),
                    data_type: PowdrrDataType::Array(Box::new(PowdrrDataType::Object(Box::new(
                        self_field_schema,
                    )))),
                };
                Some(merged_field)
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

    pub fn merge_from(&mut self, other: &PowdrrSchema) -> () {
        let self_map = self.to_map();

        for other_field in other.fields.iter() {
            match self_map.get(&other_field.name) {
                Some(self_field) => {
                    match Self::merge_field(self_field, other_field) {
                        Some(merged_field) => {
                            let position = self
                                .fields
                                .iter()
                                .position(|f| f.name == other_field.name)
                                .unwrap();
                            self.fields[position] = merged_field;
                        }
                        None => (),
                    };
                }
                None => {
                    self.fields.push(other_field.clone());
                }
            }
        }
    }

    pub(crate) fn coerce_value_option(&self, value: &mut Option<Value>) -> () {
        if value.is_none() {
            return;
        }

        self.coerce_value(value.as_mut().unwrap());
    }

    pub(crate) fn coerce_value(&self, value: &mut Value) -> () {
        // The <self> schema *should* be a superset of the schema of <value>.
        // TODO: add an assert for the above?

        // This only works for object values
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
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct NamedStructEntry {
    pub(crate) name: String,
    pub(crate) expression: SqlExpression,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) enum SqlExpression {
    And(Vec<SqlExpression>),
    Arithmetic(Box<SqlExpression>, String, Box<SqlExpression>),
    Average(Box<SqlExpression>),
    Comparison(Box<SqlExpression>, String, Box<SqlExpression>),
    Count,
    CountDistinct(Box<SqlExpression>),
    FieldRef(String, String),
    In(Box<SqlExpression>, Vec<SqlExpression>),
    IsNull(Box<SqlExpression>),
    Like(Box<SqlExpression>, Box<SqlExpression>),
    LiteralNonString(String),
    LiteralString(String),
    NamedStruct(Vec<NamedStructEntry>),
    Not(Box<SqlExpression>),
    Null(PowdrrDataType),
    Or(Vec<SqlExpression>),
}

unsafe impl Send for SqlExpression {}
unsafe impl Sync for SqlExpression {}

impl SqlExpression {
    fn lookup_field(schema: &HashMap<String, PowdrrField>, path: &String) -> Option<PowdrrField> {
        let split_path: Vec<&str> = path.split(".").collect();
        Self::lookup_field_worker(schema, &split_path, 0)
    }

    fn lookup_field_worker(
        schema: &HashMap<String, PowdrrField>,
        path: &Vec<&str>,
        index: usize,
    ) -> Option<PowdrrField> {
        match schema.get(&path.get(index).unwrap().to_string()) {
            Some(field) => {
                if index + 1 == path.len() {
                    return Some(field.clone());
                } else {
                    return match &field.data_type {
                        PowdrrDataType::Object(schema) => {
                            return Self::lookup_field_worker(&schema.to_map(), path, index + 1);
                        }
                        _ => None,
                    };
                }
            }
            None => return None,
        }
    }

    fn explode_ref(
        &self,
        table: &String,
        name: &String,
        original_schema: &HashMap<String, PowdrrField>,
        target_schema: &HashMap<String, PowdrrField>,
    ) -> HashMap<String, Self> {
        assert_eq!(table, "t");
        let denormalized_name = name.replace(".", "_");
        let original_schema_field = Self::lookup_field(original_schema, &denormalized_name);
        let target_schema_field = Self::lookup_field(target_schema, &denormalized_name);
        if original_schema_field.is_none() {
            HashMap::from([("".to_string(), self.clone())])
        } else if target_schema_field.is_some() {
            self.explode_partial_ref(
                &"".to_string(),
                &original_schema_field.unwrap(),
                &target_schema_field.unwrap(),
            )
        } else {
            self.explode_full_ref(&"".to_string(), &original_schema_field.unwrap())
        }
    }

    fn explode_partial_ref(
        &self,
        prefix: &String,
        original_field: &PowdrrField,
        target_field: &PowdrrField,
    ) -> HashMap<String, Self> {
        let target_inner_fields_map = match &target_field.data_type {
            PowdrrDataType::Object(schema) => schema.to_map(),
            PowdrrDataType::Null => {
                return HashMap::from([(
                    prefix.clone(),
                    SqlExpression::Null(original_field.data_type.clone()),
                )])
            }
            _ => {
                return HashMap::from([(prefix.clone(), self.make_exploded_ref(prefix))]);
            }
        };

        let original_inner_fields = match &original_field.data_type {
            PowdrrDataType::Object(schema) => schema.fields.clone(),
            _ => {
                return HashMap::from([(prefix.clone(), self.make_exploded_ref(prefix))]);
            }
        };

        let mut result = HashMap::new();
        for field in original_inner_fields.iter() {
            let target_field = target_inner_fields_map.get(&field.name);
            match target_field {
                Some(tf) => {
                    result.extend(self.explode_partial_ref(
                        &format!("{}_{}", prefix, field.name),
                        &field,
                        &tf,
                    ));
                }
                None => {
                    result.extend(
                        self.explode_full_ref(&format!("{}_{}", prefix, field.name), &field),
                    );
                }
            }
        }
        result
    }

    fn explode_full_ref(&self, prefix: &String, field: &PowdrrField) -> HashMap<String, Self> {
        match &field.data_type {
            PowdrrDataType::Object(schema) => {
                let mut result = HashMap::new();
                for field in schema.fields.iter() {
                    result.extend(
                        self.explode_full_ref(&format!("{}_{}", prefix, field.name), &field),
                    );
                }
                result
            }
            _ => HashMap::from([(prefix.clone(), SqlExpression::Null(field.data_type.clone()))]),
        }
    }

    fn make_exploded_ref(&self, prefix: &String) -> Self {
        match self {
            SqlExpression::FieldRef(table, name) => {
                SqlExpression::FieldRef(table.clone(), format!("{}{}", name, prefix))
            }
            _ => panic!("Expected a field ref"),
        }
    }

    fn finalize(
        &self,
        original_schema: &HashMap<String, PowdrrField>,
        target_schema: &HashMap<String, PowdrrField>,
    ) -> Self {
        match self {
            SqlExpression::And(exprs) => SqlExpression::And(
                exprs
                    .iter()
                    .map(|x| x.finalize(original_schema, target_schema))
                    .collect(),
            ),
            SqlExpression::Arithmetic(left, op, right) => SqlExpression::Arithmetic(
                Box::new(left.finalize(original_schema, target_schema)),
                op.clone(),
                Box::new(right.finalize(original_schema, target_schema)),
            ),
            SqlExpression::Average(value) => {
                SqlExpression::Average(Box::new(value.finalize(original_schema, target_schema)))
            }
            SqlExpression::Comparison(left, op, right) => {
                let final_left = left.finalize(original_schema, target_schema);
                let final_right = right.finalize(original_schema, target_schema);
                if final_left.stringize() == "null" {
                    // TODO: There seems to be a bug in datafusion where in some cases a "null = 'literal'"
                    // condition is lost. It only repro'd against a parquet file and then only when
                    // run in the test. I'm not sure why. Turning it into an unambiguous "false" for now.
                    SqlExpression::LiteralNonString("false".to_string())
                } else {
                    SqlExpression::Comparison(
                        Box::new(final_left),
                        op.clone(),
                        Box::new(final_right),
                    )
                }
            }
            SqlExpression::Count => self.clone(),
            SqlExpression::CountDistinct(value) => SqlExpression::CountDistinct(Box::new(
                value.finalize(original_schema, target_schema),
            )),
            SqlExpression::FieldRef(table, name) if table == "t" => {
                let denormalized_name = name.replace(".", "_");
                let original_schema_field = Self::lookup_field(original_schema, &denormalized_name);
                let target_schema_field = Self::lookup_field(target_schema, &denormalized_name);
                if original_schema_field.is_none() {
                    SqlExpression::LiteralNonString("null".to_string())
                } else if target_schema_field.is_some() {
                    self.populate_field(
                        &denormalized_name,
                        &original_schema_field.unwrap(),
                        &target_schema_field.unwrap(),
                    )
                } else {
                    SqlExpression::Null(original_schema_field.unwrap().data_type)
                }
            }
            SqlExpression::FieldRef(table, _name) => {
                // We don't do any rewriting for field refs that are not against the user defined data.
                assert_ne!(table, "t");
                self.clone()
            }
            SqlExpression::In(left, right) => SqlExpression::In(
                Box::new(left.finalize(original_schema, target_schema)),
                right
                    .iter()
                    .map(|x| x.finalize(original_schema, target_schema))
                    .collect(),
            ),
            SqlExpression::IsNull(value) => {
                SqlExpression::IsNull(Box::new(value.finalize(original_schema, target_schema)))
            }
            SqlExpression::Like(left, right) => SqlExpression::Like(
                Box::new(left.finalize(original_schema, target_schema)),
                Box::new(right.finalize(original_schema, target_schema)),
            ),
            SqlExpression::LiteralNonString(_) => self.clone(),
            SqlExpression::LiteralString(_) => self.clone(),
            SqlExpression::NamedStruct(entries) => SqlExpression::NamedStruct(
                entries
                    .iter()
                    .map(|x| NamedStructEntry {
                        name: x.name.clone(),
                        expression: x.expression.finalize(original_schema, target_schema),
                    })
                    .collect(),
            ),
            SqlExpression::Not(value) => {
                SqlExpression::Not(Box::new(value.finalize(original_schema, target_schema)))
            }
            SqlExpression::Null(_) => self.clone(),
            SqlExpression::Or(exprs) => SqlExpression::Or(
                exprs
                    .iter()
                    .map(|x| x.finalize(original_schema, target_schema))
                    .collect(),
            ),
        }
    }

    fn populate_field(
        &self,
        base_name: &String,
        original_field: &PowdrrField,
        target_field: &PowdrrField,
    ) -> Self {
        if original_field.data_type.type_id() != target_field.data_type.type_id() {
            todo!("Type changes are not yet supported")
        }

        // TODO: equality check, if equal return self

        let original_schema = match &original_field.data_type {
            PowdrrDataType::Object(schema) => schema.to_map(),
            _ => return self.clone(),
        };

        let target_schema = match &target_field.data_type {
            PowdrrDataType::Object(schema) => schema.to_map(),
            _ => return self.clone(),
        };

        let mut entries = vec![];
        for (field_name, original_field) in original_schema {
            let full_name = format!("{}.{}", base_name, field_name);
            let expression = match target_schema.get(&field_name) {
                Some(target_field) => {
                    self.populate_field(&full_name, &original_field, &target_field)
                }
                None => SqlExpression::Null(original_field.data_type.clone()),
            };
            entries.push(NamedStructEntry {
                name: field_name.clone(),
                expression: expression,
            });
        }
        SqlExpression::NamedStruct(entries)
    }

    fn stringize(&self) -> String {
        match self {
            SqlExpression::And(exprs) => {
                let exprs_str = exprs.iter().map(|x| x.stringize()).collect::<Vec<String>>();
                format!("({})", exprs_str.join(" AND "))
            }
            SqlExpression::Arithmetic(left, op, right) => {
                format!("({} {} {})", left.stringize(), op, right.stringize())
            }
            SqlExpression::Average(value) => {
                format!("AVG({})", value.stringize())
            }
            SqlExpression::Comparison(left, op, right) => {
                format!("({} {} {})", left.stringize(), op, right.stringize())
            }
            SqlExpression::Count => "count(1)".to_string(),
            SqlExpression::CountDistinct(value) => {
                format!("count(distinct {})", value.stringize())
            }
            SqlExpression::FieldRef(table, field) if table == "t" => {
                format!("{}.\"{}\"", table, field.replace(".", "_"))
            }
            SqlExpression::FieldRef(table, field) => {
                assert_ne!(table, "t");
                assert!(!field.contains("."), "Need to handle this case now");
                format!("{}.\"{}\"", table, field)
            }
            SqlExpression::In(left, right) => {
                format!(
                    "{} IN ({})",
                    left.stringize(),
                    right
                        .iter()
                        .map(|x| x.stringize())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            SqlExpression::IsNull(value) => {
                format!("{} IS NULL", value.stringize())
            }
            SqlExpression::Like(left, right) => {
                format!("{} LIKE {}", left.stringize(), right.stringize())
            }
            SqlExpression::LiteralNonString(value) => value.clone(),
            SqlExpression::LiteralString(value) => {
                format!("'{}'", value)
            }
            SqlExpression::NamedStruct(entries) => {
                format!(
                    "NAMED_STRUCT({})",
                    entries
                        .iter()
                        .map(|x| format!("'{}', {}", x.name, x.expression.stringize()))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            SqlExpression::Not(value) => {
                format!("NOT({})", value.stringize())
            }
            SqlExpression::Null(data_type) => match data_type {
                PowdrrDataType::Object(_) => "NULL".to_string(),
                PowdrrDataType::Array(_) => "NULL".to_string(),
                PowdrrDataType::Boolean => "false".to_string(),
                PowdrrDataType::Null => "NULL".to_string(),
                _ => format!("CAST(NULL AS {})", data_type.to_sql_type()),
            },
            SqlExpression::Or(exprs) => {
                let exprs_str = exprs.iter().map(|x| x.stringize()).collect::<Vec<String>>();
                format!("({})", exprs_str.join(" OR "))
            }
        }
    }

    fn and(exprs: Vec<SqlExpression>) -> Option<Self> {
        if exprs.len() == 0 {
            None
        } else if exprs.len() == 1 {
            Some(exprs.get(0).unwrap().clone())
        } else {
            Some(SqlExpression::And(exprs))
        }
    }

    pub fn or(exprs: Vec<SqlExpression>) -> Option<Self> {
        if exprs.len() == 0 {
            None
        } else if exprs.len() == 1 {
            Some(exprs.get(0).unwrap().clone())
        } else {
            Some(SqlExpression::Or(exprs))
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct FieldExpression {
    pub(crate) name: String,
    pub(crate) expression: SqlExpression,
}

impl FieldExpression {
    fn finalize(
        &self,
        original_schema: &HashMap<String, PowdrrField>,
        target_schema: &HashMap<String, PowdrrField>,
    ) -> Vec<Self> {
        match &self.expression {
            SqlExpression::FieldRef(table, name) if table == "t" => {
                let exploded_ref =
                    self.expression
                        .explode_ref(table, name, original_schema, target_schema);
                exploded_ref
                    .iter()
                    .map(|(name_suffix, expression)| FieldExpression {
                        name: format!("{}{}", self.name, name_suffix),
                        expression: expression.clone(),
                    })
                    .collect()
            }
            _ => {
                vec![FieldExpression {
                    name: self.name.clone(),
                    expression: self.expression.finalize(original_schema, target_schema),
                }]
            }
        }
    }

    fn stringize(&self) -> String {
        format!("{} as \"{}\"", self.expression.stringize(), self.name)
    }
}

#[derive(Clone)]
pub struct SqlBuilder {
    pub(crate) all_fields: bool,
    pub(crate) fields: Vec<FieldExpression>,
    pub(crate) joins: Vec<String>,
    filter_stack: RefCell<Vec<Vec<SqlExpression>>>,
    pub(crate) limit: Option<u64>,
    pub(crate) calculate_score: bool,
    pub(crate) order_by: Vec<SqlExpression>,
    pub(crate) group_by: Vec<SqlExpression>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SqlQuery {
    dummy: bool,
    all_fields: bool,
    fields: Vec<FieldExpression>,
    joins: String,
    filters: Option<SqlExpression>,
    limit: Option<u64>,
    order_by: Vec<SqlExpression>,
    group_by: Vec<SqlExpression>,
}

impl SqlBuilder {
    pub(crate) fn for_query(all_fields: bool) -> Self {
        SqlBuilder {
            all_fields,
            fields: vec![],
            joins: vec!["LEFT JOIN {deletes_table} dt ON dt._id_seq_no = t._id_seq_no".to_string()],
            filter_stack: RefCell::new(vec![vec![SqlExpression::IsNull(Box::new(
                SqlExpression::FieldRef("dt".to_string(), "_id_seq_no".to_string()),
            ))]]),
            limit: None,
            calculate_score: false,
            order_by: vec![],
            group_by: vec![],
        }
    }

    pub(crate) fn for_agg() -> Self {
        SqlBuilder {
            all_fields: false,
            fields: vec![],
            joins: vec![],
            filter_stack: RefCell::new(vec![vec![]]),
            limit: None,
            calculate_score: false,
            order_by: vec![],
            group_by: vec![],
        }
    }

    pub(crate) fn for_compaction() -> Self {
        SqlBuilder {
            all_fields: true,
            fields: vec![FieldExpression {
                name: "_dt_id_seq_no".to_string(),
                expression: SqlExpression::FieldRef("dt".to_string(), "_id_seq_no".to_string()),
            }],
            joins: vec![
                "FULL OUTER JOIN {deletes_table} dt ON (dt._id_seq_no = t._id_seq_no)".to_string(),
            ],
            filter_stack: RefCell::new(vec![vec![]]),
            limit: None,
            calculate_score: false,
            order_by: vec![],
            group_by: vec![],
        }
    }

    #[allow(dead_code)]
    pub fn set_all_fields_testing_only(&mut self) -> () {
        self.all_fields = true;
    }

    pub(crate) fn push_filter_context(&mut self) -> &mut Self {
        self.filter_stack.get_mut().push(vec![]);
        self
    }

    pub(crate) fn pop_filter_context(&mut self, is_and: bool) -> &mut Self {
        self.pop_and_maybe_not_filter_context(is_and, false)
    }

    pub(crate) fn pop_and_not_filter_context(&mut self, is_and: bool) -> &mut Self {
        self.pop_and_maybe_not_filter_context(is_and, true)
    }

    pub(crate) fn pop_and_maybe_not_filter_context(
        &mut self,
        is_and: bool,
        is_not: bool,
    ) -> &mut Self {
        let local_filter_stack = self.filter_stack.get_mut();
        assert!(local_filter_stack.len() > 0);

        let filter = match is_and {
            true => SqlExpression::and(local_filter_stack.pop().unwrap()),
            false => SqlExpression::or(local_filter_stack.pop().unwrap()),
        };

        if filter.is_some() {
            let local_last = local_filter_stack.last_mut().unwrap();
            if is_not {
                local_last.push(SqlExpression::Not(Box::new(filter.unwrap())));
            } else {
                local_last.push(filter.unwrap());
            }
        }
        self
    }

    pub(crate) fn filter(&mut self, filter: SqlExpression) -> &mut Self {
        let local_filter_stack = self.filter_stack.get_mut();
        local_filter_stack.last_mut().unwrap().push(filter);
        self
    }

    fn _joins(&self) -> Vec<String> {
        let mut joins_copy = self.joins.clone();
        if self.calculate_score {
            joins_copy.push(
                "INNER JOIN {target_table}_search_index si on si.doc_id = t._id_seq_no".to_string(),
            )
        }
        joins_copy
    }

    fn _filters(&self) -> Option<SqlExpression> {
        let mut local_filter_stack = self.filter_stack.borrow().clone();
        assert_eq!(local_filter_stack.len(), 1);
        let top_copy = local_filter_stack.pop().unwrap().clone();
        SqlExpression::and(top_copy)
    }

    fn _fields(&self) -> Vec<FieldExpression> {
        let mut fields_copy = self.fields.clone();
        if self.calculate_score {
            fields_copy.push(FieldExpression {
                name: "term_cnt".to_string(),
                expression: SqlExpression::FieldRef("si".to_string(), "term_cnt".to_string()),
            });
            fields_copy.push(FieldExpression {
                name: "word_cnt".to_string(),
                expression: SqlExpression::FieldRef("si".to_string(), "word_cnt".to_string()),
            });
        }
        fields_copy
    }

    pub(crate) fn build(&self) -> SqlQuery {
        SqlQuery {
            dummy: false,
            all_fields: self.all_fields,
            fields: self._fields(),
            joins: self._joins().join(" "),
            filters: self._filters(),
            limit: self.limit.clone(),
            order_by: self.order_by.clone(),
            group_by: self.group_by.clone(),
        }
    }
}

impl SqlQuery {
    pub fn dummy() -> Self {
        SqlQuery {
            dummy: false,
            all_fields: false,
            fields: vec![FieldExpression {
                name: "cnt".to_string(),
                expression: SqlExpression::Count,
            }],
            joins: "".to_string(),
            filters: None,
            limit: None,
            order_by: vec![],
            group_by: vec![],
        }
    }

    fn fields(
        &self,
        original_schema: &HashMap<String, PowdrrField>,
        target_schema: &HashMap<String, PowdrrField>,
    ) -> String {
        // Magic up missing fields as nulls
        // TODO: figure out when fields have changed types and do something
        let mut final_fields = vec![];
        let mut fields_copy = self.fields.clone();
        if self.all_fields {
            for (_, field) in original_schema.iter() {
                fields_copy.push(FieldExpression {
                    name: field.name.clone(),
                    expression: SqlExpression::FieldRef("t".to_string(), field.name.clone()),
                });
            }
        }
        fields_copy.sort_by(|a, b| a.name.partial_cmp(&b.name).unwrap());
        for field in fields_copy.iter() {
            final_fields.extend(
                field
                    .finalize(original_schema, target_schema)
                    .iter()
                    .map(|f| f.stringize()),
            );
        }
        final_fields.join(", ")
    }

    fn filters(
        &self,
        original_schema: &HashMap<String, PowdrrField>,
        target_schema: &HashMap<String, PowdrrField>,
    ) -> String {
        // "Pre-process" filters where any missing field is considered a null value.
        // TODO: we could detect cases where the filter can't possibly match to terminate early
        match &self.filters {
            Some(filter) => {
                format!(
                    " WHERE {}",
                    filter.finalize(original_schema, target_schema).stringize()
                )
            }
            None => "".to_string(),
        }
    }

    fn order_by(
        &self,
        original_schema: &HashMap<String, PowdrrField>,
        target_schema: &HashMap<String, PowdrrField>,
    ) -> String {
        match self.order_by.len() {
            0 => "".to_string(),
            _ => format!(
                " ORDER BY {}",
                self.order_by
                    .iter()
                    .map(|x| x.finalize(original_schema, target_schema).stringize())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
        }
    }

    fn group_by(
        &self,
        original_schema: &HashMap<String, PowdrrField>,
        target_schema: &HashMap<String, PowdrrField>,
    ) -> String {
        match self.group_by.len() {
            0 => "".to_string(),
            _ => format!(
                " GROUP BY {}",
                self.group_by
                    .iter()
                    .map(|x| x.finalize(original_schema, target_schema).stringize())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
        }
    }

    fn limit(&self) -> String {
        match self.limit {
            Some(limit) => format!(" LIMIT {}", limit),
            None => "".to_string(),
        }
    }

    fn from(&self) -> String {
        if self.dummy {
            " ".to_string()
        } else {
            " FROM {target_table} t ".to_string()
        }
    }

    pub(crate) fn build_same(&self, schema: &PowdrrSchema) -> String {
        self.build(schema, schema)
    }

    pub(crate) fn build(
        &self,
        original_schema: &PowdrrSchema,
        target_schema: &PowdrrSchema,
    ) -> String {
        let original_schema_map = original_schema.to_map();
        let target_schema_map = target_schema.to_map();
        let from = self.from();
        let fields = self.fields(&original_schema_map, &target_schema_map);
        let joins = &self.joins;
        let filters = self.filters(&original_schema_map, &target_schema_map);
        let order_by = self.order_by(&original_schema_map, &target_schema_map);
        let group_by = self.group_by(&original_schema_map, &target_schema_map);
        let limit = self.limit();

        format!("SELECT {fields}{from}{joins}{filters}{group_by}{order_by}{limit}")
    }

    #[allow(dead_code)]
    pub(crate) fn build_debug(&self) -> String {
        let fields = self
            .fields
            .iter()
            .map(|x| x.stringize())
            .collect::<Vec<String>>()
            .join(", ");
        let joins = self.joins.clone();
        let filters = self
            .filters
            .clone()
            .map(|x| x.stringize())
            .unwrap_or("".to_string());
        let order_by = self
            .order_by
            .iter()
            .map(|x| x.stringize())
            .collect::<Vec<String>>()
            .join(", ");
        let group_by = self
            .group_by
            .iter()
            .map(|x| x.stringize())
            .collect::<Vec<String>>()
            .join(", ");
        let limit = self.limit();
        format!("SELECT {fields} FROM {{target_table}} t {joins} WHERE {filters} GROUP BY {group_by} ORDER BY {order_by} {limit}")
    }
}

fn to_powdrr_data_type(data_type: &DataType) -> PowdrrDataType {
    match data_type {
        DataType::Null => PowdrrDataType::Null,
        DataType::Int8 => PowdrrDataType::Integer,
        DataType::Int16 => PowdrrDataType::Integer,
        DataType::Int32 => PowdrrDataType::Integer,
        DataType::Int64 => PowdrrDataType::Integer,
        DataType::UInt8 => PowdrrDataType::Integer,
        DataType::UInt16 => PowdrrDataType::Integer,
        DataType::UInt32 => PowdrrDataType::Integer,
        DataType::UInt64 => PowdrrDataType::Integer,
        DataType::Boolean => PowdrrDataType::Boolean,
        DataType::Utf8 => PowdrrDataType::String,
        DataType::Utf8View => PowdrrDataType::String,
        DataType::LargeUtf8 => PowdrrDataType::String,
        DataType::Float32 => PowdrrDataType::Float,
        DataType::Float64 => PowdrrDataType::Float,
        DataType::Struct(sub_fields) => {
            let powdrr_fields = sub_fields
                .iter()
                .map(|x| to_powdrr_field(x))
                .collect::<Vec<PowdrrField>>();
            PowdrrDataType::Object(Box::new(PowdrrSchema {
                fields: powdrr_fields,
            }))
        }
        DataType::List(field_ref) => {
            PowdrrDataType::Array(Box::new(to_powdrr_data_type(field_ref.data_type())))
        }
        DataType::FixedSizeList(field_ref, _) => {
            PowdrrDataType::Array(Box::new(to_powdrr_data_type(field_ref.data_type())))
        }
        DataType::LargeList(field_ref) => {
            PowdrrDataType::Array(Box::new(to_powdrr_data_type(field_ref.data_type())))
        }
        DataType::LargeListView(field_ref) => {
            PowdrrDataType::Array(Box::new(to_powdrr_data_type(field_ref.data_type())))
        }
        _ => panic!("Unsupported data type: {:?}", data_type),
    }
}

fn to_powdrr_field(field: &Field) -> PowdrrField {
    PowdrrField {
        name: field.name().to_string(),
        data_type: to_powdrr_data_type(field.data_type()),
    }
}

pub(crate) fn to_powdrr_schema(schema: &Schema) -> PowdrrSchema {
    let powdrr_fields = schema
        .fields
        .iter()
        .map(|x| to_powdrr_field(x))
        .collect::<Vec<PowdrrField>>();
    PowdrrSchema {
        fields: powdrr_fields,
    }
}

pub(crate) fn extract_powdrr_schema(value: &Value) -> PowdrrSchema {
    let serialized_val = serde_json::to_string(value).unwrap();
    let (schema, _) = infer_json_schema(serialized_val.as_bytes(), None).unwrap();
    to_powdrr_schema(&schema)
}

#[allow(dead_code)]
pub(crate) fn extract_powdrr_schema_str(value: &str) -> PowdrrSchema {
    let value_split = value
        .split("\n")
        .filter(|x| x.len() > 0)
        .collect::<Vec<&str>>();
    assert!(value_split.len() > 0);
    let serde_value = serde_json::from_str(value_split[0]).unwrap();
    extract_powdrr_schema(&serde_value)
}

pub(crate) fn extract_powdrr_schema_option(value: &Option<Value>) -> PowdrrSchema {
    if value.is_none() {
        PowdrrSchema::minimal()
    } else {
        extract_powdrr_schema(value.as_ref().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::schema_massager::{
        extract_powdrr_schema, to_powdrr_schema, PowdrrSchema, SqlBuilder,
    };
    use arrow_json::reader::infer_json_schema;

    #[test]
    fn test_default_missing_fields_schema() {
        let test_val_table = r#"{"_seq_no": 1, "a": 1, "b": "2", "c": 3.3, "d":{"e": 4, "f": 5}, "g": [1, 2, 3], "h": {"i": 1, "j": 2}, "k": 15 }"#;
        let test_val_file =
            r#"{"_seq_no": 1, "a": 1, "c": 3.3, "d":{"e": 4}, "g": [1, 2, 3], "k": null }"#;
        let (schema, _) = infer_json_schema(test_val_table.as_bytes(), None).unwrap();
        let powdrr_schema_table = to_powdrr_schema(&schema);
        let (schema, _) = infer_json_schema(test_val_file.as_bytes(), None).unwrap();
        let powdrr_schema_file = to_powdrr_schema(&schema);

        let sql_builder = SqlBuilder::for_query(true);
        let sql_query = sql_builder.build();
        let sql = sql_query.build(&powdrr_schema_table, &powdrr_schema_file);
        assert!(sql.contains("CAST(NULL AS STRING) as \"b\""));
        assert!(sql.contains("CAST(NULL AS BIGINT) as \"h_i\""));
        assert!(sql.contains("CAST(NULL AS BIGINT) as \"d_f\""));
        assert!(sql.contains("CAST(NULL AS BIGINT) as \"k\""));
    }

    #[test]
    fn test_merge_and_coerce() {
        let mut test_val_1 = serde_json::from_str(r#"{"_seq_no": 1, "b": "2", "c": 3.3, "d":{"f": 5, "aa": {"foo": 44}}, "g": [1, 2, 3]}"#).unwrap();
        let mut test_val_2 = serde_json::from_str(
            r#"{"_seq_no": 1, "a": 1, "c": 3.3, "d":{"e": 4}, "g": [1, 2, 3]}"#,
        )
        .unwrap();
        let test_val_schema_1 = extract_powdrr_schema(&test_val_1);
        let test_val_schema_2 = extract_powdrr_schema(&test_val_2);
        let merged_schema = PowdrrSchema::merge_all(vec![test_val_schema_1, test_val_schema_2]);
        assert_eq!(merged_schema.fields.len(), 6);
        merged_schema.coerce_value(&mut test_val_1);
        merged_schema.coerce_value(&mut test_val_2);

        let test_val_1_coerced = serde_json::to_string(&test_val_1).unwrap();
        assert!(test_val_1_coerced.contains("\"e\":null"));

        let test_val_2_coerced = serde_json::to_string(&test_val_2).unwrap();
        assert!(test_val_2_coerced.contains("\"foo\":null"));
    }
}
