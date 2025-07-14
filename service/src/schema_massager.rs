use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use arrow_json::reader::infer_json_schema;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub(crate) enum PowdrrDataType {
    String,
    Integer,
    Boolean,
    Float,
    Array(Box<PowdrrDataType>),
    Object(Box<PowdrrSchema>),
}

impl PowdrrDataType {
    pub fn is_object(&self) -> bool {
        match self {
            PowdrrDataType::Object(_) => true,
            _ => false,
        }
    }

    pub fn as_object_schema(&self) -> Option<PowdrrSchema> {
        match self {
            PowdrrDataType::Object(schema) => Some(schema.deref().clone()),
            _ => None
        }
    }
}


#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub(crate) struct PowdrrField {
    pub name: String,
    pub data_type: PowdrrDataType
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub(crate) struct PowdrrSchema {
    pub fields: Vec<PowdrrField>
}

impl PowdrrSchema {
    pub fn from(fields: &Vec<PowdrrField>) -> Self {
        PowdrrSchema{
            fields: fields.clone()
        }
    }

    pub fn to_map(&self) -> HashMap<String, PowdrrField> {
        self.fields.iter().map(|x| (x.name.clone(), x.clone())).collect::<HashMap<String, PowdrrField>>()
    }

    pub(crate) fn merge_all(schemas: Vec<Self>) -> Self {
        assert!(schemas.len() > 0);

        let mut iter = schemas.iter();
        let mut merged_schema = iter.next().unwrap().clone();

        for schema in iter {
            merged_schema.merge_from(schema);
        }
        merged_schema
    }

    pub fn merge_from(&mut self, other: &PowdrrSchema) -> () {
        let self_map = self.to_map();

        for other_field in other.fields.iter() {
            match self_map.get(&other_field.name) {
                Some(self_field) => {
                    // TODO - Compare the fields to make sure they are identical or if objects then merge

                    if other_field.data_type.is_object() && self_field.data_type.is_object() {
                        // Merge the schema in the field itself (recursive call)
                        let mut self_field_schema = self_field.data_type.as_object_schema().unwrap();
                        let other_field_schema = other_field.data_type.as_object_schema().unwrap();
                        self_field_schema.merge_from(&other_field_schema);
                        let merged_field = PowdrrField{ name: other_field.name.clone(), data_type: PowdrrDataType::Object(Box::new(self_field_schema))};
                        let position = self.fields.iter().position(|f| f.name == other_field.name).unwrap();
                        self.fields[position] = merged_field;
                    }
                },
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
                Some(field_value) => {
                    match &field.data_type {
                        PowdrrDataType::Object(field_value_schema) => {
                            field_value_schema.coerce_value(field_value);
                        },
                        _ => ()
                    }
                },
                None => {
                    value_map.insert(field.name.clone(), Self::default_serde_value(&field.data_type));
                }
            }
        }
    }

    fn default_serde_value(data_type: &PowdrrDataType) -> Value {
        match data_type {
            PowdrrDataType::Object(schema) => {
                let mut value_fields = serde_json::Map::new();
                for field in schema.fields.iter() {
                    value_fields.insert(field.name.clone(), Self::default_serde_value(&field.data_type));
                }
                Value::Object(value_fields)
            },
            _ => Value::Null
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct NamedStructEntry {
    pub(crate) name: String,
    pub(crate) expression: SqlExpression
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
    Or(Vec<SqlExpression>),
}

unsafe impl Send for SqlExpression {}
unsafe impl Sync for SqlExpression {}

impl SqlExpression {
    fn lookup_field(schema: &HashMap<String, PowdrrField>, path: &String) -> Option<PowdrrField> {
        let split_path: Vec<&str> = path.split(".").collect();
        Self::lookup_field_worker(schema, &split_path, 0)
    }

    fn lookup_field_worker(schema: &HashMap<String, PowdrrField>, path: &Vec<&str>, index: usize) -> Option<PowdrrField> {
        match schema.get(&path.get(index).unwrap().to_string()) {
            Some(field) => {
                if index + 1 == path.len() {
                    return Some(field.clone());
                } else {
                    return match &field.data_type {
                        PowdrrDataType::Object(schema) => {
                            return Self::lookup_field_worker(&schema.to_map(), path, index + 1);
                        },
                        _ => None
                    }
                }
            },
            None => {
                return None
            }
        }
    }

    fn explode_ref(&self, table: &String, name: &String, original_schema: &HashMap<String, PowdrrField>, target_schema: &HashMap<String, PowdrrField>) -> HashMap<String, Self> {
        assert_eq!(table, "t");
        let denormalized_name = name.replace(".", "_");
        let original_schema_field = Self::lookup_field(original_schema, &denormalized_name);
        let target_schema_field = Self::lookup_field(target_schema, &denormalized_name);
        if original_schema_field.is_none() {
            HashMap::from([("".to_string(), self.clone())])
        } else if target_schema_field.is_some() {
            self.explode_partial_ref(&"".to_string(), &original_schema_field.unwrap(), &target_schema_field.unwrap())
            //self.populate_field(&denormalized_name, &original_schema_field.unwrap(), &target_schema_field.unwrap())
        } else {
            self.explode_full_ref(&"".to_string(), &original_schema_field.unwrap())
        }
    }

    fn explode_partial_ref(&self, prefix: &String, original_field: &PowdrrField, target_field: &PowdrrField) -> HashMap<String, Self> {
        let original_inner_fields = match &original_field.data_type {
            PowdrrDataType::Object(schema) => {
                schema.fields.clone()
            },
            _ => {
                return HashMap::from([(
                    prefix.clone(),
                    self.make_exploded_ref(prefix)
                )]);
            }
        };

        let target_inner_fields_map = match &target_field.data_type {
            PowdrrDataType::Object(schema) => {
                schema.to_map()
            },
            _ => {
                return HashMap::from([(
                    prefix.clone(),
                    self.make_exploded_ref(prefix)
                )]);
            }
        };

        let mut result = HashMap::new();
        for field in original_inner_fields.iter() {
            let target_field = target_inner_fields_map.get(&field.name);
            match target_field {
                Some(tf) => {
                    result.extend(self.explode_partial_ref(&format!("{}_{}", prefix, field.name), &field, &tf));
                },
                None => {
                    result.extend(self.explode_full_ref(&format!("{}_{}", prefix, field.name), &field));
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
                    result.extend(self.explode_full_ref(&format!("{}_{}", prefix, field.name), &field));
                }
                result
            },
            _ => {
                HashMap::from([(prefix.clone(), Self::literal_default(field))])
            }
        }
    }

    fn make_exploded_ref(&self, prefix: &String) -> Self {
        match self {
            SqlExpression::FieldRef(table, name) => {
                SqlExpression::FieldRef(table.clone(), format!("{}{}", name, prefix))
            },
            _ => panic!("Expected a field ref")
        }
    }

    fn finalize(&self, original_schema: &HashMap<String, PowdrrField>, target_schema: &HashMap<String, PowdrrField>) -> Self {
        match self {
            SqlExpression::And(exprs) => {
                SqlExpression::And(exprs.iter().map(|x| x.finalize(original_schema, target_schema)).collect())
            },
            SqlExpression::Arithmetic(left, op, right) => {
                SqlExpression::Arithmetic(
                    Box::new(left.finalize(original_schema, target_schema)),
                    op.clone(),
                    Box::new(right.finalize(original_schema, target_schema))
                )
            },
            SqlExpression::Average(value) => {
                SqlExpression::Average(
                    Box::new(value.finalize(original_schema, target_schema))
                )
            },
            SqlExpression::Comparison(left, op, right) => {
                SqlExpression::Comparison(
                    Box::new(left.finalize(original_schema, target_schema)),
                    op.clone(),
                    Box::new(right.finalize(original_schema, target_schema))
                )
            },
            SqlExpression::Count => self.clone(),
            SqlExpression::CountDistinct(value) => {
                SqlExpression::CountDistinct(
                    Box::new(value.finalize(original_schema, target_schema)),
                )
            }
            SqlExpression::FieldRef(table, name) if table == "t" => {
                let denormalized_name = name.replace(".", "_");
                let original_schema_field = Self::lookup_field(original_schema, &denormalized_name);
                let target_schema_field = Self::lookup_field(target_schema, &denormalized_name);
                if original_schema_field.is_none() {
                    SqlExpression::LiteralNonString("null".to_string())
                } else if target_schema_field.is_some() {
                    self.populate_field(&denormalized_name, &original_schema_field.unwrap(), &target_schema_field.unwrap())
                } else {
                    Self::literal_default(&original_schema_field.unwrap())
                }
            },
            SqlExpression::FieldRef(table, _name) => {
                // We don't do any rewriting for field refs that are not against the user defined data.
                assert_ne!(table, "t");
                self.clone()
            },
            SqlExpression::In(left, right) => {
                SqlExpression::In(
                    Box::new(left.finalize(original_schema, target_schema)),
                    right.iter().map(|x| x.finalize(original_schema, target_schema)).collect()
                )
            },
            SqlExpression::IsNull(value) => {
                SqlExpression::IsNull(
                    Box::new(value.finalize(original_schema, target_schema)),
                )
            },
            SqlExpression::Like(left, right) => {
                SqlExpression::Like(
                    Box::new(left.finalize(original_schema, target_schema)),
                    Box::new(right.finalize(original_schema, target_schema))
                )
            },
            SqlExpression::LiteralNonString(_) => self.clone(),
            SqlExpression::LiteralString(_) => self.clone(),
            SqlExpression::NamedStruct(entries) => {
                SqlExpression::NamedStruct(
                    entries.iter().map(|x|NamedStructEntry{ name: x.name.clone(), expression: x.expression.finalize(original_schema, target_schema)}).collect()
                )
            }
            SqlExpression::Not(value) => {
                SqlExpression::Not(
                    Box::new(value.finalize(original_schema, target_schema)),
                )
            },
            SqlExpression::Or(exprs) => {
                SqlExpression::Or(exprs.iter().map(|x| x.finalize(original_schema, target_schema)).collect())
            }
        }
    }

    fn populate_field(&self, base_name: &String, original_field: &PowdrrField, target_field: &PowdrrField) -> Self {
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

        let mut entries = vec!();
        for (field_name, original_field) in original_schema {
            let full_name = format!("{}.{}", base_name, field_name);
            let expression = match target_schema.get(&field_name) {
                Some(target_field) => {
                    self.populate_field(&full_name, &original_field, &target_field)
                },
                None => {
                    Self::literal_default(&original_field)
                }
            };
            entries.push(NamedStructEntry {
                name: field_name.clone(),
                expression: expression,
            });
        }
        SqlExpression::NamedStruct(entries)
    }

    fn literal_default(field: &PowdrrField) -> SqlExpression {
        match &field.data_type {
            PowdrrDataType::Array(_element_type) => SqlExpression::LiteralNonString("null".to_string()),
            PowdrrDataType::Object(_schema) => SqlExpression::LiteralNonString("null".to_string()),
            _ => SqlExpression::LiteralNonString("null".to_string())
        }
    }

    fn stringize(&self) -> String {
        match self {
            SqlExpression::And(exprs) => {
                let exprs_str = exprs.iter().map(|x| x.stringize()).collect::<Vec<String>>();
                format!("({})", exprs_str.join(" AND "))
            },
            SqlExpression::Arithmetic(left, op, right) => {
                format!("({} {} {})", left.stringize(), op, right.stringize())
            },
            SqlExpression::Average(value) => {
                format!("AVG({})", value.stringize())
            },
            SqlExpression::Comparison(left, op, right) => {
                format!("({} {} {})", left.stringize(), op, right.stringize())
            },
            SqlExpression::Count => {
                "count(1)".to_string()
            },
            SqlExpression::CountDistinct(value) => {
                format!("count(distinct {})", value.stringize())
            },
            SqlExpression::FieldRef(table, field) if table == "t" => {
                format!("{}.\"{}\"", table, field.replace(".", "_"))
            },
            SqlExpression::FieldRef(table, field) => {
                assert_ne!(table, "t");
                assert!(!field.contains("."), "Need to handle this case now");
                format!("{}.\"{}\"", table, field)
            },
            SqlExpression::In(left, right) => {
                format!("{} IN ({})", left.stringize(), right.iter().map(|x| x.stringize()).collect::<Vec<String>>().join(", "))
            },
            SqlExpression::IsNull(value) => {
                format!("{} IS NULL", value.stringize())
            },
            SqlExpression::Like(left, right) => {
                format!("{} LIKE {}", left.stringize(), right.stringize())
            },
            SqlExpression::LiteralNonString(value) => {
                value.clone()
            },
            SqlExpression::LiteralString(value) => {
                format!("'{}'", value)
            },
            SqlExpression::NamedStruct(entries) => {
                format!("NAMED_STRUCT({})", entries.iter().map(|x|format!("'{}', {}", x.name, x.expression.stringize())).collect::<Vec<String>>().join(", "))
            }
            SqlExpression::Not(value) => {
                format!("NOT({})", value.stringize())
            },
            SqlExpression::Or(exprs) => {
                let exprs_str = exprs.iter().map(|x| x.stringize()).collect::<Vec<String>>();
                format!("({})", exprs_str.join(" OR "))
            },
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
    pub(crate) expression: SqlExpression
}

impl FieldExpression {
    fn finalize(&self, original_schema: &HashMap<String, PowdrrField>, target_schema: &HashMap<String, PowdrrField>) -> Vec<Self> {
        match &self.expression {
            SqlExpression::FieldRef(table, name) if table == "t" => {
                let exploded_ref = self.expression.explode_ref(table, name, original_schema, target_schema);
                exploded_ref.iter().map(|(name_suffix, expression)|FieldExpression{
                    name: format!("{}{}", self.name, name_suffix),
                    expression: expression.clone(),
                }).collect()
            },
            _ => {
                vec!(FieldExpression {
                    name: self.name.clone(),
                    expression: self.expression.finalize(original_schema, target_schema)
                })
            }
        }
    }

    fn stringize(&self) -> String {
        format!("{} as \"{}\"", self.expression.stringize(), self.name)
    }
}

#[derive(Clone)]
pub(crate) struct SqlBuilder {
    pub(crate) all_fields: bool,
    pub(crate) fields: Vec<FieldExpression>,
    pub(crate) joins: Vec<String>,
    filter_stack: RefCell<Vec<Vec<SqlExpression>>>,
    pub(crate) limit: Option<u64>,
    pub(crate) calculate_score: bool,
    pub(crate) order_by: Vec<SqlExpression>,
    pub(crate) group_by: Vec<SqlExpression>,
    top_level: bool,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SqlQuery {
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
            fields: vec!(),
            joins: vec!(),
            filter_stack: RefCell::new(vec!(vec!())),
            limit: None,
            calculate_score: false,
            order_by: vec!(),
            group_by: vec!(),
            top_level: true,
        }
    }

    pub(crate) fn for_agg() -> Self {
        SqlBuilder {
            all_fields: false,
            fields: vec!(),
            joins: vec!(),
            filter_stack: RefCell::new(vec!(vec!())),
            limit: None,
            calculate_score: false,
            order_by: vec!(),
            group_by: vec!(),
            top_level: false,
        }
    }

    #[allow(dead_code)]
    pub fn set_all_fields_testing_only(&mut self) -> () {
        self.all_fields = true;
    }

    pub(crate) fn push_filter_context(&mut self) -> &mut Self {
        self.filter_stack.get_mut().push(vec!());
        self
    }

    pub(crate) fn pop_filter_context(&mut self, is_and: bool) -> &mut Self {
        self.pop_and_maybe_not_filter_context(is_and, false)
    }

    pub(crate) fn pop_and_not_filter_context(&mut self, is_and: bool) -> &mut Self {
        self.pop_and_maybe_not_filter_context(is_and, true)
    }

    pub(crate) fn pop_and_maybe_not_filter_context(&mut self, is_and: bool, is_not: bool) -> &mut Self {
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
            joins_copy.push("INNER JOIN {target_table}_search_index si on si.doc_id = t._id".to_string())
        }
        if self.top_level {
            joins_copy.push("LEFT JOIN {deletes_table} dt ON dt._id = t._id".to_string())
        }
        joins_copy
    }

    fn _latest() -> SqlExpression {
        SqlExpression::Or(vec!(
            SqlExpression::Comparison(
                Box::new(SqlExpression::FieldRef("t".to_string(), "_seq_no".to_string())),
                ">".to_string(),
                Box::new(SqlExpression::FieldRef("dt".to_string(), "_seq_no".to_string()))
            ),
            SqlExpression::IsNull(Box::new(SqlExpression::FieldRef("dt".to_string(), "_seq_no".to_string())))
        ))
    }

    fn _filters(&self) -> Option<SqlExpression> {
        let mut local_filter_stack = self.filter_stack.borrow().clone();
        assert_eq!(local_filter_stack.len(), 1);
        let mut top_copy = local_filter_stack.pop().unwrap().clone();
        if self.top_level {
            top_copy.push(SqlBuilder::_latest())
        }
        SqlExpression::and(top_copy)
    }

    fn _fields(&self) -> Vec<FieldExpression> {
        let mut fields_copy = self.fields.clone();
        if self.calculate_score {
            fields_copy.push(FieldExpression{
                name: "term_cnt".to_string(),
                expression: SqlExpression::FieldRef("si".to_string(), "term_cnt".to_string())
            });
            fields_copy.push(FieldExpression{
                name: "word_cnt".to_string(),
                expression: SqlExpression::FieldRef("si".to_string(), "word_cnt".to_string())
            });
        }
        fields_copy
    }

    pub(crate) fn build(&self) -> SqlQuery {
        SqlQuery {
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
    fn fields(&self, original_schema: &HashMap<String, PowdrrField>, target_schema: &HashMap<String, PowdrrField>) -> String {
        // Magic up missing fields as nulls
        // TODO: figure out when fields have changed types and do something
        let mut final_fields = vec!();
        let mut fields_copy = self.fields.clone();
        if self.all_fields {
            for (_, field) in original_schema.iter() {
                fields_copy.push(FieldExpression {
                    name: field.name.clone(),
                    expression: SqlExpression::FieldRef("t".to_string(), field.name.clone())
                });
            }
        }
        for field in fields_copy.iter() {
            final_fields.extend(field.finalize(original_schema, target_schema).iter().map(|f|f.stringize()));
        }
        final_fields.join(", ")
    }

    fn filters(&self, original_schema: &HashMap<String, PowdrrField>, target_schema: &HashMap<String, PowdrrField>) -> String {
        // "Pre-process" filters where any missing field is considered a null value.
        // TODO: we could detect cases where the filter can't possibly match to terminate early
        match &self.filters {
            Some(filter) => {
                format!(" WHERE {}", filter.finalize(original_schema, target_schema).stringize())
            },
            None => "".to_string()
        }
    }

    fn order_by(&self, original_schema: &HashMap<String, PowdrrField>, target_schema: &HashMap<String, PowdrrField>) -> String {
        match self.order_by.len() {
            0 => "".to_string(),
            _ => format!(" ORDER BY {}", self.order_by.iter().map(|x| x.finalize(original_schema, target_schema).stringize()).collect::<Vec<String>>().join(", "))
        }
    }

    fn group_by(&self, original_schema: &HashMap<String, PowdrrField>, target_schema: &HashMap<String, PowdrrField>) -> String {
        match self.group_by.len() {
            0 => "".to_string(),
            _ => format!(" GROUP BY {}", self.group_by.iter().map(|x| x.finalize(original_schema, target_schema).stringize()).collect::<Vec<String>>().join(", "))
        }
    }

    fn limit(&self) -> String {
        match self.limit {
            Some(limit) => format!(" LIMIT {}", limit),
            None => "".to_string()
        }
    }

    pub(crate) fn build_same(&self, schema: &PowdrrSchema) -> String {
        self.build(schema, schema)
    }

     pub(crate) fn build(&self, original_schema: &PowdrrSchema, target_schema: &PowdrrSchema) -> String {
        let original_schema_map = original_schema.to_map();
        let target_schema_map = target_schema.to_map();
        let fields = self.fields(&original_schema_map, &target_schema_map);
        let joins = &self.joins;
        let filters = self.filters(&original_schema_map, &target_schema_map);
        let order_by = self.order_by(&original_schema_map, &target_schema_map);
        let group_by = self.group_by(&original_schema_map, &target_schema_map);
        let limit = self.limit();

        format!("SELECT {fields} FROM {{target_table}} t {joins}{filters}{group_by}{order_by}{limit}")
    }

    #[allow(dead_code)]
    pub(crate) fn build_debug(&self) -> String {
        let fields = self.fields.iter().map(|x|x.stringize()).collect::<Vec<String>>().join(", ");
        let joins = self.joins.clone();
        let filters = self.filters.clone().map(|x|x.stringize()).unwrap_or("".to_string());
        let order_by = self.order_by.iter().map(|x|x.stringize()).collect::<Vec<String>>().join(", ");
        let group_by = self.group_by.iter().map(|x|x.stringize()).collect::<Vec<String>>().join(", ");
        let limit = self.limit();
        format!("SELECT {fields} FROM {{target_table}} t {joins} WHERE {filters} GROUP BY {group_by} ORDER BY {order_by} {limit}")
    }
}


fn to_powdrr_data_type(data_type: &DataType) -> PowdrrDataType {
    match data_type {
        DataType::Null => {
            // Null is a wacky type. We could probably do anything here but we'll say
            // it is a String for now.
            PowdrrDataType::String
        },
        DataType::Int64 => PowdrrDataType::Integer,
        DataType::Boolean => PowdrrDataType::Boolean,
        DataType::Utf8 => PowdrrDataType::String,
        DataType::Utf8View => PowdrrDataType::String,
        DataType::LargeUtf8 => PowdrrDataType::String,
        DataType::Float64 => PowdrrDataType::Float,
        DataType::Struct(sub_fields) => {
            let powdrr_fields = sub_fields.iter().map(|x|to_powdrr_field(x)).collect::<Vec<PowdrrField>>();
            PowdrrDataType::Object(Box::new(PowdrrSchema{ fields: powdrr_fields }))
        },
        DataType::List(field_ref) => PowdrrDataType::Array(Box::new(to_powdrr_data_type(field_ref.data_type()))),
        DataType::FixedSizeList(field_ref, _) => PowdrrDataType::Array(Box::new(to_powdrr_data_type(field_ref.data_type()))),
        DataType::LargeList(field_ref) => PowdrrDataType::Array(Box::new(to_powdrr_data_type(field_ref.data_type()))),
        DataType::LargeListView(field_ref) => PowdrrDataType::Array(Box::new(to_powdrr_data_type(field_ref.data_type()))),
        _ => panic!("Unsupported data type: {:?}", data_type)
    }
}

fn to_powdrr_field(field: &Field) -> PowdrrField {
    PowdrrField{ name: field.name().to_string(), data_type: to_powdrr_data_type(field.data_type()) }
}

pub(crate) fn to_powdrr_schema(schema: &Schema) -> PowdrrSchema {
    let powdrr_fields = schema.fields.iter().map(|x|to_powdrr_field(x)).collect::<Vec<PowdrrField>>();
    PowdrrSchema{ fields: powdrr_fields }
}

pub(crate) fn extract_powdrr_schema(value: &Value) -> PowdrrSchema {
    let serialized_val = serde_json::to_string(value).unwrap();
    let (schema, _) = infer_json_schema(serialized_val.as_bytes(), None).unwrap();
    to_powdrr_schema(&schema)
}

#[allow(dead_code)]
pub(crate) fn extract_powdrr_schema_str(value: &str) -> PowdrrSchema {
    let value_split = value.split("\n").filter(|x|x.len() > 0).collect::<Vec<&str>>();
    let serde_values = value_split.iter().map(|x|serde_json::from_str(x).unwrap()).collect::<Vec<Value>>();
    assert!(serde_values.len() > 0);
    extract_powdrr_schema(&serde_values[0])
}

pub(crate) fn extract_powdrr_schema_option(value: &Option<Value>) -> PowdrrSchema {
    if value.is_none() {
        PowdrrSchema{ fields: vec!() }
    } else {
        extract_powdrr_schema(value.as_ref().unwrap())
    }
}


#[cfg(test)]
mod tests {
    use arrow_json::reader::infer_json_schema;
    use crate::schema_massager::{extract_powdrr_schema, to_powdrr_schema, PowdrrSchema, SqlBuilder};

    #[test]
    fn test_default_missing_fields_schema() {
        let test_val_table = r#"{"_seq_no": 1, "a": 1, "b": "2", "c": 3.3, "d":{"e": 4, "f": 5}, "g": [1, 2, 3], "h": {"i": 1, "j": 2}}"#;
        let test_val_file = r#"{"_seq_no": 1, "a": 1, "c": 3.3, "d":{"e": 4}, "g": [1, 2, 3]}"#;
        let (schema, _) = infer_json_schema(test_val_table.as_bytes(), None).unwrap();
        let powdrr_schema_table = to_powdrr_schema(&schema);
        let (schema, _) = infer_json_schema(test_val_file.as_bytes(), None).unwrap();
        let powdrr_schema_file = to_powdrr_schema(&schema);

        let sql_builder = SqlBuilder::for_query(true);
        let sql_query = sql_builder.build();
        let sql = sql_query.build(&powdrr_schema_table, &powdrr_schema_file);
        assert!(sql.contains("null as \"b\""));
        assert!(sql.contains("null as \"d_f\""));
    }

    #[test]
    fn test_merge_and_coerce() {
        let mut test_val_1 = serde_json::from_str(r#"{"_seq_no": 1, "b": "2", "c": 3.3, "d":{"f": 5, "aa": {"foo": 44}}, "g": [1, 2, 3]}"#).unwrap();
        let mut test_val_2 = serde_json::from_str(r#"{"_seq_no": 1, "a": 1, "c": 3.3, "d":{"e": 4}, "g": [1, 2, 3]}"#).unwrap();
        let test_val_schema_1 = extract_powdrr_schema(&test_val_1);
        let test_val_schema_2 = extract_powdrr_schema(&test_val_2);
        let merged_schema = PowdrrSchema::merge_all(vec!(test_val_schema_1, test_val_schema_2));
        assert_eq!(merged_schema.fields.len(), 6);
        merged_schema.coerce_value(&mut test_val_1);
        merged_schema.coerce_value(&mut test_val_2);

        let test_val_1_coerced = serde_json::to_string(&test_val_1).unwrap();
        assert!(test_val_1_coerced.contains("\"e\":null"));

        let test_val_2_coerced = serde_json::to_string(&test_val_2).unwrap();
        assert!(test_val_2_coerced.contains("\"foo\":null"));
    }
}