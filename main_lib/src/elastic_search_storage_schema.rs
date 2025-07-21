use serde_json::Value;
use crate::elastic_search_common::create_denormalized_value;
use crate::elastic_search_ingest::WriteBuffer;
use crate::schema_massager::{extract_powdrr_schema_option, PowdrrSchema};

#[derive(Clone)]
pub(crate) struct RecordInput {
    id: String,
    seq_no: i64,
    version: u64,
    existing_normalized: Option<Value>,
    source: Option<Value>,
    source_str: Option<String>,
}

impl RecordInput {
    pub fn new(id: String, seq_no: i64, version: u64, source: &Value) -> Self {
        RecordInput {
            id,
            version,
            seq_no,
            existing_normalized: None,
            source: Some(source.clone()),
            source_str: None,
        }
    }

    pub fn from_record(value: &Value) -> Self {
        let value_map = value.as_object().unwrap().clone();
        let id = value_map.get("_id").unwrap().as_str().unwrap().to_string();
        let version = value_map.get("_version").unwrap().as_u64().unwrap();
        let seq_no = value_map.get("_seq_no").unwrap().as_i64().unwrap();
        let source = value_map.get("_source").unwrap().as_str().unwrap();
        RecordInput {
            id,
            version,
            seq_no,
            existing_normalized: None,
            source: None,
            source_str: Some(source.to_string()),
        }
    }

    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    #[allow(dead_code)]
    pub fn seq_no(&self) -> i64 {
        self.seq_no
    }

    #[allow(dead_code)]
    pub fn existing_normalized(&self) -> Option<&Value> {
        self.existing_normalized.as_ref()
    }

    pub fn source(&self) -> Option<&Value> {
        self.source.as_ref()
    }

    #[allow(dead_code)]
    pub fn source_str(&self) -> Option<&String> {
        self.source_str.as_ref()
    }

    pub fn ensure_normalized_value(&mut self) -> () {
        if self.existing_normalized.is_none() {
            self.ensure_source();
            let mut values = serde_json::Map::new();
            let denormalized_value = create_denormalized_value(self.source.as_ref().unwrap());
            values.insert("_id".to_string(), Value::String(self.id.clone()));
            values.insert("_seq_no".to_string(), Value::Number(self.seq_no.into()));
            values.insert("_id_seq_no".to_string(), Value::String(format!("{}_{}", self.id, self.seq_no)));
            values.insert("_version".to_string(), Value::Number(self.version.into()));
            if self.source_str.is_some() {
                values.insert("_source".to_string(), Value::String(self.source_str.as_ref().unwrap().clone()));
            } else {
                values.insert("_source".to_string(), Value::String(serde_json::to_string(&self.source).unwrap()));
            }
            values.extend(denormalized_value.as_object().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())));
            self.existing_normalized = Some(Value::Object(values));
        }
    }

    pub fn ensure_source(&mut self) -> () {
        if self.source.is_none() && self.source_str.is_some() {
            self.source = Some(serde_json::from_str(&self.source_str.as_ref().unwrap()).unwrap());
        }
    }

    #[allow(dead_code)]
    pub fn ensure_source_str(&mut self) -> () {
        if self.source.is_some() && self.source_str.is_none() {
            self.source_str = Some(serde_json::to_string(&self.source.as_ref().unwrap()).unwrap());
        }
    }

    pub fn to_record(&self) -> Value {
        assert!(self.existing_normalized.is_some(), "You forgot to call ensure_normalized_value() on the record before calling to_record()");
        self.existing_normalized.as_ref().unwrap().clone()
    }
}

#[derive(Clone)]
pub(crate) struct WriteBufferBuilder {
    pub records: Vec<RecordInput>
}

impl WriteBufferBuilder {
    pub fn new() -> Self {
        WriteBufferBuilder{ records: vec!() }
    }

    pub fn extend(&mut self, builder: &WriteBufferBuilder) {
        self.records.extend(builder.records.iter().map(|r| r.clone()));
    }

    pub fn build(&mut self) -> WriteBuffer {
        self.records.iter_mut().for_each(|r| r.ensure_normalized_value());
        let input_schemas = self.records.iter().map(|v|extract_powdrr_schema_option(&v.existing_normalized)).collect::<Vec<PowdrrSchema>>();
        let merged_schema = PowdrrSchema::merge_all(input_schemas);
        self.records.iter_mut().for_each(|r| merged_schema.coerce_value_option(&mut r.existing_normalized));

        let final_records = self.records.iter().map(|r| r.to_record()).collect::<Vec<Value>>();
        WriteBuffer::from(merged_schema, final_records.iter().map(|r|serde_json::to_string(&r).unwrap()).collect())
    }
}


#[cfg(test)]
mod tests {
    use crate::elastic_search_storage_schema::{RecordInput, WriteBufferBuilder};
    use crate::schema_massager::PowdrrDataType;

    #[test]
    fn test_builder_basic() {
        let mut builder = WriteBufferBuilder::new();
        builder.records.push(RecordInput::new(
            "abc".to_string(),
            1,
            1,
            &serde_json::from_str(r#"{"a": 1, "b": "2", "c": 3.3, "d":{"e": 4, "f": 5}, "g": [1, 2, 3]}"#).unwrap()
        ));
        builder.records.push(RecordInput::new(
            "def".to_string(),
            1,
            1,
            &serde_json::from_str(r#"{"a": 2, "c": 4.3, "d":{"e": 8}, "g": [4, 5, 6]}"#).unwrap(),
        ));

        let buffer = builder.build();
        assert_eq!(buffer.num_records(), 2);
        assert!(buffer.schema.is_some());
        let schema = buffer.schema.as_ref().unwrap();
        assert_eq!(schema.fields.len(), 11);
        let schema_map = schema.to_map();
        let source_field = schema_map.get("_source").unwrap();
        assert_eq!(source_field.data_type, PowdrrDataType::String);
        assert!(schema_map.contains_key("d_e"));
        assert!(schema_map.contains_key("d_f"));
    }
}