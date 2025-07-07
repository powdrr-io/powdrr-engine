use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::elastic_search_ingest::WriteBuffer;
use crate::schema_massager::{extract_powdrr_schema_option, PowdrrSchema};

#[derive(Clone)]
pub(crate) struct RecordInput {
    pub _id: String,
    pub _seq_no: i64,
    pub _version: i64,
    pub existing_normalized: Option<Value>,
    pub source: Value
}

impl RecordInput {
    fn ensure_normalized_value(&mut self) -> () {
        if self.existing_normalized.is_none() {
            let mut values = serde_json::Map::new();
            values.insert("_id".to_string(), Value::String(self._id.clone()));
            values.insert("_seq_no".to_string(), Value::Number(self._seq_no.into()));
            values.insert("_version".to_string(), Value::Number(self._version.into()));
            values.insert("source".to_string(), self.source.clone());
            self.existing_normalized = Some(Value::Object(values));
        }
    }

    fn to_record(&self) -> Record {
        Record {
            normalized: self.existing_normalized.as_ref().unwrap().clone(),
            source: self.source.clone()
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Record {
    pub normalized: Value,
    pub source: Value
}


pub(crate) struct WriteBufferBuilder {
    pub records: Vec<RecordInput>
}

impl WriteBufferBuilder {
    pub fn new() -> Self {
        WriteBufferBuilder{ records: vec!() }
    }

    pub fn build(&mut self) -> WriteBuffer {
        self.records.iter_mut().for_each(|r| r.ensure_normalized_value());
        let input_schemas = self.records.iter().map(|v|extract_powdrr_schema_option(&v.existing_normalized)).collect();
        let merged_schema = PowdrrSchema::merge_all(input_schemas);

        self.records.iter_mut().for_each(|r| merged_schema.coerce_value_option(&mut r.existing_normalized));

        let final_records = self.records.iter().map(|r| r.to_record()).collect::<Vec<Record>>();
        WriteBuffer::from(merged_schema, final_records.iter().map(|r|serde_json::to_string(&r).unwrap()).collect())
    }
}


#[cfg(test)]
mod tests {
    use arrow_json::reader::infer_json_schema;
    use crate::elastic_search_storage_schema::{RecordInput, WriteBufferBuilder};
    use crate::schema_massager::{extract_powdrr_schema, to_powdrr_schema, PowdrrDataType, PowdrrSchema, SqlBuilder};

    #[test]
    fn test_builder_basic() {
        let mut builder = WriteBufferBuilder::new();
        builder.records.push(RecordInput {
            _id: "abc".to_string(),
            _seq_no: 1,
            _version: 1,
            existing_normalized: None,
            source: serde_json::from_str(r#"{"a": 1, "b": "2", "c": 3.3, "d":{"e": 4, "f": 5}, "g": [1, 2, 3]}"#).unwrap(),
        });
        builder.records.push(RecordInput {
            _id: "def".to_string(),
            _seq_no: 1,
            _version: 1,
            existing_normalized: None,
            source: serde_json::from_str(r#"{"a": 2, "c": 4.3, "d":{"e": 8}, "g": [4, 5, 6]}"#).unwrap(),
        });

        let buffer = builder.build();
        assert_eq!(buffer.lines.len(), 2);
        assert!(buffer.schema.is_some());
        let schema = buffer.schema.as_ref().unwrap();
        assert_eq!(schema.fields.len(), 4);
        let source_field = schema.fields.get(3).unwrap();
        assert_eq!(source_field.name, "source");
        match &source_field.data_type {
            PowdrrDataType::Object(source_schema) => {
                assert_eq!(source_schema.fields.len(), 5);
            },
            _ => panic!("unexpected schema")
        };
    }
}