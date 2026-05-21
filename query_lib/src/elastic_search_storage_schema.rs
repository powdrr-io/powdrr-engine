use crate::distributed_cache;
use crate::elastic_search_common::create_denormalized_value;
use crate::elastic_search_ingest::{IngestError, WriteBuffer, commit_speedboat};
use crate::elastic_search_responses::{OperationResult, QueryResultHit, Shards};
use crate::schema_massager::{PowdrrSchema, extract_powdrr_schema_option};
use serde_json::Value;

#[derive(Clone)]
pub(crate) struct RecordInput {
    id: String,
    version: u64,
    status: Option<u32>,
    existing_normalized: Option<Value>,
    source: Option<Value>,
    source_str: Option<String>,
}

impl RecordInput {
    pub fn new(id: String, version: u64, source: &Value, status: Option<u32>) -> Self {
        RecordInput {
            id,
            version,
            status,
            existing_normalized: None,
            source: Some(source.clone()),
            source_str: None,
        }
    }

    pub fn from_record(value: &Value) -> Self {
        let value_map = value.as_object().unwrap().clone();
        let id = value_map.get("_id").unwrap().as_str().unwrap().to_string();
        let version = value_map.get("_version").unwrap().as_u64().unwrap();
        let source = value_map.get("_source").unwrap().as_str().unwrap();
        RecordInput {
            id,
            version,
            status: None,
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

    pub fn ensure_normalized_value(&mut self, seq_no: Option<u64>) -> () {
        if self.existing_normalized.is_none() {
            assert!(
                seq_no.is_some(),
                "Need to pass in a seq_no when calling ensure_normalized_value() on a record that doesn't have one already."
            );
            self.ensure_source();
            let mut values = serde_json::Map::new();
            let denormalized_value = create_denormalized_value(self.source.as_ref().unwrap());
            values.insert("_id".to_string(), Value::String(self.id.clone()));
            values.insert("_seq_no".to_string(), Value::Number(seq_no.unwrap().into()));
            values.insert(
                "_id_seq_no".to_string(),
                Value::String(format!("{}_{}", self.id, seq_no.unwrap())),
            );
            values.insert("_version".to_string(), Value::Number(self.version.into()));
            if self.source_str.is_some() {
                values.insert(
                    "_source".to_string(),
                    Value::String(self.source_str.as_ref().unwrap().clone()),
                );
            } else {
                values.insert(
                    "_source".to_string(),
                    Value::String(serde_json::to_string(&self.source).unwrap()),
                );
            }
            values.extend(
                denormalized_value
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
            self.existing_normalized = Some(Value::Object(values));
        }
    }

    pub fn ensure_source(&mut self) -> () {
        if self.source.is_none() && self.source_str.is_some() {
            self.source = Some(serde_json::from_str(&self.source_str.as_ref().unwrap()).unwrap());
        }
    }

    pub fn as_record(&mut self, seq_no: Option<u64>) -> Value {
        self.ensure_normalized_value(seq_no);
        self.existing_normalized.as_ref().unwrap().clone()
    }

    pub fn as_operation(&self, table: &String, seq_no: u64, update: bool) -> OperationResult {
        if update {
            OperationResult {
                _index: table.clone(),
                _id: self.id().clone(),
                _version: self.version(),
                result: "updated".to_string(),
                _shards: Shards {
                    total: 1,
                    successful: 1,
                    failed: 0,
                },
                status: self.status,
                _seq_no: seq_no,
                _primary_term: 1,
                get: Some(QueryResultHit {
                    _index: None,
                    _id: None,
                    _version: 1,
                    _seq_no: seq_no,
                    _score: None,
                    _primary_term: Some(1),
                    found: Some(true),
                    sort: None,
                    _source: self.source().cloned().unwrap(),
                }),
            }
        } else {
            OperationResult {
                _index: table.clone(),
                _id: self.id().clone(),
                _version: self.version(),
                result: "created".to_string(),
                _shards: Shards {
                    total: 1,
                    successful: 1,
                    failed: 0,
                },
                status: self.status,
                _seq_no: seq_no,
                _primary_term: 1,
                get: None,
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct FullRecord {
    pub record_input: RecordInput,
    pub seq_no: u64,
}

impl FullRecord {
    pub fn from_record(value: &Value) -> Self {
        let seq_no = value
            .as_object()
            .unwrap()
            .get("_seq_no")
            .unwrap()
            .as_u64()
            .unwrap();
        let record_input = RecordInput::from_record(value);
        FullRecord {
            record_input,
            seq_no,
        }
    }
}

#[derive(Clone)]
pub struct RecordDelete {
    id: String,
    seq_no: u64,
    version: u64,
}

impl RecordDelete {
    pub fn new(id: &String, seq_no: u64, version: u64) -> Self {
        RecordDelete {
            id: id.clone(),
            seq_no,
            version,
        }
    }

    pub fn as_value(&self) -> Value {
        Value::Object(serde_json::Map::from_iter(vec![(
            "_id_seq_no".to_string(),
            Value::String(format!("{}_{}", self.id, self.seq_no)),
        )]))
    }

    pub fn as_operation(&self, table: &String) -> OperationResult {
        OperationResult {
            _index: table.clone(),
            _id: self.id.clone(),
            _version: self.version,
            result: "delete".to_string(),
            _shards: Shards {
                total: 1,
                successful: 1,
                failed: 0,
            },
            _seq_no: self.seq_no,
            _primary_term: 1,
            status: None,
            get: None,
        }
    }
}

pub struct SpeedboatCommitResult {
    pub operations: Vec<OperationResult>,
}

#[derive(Clone)]
pub(crate) struct SpeedboatCommitBuilder {
    table_name: String,
    insert_records: Vec<RecordInput>,
    update_records: Vec<RecordInput>,
    delete_records: Vec<RecordDelete>,
}

impl SpeedboatCommitBuilder {
    pub fn new(table_name: &String) -> Self {
        SpeedboatCommitBuilder {
            table_name: table_name.clone(),
            insert_records: vec![],
            update_records: vec![],
            delete_records: vec![],
        }
    }

    pub fn insert(&mut self, record: &RecordInput) -> () {
        self.insert_records.push(record.clone());
    }

    pub fn update(&mut self, record: &RecordInput) -> () {
        self.update_records.push(record.clone());
    }

    pub fn delete(&mut self, record: &RecordDelete) -> () {
        self.delete_records.push(record.clone())
    }

    pub fn num_inserts(&self) -> usize {
        self.insert_records.len()
    }

    pub fn num_updates(&self) -> usize {
        self.update_records.len()
    }

    pub fn num_deletes(&self) -> usize {
        self.delete_records.len()
    }

    pub fn extend(&mut self, builder: &SpeedboatCommitBuilder) {
        self.insert_records
            .extend(builder.insert_records.iter().map(|r| r.clone()));
        self.update_records
            .extend(builder.update_records.iter().map(|r| r.clone()));
        self.delete_records
            .extend(builder.delete_records.iter().map(|r| r.clone()));
    }

    pub fn build_buffers(&mut self) -> (WriteBuffer, WriteBuffer, Vec<OperationResult>) {
        let mut operations = vec![];
        let seq_no_vec = distributed_cache::report_table_changes(
            &self.table_name,
            self.insert_records.len(),
            self.update_records.len(),
            self.delete_records.len(),
        )
        .unwrap();
        let insert_update_write_buffer = if self.insert_records.len() > 0
            || self.update_records.len() > 0
        {
            self.insert_records
                .iter_mut()
                .chain(self.update_records.iter_mut())
                .zip(seq_no_vec.iter())
                .for_each(|(record, seq_no)| record.ensure_normalized_value(Some(*seq_no as u64)));

            operations.extend(
                self.insert_records
                    .iter()
                    .zip(seq_no_vec.iter())
                    .map(|(record, seq_no)| record.as_operation(&self.table_name, *seq_no, false)),
            );
            operations.extend(
                self.update_records
                    .iter()
                    .zip(seq_no_vec[self.insert_records.len()..].iter())
                    .map(|(record, seq_no)| record.as_operation(&self.table_name, *seq_no, true)),
            );

            let input_schemas = self
                .insert_records
                .iter()
                .chain(self.update_records.iter())
                .map(|v| extract_powdrr_schema_option(&v.existing_normalized))
                .collect::<Vec<PowdrrSchema>>();
            let merged_schema = PowdrrSchema::merge_all(input_schemas);
            self.insert_records
                .iter_mut()
                .chain(self.update_records.iter_mut())
                .for_each(|r| merged_schema.coerce_value_option(&mut r.existing_normalized));

            let final_records = self
                .insert_records
                .iter_mut()
                .chain(self.update_records.iter_mut())
                .map(|r| r.as_record(None))
                .collect::<Vec<Value>>();
            WriteBuffer::insert_and_update(
                merged_schema,
                final_records.iter().map(|r| r.clone()).collect(),
            )
        } else {
            WriteBuffer::empty()
        };

        operations.extend(
            self.delete_records
                .iter()
                .map(|r| r.as_operation(&self.table_name)),
        );
        let delete_write_buffer =
            WriteBuffer::delete(self.delete_records.iter().map(|r| r.as_value()).collect());
        (insert_update_write_buffer, delete_write_buffer, operations)
    }

    pub async fn commit(&mut self) -> Result<SpeedboatCommitResult, IngestError> {
        let (insert_update_write_buffer, delete_write_buffer, operations) = self.build_buffers();
        commit_speedboat(
            &self.table_name,
            &insert_update_write_buffer,
            &delete_write_buffer,
            None,
            &"commit".to_string(),
        )
        .await?;

        Ok(SpeedboatCommitResult { operations })
    }
}

#[cfg(test)]
mod tests {
    use crate::elastic_search_storage_schema::{RecordInput, SpeedboatCommitBuilder};
    use crate::schema_massager::PowdrrDataType;

    #[test]
    fn test_builder_basic() {
        let mut builder = SpeedboatCommitBuilder::new(&"fake".to_string());
        builder.insert_records.push(RecordInput::new(
            "abc".to_string(),
            1,
            &serde_json::from_str(
                r#"{"a": 1, "b": "2", "c": 3.3, "d":{"e": 4, "f": 5}, "g": [1, 2, 3]}"#,
            )
            .unwrap(),
            None,
        ));
        builder.insert_records.push(RecordInput::new(
            "def".to_string(),
            1,
            &serde_json::from_str(r#"{"a": 2, "c": 4.3, "d":{"e": 8}, "g": [4, 5, 6]}"#).unwrap(),
            None,
        ));

        let (insert_buffer, _, _) = builder.build_buffers();
        assert_eq!(insert_buffer.num_records(), 2);
        assert!(insert_buffer.schema().is_some());
        let schema = insert_buffer.schema().as_ref().unwrap().clone();
        assert_eq!(schema.fields().len(), 10);
        let schema_map = schema.to_map();
        let source_field = schema_map.get("_source").unwrap();
        assert_eq!(source_field.data_type, PowdrrDataType::String);
        assert!(schema_map.contains_key("d_e"));
        assert!(schema_map.contains_key("d_f"));
    }
}
