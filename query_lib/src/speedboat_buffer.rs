use crate::data_access;
use crate::schema_massager::PowdrrSchema;
use datafusion::arrow::ipc::writer::FileWriter;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

#[derive(Debug)]
pub struct SpeedboatBufferError {
    pub message: String,
}

#[derive(Clone)]
pub struct WriteBuffer {
    lines: Vec<Value>,
    schema: Option<PowdrrSchema>,
}

pub const JSON_MODE: bool = false;

impl WriteBuffer {
    pub fn empty() -> Self {
        WriteBuffer {
            lines: vec![],
            schema: None,
        }
    }

    pub fn insert_and_update(schema: PowdrrSchema, lines: Vec<Value>) -> Self {
        WriteBuffer {
            lines,
            schema: Some(schema),
        }
    }

    pub fn delete(lines: Vec<Value>) -> Self {
        WriteBuffer {
            lines,
            schema: Some(PowdrrSchema::deletes()),
        }
    }

    pub fn write_to_file(&self, file_name: &String) -> Result<u64, SpeedboatBufferError> {
        if JSON_MODE {
            self.write_to_json_file(&format!("{}.json", file_name))
        } else {
            self.write_to_arrow_file(&format!("{}.arrow", file_name))
        }
    }

    fn write_to_json_file(&self, file_name: &String) -> Result<u64, SpeedboatBufferError> {
        assert!(!self.lines.is_empty(), "Cannot write empty buffer to file");
        ensure_local_parent_dir(file_name)?;
        let mut file_write = File::create(file_name).expect("Cannot create file");
        for line in &self.lines {
            match writeln!(&mut file_write, "{}", line) {
                Err(e) => {
                    return Err(SpeedboatBufferError {
                        message: e.to_string(),
                    });
                }
                _ => (),
            }
        }
        Ok(self
            .lines
            .iter()
            .map(|l| l.to_string().len())
            .sum::<usize>() as u64)
    }

    fn write_to_arrow_file(&self, file_name: &String) -> Result<u64, SpeedboatBufferError> {
        assert!(!self.lines.is_empty(), "Cannot write empty buffer to file");
        assert!(self.schema.is_some(), "Cannot write buffer without schema");
        ensure_local_parent_dir(file_name)?;
        let bytes = self.arrow_file_bytes()?;
        let mut file = File::create(file_name).unwrap();
        file.write_all(&bytes).unwrap();
        Ok(bytes.len() as u64)
    }

    pub async fn write_to_arrow_s3(&self, s3_path: &String) -> Result<u64, SpeedboatBufferError> {
        assert!(!self.lines.is_empty(), "Cannot write empty buffer to file");
        assert!(self.schema.is_some(), "Cannot write buffer without schema");
        let full_s3_path = format!("{}.arrow", s3_path);
        let bytes = self.arrow_file_bytes()?;
        data_access::put_s3_file(&full_s3_path, &bytes)
            .await
            .map_err(|e| SpeedboatBufferError {
                message: e.to_string(),
            })?;
        Ok(bytes.len() as u64)
    }

    pub fn as_byte_vec(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        for line in &self.lines {
            buffer.extend(line.to_string().as_bytes());
            buffer.push(b'\n');
        }
        buffer
    }

    pub fn arrow_file_bytes(&self) -> Result<Vec<u8>, SpeedboatBufferError> {
        assert!(!self.lines.is_empty(), "Cannot write empty buffer to file");
        assert!(self.schema.is_some(), "Cannot write buffer without schema");
        let arrow_schema = self.schema.as_ref().unwrap().to_arrow_schema();
        let fields = arrow_schema.fields.as_ref();
        let record_batch = serde_arrow::to_record_batch(fields, &self.lines).unwrap();
        let mut bytes = Vec::new();
        let mut writer = FileWriter::try_new(&mut bytes, &record_batch.schema()).map_err(|e| {
            SpeedboatBufferError {
                message: e.to_string(),
            }
        })?;
        writer
            .write(&record_batch)
            .map_err(|e| SpeedboatBufferError {
                message: e.to_string(),
            })?;
        writer.finish().map_err(|e| SpeedboatBufferError {
            message: e.to_string(),
        })?;
        Ok(bytes)
    }

    pub fn stable_segment_id(
        &self,
        index: &String,
        label: &String,
    ) -> Result<String, SpeedboatBufferError> {
        let mut hasher = Sha256::new();
        hasher.update(index.as_bytes());
        hasher.update([0]);
        hasher.update(label.as_bytes());
        hasher.update([0]);
        if let Some(schema) = &self.schema {
            let schema_bytes = serde_json::to_vec(schema).map_err(|e| SpeedboatBufferError {
                message: format!("Failed to serialize speedboat schema for segment id: {e}"),
            })?;
            hasher.update(schema_bytes);
        }
        hasher.update([0xff]);
        for line in &self.lines {
            let line_bytes = serde_json::to_vec(line).map_err(|e| SpeedboatBufferError {
                message: format!("Failed to serialize speedboat line for segment id: {e}"),
            })?;
            hasher.update((line_bytes.len() as u64).to_le_bytes());
            hasher.update(line_bytes);
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    pub fn schema(&self) -> Option<PowdrrSchema> {
        self.schema.clone()
    }

    pub fn num_records(&self) -> usize {
        self.lines.len()
    }
}

fn ensure_local_parent_dir(file_name: &str) -> Result<(), SpeedboatBufferError> {
    if let Some(parent) = Path::new(file_name).parent() {
        fs::create_dir_all(parent).map_err(|e| SpeedboatBufferError {
            message: format!("Failed to create local speedboat directory: {e}"),
        })?;
    }
    Ok(())
}
