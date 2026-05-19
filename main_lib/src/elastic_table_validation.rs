use crate::data_contract::FileDescriptor;
use crate::schema_massager::PowdrrDataType;
use serde::Serialize;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ElasticTableValidation {
    pub file_count: usize,
    pub doc_id_field: String,
    pub doc_id_type: PowdrrDataType,
    pub indexed_string_fields: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct ElasticTableValidationError {
    message: String,
}

impl ElasticTableValidationError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for ElasticTableValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl Error for ElasticTableValidationError {}

pub(crate) fn validate_elastic_table_files(
    file_descriptors: &[FileDescriptor],
    doc_id_field: &str,
) -> Result<ElasticTableValidation, ElasticTableValidationError> {
    validate_doc_id_field_name(doc_id_field)?;

    let mut doc_id_type = None;
    let mut indexed_string_fields = BTreeSet::new();

    for file_descriptor in file_descriptors.iter() {
        let doc_id = file_descriptor
            .schema
            .fields()
            .iter()
            .find(|field| field.name == doc_id_field)
            .ok_or_else(|| {
                ElasticTableValidationError::new(format!(
                    "File {} is missing doc id field {}",
                    file_descriptor.file_path, doc_id_field
                ))
            })?;

        if !is_supported_doc_id_type(&doc_id.data_type) {
            return Err(ElasticTableValidationError::new(format!(
                "File {} has unsupported doc id type {:?} for field {}. Elastic serving currently requires a stable scalar id column.",
                file_descriptor.file_path, doc_id.data_type, doc_id_field
            )));
        }

        match &doc_id_type {
            Some(existing) if existing != &doc_id.data_type => {
                return Err(ElasticTableValidationError::new(format!(
                    "File {} has doc id field {} with type {:?}, but other files use {:?}. Elastic serving currently requires the doc id type to match across every file in the table.",
                    file_descriptor.file_path,
                    doc_id_field,
                    doc_id.data_type,
                    existing
                )));
            }
            Some(_) => {}
            None => {
                doc_id_type = Some(doc_id.data_type.clone());
            }
        }

        let file_indexed_fields = file_descriptor
            .schema
            .fields()
            .iter()
            .filter(|field| field.name != doc_id_field)
            .filter(|field| matches!(field.data_type, PowdrrDataType::String))
            .map(|field| field.name.clone())
            .collect::<Vec<String>>();

        if file_indexed_fields.is_empty() {
            return Err(ElasticTableValidationError::new(format!(
                "File {} has no searchable top-level string columns besides {}. Elastic serving currently builds one _search_index.parquet sidecar per data file, so every file must expose at least one additional top-level string column.",
                file_descriptor.file_path, doc_id_field
            )));
        }

        indexed_string_fields.extend(file_indexed_fields);
    }

    let doc_id_type = doc_id_type.ok_or_else(|| {
        ElasticTableValidationError::new(
            "Elastic table validation requires at least one data file to inspect",
        )
    })?;

    Ok(ElasticTableValidation {
        file_count: file_descriptors.len(),
        doc_id_field: doc_id_field.to_string(),
        doc_id_type,
        indexed_string_fields: indexed_string_fields.into_iter().collect(),
    })
}

fn validate_doc_id_field_name(doc_id_field: &str) -> Result<(), ElasticTableValidationError> {
    let mut chars = doc_id_field.chars();
    let Some(first) = chars.next() else {
        return Err(ElasticTableValidationError::new(
            "Doc id field name cannot be empty",
        ));
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(ElasticTableValidationError::new(format!(
            "Doc id field {} is not supported. Elastic index generation currently requires a SQL identifier that starts with a letter or underscore and only contains ASCII letters, numbers, and underscores.",
            doc_id_field
        )));
    }

    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(ElasticTableValidationError::new(format!(
            "Doc id field {} is not supported. Elastic index generation currently requires a SQL identifier that starts with a letter or underscore and only contains ASCII letters, numbers, and underscores.",
            doc_id_field
        )));
    }

    Ok(())
}

fn is_supported_doc_id_type(data_type: &PowdrrDataType) -> bool {
    matches!(
        data_type,
        PowdrrDataType::Boolean
            | PowdrrDataType::Float
            | PowdrrDataType::Integer
            | PowdrrDataType::String
    )
}

#[cfg(test)]
mod tests {
    use super::validate_elastic_table_files;
    use crate::data_contract::FileDescriptor;
    use crate::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema};

    fn descriptor(file_path: &str, fields: Vec<PowdrrField>) -> FileDescriptor {
        FileDescriptor {
            file_path: file_path.to_string(),
            schema: PowdrrSchema::from(&fields),
            size: 128,
        }
    }

    #[test]
    fn validates_table_with_consistent_doc_ids_and_searchable_fields() {
        let report = validate_elastic_table_files(
            &vec![
                descriptor(
                    "file:///tmp/part-1.parquet",
                    vec![
                        PowdrrField {
                            name: "_id_seq_no".to_string(),
                            data_type: PowdrrDataType::String,
                        },
                        PowdrrField {
                            name: "message".to_string(),
                            data_type: PowdrrDataType::String,
                        },
                    ],
                ),
                descriptor(
                    "file:///tmp/part-2.parquet",
                    vec![
                        PowdrrField {
                            name: "_id_seq_no".to_string(),
                            data_type: PowdrrDataType::String,
                        },
                        PowdrrField {
                            name: "service".to_string(),
                            data_type: PowdrrDataType::String,
                        },
                    ],
                ),
            ],
            "_id_seq_no",
        )
        .unwrap();

        assert_eq!(report.file_count, 2);
        assert_eq!(report.doc_id_type, PowdrrDataType::String);
        assert_eq!(
            report.indexed_string_fields,
            vec!["message".to_string(), "service".to_string()]
        );
    }

    #[test]
    fn errors_when_doc_id_field_name_is_not_sql_safe() {
        let error = validate_elastic_table_files(
            &vec![descriptor(
                "file:///tmp/part-1.parquet",
                vec![
                    PowdrrField {
                        name: "doc.id".to_string(),
                        data_type: PowdrrDataType::String,
                    },
                    PowdrrField {
                        name: "message".to_string(),
                        data_type: PowdrrDataType::String,
                    },
                ],
            )],
            "doc.id",
        )
        .unwrap_err();

        assert!(error.to_string().contains("SQL identifier"));
    }

    #[test]
    fn errors_when_doc_id_field_is_missing() {
        let error = validate_elastic_table_files(
            &vec![descriptor(
                "file:///tmp/part-1.parquet",
                vec![PowdrrField {
                    name: "message".to_string(),
                    data_type: PowdrrDataType::String,
                }],
            )],
            "_id_seq_no",
        )
        .unwrap_err();

        assert!(error.to_string().contains("missing doc id field"));
    }

    #[test]
    fn errors_when_doc_id_type_differs_across_files() {
        let error = validate_elastic_table_files(
            &vec![
                descriptor(
                    "file:///tmp/part-1.parquet",
                    vec![
                        PowdrrField {
                            name: "_id_seq_no".to_string(),
                            data_type: PowdrrDataType::String,
                        },
                        PowdrrField {
                            name: "message".to_string(),
                            data_type: PowdrrDataType::String,
                        },
                    ],
                ),
                descriptor(
                    "file:///tmp/part-2.parquet",
                    vec![
                        PowdrrField {
                            name: "_id_seq_no".to_string(),
                            data_type: PowdrrDataType::Integer,
                        },
                        PowdrrField {
                            name: "service".to_string(),
                            data_type: PowdrrDataType::String,
                        },
                    ],
                ),
            ],
            "_id_seq_no",
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("type to match across every file"));
    }

    #[test]
    fn errors_when_file_has_no_searchable_string_columns() {
        let error = validate_elastic_table_files(
            &vec![descriptor(
                "file:///tmp/part-1.parquet",
                vec![
                    PowdrrField {
                        name: "_id_seq_no".to_string(),
                        data_type: PowdrrDataType::String,
                    },
                    PowdrrField {
                        name: "count".to_string(),
                        data_type: PowdrrDataType::Integer,
                    },
                ],
            )],
            "_id_seq_no",
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("no searchable top-level string columns"));
    }
}
