use crate::data_access::execute_sql;
use crate::elastic_search_common::CommandError;
use crate::elastic_search_responses::{
    AggregationResult, AverageAggregationResult, CardinalityAggregationResult,
    FilterAggregationResult, HistogramAggregationResult, RangeAggregationBucket,
    RangeAggregationResult, TermAggregationBucket, TermAggregationResult,
};
use crate::schema_massager::{
    PowdrrDataType, PowdrrField, PowdrrSchema, SqlQuery, to_powdrr_schema,
};
use arrow_json::WriterBuilder;
use arrow_json::writer::LineDelimited;
use datafusion::{arrow::array::RecordBatch, prelude::DataFrame};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::LazyLock;

static TERM_AGG_SCHEMA: LazyLock<PowdrrSchema> = LazyLock::new(|| {
    PowdrrSchema::from(&vec![
        PowdrrField {
            name: "field_name".to_string(),
            data_type: PowdrrDataType::String,
        },
        PowdrrField {
            name: "cnt".to_string(),
            data_type: PowdrrDataType::String,
        },
    ])
});

static RANGE_AGG_SCHEMA: LazyLock<PowdrrSchema> = LazyLock::new(|| {
    PowdrrSchema::from(&vec![
        PowdrrField {
            name: "field_name".to_string(),
            data_type: PowdrrDataType::String,
        },
        PowdrrField {
            name: "cnt".to_string(),
            data_type: PowdrrDataType::String,
        },
    ])
});

static AVERAGE_AGG_SCHEMA: LazyLock<PowdrrSchema> = LazyLock::new(|| {
    PowdrrSchema::from(&vec![
        PowdrrField {
            name: "field_name".to_string(),
            data_type: PowdrrDataType::String,
        },
        PowdrrField {
            name: "cnt".to_string(),
            data_type: PowdrrDataType::String,
        },
    ])
});

static CARDINALITY_AGG_SCHEMA: LazyLock<PowdrrSchema> = LazyLock::new(|| {
    PowdrrSchema::from(&vec![
        PowdrrField {
            name: "field_name".to_string(),
            data_type: PowdrrDataType::String,
        },
        PowdrrField {
            name: "cnt".to_string(),
            data_type: PowdrrDataType::String,
        },
    ])
});

static FILTER_AGG_SCHEMA: LazyLock<PowdrrSchema> = LazyLock::new(|| {
    PowdrrSchema::from(&vec![
        PowdrrField {
            name: "field_name".to_string(),
            data_type: PowdrrDataType::String,
        },
        PowdrrField {
            name: "cnt".to_string(),
            data_type: PowdrrDataType::String,
        },
    ])
});

#[derive(Clone, Serialize, Deserialize)]
pub struct ScriptBlock {
    pub source: String,
    pub lang: String,
    #[serde(default)]
    pub params: Value,
}

pub struct SerdeValueResult {
    pub values: Vec<Value>,
    pub schema: Option<PowdrrSchema>,
}

pub async fn df_to_serde_value(data_frame: &DataFrame) -> Result<SerdeValueResult, CommandError> {
    let record_batches: Vec<RecordBatch> = match data_frame.clone().collect().await {
        Ok(b) => b,
        Err(e) => {
            return Err(CommandError {
                message: e.to_string(),
            });
        }
    };

    batches_to_serde_value(&record_batches).await
}

pub async fn batches_to_serde_value(
    record_batches: &Vec<RecordBatch>,
) -> Result<SerdeValueResult, CommandError> {
    let schema = match record_batches.len() {
        0 => None,
        _ => Some(to_powdrr_schema(&record_batches.get(0).unwrap().schema())),
    };

    let record_batch_references: Vec<&RecordBatch> = record_batches.iter().collect();

    let buf = Vec::new();
    let builder = WriterBuilder::new().with_explicit_nulls(true);
    let mut writer = builder.build::<_, LineDelimited>(buf);
    writer
        .write_batches(record_batch_references.as_slice())
        .unwrap();
    writer.finish().unwrap();

    let buf = writer.into_inner();
    let reader = String::from_utf8(buf).unwrap();
    let parsed_json: Vec<Value> = reader
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();

    Ok(SerdeValueResult {
        values: parsed_json,
        schema,
    })
}

#[derive(Clone)]
pub(crate) struct TermAggProcessor {
    pub(crate) sql: SqlQuery,
}

impl TermAggProcessor {
    fn create_aggregation_bucket(value: &Value) -> TermAggregationBucket {
        let value_map = value.as_object().unwrap();
        let key = match value_map.get("field_name") {
            Some(v) => {
                if v.is_string() {
                    v.as_str().unwrap()
                } else if v.is_null() {
                    "null"
                } else {
                    panic!("nope")
                }
            }
            None => {
                let value_str = serde_json::to_string(&value).unwrap();
                println!("value_str: {}", value_str);
                panic!("nope")
            }
        };
        let doc_count = value_map.get("cnt").unwrap().as_u64().unwrap();

        TermAggregationBucket {
            key: key.to_string(),
            doc_count,
            aggs: Default::default(),
        }
    }

    async fn create_buckets(
        schema: Option<PowdrrSchema>,
        table_name: &String,
        query: &SqlQuery,
    ) -> Result<Vec<TermAggregationBucket>, CommandError> {
        let final_sql = query
            .build_same(&schema.unwrap_or_else(|| TERM_AGG_SCHEMA.clone()))
            .replace("{target_table}", table_name);
        let data_frame = match execute_sql(&final_sql).await {
            Ok(df) => df,
            Err(_) => panic!("nope"),
        };

        assert_eq!(data_frame.schema().columns().len(), 2);

        let serde_result = df_to_serde_value(&data_frame).await?;

        Ok(serde_result
            .values
            .iter()
            .map(TermAggProcessor::create_aggregation_bucket)
            .collect())
    }

    async fn process(
        &self,
        schema: Option<PowdrrSchema>,
        table_name: Option<String>,
        subaggregations: Option<Vec<Aggregation>>,
    ) -> Result<AggregationResult, CommandError> {
        let child_aggs =
            process_aggregations(schema.clone(), subaggregations, table_name.clone()).await?;

        let buckets = match &table_name {
            Some(t) => TermAggProcessor::create_buckets(schema.clone(), t, &self.sql).await?,
            None => vec![],
        };

        Ok(AggregationResult::Terms(TermAggregationResult {
            doc_count_error_upper_bound: 0,
            sum_other_doc_count: 0,
            buckets,
            aggs: child_aggs,
        }))
    }
}

#[derive(Clone)]
pub(crate) struct RangeAggBucket {
    pub(crate) sql: SqlQuery,
    pub(crate) key: String,
    pub(crate) from: u64,
    pub(crate) from_as_string: String,
    pub(crate) to: u64,
    pub(crate) to_as_string: String,
    pub(crate) subaggregations: Option<Vec<Aggregation>>,
}

#[derive(Clone)]
pub(crate) struct RangeAggProcessor {
    pub(crate) buckets: Vec<RangeAggBucket>,
}

impl RangeAggProcessor {
    async fn create_aggregation_bucket(
        schema: Option<PowdrrSchema>,
        bucket_spec: &RangeAggBucket,
        table_name: Option<String>,
    ) -> Result<RangeAggregationBucket, CommandError> {
        let child_aggs = process_aggregations(
            schema.clone(),
            bucket_spec.subaggregations.clone(),
            table_name.clone(),
        )
        .await?;

        let doc_count = match &table_name {
            Some(t) => {
                let final_sql = bucket_spec
                    .sql
                    .build_same(&schema.unwrap_or_else(|| RANGE_AGG_SCHEMA.clone()))
                    .replace("{target_table}", t);
                let data_frame = match execute_sql(&final_sql).await {
                    Ok(df) => df,
                    Err(_) => panic!("nope"),
                };

                assert_eq!(data_frame.schema().columns().len(), 1);

                let serde_result = df_to_serde_value(&data_frame).await?;

                serde_result
                    .values
                    .get(0)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .get("cnt")
                    .unwrap()
                    .as_u64()
                    .unwrap()
            }
            None => 0,
        };

        Ok(RangeAggregationBucket {
            key: bucket_spec.key.clone(),
            from: bucket_spec.from,
            from_as_string: bucket_spec.from_as_string.clone(),
            to: bucket_spec.to,
            to_as_string: bucket_spec.to_as_string.clone(),
            doc_count,
            aggs: child_aggs,
        })
    }

    async fn create_buckets(
        &self,
        schema: Option<PowdrrSchema>,
        table_name: Option<String>,
    ) -> Result<Vec<RangeAggregationBucket>, CommandError> {
        let mut buckets = vec![];
        for bucket_spec in self.buckets.iter() {
            buckets.push(
                RangeAggProcessor::create_aggregation_bucket(
                    schema.clone(),
                    bucket_spec,
                    table_name.clone(),
                )
                .await?,
            );
        }
        Ok(buckets)
    }

    async fn process(
        &self,
        schema: Option<PowdrrSchema>,
        table_name: Option<String>,
        subaggregations: Option<Vec<Aggregation>>,
    ) -> Result<AggregationResult, CommandError> {
        assert!(subaggregations.is_none());

        let buckets = self.create_buckets(schema, table_name).await?;

        Ok(AggregationResult::Range(RangeAggregationResult { buckets }))
    }
}

#[derive(Clone)]
pub(crate) struct AverageAggProcessor {
    pub(crate) sql: SqlQuery,
}

impl AverageAggProcessor {
    async fn calculate_average(table_name: &String, query: &String) -> Result<f64, CommandError> {
        let final_sql = query.replace("{target_table}", table_name);
        let data_frame = match execute_sql(&final_sql).await {
            Ok(df) => df,
            Err(e) => {
                return Err(CommandError {
                    message: format!("{}", e),
                });
            }
        };

        assert_eq!(data_frame.schema().columns().len(), 1);

        let serde_result = df_to_serde_value(&data_frame).await?;

        Ok(serde_result
            .values
            .get(0)
            .unwrap()
            .as_object()
            .unwrap()
            .get("avg")
            .unwrap()
            .as_f64()
            .unwrap())
    }

    async fn process(
        &self,
        schema: Option<PowdrrSchema>,
        table_name: Option<String>,
        subaggregations: Option<Vec<Aggregation>>,
    ) -> Result<AggregationResult, CommandError> {
        let child_aggs =
            process_aggregations(schema.clone(), subaggregations, table_name.clone()).await?;

        let avg = match &table_name {
            Some(t) => {
                AverageAggProcessor::calculate_average(
                    t,
                    &self
                        .sql
                        .build_same(&schema.unwrap_or_else(|| AVERAGE_AGG_SCHEMA.clone())),
                )
                .await?
            }
            None => 0.0,
        };

        Ok(AggregationResult::Average(AverageAggregationResult {
            value: avg,
            aggs: child_aggs,
        }))
    }
}

#[derive(Clone)]
pub(crate) struct CardinalityAggProcessor {
    pub(crate) sql: SqlQuery,
}

impl CardinalityAggProcessor {
    async fn calculate_cardinality(
        table_name: &String,
        query: &String,
    ) -> Result<u64, CommandError> {
        let final_sql = query.replace("{target_table}", table_name);
        let data_frame = match execute_sql(&final_sql).await {
            Ok(df) => df,
            Err(e) => {
                return Err(CommandError {
                    message: format!("{}", e),
                });
            }
        };

        assert_eq!(data_frame.schema().columns().len(), 1);

        let serde_result = df_to_serde_value(&data_frame).await?;

        Ok(serde_result
            .values
            .get(0)
            .unwrap()
            .as_object()
            .unwrap()
            .get("type_count")
            .unwrap()
            .as_u64()
            .unwrap())
    }

    async fn process(
        &self,
        schema: Option<PowdrrSchema>,
        table_name: Option<String>,
        subaggregations: Option<Vec<Aggregation>>,
    ) -> Result<AggregationResult, CommandError> {
        let child_aggs =
            process_aggregations(schema.clone(), subaggregations, table_name.clone()).await?;

        let value = match &table_name {
            Some(t) => {
                CardinalityAggProcessor::calculate_cardinality(
                    t,
                    &self
                        .sql
                        .build_same(&schema.unwrap_or_else(|| CARDINALITY_AGG_SCHEMA.clone())),
                )
                .await?
            }
            None => 0,
        };
        Ok(AggregationResult::Cardinality(
            CardinalityAggregationResult {
                value,
                aggs: child_aggs,
            },
        ))
    }
}

#[derive(Clone)]
pub(crate) struct DateHistogramAggBucket {
    #[allow(dead_code)]
    pub(crate) subaggregations: Option<Vec<Aggregation>>,
}

#[derive(Clone)]
pub(crate) struct DateHistogramAggProcessor {
    #[allow(dead_code)]
    pub(crate) buckets: Vec<DateHistogramAggBucket>,
}

impl DateHistogramAggProcessor {
    async fn process(
        &self,
        _schema: Option<PowdrrSchema>,
        _table_name: Option<String>,
        subaggregations: Option<Vec<Aggregation>>,
    ) -> Result<AggregationResult, CommandError> {
        assert!(subaggregations.is_none());
        Ok(AggregationResult::Histogram(HistogramAggregationResult {
            buckets: vec![],
        }))
    }
}

#[derive(Clone)]
pub(crate) struct FilterAggProcessor {
    pub(crate) sql: SqlQuery,
}

impl FilterAggProcessor {
    async fn process(
        &self,
        schema: Option<PowdrrSchema>,
        table_name: Option<String>,
        subaggregations: Option<Vec<Aggregation>>,
    ) -> Result<AggregationResult, CommandError> {
        let doc_count = match &table_name {
            Some(t) => {
                let final_sql = self
                    .sql
                    .build_same(&schema.clone().unwrap_or_else(|| FILTER_AGG_SCHEMA.clone()))
                    .replace("{target_table}", t);
                let data_frame = execute_sql(&final_sql).await.unwrap();
                assert_eq!(data_frame.schema().columns().len(), 1);
                let serde_result = df_to_serde_value(&data_frame).await?;
                serde_result
                    .values
                    .get(0)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .get("cnt")
                    .unwrap()
                    .as_u64()
                    .unwrap()
            }
            None => 0,
        };
        let child_aggs =
            process_aggregations(schema.clone(), subaggregations, table_name.clone()).await?;

        Ok(AggregationResult::Filter(FilterAggregationResult {
            doc_count,
            aggs: child_aggs,
        }))
    }
}

#[derive(Clone)]
pub(crate) struct MissingAggProcessor {}

impl MissingAggProcessor {
    async fn process(
        &self,
        schema: Option<PowdrrSchema>,
        table_name: Option<String>,
        subaggregations: Option<Vec<Aggregation>>,
    ) -> Result<AggregationResult, CommandError> {
        let child_aggs = process_aggregations(schema.clone(), subaggregations, table_name).await?;

        Ok(AggregationResult::Filter(FilterAggregationResult {
            doc_count: 0,
            aggs: child_aggs,
        }))
    }
}

#[derive(Clone)]
pub(crate) enum AggProcessor {
    Average(AverageAggProcessor),
    Cardinality(CardinalityAggProcessor),
    DateHistogram(DateHistogramAggProcessor),
    Filter(FilterAggProcessor),
    Missing(MissingAggProcessor),
    Range(RangeAggProcessor),
    Term(TermAggProcessor),
}

#[derive(Clone)]
pub struct Aggregation {
    pub(crate) name: String,
    pub(crate) processor: AggProcessor,
    pub(crate) subaggregations: Option<Vec<Aggregation>>,
}

pub(crate) async fn process_aggregation(
    schema: Option<PowdrrSchema>,
    aggregation: &Aggregation,
    table_name: Option<String>,
) -> Result<AggregationResult, CommandError> {
    match &aggregation.processor {
        AggProcessor::Average(average) => {
            average
                .process(schema, table_name, aggregation.subaggregations.clone())
                .await
        }
        AggProcessor::Cardinality(cardinality) => {
            cardinality
                .process(schema, table_name, aggregation.subaggregations.clone())
                .await
        }
        AggProcessor::DateHistogram(date_histogram) => {
            date_histogram
                .process(schema, table_name, aggregation.subaggregations.clone())
                .await
        }
        AggProcessor::Filter(filter) => {
            filter
                .process(schema, table_name, aggregation.subaggregations.clone())
                .await
        }
        AggProcessor::Missing(missing) => {
            missing
                .process(schema, table_name, aggregation.subaggregations.clone())
                .await
        }
        AggProcessor::Range(range) => {
            range
                .process(schema, table_name, aggregation.subaggregations.clone())
                .await
        }
        AggProcessor::Term(term) => {
            term.process(schema, table_name, aggregation.subaggregations.clone())
                .await
        }
    }
}

pub(crate) async fn process_aggregations(
    schema: Option<PowdrrSchema>,
    aggregations: Option<Vec<Aggregation>>,
    table_name: Option<String>,
) -> Result<HashMap<String, AggregationResult>, CommandError> {
    let mut results = HashMap::new();
    if let Some(aggregations) = aggregations {
        for aggregation in aggregations {
            results.insert(
                aggregation.name.clone(),
                Box::pin(process_aggregation(
                    schema.clone(),
                    &aggregation,
                    table_name.clone(),
                ))
                .await?,
            );
        }
    }
    Ok(results)
}
