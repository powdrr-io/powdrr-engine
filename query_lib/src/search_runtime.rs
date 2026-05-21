use crate::data_access::execute_sql;
use crate::elastic_search_common::CommandError;
use crate::elastic_search_responses::{
    AggregationResult, AverageAggregationResult, CardinalityAggregationResult,
    FilterAggregationResult, HistogramAggregationResult, RangeAggregationBucket,
    RangeAggregationResult, TermAggregationBucket, TermAggregationResult,
};
use crate::schema_massager::{
    to_powdrr_schema, PowdrrDataType, PowdrrField, PowdrrSchema, SqlQuery,
};
use arrow_json::writer::LineDelimited;
use arrow_json::WriterBuilder;
use datafusion::{
    arrow::{
        array::{
            Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array,
            Int64Array, LargeListArray, LargeStringArray, ListArray, RecordBatch, StringArray,
            StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray, UInt32Array, UInt64Array,
        },
        datatypes::{DataType, TimeUnit},
    },
    prelude::DataFrame,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
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

    let parsed_json = match record_batches_to_values(record_batches) {
        Ok(values) => values,
        Err(_) => batches_to_serde_value_via_arrow_json(record_batches)?,
    };

    Ok(SerdeValueResult {
        values: parsed_json,
        schema,
    })
}

fn batches_to_serde_value_via_arrow_json(
    record_batches: &Vec<RecordBatch>,
) -> Result<Vec<Value>, CommandError> {
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
    Ok(reader
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect())
}

pub(crate) fn record_batches_to_values(
    record_batches: &[RecordBatch],
) -> Result<Vec<Value>, CommandError> {
    let mut values = Vec::new();
    for batch in record_batches {
        values.extend(record_batch_to_values(batch)?);
    }
    Ok(values)
}

pub(crate) fn record_batch_to_values(batch: &RecordBatch) -> Result<Vec<Value>, CommandError> {
    let schema = batch.schema();
    let mut values = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let mut value_map = Map::with_capacity(batch.num_columns());
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            value_map.insert(
                field.name().clone(),
                array_value_to_json(column.as_ref(), row_index)?,
            );
        }
        values.push(Value::Object(value_map));
    }
    Ok(values)
}

pub(crate) fn array_value_to_json(
    array: &dyn Array,
    row_index: usize,
) -> Result<Value, CommandError> {
    if array.is_null(row_index) {
        return Ok(Value::Null);
    }

    match array.data_type() {
        DataType::Boolean => Ok(Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean array")
                .value(row_index),
        )),
        DataType::Utf8 => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string array")
                .value(row_index)
                .to_string(),
        )),
        DataType::LargeUtf8 => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large string array")
                .value(row_index)
                .to_string(),
        )),
        DataType::Int32 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32 array")
                .value(row_index),
        )),
        DataType::Int64 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array")
                .value(row_index),
        )),
        DataType::UInt32 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("uint32 array")
                .value(row_index),
        )),
        DataType::UInt64 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("uint64 array")
                .value(row_index),
        )),
        DataType::Float32 => Ok(json_number_value(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32 array")
                .value(row_index) as f64,
        )),
        DataType::Float64 => Ok(json_number_value(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 array")
                .value(row_index),
        )),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("timestamp second array")
                .value(row_index),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("timestamp millisecond array")
                .value(row_index),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp microsecond array")
                .value(row_index),
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("timestamp nanosecond array")
                .value(row_index),
        )),
        DataType::Date32 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("date32 array")
                .value(row_index),
        )),
        DataType::Date64 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<Date64Array>()
                .expect("date64 array")
                .value(row_index),
        )),
        DataType::Struct(fields) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct array");
            let mut value_map = Map::with_capacity(fields.len());
            for (field, column) in fields.iter().zip(struct_array.columns()) {
                value_map.insert(
                    field.name().clone(),
                    array_value_to_json(column.as_ref(), row_index)?,
                );
            }
            Ok(Value::Object(value_map))
        }
        DataType::List(_) => {
            let list_array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("list array");
            list_array_to_json(list_array.value(row_index).as_ref())
        }
        DataType::LargeList(_) => {
            let list_array = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("large list array");
            list_array_to_json(list_array.value(row_index).as_ref())
        }
        unsupported => Err(CommandError {
            message: format!(
                "Direct batch JSON conversion does not support Arrow type {:?}",
                unsupported
            ),
        }),
    }
}

fn list_array_to_json(array: &dyn Array) -> Result<Value, CommandError> {
    let mut values = Vec::with_capacity(array.len());
    for row_index in 0..array.len() {
        values.push(array_value_to_json(array, row_index)?);
    }
    Ok(Value::Array(values))
}

fn json_number_value(value: f64) -> Value {
    Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
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
