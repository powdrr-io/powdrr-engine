use datafusion::dataframe::DataFrame;
use crate::elastic_search_parser::AggSpec;
use crate::elastic_search_responses::AggregationBucket;

pub(crate) fn to_aggregation_response(_input_data: DataFrame, _agg_spec: &AggSpec) -> Vec<AggregationBucket> {
    vec!()
}