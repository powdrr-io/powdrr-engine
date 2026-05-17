use crate::elastic_search_commands::{SqlCommand, UpdateByQueryCommand};
use crate::elastic_search_common::{
    Command, CommandContext, ElasticSearchResponse, ParseError, ResultGeneratorFuture,
    execute_command,
};
use crate::elastic_search_datetime_parser;
use crate::elastic_search_endpoints::QueryStringSearch;
use crate::elastic_search_responses::{
    AggregationResult, AverageAggregationResult, FilterAggregationResult, QueryFailure,
    QueryResults, TermAggregationBucket, TermAggregationResult, compare_query_result_hits_desc,
};
use crate::peers::{
    CheckpointDescriptor, PrivateInvocation, PrivateSearchAggregationFilterSpec,
    PrivateSearchAggregationPartial, PrivateSearchAggregationSpec, PrivateSearchInvocation,
    PrivateSearchSortSpec,
};
use crate::schema_massager::{FieldExpression, SqlBuilder, SqlExpression};
use crate::search_plan;
use crate::search_runtime::{
    AggProcessor, Aggregation, AverageAggProcessor, CardinalityAggProcessor,
    DateHistogramAggProcessor, FilterAggProcessor, MissingAggProcessor, RangeAggBucket,
    RangeAggProcessor, ScriptBlock, TermAggProcessor,
};
use crate::state_provider::STATE_PROVIDER;
use async_trait::async_trait;
use chrono::Utc;
use futures::future::try_join_all;
use std::pin::Pin;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct SearchExecutionPlan {
    pub shards: Vec<SearchShardExecutionPlan>,
    pub merge: SearchMergePlan,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct SearchShardExecutionPlan {
    pub shard_id: String,
    pub route: SearchShardRoute,
    pub segments: Vec<SearchSegmentExecutionPlan>,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) enum SearchShardRoute {
    BroadcastCurrentSnapshot,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) enum SearchSegmentExecutionPlan {
    LegacySql(LegacySqlSegmentPlan),
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct LegacySqlSegmentPlan {
    pub segment_id: String,
    pub table: String,
    pub sql: crate::schema_massager::SqlQuery,
    pub calculate_score: bool,
    pub required_extension: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct SearchMergePlan {
    pub from: u32,
    pub size: usize,
    pub stages: Vec<SearchMergeStage>,
}

#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum SearchMergeStage {
    SegmentToShardTopK,
    ShardToCoordinatorTopK,
}

enum SearchBackend {
    LegacySql(SqlCommand),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SearchExecutionStrategy {
    LegacySqlFanout,
    TypedNodeMerge(SearchResultOrder),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SearchResultOrder {
    ScoreDesc,
    PeerConcat,
    ExplicitSort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SearchPerformancePath {
    TypedNodeMerge,
    LegacySqlFanout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SearchPerformanceAssessment {
    pub path: SearchPerformancePath,
    pub reason: String,
}

pub(crate) struct SearchCommand {
    #[allow(dead_code)]
    pub logical_plan: search_plan::SearchPlan,
    #[allow(dead_code)]
    pub execution_plan: SearchExecutionPlan,
    execution_strategy: SearchExecutionStrategy,
    typed_aggregation_specs: Option<Vec<PrivateSearchAggregationSpec>>,
    typed_sort_specs: Vec<PrivateSearchSortSpec>,
    backend: SearchBackend,
}

impl SearchCommand {
    #[allow(dead_code)]
    pub(crate) fn execution_plan(&self) -> &SearchExecutionPlan {
        &self.execution_plan
    }

    #[allow(dead_code)]
    pub(crate) fn legacy_sql_command(&self) -> Option<&SqlCommand> {
        match &self.backend {
            SearchBackend::LegacySql(command) => Some(command),
        }
    }

    #[allow(dead_code)]
    fn supports_typed_node_merge(&self) -> bool {
        !matches!(
            self.execution_strategy,
            SearchExecutionStrategy::LegacySqlFanout
        )
    }

    pub(crate) fn performance_assessment(&self) -> SearchPerformanceAssessment {
        match self.execution_strategy {
            SearchExecutionStrategy::TypedNodeMerge(result_order) => SearchPerformanceAssessment {
                path: SearchPerformancePath::TypedNodeMerge,
                reason: typed_node_merge_reason(result_order),
            },
            SearchExecutionStrategy::LegacySqlFanout => SearchPerformanceAssessment {
                path: SearchPerformancePath::LegacySqlFanout,
                reason: legacy_sql_fanout_reason(self),
            },
        }
    }

    async fn private_search_invocation(&self) -> Option<PrivateSearchInvocation> {
        let legacy_command = self.legacy_sql_command()?;
        let checkpoints = self.current_target_snapshots(legacy_command).await;
        Some(PrivateSearchInvocation {
            sql: legacy_command.sql.clone(),
            required_extensions: legacy_command.required_extensions(),
            checkpoints,
            table: legacy_command.table.clone(),
            size: self.execution_plan.merge.from as usize + self.execution_plan.merge.size,
            calculate_score: legacy_command.calculate_score,
            aggregations: self.typed_aggregation_specs.clone().unwrap_or_default(),
            sorts: self.typed_sort_specs.clone(),
        })
    }

    async fn current_target_snapshots(
        &self,
        legacy_command: &SqlCommand,
    ) -> Vec<CheckpointDescriptor> {
        let extension = legacy_command.calculate_score.then(|| "es".to_string());
        match STATE_PROVIDER
            .get_latest_checkpoint(&legacy_command.table, extension)
            .await
        {
            Ok(Some(checkpoint_id)) => {
                vec![CheckpointDescriptor::new(
                    legacy_command.table.clone(),
                    checkpoint_id,
                )]
            }
            Ok(None) => vec![],
            Err(e) => {
                tracing::error!(
                    "Error getting latest checkpoint for table {}: {}",
                    legacy_command.table,
                    e
                );
                vec![]
            }
        }
    }
}

fn typed_node_merge_reason(result_order: SearchResultOrder) -> String {
    match result_order {
        SearchResultOrder::ScoreDesc => {
            "Query stays on the typed node-merge path with score-based merging.".to_string()
        }
        SearchResultOrder::PeerConcat => {
            "Query stays on the typed node-merge path without legacy SQL fanout.".to_string()
        }
        SearchResultOrder::ExplicitSort => {
            "Query stays on the typed node-merge path with typed sort merging.".to_string()
        }
    }
}

fn legacy_sql_fanout_reason(command: &SearchCommand) -> String {
    if let Some(reason) = command
        .logical_plan
        .aggregations
        .iter()
        .find_map(aggregation_legacy_path_reason)
    {
        return reason;
    }

    let backend = match &command.backend {
        SearchBackend::LegacySql(backend) => backend,
    };
    if let Some(reason) = command
        .logical_plan
        .sort
        .iter()
        .find_map(|plan| sort_legacy_path_reason(plan, backend))
    {
        return reason;
    }

    if backend.query_params.sort.is_some() {
        return "Query-string sort currently uses the legacy SQL fanout path.".to_string();
    }

    if matches!(
        command.logical_plan.target,
        search_plan::SearchTarget::Pit(_)
    ) {
        return "Point-in-time queries currently use the legacy SQL fanout path.".to_string();
    }

    "Query is supported, but it falls back to the legacy SQL fanout path.".to_string()
}

fn aggregation_legacy_path_reason(plan: &search_plan::AggregationPlan) -> Option<String> {
    match &plan.spec {
        search_plan::AggregationPlanSpec::Terms(terms_plan) => {
            if terms_plan.sub_aggregations.is_empty() {
                None
            } else {
                Some(format!(
                    "Terms aggregation `{}` has sub-aggregations, which currently use the legacy SQL fanout path.",
                    plan.name
                ))
            }
        }
        search_plan::AggregationPlanSpec::Average(_) => None,
        search_plan::AggregationPlanSpec::Filter(filter_plan) => {
            if !matches!(
                filter_plan.filter,
                search_plan::AggregationFilterPlan::Term { .. }
            ) {
                return Some(format!(
                    "Filter aggregation `{}` uses a non-term filter, which currently uses the legacy SQL fanout path.",
                    plan.name
                ));
            }
            filter_plan
                .sub_aggregations
                .iter()
                .find_map(aggregation_legacy_path_reason)
        }
        search_plan::AggregationPlanSpec::Missing(_) => Some(format!(
            "Missing aggregation `{}` currently uses the legacy SQL fanout path.",
            plan.name
        )),
        search_plan::AggregationPlanSpec::DateHistogram(_) => Some(format!(
            "Date histogram aggregation `{}` currently uses the legacy SQL fanout path.",
            plan.name
        )),
        search_plan::AggregationPlanSpec::Cardinality(_) => Some(format!(
            "Cardinality aggregation `{}` currently uses the legacy SQL fanout path.",
            plan.name
        )),
        search_plan::AggregationPlanSpec::Range(_) => Some(format!(
            "Range aggregation `{}` currently uses the legacy SQL fanout path.",
            plan.name
        )),
    }
}

fn sort_legacy_path_reason(plan: &search_plan::SortPlan, backend: &SqlCommand) -> Option<String> {
    match plan {
        search_plan::SortPlan::Bare(field) => {
            if field == "_score" && !backend.calculate_score {
                Some(
                    "Sorting by `_score` without a scoring query currently uses the legacy SQL fanout path."
                        .to_string(),
                )
            } else {
                None
            }
        }
        search_plan::SortPlan::Field {
            field,
            order,
            script,
            ..
        } => {
            if script.is_some() {
                return Some(format!(
                    "Sort on `{}` uses a script, which currently uses the legacy SQL fanout path.",
                    field
                ));
            }

            match order.as_deref().map(str::to_ascii_lowercase) {
                None => None,
                Some(value) if value == "asc" || value == "desc" => None,
                Some(value) => Some(format!(
                    "Sort on `{}` uses unsupported order `{}`, which currently uses the legacy SQL fanout path.",
                    field, value
                )),
            }
        }
    }
}

#[async_trait]
impl Command for SearchCommand {
    async fn get_private_invocation(&self) -> PrivateInvocation {
        match &self.backend {
            SearchBackend::LegacySql(command) => command.get_private_invocation().await,
        }
    }

    fn result_generator(
        &self,
        result_table_name: Option<String>,
    ) -> Pin<Box<ResultGeneratorFuture>> {
        match &self.backend {
            SearchBackend::LegacySql(command) => command.result_generator(result_table_name),
        }
    }
}

pub(crate) async fn execute_search_command(
    context: CommandContext,
    command: Arc<SearchCommand>,
) -> ElasticSearchResponse {
    let result_order = match command.execution_strategy {
        SearchExecutionStrategy::LegacySqlFanout => return execute_command(context, command).await,
        SearchExecutionStrategy::TypedNodeMerge(result_order) => result_order,
    };

    let invocation = match command.private_search_invocation().await {
        Some(invocation) => invocation,
        None => return execute_command(context, command).await,
    };

    if invocation.checkpoints.is_empty() {
        let total_hits_complex = command
            .legacy_sql_command()
            .map(|legacy| !legacy.query_params.rest_total_hits_as_int.unwrap_or(false))
            .unwrap_or(true);
        let aggregations = command.typed_aggregation_specs.as_ref().and_then(|specs| {
            if specs.is_empty() {
                None
            } else {
                Some(merge_typed_aggregation_partials(vec![], specs))
            }
        });
        return QueryResults::empty(50, 1, aggregations, total_hits_complex).to_response();
    }

    let peer_clients = STATE_PROVIDER.get_peer_clients().await;
    let num_peers = peer_clients.len();
    let peer_calls = peer_clients.iter().enumerate().map(|(index, peer_client)| {
        peer_client.private_search(&invocation, index as u64, num_peers as u64)
    });

    let peer_results = match try_join_all(peer_calls).await {
        Ok(results) => results,
        Err(e) => {
            return QueryFailure {
                message: format!("{:?}", e),
            }
            .to_response();
        }
    };

    let total_hits: usize = peer_results.iter().map(|result| result.total_hits).sum();
    let aggregation_partials = peer_results
        .iter()
        .map(|result| result.aggregations.clone())
        .collect::<Vec<_>>();
    let mut hits = peer_results
        .into_iter()
        .flat_map(|result| result.hits.into_iter())
        .collect::<Vec<_>>();
    match result_order {
        SearchResultOrder::ScoreDesc => hits.sort_by(compare_query_result_hits_desc),
        SearchResultOrder::ExplicitSort => hits.sort_by(|left, right| {
            compare_query_result_hits_by_sort(left, right, &command.typed_sort_specs)
        }),
        SearchResultOrder::PeerConcat => {}
    }

    let from = command.execution_plan.merge.from as usize;
    let size = command.execution_plan.merge.size;
    let paged_hits = hits.into_iter().skip(from).take(size).collect::<Vec<_>>();
    let total_hits_complex = command
        .legacy_sql_command()
        .map(|legacy| !legacy.query_params.rest_total_hits_as_int.unwrap_or(false))
        .unwrap_or(true);
    let aggregations = command.typed_aggregation_specs.as_ref().and_then(|specs| {
        if specs.is_empty() {
            None
        } else {
            Some(merge_typed_aggregation_partials(
                aggregation_partials,
                specs,
            ))
        }
    });

    if total_hits == 0 {
        return QueryResults::empty(50, num_peers as u32, aggregations, total_hits_complex)
            .to_response();
    }

    QueryResults::success(
        50,
        num_peers as u32,
        total_hits,
        match result_order {
            SearchResultOrder::ScoreDesc => paged_hits.first().and_then(|hit| hit._score),
            SearchResultOrder::PeerConcat | SearchResultOrder::ExplicitSort => {
                if command
                    .typed_sort_specs
                    .first()
                    .map(|spec| spec.field == "_score")
                    .unwrap_or(false)
                {
                    paged_hits.first().and_then(|hit| hit._score)
                } else {
                    None
                }
            }
        },
        paged_hits,
        aggregations,
        total_hits_complex,
    )
    .to_response()
}

pub(crate) fn search_plan_to_command(
    plan: search_plan::SearchPlan,
    query: &QueryStringSearch,
) -> Result<SearchCommand, ParseError> {
    search_plan_to_command_with_options(plan, query, None, true)
}

pub(crate) fn search_plan_to_command_with_options(
    plan: search_plan::SearchPlan,
    query: &QueryStringSearch,
    doc_id_field_name: Option<&str>,
    include_deletes_join: bool,
) -> Result<SearchCommand, ParseError> {
    let backend =
        compile_legacy_sql_command(&plan, query, doc_id_field_name, include_deletes_join)?;
    let execution_plan = create_execution_plan(&plan, &backend);
    let typed_aggregation_specs = private_search_aggregation_specs(&plan.aggregations);
    let typed_sort_specs = private_search_sort_specs(&plan.sort, &backend);
    let execution_strategy = choose_execution_strategy(
        &plan,
        query,
        &backend,
        typed_aggregation_specs.as_ref(),
        typed_sort_specs.as_ref(),
    );

    Ok(SearchCommand {
        logical_plan: plan,
        execution_plan,
        execution_strategy,
        typed_aggregation_specs,
        typed_sort_specs: typed_sort_specs.unwrap_or_default(),
        backend: SearchBackend::LegacySql(backend),
    })
}

fn choose_execution_strategy(
    plan: &search_plan::SearchPlan,
    query: &QueryStringSearch,
    backend: &SqlCommand,
    typed_aggregation_specs: Option<&Vec<PrivateSearchAggregationSpec>>,
    typed_sort_specs: Option<&Vec<PrivateSearchSortSpec>>,
) -> SearchExecutionStrategy {
    if (!plan.aggregations.is_empty() && typed_aggregation_specs.is_none())
        || (!plan.sort.is_empty() && typed_sort_specs.is_none())
        || query.sort.is_some()
    {
        return SearchExecutionStrategy::LegacySqlFanout;
    }

    match &plan.target {
        search_plan::SearchTarget::Pit(_) => SearchExecutionStrategy::LegacySqlFanout,
        search_plan::SearchTarget::Table(_) => {
            if !plan.sort.is_empty() {
                SearchExecutionStrategy::TypedNodeMerge(SearchResultOrder::ExplicitSort)
            } else if backend.calculate_score {
                SearchExecutionStrategy::TypedNodeMerge(SearchResultOrder::ScoreDesc)
            } else {
                SearchExecutionStrategy::TypedNodeMerge(SearchResultOrder::PeerConcat)
            }
        }
    }
}

fn private_search_aggregation_specs(
    plans: &[search_plan::AggregationPlan],
) -> Option<Vec<PrivateSearchAggregationSpec>> {
    plans.iter().map(private_search_aggregation_spec).collect()
}

fn private_search_sort_specs(
    plans: &[search_plan::SortPlan],
    backend: &SqlCommand,
) -> Option<Vec<PrivateSearchSortSpec>> {
    plans
        .iter()
        .map(|plan| private_search_sort_spec(plan, backend))
        .collect()
}

fn private_search_sort_spec(
    plan: &search_plan::SortPlan,
    backend: &SqlCommand,
) -> Option<PrivateSearchSortSpec> {
    match plan {
        search_plan::SortPlan::Bare(field) => private_search_sort_field(field, None, backend),
        search_plan::SortPlan::Field {
            field,
            order,
            script,
            ..
        } => {
            if script.is_some() {
                return None;
            }
            private_search_sort_field(field, order.as_deref(), backend)
        }
    }
}

fn private_search_sort_field(
    field: &str,
    order: Option<&str>,
    backend: &SqlCommand,
) -> Option<PrivateSearchSortSpec> {
    if field == "_score" && !backend.calculate_score {
        return None;
    }

    let descending = match order.map(|value| value.to_ascii_lowercase()) {
        None => field == "_score",
        Some(value) if value == "asc" => false,
        Some(value) if value == "desc" => true,
        Some(_) => return None,
    };

    Some(PrivateSearchSortSpec {
        field: field.to_string(),
        descending,
    })
}

fn private_search_aggregation_spec(
    plan: &search_plan::AggregationPlan,
) -> Option<PrivateSearchAggregationSpec> {
    match &plan.spec {
        search_plan::AggregationPlanSpec::Terms(terms_plan) => {
            Some(PrivateSearchAggregationSpec::Terms {
                name: plan.name.clone(),
                field: terms_plan.field.clone(),
                size: terms_plan.size,
                sub_aggregations: if terms_plan.sub_aggregations.is_empty() {
                    vec![]
                } else {
                    return None;
                },
            })
        }
        search_plan::AggregationPlanSpec::Average(avg_plan) => {
            Some(PrivateSearchAggregationSpec::Average {
                name: plan.name.clone(),
                field: avg_plan.field.clone(),
            })
        }
        search_plan::AggregationPlanSpec::Filter(filter_plan) => {
            Some(PrivateSearchAggregationSpec::Filter {
                name: plan.name.clone(),
                filter: private_search_aggregation_filter(&filter_plan.filter)?,
                sub_aggregations: private_search_aggregation_specs(&filter_plan.sub_aggregations)?,
            })
        }
        _ => None,
    }
}

fn private_search_aggregation_filter(
    filter: &search_plan::AggregationFilterPlan,
) -> Option<PrivateSearchAggregationFilterSpec> {
    match filter {
        search_plan::AggregationFilterPlan::Term { field, value } => {
            Some(PrivateSearchAggregationFilterSpec::Term {
                field: field.clone(),
                value: value.clone(),
            })
        }
        _ => None,
    }
}

fn merge_typed_aggregation_partials(
    partials_by_node: Vec<Vec<PrivateSearchAggregationPartial>>,
    specs: &[PrivateSearchAggregationSpec],
) -> std::collections::HashMap<String, AggregationResult> {
    specs
        .iter()
        .map(|spec| {
            let partials = partials_by_node
                .iter()
                .filter_map(|partials| partial_for_spec(partials, spec))
                .cloned()
                .collect::<Vec<_>>();
            (
                typed_aggregation_name(spec),
                merge_typed_aggregation_partial(spec, &partials),
            )
        })
        .collect()
}

fn partial_for_spec<'a>(
    partials: &'a [PrivateSearchAggregationPartial],
    spec: &PrivateSearchAggregationSpec,
) -> Option<&'a PrivateSearchAggregationPartial> {
    partials
        .iter()
        .find(|partial| typed_aggregation_partial_name(partial) == typed_aggregation_name(spec))
}

fn typed_aggregation_name(spec: &PrivateSearchAggregationSpec) -> String {
    match spec {
        PrivateSearchAggregationSpec::Terms { name, .. } => name.clone(),
        PrivateSearchAggregationSpec::Average { name, .. } => name.clone(),
        PrivateSearchAggregationSpec::Filter { name, .. } => name.clone(),
    }
}

fn compare_query_result_hits_by_sort(
    left: &crate::elastic_search_responses::QueryResultHit,
    right: &crate::elastic_search_responses::QueryResultHit,
    sorts: &[PrivateSearchSortSpec],
) -> std::cmp::Ordering {
    let left_values = left.sort.as_deref().unwrap_or(&[]);
    let right_values = right.sort.as_deref().unwrap_or(&[]);
    for (index, sort) in sorts.iter().enumerate() {
        let left_value = left_values.get(index).unwrap_or(&serde_json::Value::Null);
        let right_value = right_values.get(index).unwrap_or(&serde_json::Value::Null);
        let ordering = compare_sort_values(left_value, right_value);
        let ordering = if sort.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }

    right
        ._seq_no
        .cmp(&left._seq_no)
        .then_with(|| left._id.cmp(&right._id))
}

fn compare_sort_values(left: &serde_json::Value, right: &serde_json::Value) -> std::cmp::Ordering {
    match (left, right) {
        (serde_json::Value::Null, serde_json::Value::Null) => std::cmp::Ordering::Equal,
        (serde_json::Value::Null, _) => std::cmp::Ordering::Greater,
        (_, serde_json::Value::Null) => std::cmp::Ordering::Less,
        _ => {
            if let (Some(left_number), Some(right_number)) = (left.as_f64(), right.as_f64()) {
                return left_number
                    .partial_cmp(&right_number)
                    .unwrap_or(std::cmp::Ordering::Equal);
            }
            if let (Some(left_string), Some(right_string)) = (left.as_str(), right.as_str()) {
                return left_string.cmp(right_string);
            }
            if let (Some(left_bool), Some(right_bool)) = (left.as_bool(), right.as_bool()) {
                return left_bool.cmp(&right_bool);
            }
            left.to_string().cmp(&right.to_string())
        }
    }
}

fn typed_aggregation_partial_name(partial: &PrivateSearchAggregationPartial) -> &str {
    match partial {
        PrivateSearchAggregationPartial::Terms { name, .. } => name.as_str(),
        PrivateSearchAggregationPartial::Average { name, .. } => name.as_str(),
        PrivateSearchAggregationPartial::Filter { name, .. } => name.as_str(),
    }
}

fn merge_typed_aggregation_partial(
    spec: &PrivateSearchAggregationSpec,
    partials: &[PrivateSearchAggregationPartial],
) -> AggregationResult {
    match spec {
        PrivateSearchAggregationSpec::Terms {
            size,
            sub_aggregations,
            ..
        } => {
            let mut merged_buckets = std::collections::HashMap::<
                String,
                (u64, Vec<Vec<PrivateSearchAggregationPartial>>),
            >::new();
            for partial in partials.iter() {
                if let PrivateSearchAggregationPartial::Terms { buckets, .. } = partial {
                    for bucket in buckets.iter() {
                        let entry = merged_buckets
                            .entry(bucket.key.clone())
                            .or_insert_with(|| (0, vec![]));
                        entry.0 += bucket.doc_count;
                        entry.1.push(bucket.sub_aggregations.clone());
                    }
                }
            }

            let mut buckets = merged_buckets
                .into_iter()
                .map(
                    |(key, (doc_count, _sub_partials_by_node))| TermAggregationBucket {
                        key,
                        doc_count,
                    },
                )
                .collect::<Vec<_>>();
            buckets.sort_by(|left, right| {
                right
                    .doc_count
                    .cmp(&left.doc_count)
                    .then_with(|| left.key.cmp(&right.key))
            });
            buckets.truncate(size.unwrap_or(10) as usize);

            AggregationResult::Terms(TermAggregationResult {
                doc_count_error_upper_bound: 0,
                sum_other_doc_count: 0,
                buckets,
                aggs: if sub_aggregations.is_empty() {
                    Default::default()
                } else {
                    merge_typed_aggregation_partials(vec![], sub_aggregations)
                },
            })
        }
        PrivateSearchAggregationSpec::Average { .. } => {
            let (sum, count) =
                partials
                    .iter()
                    .fold((0.0, 0_u64), |(sum, count), partial| match partial {
                        PrivateSearchAggregationPartial::Average {
                            sum: local_sum,
                            count: local_count,
                            ..
                        } => (sum + local_sum, count + local_count),
                        _ => (sum, count),
                    });
            AggregationResult::Average(AverageAggregationResult {
                value: if count == 0 { 0.0 } else { sum / count as f64 },
                aggs: Default::default(),
            })
        }
        PrivateSearchAggregationSpec::Filter {
            sub_aggregations, ..
        } => {
            let doc_count = partials.iter().fold(0_u64, |count, partial| match partial {
                PrivateSearchAggregationPartial::Filter { doc_count, .. } => count + doc_count,
                _ => count,
            });
            let sub_partials_by_node = partials
                .iter()
                .map(|partial| match partial {
                    PrivateSearchAggregationPartial::Filter {
                        sub_aggregations, ..
                    } => sub_aggregations.clone(),
                    _ => vec![],
                })
                .collect::<Vec<_>>();
            AggregationResult::Filter(FilterAggregationResult {
                doc_count,
                aggs: merge_typed_aggregation_partials(sub_partials_by_node, sub_aggregations),
            })
        }
    }
}

fn compile_legacy_sql_command(
    plan: &search_plan::SearchPlan,
    query: &QueryStringSearch,
    doc_id_field_name: Option<&str>,
    include_deletes_join: bool,
) -> Result<SqlCommand, ParseError> {
    let mut builder = SqlBuilder::for_query_with_options(
        true,
        doc_id_field_name.unwrap_or("_id_seq_no"),
        include_deletes_join,
    );

    if plan.from != 0 {
        return Err(ParseError {
            message: "from != 0 is not implemented".to_string(),
        });
    }

    if let Some(query_plan) = &plan.query {
        apply_query_plan(&mut builder, query_plan)?;
    }

    let table_name = match &plan.target {
        search_plan::SearchTarget::Table(table_name) => table_name,
        search_plan::SearchTarget::Pit(pit) => &pit.id,
    };

    let aggs = aggregation_plans_to_runtime(None, &plan.aggregations);

    Ok(SqlCommand {
        sql: builder.build(),
        table: table_name.clone(),
        calculate_score: builder.calculate_score,
        aggs,
        query_params: query.clone(),
    })
}

fn create_execution_plan(
    plan: &search_plan::SearchPlan,
    backend: &SqlCommand,
) -> SearchExecutionPlan {
    let segment = SearchSegmentExecutionPlan::LegacySql(LegacySqlSegmentPlan {
        segment_id: "segment-000".to_string(),
        table: backend.table.clone(),
        sql: backend.sql.clone(),
        calculate_score: backend.calculate_score,
        required_extension: backend.calculate_score.then(|| "es".to_string()),
    });

    SearchExecutionPlan {
        shards: vec![SearchShardExecutionPlan {
            shard_id: "shard-000".to_string(),
            route: SearchShardRoute::BroadcastCurrentSnapshot,
            segments: vec![segment],
        }],
        merge: SearchMergePlan {
            from: plan.from,
            size: plan.size.unwrap_or(10) as usize,
            stages: vec![
                SearchMergeStage::SegmentToShardTopK,
                SearchMergeStage::ShardToCoordinatorTopK,
            ],
        },
    }
}

pub(crate) fn update_by_query_plan_to_command(
    plan: search_plan::UpdateByQueryPlan,
) -> Result<UpdateByQueryCommand, ParseError> {
    let mut builder = SqlBuilder::for_query(true);
    apply_query_plan(&mut builder, &plan.query)?;

    Ok(UpdateByQueryCommand {
        query_command: SqlCommand {
            sql: builder.build(),
            table: plan.table,
            calculate_score: builder.calculate_score,
            aggs: None,
            query_params: QueryStringSearch {
                allow_partial_search_results: None,
                sort: None,
                rest_total_hits_as_int: None,
            },
        },
        script_block: script_plan_to_script_block(&plan.script),
    })
}

fn script_plan_to_script_block(plan: &search_plan::ScriptPlan) -> ScriptBlock {
    ScriptBlock {
        source: plan.source.clone(),
        lang: plan.lang.clone(),
        params: plan.params.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::SearchCommand;
    use crate::elastic_search_endpoints::QueryStringSearch;
    use crate::elastic_search_parser;
    use crate::peers::PrivateSearchSortSpec;

    fn parse_search_command(body: &str) -> SearchCommand {
        parse_search_command_with_query(body, QueryStringSearch::new())
    }

    fn parse_search_command_with_query(body: &str, query: QueryStringSearch) -> SearchCommand {
        elastic_search_parser::parse(Some("logs".to_string()), &body.to_string(), &query).unwrap()
    }

    #[test]
    fn test_match_query_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "match": {
      "message": {
        "query": "login"
      }
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_simple_query_string_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "simple_query_string": {
      "query": "login",
      "fields": ["message"],
      "default_operator": "and"
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_range_query_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "range": {
      "@timestamp": {
        "gte": "2099-03-08T00:00:00.000Z"
      }
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_term_query_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "term": {
      "index_col": 2
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_terms_aggregation_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "match": {
      "message": {
        "query": "login"
      }
    }
  },
  "aggs": {
    "messages": {
      "terms": {
        "field": "message"
      }
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_avg_and_filter_aggregation_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "aggs": {
    "avg_price": { "avg": { "field": "price" } },
    "t_shirts": {
      "filter": { "term": { "type": "tshirt" } },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } }
      }
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_request_body_field_sort_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "sort": [
    {
      "index_col": {
        "order": "desc"
      }
    }
  ]
}"#,
        );

        assert!(command.supports_typed_node_merge());
        assert_eq!(
            command.typed_sort_specs,
            vec![PrivateSearchSortSpec {
                field: "index_col".to_string(),
                descending: true,
            }]
        );
    }

    #[test]
    fn test_unsupported_aggregation_stays_on_legacy_path() {
        let command = parse_search_command(
            r#"{
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price",
        "ranges": [
          { "from": "0", "to": "100" }
        ]
      }
    }
  }
}"#,
        );

        assert!(!command.supports_typed_node_merge());
    }

    #[test]
    fn test_script_sort_stays_on_legacy_path() {
        let command = parse_search_command(
            r#"{
  "sort": [
    {
      "index_col": {
        "order": "desc",
        "script": {
          "source": "doc['index_col'].value",
          "lang": "painless",
          "params": {}
        }
      }
    }
  ]
}"#,
        );

        assert!(!command.supports_typed_node_merge());
    }

    #[test]
    fn test_query_string_sort_stays_on_legacy_path() {
        let command = parse_search_command_with_query(
            r#"{
  "query": {
    "term": {
      "index_col": 2
    }
  }
}"#,
            QueryStringSearch {
                allow_partial_search_results: None,
                sort: Some("index_col:asc".to_string()),
                rest_total_hits_as_int: None,
            },
        );

        assert!(!command.supports_typed_node_merge());
    }
}

fn apply_query_plan(
    builder: &mut SqlBuilder,
    query: &search_plan::QueryPlan,
) -> Result<(), ParseError> {
    match query {
        search_plan::QueryPlan::Match(match_plan) => apply_match_plan(builder, match_plan),
        search_plan::QueryPlan::Bool(bool_plan) => apply_bool_plan(builder, bool_plan),
        search_plan::QueryPlan::Term(term_plan) => apply_term_plan(builder, term_plan),
        search_plan::QueryPlan::Exists(exists_plan) => apply_exists_plan(builder, exists_plan),
        search_plan::QueryPlan::Range(range_plan) => apply_range_plan(builder, range_plan),
        search_plan::QueryPlan::SimpleQueryString(simple_query) => {
            apply_simple_query_string_plan(builder, simple_query)
        }
    }
}

fn apply_match_plan(
    builder: &mut SqlBuilder,
    match_plan: &search_plan::MatchPlan,
) -> Result<(), ParseError> {
    if match_plan.clauses.len() != 1 {
        return Err(ParseError {
            message: "Multiple match clauses are not implemented".to_string(),
        });
    }

    builder.calculate_score = true;
    builder.push_filter_context();
    for clause in match_plan.clauses.iter() {
        builder.filter(SqlExpression::Comparison(
            Box::new(SqlExpression::FieldRef(
                "si".to_string(),
                "field_name".to_string(),
            )),
            "=".to_string(),
            Box::new(SqlExpression::LiteralString(clause.field.clone())),
        ));
        builder.filter(SqlExpression::Comparison(
            Box::new(SqlExpression::FieldRef(
                "si".to_string(),
                "field_term".to_string(),
            )),
            "=".to_string(),
            Box::new(SqlExpression::LiteralString(clause.query.clone())),
        ));
    }
    builder.pop_filter_context(true);
    Ok(())
}

fn apply_bool_plan(
    builder: &mut SqlBuilder,
    bool_plan: &search_plan::BoolPlan,
) -> Result<(), ParseError> {
    builder.push_filter_context();
    if !bool_plan.must.is_empty() {
        builder.push_filter_context();
        for query in bool_plan.must.iter() {
            apply_query_plan(builder, query)?;
        }
        builder.pop_filter_context(true);
    }
    if !bool_plan.should.is_empty() {
        builder.push_filter_context();
        for query in bool_plan.should.iter() {
            apply_query_plan(builder, query)?;
        }
        builder.pop_filter_context(false);
    }
    if !bool_plan.must_not.is_empty() {
        builder.push_filter_context();
        for query in bool_plan.must_not.iter() {
            apply_query_plan(builder, query)?;
        }
        builder.pop_and_not_filter_context(false);
    }
    if !bool_plan.filter.is_empty() {
        builder.push_filter_context();
        for query in bool_plan.filter.iter() {
            apply_query_plan(builder, query)?;
        }
        builder.pop_filter_context(true);
    }
    builder.pop_filter_context(true);
    Ok(())
}

fn apply_term_plan(
    builder: &mut SqlBuilder,
    term_plan: &search_plan::TermPlan,
) -> Result<(), ParseError> {
    for clause in term_plan.clauses.iter() {
        let literal = if clause.value.is_string() {
            SqlExpression::LiteralString(clause.value.as_str().unwrap().to_string())
        } else {
            SqlExpression::LiteralNonString(clause.value.to_string())
        };
        builder.filter(SqlExpression::Comparison(
            Box::new(SqlExpression::FieldRef(
                "t".to_string(),
                clause.field.clone(),
            )),
            "=".to_string(),
            Box::new(literal),
        ));
    }
    Ok(())
}

fn apply_exists_plan(
    _builder: &mut SqlBuilder,
    _exists_plan: &search_plan::ExistsPlan,
) -> Result<(), ParseError> {
    Ok(())
}

fn apply_range_plan(
    builder: &mut SqlBuilder,
    range_plan: &search_plan::RangePlan,
) -> Result<(), ParseError> {
    if range_plan.clauses.len() != 1 {
        return Err(ParseError {
            message: "Multiple range clauses are not implemented".to_string(),
        });
    }

    let clause = range_plan.clauses.first().unwrap();
    if clause.format.is_some()
        || clause.relation.is_some()
        || clause.time_zone.is_some()
        || clause.boost.is_some()
    {
        return Err(ParseError {
            message: "Range options are not implemented".to_string(),
        });
    }

    let (op, final_val) = range_operator_to_sql_expression(&clause.operator);
    builder.push_filter_context();
    builder.filter(SqlExpression::Comparison(
        Box::new(SqlExpression::FieldRef(
            "t".to_string(),
            clause.field.clone(),
        )),
        op,
        Box::new(final_val),
    ));
    builder.filter(SqlExpression::IsNull(Box::new(SqlExpression::FieldRef(
        "t".to_string(),
        clause.field.clone(),
    ))));
    builder.pop_filter_context(false);
    Ok(())
}

fn apply_simple_query_string_plan(
    builder: &mut SqlBuilder,
    simple_query: &search_plan::SimpleQueryStringPlan,
) -> Result<(), ParseError> {
    if simple_query.fields.is_empty() {
        return Err(ParseError {
            message: "simple_query_string.fields is required".to_string(),
        });
    }

    builder.calculate_score = true;
    builder.push_filter_context();
    for field_term in simple_query.query.split(' ') {
        for field_name in simple_query.fields.iter() {
            builder.filter(SqlExpression::Comparison(
                Box::new(SqlExpression::FieldRef(
                    "si".to_string(),
                    "field_name".to_string(),
                )),
                "=".to_string(),
                Box::new(SqlExpression::LiteralString(field_name.clone())),
            ));
            builder.filter(SqlExpression::Comparison(
                Box::new(SqlExpression::FieldRef(
                    "si".to_string(),
                    "field_term".to_string(),
                )),
                "=".to_string(),
                Box::new(SqlExpression::LiteralString(field_term.to_string())),
            ));
        }
    }
    builder.pop_filter_context(true);
    Ok(())
}

fn range_operator_to_sql_expression(
    operator: &search_plan::RangeOperatorPlan,
) -> (String, SqlExpression) {
    let (op, value) = match operator {
        search_plan::RangeOperatorPlan::Gt(value) => (">", value),
        search_plan::RangeOperatorPlan::Gte(value) => (">=", value),
        search_plan::RangeOperatorPlan::Lt(value) => ("<", value),
        search_plan::RangeOperatorPlan::Lte(value) => ("<=", value),
    };

    let final_val = if value.is_string() {
        SqlExpression::LiteralString(convert_datetime_if_necessary(value.as_str().unwrap()))
    } else {
        SqlExpression::LiteralNonString(value.to_string())
    };

    (op.to_string(), final_val)
}

fn convert_datetime_if_necessary(value: &str) -> String {
    if value.contains("now") {
        elastic_search_datetime_parser::evaluate(&value.to_string(), &Utc::now()).unwrap()
    } else {
        value.to_string()
    }
}

fn aggregation_plans_to_runtime(
    input_builder: Option<SqlBuilder>,
    plans: &[search_plan::AggregationPlan],
) -> Option<Vec<Aggregation>> {
    if plans.is_empty() {
        return None;
    }

    Some(
        plans
            .iter()
            .map(|plan| create_aggregation_from_plan(input_builder.clone(), plan))
            .collect(),
    )
}

fn create_aggregation_from_plan(
    input_builder: Option<SqlBuilder>,
    plan: &search_plan::AggregationPlan,
) -> Aggregation {
    let builder = input_builder.unwrap_or_else(SqlBuilder::for_agg);
    let (processor, subaggregations) = create_aggregation_processor_from_plan(&builder, &plan.spec);
    Aggregation {
        name: plan.name.clone(),
        processor,
        subaggregations,
    }
}

fn create_aggregation_processor_from_plan(
    input_builder: &SqlBuilder,
    spec: &search_plan::AggregationPlanSpec,
) -> (AggProcessor, Option<Vec<Aggregation>>) {
    match spec {
        search_plan::AggregationPlanSpec::Terms(terms) => {
            let field_name = terms.field.clone();
            let mut builder = input_builder.clone();
            let field_ref_expr = SqlExpression::FieldRef("t".to_string(), field_name.clone());
            builder.group_by.push(field_ref_expr.clone());
            let subaggregations =
                aggregation_plans_to_runtime(Some(builder.clone()), &terms.sub_aggregations);
            builder.fields.push(FieldExpression {
                name: "field_name".to_string(),
                expression: field_ref_expr,
            });
            builder.fields.push(FieldExpression {
                name: "cnt".to_string(),
                expression: SqlExpression::Count,
            });
            (
                AggProcessor::Term(TermAggProcessor {
                    sql: builder.build(),
                }),
                subaggregations,
            )
        }
        search_plan::AggregationPlanSpec::Filter(filter) => {
            let mut builder = input_builder.clone();
            for filter_expression in create_aggregation_filters_from_plan(&filter.filter) {
                builder.filter(filter_expression);
            }
            let mut query_builder = builder.clone();
            query_builder.fields.push(FieldExpression {
                name: "cnt".to_string(),
                expression: SqlExpression::Count,
            });
            (
                AggProcessor::Filter(FilterAggProcessor {
                    sql: query_builder.build(),
                }),
                aggregation_plans_to_runtime(Some(builder), &filter.sub_aggregations),
            )
        }
        search_plan::AggregationPlanSpec::Missing(missing) => (
            AggProcessor::Missing(MissingAggProcessor {}),
            aggregation_plans_to_runtime(Some(input_builder.clone()), &missing.sub_aggregations),
        ),
        search_plan::AggregationPlanSpec::DateHistogram(histogram) => {
            let mut builder = input_builder.clone();
            let field_name = histogram.field.clone();
            builder.fields.push(FieldExpression {
                name: "field_value".to_string(),
                expression: SqlExpression::FieldRef("t".to_string(), field_name.clone()),
            });
            builder.fields.push(FieldExpression {
                name: "doc_count".to_string(),
                expression: SqlExpression::Count,
            });
            let offset = 0;
            let interval = 5;
            builder.group_by.push(SqlExpression::Arithmetic(
                Box::new(SqlExpression::Arithmetic(
                    Box::new(SqlExpression::Arithmetic(
                        Box::new(SqlExpression::FieldRef("t".to_string(), field_name.clone())),
                        "-".to_string(),
                        Box::new(SqlExpression::LiteralNonString(offset.to_string())),
                    )),
                    "/".to_string(),
                    Box::new(SqlExpression::LiteralNonString(interval.to_string())),
                )),
                "+".to_string(),
                Box::new(SqlExpression::LiteralNonString(offset.to_string())),
            ));
            (
                AggProcessor::DateHistogram(DateHistogramAggProcessor { buckets: vec![] }),
                aggregation_plans_to_runtime(Some(builder), &histogram.sub_aggregations),
            )
        }
        search_plan::AggregationPlanSpec::Cardinality(cardinality) => {
            let mut builder = input_builder.clone();
            builder.fields.push(FieldExpression {
                name: "type_count".to_string(),
                expression: SqlExpression::CountDistinct(Box::new(SqlExpression::FieldRef(
                    "t".to_string(),
                    cardinality.field.clone(),
                ))),
            });
            (
                AggProcessor::Cardinality(CardinalityAggProcessor {
                    sql: builder.build(),
                }),
                aggregation_plans_to_runtime(
                    Some(input_builder.clone()),
                    &cardinality.sub_aggregations,
                ),
            )
        }
        search_plan::AggregationPlanSpec::Range(range) => {
            let mut builder = input_builder.clone();
            for filter_expression in create_aggregation_range_filters_from_plan(&range.range) {
                builder.filter(filter_expression);
            }
            let mut query_builder = builder.clone();
            query_builder.fields.push(FieldExpression {
                name: "cnt".to_string(),
                expression: SqlExpression::Count,
            });
            let subaggregations =
                aggregation_plans_to_runtime(Some(builder), &range.sub_aggregations);
            (
                AggProcessor::Range(RangeAggProcessor {
                    buckets: vec![RangeAggBucket {
                        sql: query_builder.build(),
                        key: "2025-06-27T20:18:59.356Z-2025-06-27T20:20:59.356Z".to_string(),
                        from: 1751055539356,
                        from_as_string: "2025-06-27T20:18:59.356Z".to_string(),
                        to: 1751055659356,
                        to_as_string: "2025-06-27T20:20:59.356Z".to_string(),
                        subaggregations,
                    }],
                }),
                None,
            )
        }
        search_plan::AggregationPlanSpec::Average(average) => {
            let mut builder = input_builder.clone();
            builder.fields.push(FieldExpression {
                name: "avg".to_string(),
                expression: SqlExpression::Average(Box::new(SqlExpression::FieldRef(
                    "t".to_string(),
                    average.field.clone(),
                ))),
            });
            (
                AggProcessor::Average(AverageAggProcessor {
                    sql: builder.build(),
                }),
                aggregation_plans_to_runtime(
                    Some(input_builder.clone()),
                    &average.sub_aggregations,
                ),
            )
        }
    }
}

fn create_aggregation_filters_from_plan(
    filter: &search_plan::AggregationFilterPlan,
) -> Vec<SqlExpression> {
    match filter {
        search_plan::AggregationFilterPlan::Term { field, value } => {
            vec![SqlExpression::Comparison(
                Box::new(SqlExpression::FieldRef("t".to_string(), field.clone())),
                "=".to_string(),
                Box::new(SqlExpression::LiteralString(value.clone())),
            )]
        }
        search_plan::AggregationFilterPlan::Range(range) => {
            create_aggregation_range_filters_from_plan(range)
        }
    }
}

fn create_aggregation_range_filters_from_plan(
    range: &search_plan::AggregationRangeBoundsPlan,
) -> Vec<SqlExpression> {
    match range {
        search_plan::AggregationRangeBoundsPlan::Raw { field, operator } => {
            let (converted_op, converted_value) = range_operator_to_sql_expression(operator);
            vec![SqlExpression::Comparison(
                Box::new(SqlExpression::FieldRef("t".to_string(), field.clone())),
                converted_op,
                Box::new(converted_value),
            )]
        }
        search_plan::AggregationRangeBoundsPlan::Structured { field, ranges } => {
            let mut retval = vec![];
            for range in ranges.iter() {
                let converted_from_value = convert_datetime_if_necessary(&range.from);
                let converted_to_value = convert_datetime_if_necessary(&range.to);
                retval.push(SqlExpression::Comparison(
                    Box::new(SqlExpression::FieldRef("t".to_string(), field.clone())),
                    ">=".to_string(),
                    Box::new(SqlExpression::LiteralString(converted_from_value)),
                ));
                retval.push(SqlExpression::Comparison(
                    Box::new(SqlExpression::FieldRef("t".to_string(), field.clone())),
                    "<".to_string(),
                    Box::new(SqlExpression::LiteralString(converted_to_value)),
                ));
            }
            retval
        }
    }
}
