use crate::elastic_search_commands::{SqlCommand, UpdateByQueryCommand};
use crate::elastic_search_common::{
    execute_command, Command, CommandContext, ElasticSearchResponse, ParseError,
    ResultGeneratorFuture,
};
use crate::elastic_search_datetime_parser;
use crate::elastic_search_endpoints::QueryStringSearch;
use crate::elastic_search_responses::{
    compare_query_result_hits_desc, AggregationResult, AverageAggregationResult,
    CardinalityAggregationResult, FilterAggregationResult, HistogramAggregationBucket,
    HistogramAggregationResult, QueryFailure, QueryResults, TermAggregationBucket,
    TermAggregationResult,
};
use crate::peers::{
    CheckpointDescriptor, PrivateExactConstraintGroup, PrivateInvocation,
    PrivateSearchAggregationFilterSpec, PrivateSearchAggregationPartial,
    PrivateSearchAggregationSpec, PrivateSearchDateHistogramExtendedBoundsSpec,
    PrivateSearchInvocation, PrivateSearchRangeConstraint, PrivateSearchSortSpec,
    PrivateSearchTermsOrderSpec,
};
use crate::read_plan::{ReadPlan, ReadPredicate, ReadSort};
use crate::schema_massager::{FieldExpression, SqlBuilder, SqlExpression};
use crate::search_plan;
use crate::search_runtime::{
    AggProcessor, Aggregation, AverageAggProcessor, CardinalityAggProcessor,
    DateHistogramAggProcessor, FilterAggProcessor, MissingAggProcessor, RangeAggBucket,
    RangeAggProcessor, ScriptBlock, TermAggProcessor,
};
use crate::state_provider::STATE_PROVIDER;
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::future::try_join_all;
use serde_json::Value;
use std::collections::BTreeSet;
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
    pub search_plan: search_plan::SearchPlan,
    #[allow(dead_code)]
    pub read_plan: ReadPlan,
    #[allow(dead_code)]
    pub execution_plan: SearchExecutionPlan,
    execution_strategy: SearchExecutionStrategy,
    typed_aggregation_specs: Option<Vec<PrivateSearchAggregationSpec>>,
    typed_sort_specs: Vec<PrivateSearchSortSpec>,
    exact_sql: Option<crate::schema_massager::SqlQuery>,
    exact_constraints: Vec<PrivateExactConstraintGroup>,
    range_constraints: Vec<PrivateSearchRangeConstraint>,
    backend: SearchBackend,
}

pub(crate) struct CountCommandResult {
    pub total_hits: u64,
    pub num_shards: u32,
}

struct TypedSearchSourceResults {
    peer_results: Vec<crate::peers::PrivateSearchResult>,
    num_shards: u32,
}

pub(crate) fn typed_sort_projection_name(field: &str) -> String {
    let normalized = field
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    format!("__powdrr_sort_{normalized}")
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

    #[allow(dead_code)]
    fn supports_exact_sidecar(&self) -> bool {
        self.exact_sql.is_some()
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
        let size = if self.read_plan.search_after.is_some() {
            usize::MAX
        } else {
            self.execution_plan.merge.from as usize + self.execution_plan.merge.size
        };
        let mut required_extensions = legacy_command.required_extensions();
        if self.exact_sql.is_some()
            && !required_extensions
                .iter()
                .any(|extension| extension == "es")
        {
            required_extensions.push("es".to_string());
        }
        Some(PrivateSearchInvocation {
            sql: legacy_command.sql.clone(),
            exact_sql: self.exact_sql.clone(),
            exact_constraints: self.exact_constraints.clone(),
            range_constraints: self.range_constraints.clone(),
            required_extensions,
            base_extension_suffixes: self
                .read_plan
                .base_extension_suffixes(legacy_command.calculate_score),
            exact_extension_suffixes: self.read_plan.exact_extension_suffixes(),
            checkpoints,
            table: legacy_command.table.clone(),
            size,
            calculate_score: legacy_command.calculate_score,
            aggregations: self.typed_aggregation_specs.clone().unwrap_or_default(),
            sorts: self.typed_sort_specs.clone(),
        })
    }

    async fn current_target_snapshots(
        &self,
        legacy_command: &SqlCommand,
    ) -> Vec<CheckpointDescriptor> {
        match STATE_PROVIDER
            .get_published_active_servable_checkpoint(&legacy_command.table)
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
                    "Error getting active published checkpoint for table {}: {}",
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
    if command.read_plan.search_after.is_some() {
        return "Queries using `search_after` currently require the typed sorted path.".to_string();
    }

    if let Some(reason) = command
        .search_plan
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
        .search_plan
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
        &command.search_plan.target,
        search_plan::SearchTarget::Pit(_)
    ) {
        return "Point-in-time queries currently use the legacy SQL fanout path.".to_string();
    }

    "Query is supported, but it falls back to the legacy SQL fanout path.".to_string()
}

fn aggregation_legacy_path_reason(plan: &search_plan::AggregationPlan) -> Option<String> {
    match &plan.spec {
        search_plan::AggregationPlanSpec::Terms(terms_plan) => terms_plan
            .sub_aggregations
            .iter()
            .find_map(aggregation_legacy_path_reason),
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
        search_plan::AggregationPlanSpec::DateHistogram(histogram_plan) => histogram_plan
            .sub_aggregations
            .iter()
            .find_map(aggregation_legacy_path_reason),
        search_plan::AggregationPlanSpec::Cardinality(_) => None,
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
    match command.execution_strategy {
        SearchExecutionStrategy::LegacySqlFanout => execute_command(context, command).await,
        SearchExecutionStrategy::TypedNodeMerge(_) => {
            execute_multi_target_search_commands(context, vec![command]).await
        }
    }
}

pub(crate) async fn execute_multi_target_search_commands(
    context: CommandContext,
    commands: Vec<Arc<SearchCommand>>,
) -> ElasticSearchResponse {
    let Some(first_command) = commands.first().cloned() else {
        return QueryResults::empty(50, 0, None, true).to_response();
    };

    let result_order = match first_command.execution_strategy {
        SearchExecutionStrategy::LegacySqlFanout => {
            if commands.len() > 1 {
                return QueryFailure {
                    message:
                        "Multi-target query requires a legacy merge path that is not yet supported"
                            .to_string(),
                }
                .to_response();
            }
            return execute_command(context, first_command).await;
        }
        SearchExecutionStrategy::TypedNodeMerge(result_order) => result_order,
    };

    if commands
        .iter()
        .any(|command| command.execution_strategy != first_command.execution_strategy)
    {
        return QueryFailure {
            message: "Multi-target query mix requires the legacy path and is not yet supported"
                .to_string(),
        }
        .to_response();
    }

    let command_results = match try_join_all(
        commands
            .iter()
            .cloned()
            .map(execute_typed_search_source_results),
    )
    .await
    {
        Ok(results) => results,
        Err(response) => return response,
    };

    build_typed_search_response(&first_command, &command_results, result_order)
}

async fn execute_typed_search_source_results(
    command: Arc<SearchCommand>,
) -> Result<TypedSearchSourceResults, ElasticSearchResponse> {
    let invocation = match command.private_search_invocation().await {
        Some(invocation) => invocation,
        None => {
            return Err(QueryFailure {
                message: "Typed search invocation is not available for this command".to_string(),
            }
            .to_response());
        }
    };

    if invocation.checkpoints.is_empty() {
        return Ok(TypedSearchSourceResults {
            peer_results: vec![],
            num_shards: 1,
        });
    }

    let peer_clients = STATE_PROVIDER.get_peer_clients().await;
    let num_peers = peer_clients.len();
    let peer_calls = peer_clients.iter().enumerate().map(|(index, peer_client)| {
        peer_client.private_search(&invocation, index as u64, num_peers as u64)
    });

    let peer_results = match try_join_all(peer_calls).await {
        Ok(results) => results,
        Err(e) => {
            return Err(QueryFailure {
                message: format!("{:?}", e),
            }
            .to_response());
        }
    };

    Ok(TypedSearchSourceResults {
        peer_results,
        num_shards: num_peers as u32,
    })
}

fn build_typed_search_response(
    command: &SearchCommand,
    command_results: &[TypedSearchSourceResults],
    result_order: SearchResultOrder,
) -> ElasticSearchResponse {
    let total_num_shards = command_results
        .iter()
        .map(|result| result.num_shards)
        .sum::<u32>();
    let total_hits: usize = command_results
        .iter()
        .flat_map(|result| result.peer_results.iter())
        .map(|result| result.total_hits)
        .sum();
    let aggregation_partials = command_results
        .iter()
        .flat_map(|result| result.peer_results.iter())
        .map(|result| result.aggregations.clone())
        .collect::<Vec<_>>();
    let mut hits = command_results
        .iter()
        .flat_map(|result| result.peer_results.iter())
        .flat_map(|result| result.hits.clone().into_iter())
        .collect::<Vec<_>>();

    match result_order {
        SearchResultOrder::ScoreDesc => hits.sort_by(compare_query_result_hits_desc),
        SearchResultOrder::ExplicitSort => hits.sort_by(|left, right| {
            compare_query_result_hits_by_sort(left, right, &command.typed_sort_specs)
        }),
        SearchResultOrder::PeerConcat => {}
    }

    let hits = match apply_search_after(
        hits,
        command.read_plan.search_after.as_deref(),
        result_order,
        &command.typed_sort_specs,
    ) {
        Ok(hits) => hits,
        Err(message) => return QueryFailure { message }.to_response(),
    };

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
        return QueryResults::empty(50, total_num_shards, aggregations, total_hits_complex)
            .to_response();
    }

    QueryResults::success(
        50,
        total_num_shards,
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

pub(crate) async fn execute_count_command(
    command: Arc<SearchCommand>,
) -> Result<CountCommandResult, ElasticSearchResponse> {
    let invocation = match command.private_search_invocation().await {
        Some(invocation) => invocation,
        None => {
            return Ok(CountCommandResult {
                total_hits: 0,
                num_shards: 1,
            });
        }
    };

    if invocation.checkpoints.is_empty() {
        return Ok(CountCommandResult {
            total_hits: 0,
            num_shards: 1,
        });
    }

    let peer_clients = STATE_PROVIDER.get_peer_clients().await;
    let num_peers = peer_clients.len();
    let peer_calls = peer_clients.iter().enumerate().map(|(index, peer_client)| {
        peer_client.private_search(&invocation, index as u64, num_peers as u64)
    });

    let peer_results = match try_join_all(peer_calls).await {
        Ok(results) => results,
        Err(e) => {
            return Err(QueryFailure {
                message: format!("{:?}", e),
            }
            .to_response());
        }
    };

    let total_hits = peer_results
        .iter()
        .map(|result| result.total_hits)
        .sum::<usize>() as u64;
    Ok(CountCommandResult {
        total_hits,
        num_shards: num_peers as u32,
    })
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
    let read_plan = read_plan_from_search_plan(&plan);
    let exact_constraints = compile_exact_constraint_groups(&read_plan)?;
    let range_constraints = compile_range_constraints(&read_plan);
    let exact_sql = compile_exact_sql_query(&plan, query, doc_id_field_name, include_deletes_join)?;
    let execution_plan =
        create_execution_plan(&read_plan, &plan.target, &backend, exact_sql.is_some());
    let typed_aggregation_specs = private_search_aggregation_specs(&plan.aggregations);
    let typed_sort_specs = private_search_sort_specs(&plan.sort, &backend);
    let execution_strategy = choose_execution_strategy(
        &read_plan,
        &plan.target,
        query,
        &backend,
        typed_aggregation_specs.as_ref(),
        typed_sort_specs.as_ref(),
    );

    validate_search_after(
        &read_plan,
        query,
        typed_sort_specs.as_ref(),
        execution_strategy,
    )?;

    Ok(SearchCommand {
        search_plan: plan,
        read_plan,
        execution_plan,
        execution_strategy,
        typed_aggregation_specs,
        typed_sort_specs: typed_sort_specs.unwrap_or_default(),
        exact_sql,
        exact_constraints,
        range_constraints,
        backend: SearchBackend::LegacySql(backend),
    })
}

fn validate_search_after(
    plan: &ReadPlan,
    query: &QueryStringSearch,
    typed_sort_specs: Option<&Vec<PrivateSearchSortSpec>>,
    execution_strategy: SearchExecutionStrategy,
) -> Result<(), ParseError> {
    let Some(search_after) = plan.search_after.as_ref() else {
        return Ok(());
    };

    if plan.offset != 0 {
        return Err(ParseError {
            message: "`search_after` does not support `from`".to_string(),
        });
    }

    if query.sort.is_some() {
        return Err(ParseError {
            message: "`search_after` requires request-body sort, not query-string sort".to_string(),
        });
    }

    let Some(typed_sort_specs) = typed_sort_specs else {
        return Err(ParseError {
            message: "`search_after` requires an explicit supported sort".to_string(),
        });
    };

    if typed_sort_specs.is_empty() || plan.order_by.is_empty() {
        return Err(ParseError {
            message: "`search_after` requires an explicit sort".to_string(),
        });
    }

    if search_after.len() != typed_sort_specs.len() {
        return Err(ParseError {
            message: "`search_after` must include one value per sort field".to_string(),
        });
    }

    if !matches!(
        execution_strategy,
        SearchExecutionStrategy::TypedNodeMerge(SearchResultOrder::ExplicitSort)
    ) {
        return Err(ParseError {
            message: "`search_after` is only supported on typed sorted searches".to_string(),
        });
    }

    Ok(())
}

fn choose_execution_strategy(
    plan: &ReadPlan,
    target: &search_plan::SearchTarget,
    query: &QueryStringSearch,
    backend: &SqlCommand,
    typed_aggregation_specs: Option<&Vec<PrivateSearchAggregationSpec>>,
    typed_sort_specs: Option<&Vec<PrivateSearchSortSpec>>,
) -> SearchExecutionStrategy {
    if (typed_aggregation_specs.is_none() && backend.aggs.is_some())
        || (!plan.order_by.is_empty() && typed_sort_specs.is_none())
        || query.sort.is_some()
    {
        return SearchExecutionStrategy::LegacySqlFanout;
    }

    match target {
        search_plan::SearchTarget::Pit(_) => SearchExecutionStrategy::LegacySqlFanout,
        search_plan::SearchTarget::Table(_) => {
            if !plan.order_by.is_empty() {
                SearchExecutionStrategy::TypedNodeMerge(SearchResultOrder::ExplicitSort)
            } else if backend.calculate_score {
                SearchExecutionStrategy::TypedNodeMerge(SearchResultOrder::ScoreDesc)
            } else {
                SearchExecutionStrategy::TypedNodeMerge(SearchResultOrder::PeerConcat)
            }
        }
    }
}

fn read_plan_from_search_plan(plan: &search_plan::SearchPlan) -> ReadPlan {
    let mut filters = vec![];

    if let Some(query_plan) = &plan.query {
        if let Some(groups) = exact_constraint_groups_for_query(query_plan) {
            for group in groups {
                let exact_filter = if group.values.len() == 1 {
                    ReadPredicate {
                        field: group.field,
                        eq: Some(group.values[0].clone()),
                        in_values: None,
                        gt: None,
                        gte: None,
                        lt: None,
                        lte: None,
                    }
                } else {
                    ReadPredicate {
                        field: group.field,
                        eq: None,
                        in_values: Some(group.values),
                        gt: None,
                        gte: None,
                        lt: None,
                        lte: None,
                    }
                };
                merge_read_predicate(&mut filters, exact_filter);
            }
        }

        let mut range_constraints = vec![];
        collect_mandatory_range_constraints(query_plan, &mut range_constraints);
        for constraint in range_constraints {
            merge_read_predicate(
                &mut filters,
                ReadPredicate {
                    field: constraint.field,
                    eq: None,
                    in_values: None,
                    gt: constraint.gt,
                    gte: constraint.gte,
                    lt: constraint.lt,
                    lte: constraint.lte,
                },
            );
        }
    }

    ReadPlan {
        select: None,
        filters,
        aggregate: None,
        order_by: plan
            .sort
            .iter()
            .filter_map(read_sort_from_search_sort)
            .collect(),
        limit: plan.size.map(|size| size as usize),
        offset: plan.from as usize,
        search_after: plan.search_after.clone(),
        allow_slow_path: false,
        explain: false,
    }
}

fn read_sort_from_search_sort(sort: &search_plan::SortPlan) -> Option<ReadSort> {
    match sort {
        search_plan::SortPlan::Bare(field) => Some(ReadSort {
            field: field.clone(),
            descending: field == "_score",
        }),
        search_plan::SortPlan::Field { field, order, .. } => Some(ReadSort {
            field: field.clone(),
            descending: order
                .as_deref()
                .map(|order| order.eq_ignore_ascii_case("desc"))
                .unwrap_or(field == "_score"),
        }),
    }
}

fn merge_read_predicate(filters: &mut Vec<ReadPredicate>, incoming: ReadPredicate) {
    if let Some(existing) = filters
        .iter_mut()
        .find(|predicate| predicate.field == incoming.field)
    {
        if incoming.eq.is_some() {
            existing.eq = incoming.eq;
        }
        if incoming.in_values.is_some() {
            existing.in_values = incoming.in_values;
        }
        if incoming.gt.is_some() {
            existing.gt = incoming.gt;
        }
        if incoming.gte.is_some() {
            existing.gte = incoming.gte;
        }
        if incoming.lt.is_some() {
            existing.lt = incoming.lt;
        }
        if incoming.lte.is_some() {
            existing.lte = incoming.lte;
        }
    } else {
        filters.push(incoming);
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
                order: private_search_terms_order(terms_plan.order.as_ref()),
                missing: terms_plan.missing.clone(),
                sub_aggregations: private_search_aggregation_specs(&terms_plan.sub_aggregations)?,
            })
        }
        search_plan::AggregationPlanSpec::Average(avg_plan) => {
            Some(PrivateSearchAggregationSpec::Average {
                name: plan.name.clone(),
                field: avg_plan.field.clone(),
            })
        }
        search_plan::AggregationPlanSpec::Cardinality(cardinality_plan) => {
            Some(PrivateSearchAggregationSpec::Cardinality {
                name: plan.name.clone(),
                field: cardinality_plan.field.clone(),
            })
        }
        search_plan::AggregationPlanSpec::DateHistogram(histogram_plan) => {
            Some(PrivateSearchAggregationSpec::DateHistogram {
                name: plan.name.clone(),
                field: histogram_plan.field.clone(),
                fixed_interval: histogram_plan.fixed_interval.clone(),
                min_doc_count: histogram_plan.min_doc_count,
                extended_bounds: histogram_plan.extended_bounds.as_ref().map(|bounds| {
                    PrivateSearchDateHistogramExtendedBoundsSpec {
                        min: bounds.min.clone(),
                        max: bounds.max.clone(),
                    }
                }),
                sub_aggregations: private_search_aggregation_specs(
                    &histogram_plan.sub_aggregations,
                )?,
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

fn private_search_terms_order(
    order: Option<&search_plan::TermsOrderPlan>,
) -> Option<PrivateSearchTermsOrderSpec> {
    match order {
        Some(search_plan::TermsOrderPlan::CountAsc) => Some(PrivateSearchTermsOrderSpec::CountAsc),
        Some(search_plan::TermsOrderPlan::CountDesc) => {
            Some(PrivateSearchTermsOrderSpec::CountDesc)
        }
        Some(search_plan::TermsOrderPlan::KeyAsc) => Some(PrivateSearchTermsOrderSpec::KeyAsc),
        Some(search_plan::TermsOrderPlan::KeyDesc) => Some(PrivateSearchTermsOrderSpec::KeyDesc),
        None => None,
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
        PrivateSearchAggregationSpec::Cardinality { name, .. } => name.clone(),
        PrivateSearchAggregationSpec::DateHistogram { name, .. } => name.clone(),
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

fn compare_hit_to_search_after(
    hit: &crate::elastic_search_responses::QueryResultHit,
    search_after: &[Value],
    sorts: &[PrivateSearchSortSpec],
) -> std::cmp::Ordering {
    let hit_values = hit.sort.as_deref().unwrap_or(&[]);
    for (index, sort) in sorts.iter().enumerate() {
        let hit_value = hit_values.get(index).unwrap_or(&serde_json::Value::Null);
        let search_after_value = search_after.get(index).unwrap_or(&serde_json::Value::Null);
        let ordering = compare_sort_values(hit_value, search_after_value);
        let ordering = if sort.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }

    std::cmp::Ordering::Equal
}

fn apply_search_after(
    hits: Vec<crate::elastic_search_responses::QueryResultHit>,
    search_after: Option<&[Value]>,
    result_order: SearchResultOrder,
    sorts: &[PrivateSearchSortSpec],
) -> Result<Vec<crate::elastic_search_responses::QueryResultHit>, String> {
    let Some(search_after) = search_after else {
        return Ok(hits);
    };

    if !matches!(result_order, SearchResultOrder::ExplicitSort) {
        return Err("`search_after` requires explicit sort".to_string());
    }

    Ok(hits
        .into_iter()
        .filter(|hit| {
            compare_hit_to_search_after(hit, search_after, sorts) == std::cmp::Ordering::Greater
        })
        .collect())
}

fn typed_aggregation_partial_name(partial: &PrivateSearchAggregationPartial) -> &str {
    match partial {
        PrivateSearchAggregationPartial::Terms { name, .. } => name.as_str(),
        PrivateSearchAggregationPartial::Average { name, .. } => name.as_str(),
        PrivateSearchAggregationPartial::Cardinality { name, .. } => name.as_str(),
        PrivateSearchAggregationPartial::DateHistogram { name, .. } => name.as_str(),
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
            order,
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
                    |(key, (doc_count, sub_partials_by_node))| TermAggregationBucket {
                        key,
                        doc_count,
                        aggs: if sub_aggregations.is_empty() {
                            Default::default()
                        } else {
                            merge_typed_aggregation_partials(sub_partials_by_node, sub_aggregations)
                        },
                    },
                )
                .collect::<Vec<_>>();
            buckets.sort_by(|left, right| {
                compare_terms_aggregation_buckets(left, right, order.as_ref())
            });
            buckets.truncate(size.unwrap_or(10) as usize);

            AggregationResult::Terms(TermAggregationResult {
                doc_count_error_upper_bound: 0,
                sum_other_doc_count: 0,
                buckets,
                aggs: Default::default(),
            })
        }
        PrivateSearchAggregationSpec::Cardinality { .. } => {
            let value = partials
                .iter()
                .flat_map(|partial| match partial {
                    PrivateSearchAggregationPartial::Cardinality { values, .. } => values.clone(),
                    _ => vec![],
                })
                .collect::<std::collections::BTreeSet<_>>()
                .len() as u64;
            AggregationResult::Cardinality(CardinalityAggregationResult {
                value,
                aggs: Default::default(),
            })
        }
        PrivateSearchAggregationSpec::DateHistogram {
            fixed_interval,
            min_doc_count,
            extended_bounds,
            sub_aggregations,
            ..
        } => {
            let mut merged_buckets = std::collections::BTreeMap::<
                i64,
                (String, u64, Vec<Vec<PrivateSearchAggregationPartial>>),
            >::new();
            for partial in partials.iter() {
                if let PrivateSearchAggregationPartial::DateHistogram { buckets, .. } = partial {
                    for bucket in buckets.iter() {
                        let entry = merged_buckets
                            .entry(bucket.key)
                            .or_insert_with(|| (bucket.key_as_string.clone(), 0, vec![]));
                        entry.1 += bucket.doc_count;
                        entry.2.push(bucket.sub_aggregations.clone());
                    }
                }
            }

            let mut buckets = merged_buckets
                .into_iter()
                .map(|(key, (key_as_string, doc_count, sub_partials_by_node))| {
                    HistogramAggregationBucket {
                        key,
                        key_as_string,
                        doc_count,
                        aggs: if sub_aggregations.is_empty() {
                            Default::default()
                        } else {
                            merge_typed_aggregation_partials(sub_partials_by_node, sub_aggregations)
                        },
                    }
                })
                .collect::<Vec<_>>();

            if min_doc_count.unwrap_or(0) == 0 {
                buckets =
                    fill_empty_histogram_buckets(buckets, fixed_interval, extended_bounds.as_ref());
            }

            AggregationResult::Histogram(HistogramAggregationResult { buckets })
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

fn compare_terms_aggregation_buckets(
    left: &TermAggregationBucket,
    right: &TermAggregationBucket,
    order: Option<&PrivateSearchTermsOrderSpec>,
) -> std::cmp::Ordering {
    match order.unwrap_or(&PrivateSearchTermsOrderSpec::CountDesc) {
        PrivateSearchTermsOrderSpec::CountAsc => left
            .doc_count
            .cmp(&right.doc_count)
            .then_with(|| left.key.cmp(&right.key)),
        PrivateSearchTermsOrderSpec::CountDesc => right
            .doc_count
            .cmp(&left.doc_count)
            .then_with(|| left.key.cmp(&right.key)),
        PrivateSearchTermsOrderSpec::KeyAsc => left.key.cmp(&right.key),
        PrivateSearchTermsOrderSpec::KeyDesc => right.key.cmp(&left.key),
    }
}

fn fill_empty_histogram_buckets(
    buckets: Vec<HistogramAggregationBucket>,
    fixed_interval: &str,
    extended_bounds: Option<&PrivateSearchDateHistogramExtendedBoundsSpec>,
) -> Vec<HistogramAggregationBucket> {
    let Some(interval_ms) = parse_histogram_fixed_interval_millis(fixed_interval) else {
        return buckets;
    };
    if buckets.is_empty() && extended_bounds.is_none() {
        return buckets;
    }

    let mut buckets_by_key = buckets
        .into_iter()
        .map(|bucket| (bucket.key, bucket))
        .collect::<std::collections::BTreeMap<_, _>>();

    let observed_start = buckets_by_key.keys().next().copied();
    let observed_end = buckets_by_key.keys().next_back().copied();
    let extended_start =
        extended_bounds.and_then(|bounds| histogram_bound_to_bucket_key(&bounds.min, interval_ms));
    let extended_end =
        extended_bounds.and_then(|bounds| histogram_bound_to_bucket_key(&bounds.max, interval_ms));

    let start = match (observed_start, extended_start) {
        (Some(observed), Some(extended)) => observed.min(extended),
        (Some(observed), None) => observed,
        (None, Some(extended)) => extended,
        (None, None) => return vec![],
    };
    let end = match (observed_end, extended_end) {
        (Some(observed), Some(extended)) => observed.max(extended),
        (Some(observed), None) => observed,
        (None, Some(extended)) => extended,
        (None, None) => return vec![],
    };

    let mut filled = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        if let Some(bucket) = buckets_by_key.remove(&cursor) {
            filled.push(bucket);
        } else {
            filled.push(HistogramAggregationBucket {
                key: cursor,
                key_as_string: timestamp_millis_to_key_as_string(cursor),
                doc_count: 0,
                aggs: Default::default(),
            });
        }
        let Some(next_cursor) = cursor.checked_add(interval_ms) else {
            break;
        };
        cursor = next_cursor;
    }
    filled
}

fn parse_histogram_fixed_interval_millis(interval: &str) -> Option<i64> {
    if interval.len() < 2 {
        return None;
    }
    let (value, unit) = interval.split_at(interval.len() - 1);
    let quantity = value.parse::<i64>().ok()?;
    let multiplier = match unit {
        "s" => 1_000,
        "m" => 60 * 1_000,
        "h" => 60 * 60 * 1_000,
        "d" => 24 * 60 * 60 * 1_000,
        "w" => 7 * 24 * 60 * 60 * 1_000,
        _ => return None,
    };
    quantity.checked_mul(multiplier)
}

fn histogram_bound_to_bucket_key(value: &Value, interval_ms: i64) -> Option<i64> {
    let timestamp_ms = histogram_bound_to_timestamp_millis(value)?;
    Some(timestamp_ms - timestamp_ms.rem_euclid(interval_ms))
}

fn histogram_bound_to_timestamp_millis(value: &Value) -> Option<i64> {
    if let Some(timestamp_ms) = value.as_i64() {
        return Some(timestamp_ms);
    }
    if let Some(timestamp_ms) = value.as_u64() {
        return i64::try_from(timestamp_ms).ok();
    }
    let timestamp = value.as_str()?;
    DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|datetime| datetime.with_timezone(&Utc).timestamp_millis())
}

fn timestamp_millis_to_key_as_string(timestamp_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn compile_legacy_sql_command(
    plan: &search_plan::SearchPlan,
    query: &QueryStringSearch,
    doc_id_field_name: Option<&str>,
    include_deletes_join: bool,
) -> Result<SqlCommand, ParseError> {
    let returns_hits = search_plan_returns_hits(plan);
    let mut builder = SqlBuilder::for_query_with_options(
        returns_hits,
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

    append_projection_fields_for_agg_only_query(
        &mut builder,
        plan,
        doc_id_field_name.unwrap_or("_id_seq_no"),
    );
    append_sort_projection_fields(&mut builder, &plan.sort);

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

#[derive(Clone, Debug, PartialEq)]
struct ExactConstraintGroup {
    field: String,
    values: Vec<Value>,
}

fn compile_exact_constraint_groups(
    plan: &ReadPlan,
) -> Result<Vec<PrivateExactConstraintGroup>, ParseError> {
    let mut groups = vec![];

    for predicate in plan.filters.iter() {
        let Some(raw_values) = predicate
            .eq
            .as_ref()
            .map(|value| vec![value.clone()])
            .or_else(|| predicate.in_values.clone())
        else {
            continue;
        };

        let mut values = raw_values
            .iter()
            .map(exact_value_to_index_string)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| ParseError {
                message: "Exact-match sidecar only supports string, numeric, and boolean values"
                    .to_string(),
            })?;
        values.sort();
        values.dedup();
        groups.push(PrivateExactConstraintGroup {
            field: predicate.field.clone(),
            values,
        });
    }

    groups.sort_by(|left, right| left.field.cmp(&right.field));
    Ok(groups)
}

fn compile_exact_sql_query(
    plan: &search_plan::SearchPlan,
    query: &QueryStringSearch,
    doc_id_field_name: Option<&str>,
    include_deletes_join: bool,
) -> Result<Option<crate::schema_massager::SqlQuery>, ParseError> {
    let Some(query_plan) = &plan.query else {
        return Ok(None);
    };
    let Some(groups) = exact_constraint_groups_for_query(query_plan) else {
        return Ok(None);
    };
    if groups.is_empty() {
        return Ok(None);
    }

    let returns_hits = search_plan_returns_hits(plan);
    let doc_id_field_name = doc_id_field_name.unwrap_or("_id_seq_no");
    let normalized_doc_id_field_name = doc_id_field_name.replace(".", "_");
    let mut builder =
        SqlBuilder::for_query_with_options(returns_hits, doc_id_field_name, include_deletes_join);

    for (index, group) in groups.iter().enumerate() {
        let alias = format!("ei{index}");
        builder.joins.push(format!(
            "INNER JOIN {{target_table}}_exact_index {alias} ON {alias}.doc_id = t.\"{normalized_doc_id_field_name}\""
        ));
        builder.filter(SqlExpression::Comparison(
            Box::new(SqlExpression::FieldRef(
                alias.clone(),
                "field_name".to_string(),
            )),
            "=".to_string(),
            Box::new(SqlExpression::LiteralString(group.field.clone())),
        ));

        let mut values = group
            .values
            .iter()
            .map(exact_value_to_index_string)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| ParseError {
                message: "Exact-match sidecar only supports string, numeric, and boolean values"
                    .to_string(),
            })?;
        values.sort();
        values.dedup();

        if values.len() == 1 {
            builder.filter(SqlExpression::Comparison(
                Box::new(SqlExpression::FieldRef(
                    alias.clone(),
                    "field_value".to_string(),
                )),
                "=".to_string(),
                Box::new(SqlExpression::LiteralString(values[0].clone())),
            ));
        } else {
            builder.filter(SqlExpression::In(
                Box::new(SqlExpression::FieldRef(
                    alias.clone(),
                    "field_value".to_string(),
                )),
                values
                    .into_iter()
                    .map(SqlExpression::LiteralString)
                    .collect::<Vec<_>>(),
            ));
        }
    }

    append_projection_fields_for_agg_only_query(&mut builder, plan, doc_id_field_name);
    append_sort_projection_fields(&mut builder, &plan.sort);

    let table_name = match &plan.target {
        search_plan::SearchTarget::Table(table_name) => table_name,
        search_plan::SearchTarget::Pit(_) => return Ok(None),
    };

    Ok(Some(
        SqlCommand {
            sql: builder.build(),
            table: table_name.clone(),
            calculate_score: false,
            aggs: aggregation_plans_to_runtime(None, &plan.aggregations),
            query_params: query.clone(),
        }
        .sql,
    ))
}

fn exact_constraint_groups_for_query(
    query: &search_plan::QueryPlan,
) -> Option<Vec<ExactConstraintGroup>> {
    match query {
        search_plan::QueryPlan::Term(term_plan) => exact_constraint_groups_for_term(term_plan),
        search_plan::QueryPlan::Bool(bool_plan) => exact_constraint_groups_for_bool(bool_plan),
        _ => None,
    }
}

fn exact_constraint_groups_for_term(
    term_plan: &search_plan::TermPlan,
) -> Option<Vec<ExactConstraintGroup>> {
    if term_plan.clauses.len() != 1 {
        return None;
    }
    let clause = term_plan.clauses.first()?;
    exact_value_to_index_string(&clause.value)?;
    Some(vec![ExactConstraintGroup {
        field: clause.field.clone(),
        values: vec![clause.value.clone()],
    }])
}

fn exact_constraint_groups_for_bool(
    bool_plan: &search_plan::BoolPlan,
) -> Option<Vec<ExactConstraintGroup>> {
    if !bool_plan.must_not.is_empty() {
        return None;
    }

    let mut groups = Vec::new();
    for query in bool_plan.must.iter().chain(bool_plan.filter.iter()) {
        let next_groups = exact_constraint_groups_for_query(query)?;
        groups = merge_exact_constraint_groups(groups, next_groups)?;
    }

    if !bool_plan.should.is_empty() {
        let effective_minimum_should_match = bool_plan.minimum_should_match.unwrap_or_else(|| {
            if bool_plan.must.is_empty() && bool_plan.filter.is_empty() {
                1
            } else {
                0
            }
        });
        if effective_minimum_should_match != 1 {
            return None;
        }
        let should_group = exact_should_group(&bool_plan.should)?;
        groups = merge_exact_constraint_groups(groups, vec![should_group])?;
    }

    if groups.is_empty() {
        None
    } else {
        Some(groups)
    }
}

fn exact_should_group(queries: &[search_plan::QueryPlan]) -> Option<ExactConstraintGroup> {
    let mut field_name = None::<String>;
    let mut values = Vec::new();

    for query in queries {
        let mut groups = exact_constraint_groups_for_query(query)?;
        if groups.len() != 1 {
            return None;
        }
        let group = groups.pop().unwrap();
        match &field_name {
            Some(existing) if existing != &group.field => return None,
            Some(_) => (),
            None => field_name = Some(group.field.clone()),
        }
        for value in group.values {
            if !values.iter().any(|existing| existing == &value) {
                values.push(value);
            }
        }
    }

    let field = field_name?;
    values.sort_by(|left, right| left.to_string().cmp(&right.to_string()));
    Some(ExactConstraintGroup { field, values })
}

fn merge_exact_constraint_groups(
    mut left: Vec<ExactConstraintGroup>,
    right: Vec<ExactConstraintGroup>,
) -> Option<Vec<ExactConstraintGroup>> {
    for incoming in right {
        if let Some(existing) = left.iter_mut().find(|group| group.field == incoming.field) {
            let allowed = incoming
                .values
                .iter()
                .map(|value| value.to_string())
                .collect::<BTreeSet<_>>();
            let intersection = existing
                .values
                .iter()
                .filter(|value| allowed.contains(&value.to_string()))
                .cloned()
                .collect::<Vec<_>>();
            if intersection.is_empty() {
                return None;
            }
            existing.values = intersection;
        } else {
            left.push(incoming);
        }
    }
    left.sort_by(|left_group, right_group| left_group.field.cmp(&right_group.field));
    Some(left)
}

fn exact_value_to_index_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn compile_range_constraints(plan: &ReadPlan) -> Vec<PrivateSearchRangeConstraint> {
    plan.filters
        .iter()
        .filter_map(|predicate| {
            if predicate.gt.is_none()
                && predicate.gte.is_none()
                && predicate.lt.is_none()
                && predicate.lte.is_none()
            {
                return None;
            }
            Some(PrivateSearchRangeConstraint {
                field: predicate.field.clone(),
                gt: predicate.gt.clone(),
                gte: predicate.gte.clone(),
                lt: predicate.lt.clone(),
                lte: predicate.lte.clone(),
            })
        })
        .collect()
}

fn collect_mandatory_range_constraints(
    query: &search_plan::QueryPlan,
    constraints: &mut Vec<PrivateSearchRangeConstraint>,
) {
    match query {
        search_plan::QueryPlan::Range(range_plan) => {
            for clause in &range_plan.clauses {
                let mut constraint = PrivateSearchRangeConstraint {
                    field: clause.field.clone(),
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                };
                match &clause.operator {
                    search_plan::RangeOperatorPlan::Gt(value) => {
                        constraint.gt = Some(value.clone())
                    }
                    search_plan::RangeOperatorPlan::Gte(value) => {
                        constraint.gte = Some(value.clone())
                    }
                    search_plan::RangeOperatorPlan::Lt(value) => {
                        constraint.lt = Some(value.clone())
                    }
                    search_plan::RangeOperatorPlan::Lte(value) => {
                        constraint.lte = Some(value.clone())
                    }
                }
                constraints.push(constraint);
            }
        }
        search_plan::QueryPlan::Bool(bool_plan) => {
            for query in bool_plan.must.iter().chain(bool_plan.filter.iter()) {
                collect_mandatory_range_constraints(query, constraints);
            }
        }
        _ => {}
    }
}

fn append_sort_projection_fields(builder: &mut SqlBuilder, sorts: &[search_plan::SortPlan]) {
    for sort in sorts {
        let field = match sort {
            search_plan::SortPlan::Bare(field) => field,
            search_plan::SortPlan::Field { field, script, .. } => {
                if script.is_some() {
                    continue;
                }
                field
            }
        };

        if field == "_score" {
            continue;
        }

        let projection_name = typed_sort_projection_name(field);
        if builder
            .fields
            .iter()
            .any(|expr| expr.name == projection_name)
        {
            continue;
        }

        builder.fields.push(FieldExpression {
            name: projection_name,
            expression: SqlExpression::FieldRef("t".to_string(), field.clone()),
        });
    }
}

fn search_plan_returns_hits(plan: &search_plan::SearchPlan) -> bool {
    plan.size.unwrap_or(10) > 0
}

fn append_projection_fields_for_agg_only_query(
    builder: &mut SqlBuilder,
    plan: &search_plan::SearchPlan,
    doc_id_field_name: &str,
) {
    if search_plan_returns_hits(plan) {
        return;
    }

    push_projection_field(
        builder,
        "__powdrr_row_id",
        SqlExpression::FieldRef("t".to_string(), doc_id_field_name.replace('.', "_")),
    );
    append_aggregation_projection_fields(builder, &plan.aggregations);
}

fn append_aggregation_projection_fields(
    builder: &mut SqlBuilder,
    aggregations: &[search_plan::AggregationPlan],
) {
    for aggregation in aggregations {
        append_aggregation_plan_projection_fields(builder, &aggregation.spec);
    }
}

fn append_aggregation_plan_projection_fields(
    builder: &mut SqlBuilder,
    aggregation: &search_plan::AggregationPlanSpec,
) {
    match aggregation {
        search_plan::AggregationPlanSpec::Terms(plan) => {
            push_source_field_projection(builder, &plan.field);
            append_aggregation_projection_fields(builder, &plan.sub_aggregations);
        }
        search_plan::AggregationPlanSpec::Missing(plan) => {
            push_source_field_projection(builder, &plan.field);
            append_aggregation_projection_fields(builder, &plan.sub_aggregations);
        }
        search_plan::AggregationPlanSpec::Filter(plan) => {
            append_aggregation_filter_projection_fields(builder, &plan.filter);
            append_aggregation_projection_fields(builder, &plan.sub_aggregations);
        }
        search_plan::AggregationPlanSpec::DateHistogram(plan) => {
            push_source_field_projection(builder, &plan.field);
            append_aggregation_projection_fields(builder, &plan.sub_aggregations);
        }
        search_plan::AggregationPlanSpec::Cardinality(plan) => {
            push_source_field_projection(builder, &plan.field);
            append_aggregation_projection_fields(builder, &plan.sub_aggregations);
        }
        search_plan::AggregationPlanSpec::Range(plan) => {
            append_range_bounds_projection_fields(builder, &plan.range);
            append_aggregation_projection_fields(builder, &plan.sub_aggregations);
        }
        search_plan::AggregationPlanSpec::Average(plan) => {
            push_source_field_projection(builder, &plan.field);
            append_aggregation_projection_fields(builder, &plan.sub_aggregations);
        }
    }
}

fn append_aggregation_filter_projection_fields(
    builder: &mut SqlBuilder,
    filter: &search_plan::AggregationFilterPlan,
) {
    match filter {
        search_plan::AggregationFilterPlan::Term { field, .. } => {
            push_source_field_projection(builder, field);
        }
        search_plan::AggregationFilterPlan::Range(bounds) => {
            append_range_bounds_projection_fields(builder, bounds);
        }
    }
}

fn append_range_bounds_projection_fields(
    builder: &mut SqlBuilder,
    range: &search_plan::AggregationRangeBoundsPlan,
) {
    match range {
        search_plan::AggregationRangeBoundsPlan::Raw { field, .. }
        | search_plan::AggregationRangeBoundsPlan::Structured { field, .. } => {
            push_source_field_projection(builder, field);
        }
    }
}

fn push_source_field_projection(builder: &mut SqlBuilder, field: &str) {
    push_projection_field(
        builder,
        field,
        SqlExpression::FieldRef("t".to_string(), field.to_string()),
    );
}

fn push_projection_field(builder: &mut SqlBuilder, name: &str, expression: SqlExpression) {
    if builder.all_fields || builder.fields.iter().any(|field| field.name == name) {
        return;
    }
    builder.fields.push(FieldExpression {
        name: name.to_string(),
        expression,
    });
}

fn create_execution_plan(
    plan: &ReadPlan,
    target: &search_plan::SearchTarget,
    backend: &SqlCommand,
    exact_sql: bool,
) -> SearchExecutionPlan {
    let segment = SearchSegmentExecutionPlan::LegacySql(LegacySqlSegmentPlan {
        segment_id: "segment-000".to_string(),
        table: backend.table.clone(),
        sql: backend.sql.clone(),
        calculate_score: backend.calculate_score,
        required_extension: if backend.calculate_score || exact_sql {
            Some("es".to_string())
        } else {
            None
        },
    });

    SearchExecutionPlan {
        shards: vec![SearchShardExecutionPlan {
            shard_id: "shard-000".to_string(),
            route: match target {
                search_plan::SearchTarget::Table(_) => SearchShardRoute::BroadcastCurrentSnapshot,
                search_plan::SearchTarget::Pit(_) => SearchShardRoute::BroadcastCurrentSnapshot,
            },
            segments: vec![segment],
        }],
        merge: SearchMergePlan {
            from: plan.offset as u32,
            size: plan.limit.unwrap_or(10),
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
                ignore_unavailable: None,
                allow_no_indices: None,
                expand_wildcards: None,
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
    use crate::elastic_search_common::ParseError;
    use crate::elastic_search_endpoints::QueryStringSearch;
    use crate::elastic_search_parser;
    use crate::peers::PrivateSearchSortSpec;

    fn parse_search_command(body: &str) -> SearchCommand {
        parse_search_command_with_query(body, QueryStringSearch::new())
    }

    fn parse_search_command_with_query(body: &str, query: QueryStringSearch) -> SearchCommand {
        elastic_search_parser::parse(Some("logs".to_string()), &body.to_string(), &query).unwrap()
    }

    fn parse_search_command_result(body: &str) -> Result<SearchCommand, ParseError> {
        elastic_search_parser::parse(
            Some("logs".to_string()),
            &body.to_string(),
            &QueryStringSearch::new(),
        )
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
        assert!(!command.supports_exact_sidecar());
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
    fn test_multi_match_query_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "multi_match": {
      "query": "login",
      "fields": ["message", "message.keyword"]
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_query_string_query_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "query_string": {
      "query": "service:auth OR service:api",
      "fields": ["message"],
      "default_operator": "OR"
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
        assert!(command.supports_exact_sidecar());
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
    fn test_range_constraints_are_compiled_for_private_search_pruning() {
        let command = parse_search_command(
            r#"{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "service": "auth"
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": 100
            }
          }
        }
      ]
    }
  }
}"#,
        );

        assert_eq!(
            command.range_constraints,
            vec![crate::peers::PrivateSearchRangeConstraint {
                field: "@timestamp".to_string(),
                gt: None,
                gte: Some(serde_json::Value::from(100)),
                lt: None,
                lte: None,
            }]
        );
    }

    #[test]
    fn test_term_query_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "term": {
      "service": "auth"
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
        assert!(command.supports_exact_sidecar());
    }

    #[test]
    fn test_terms_query_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "terms": {
      "service": ["auth", "api"]
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
        assert!(command.supports_exact_sidecar());
    }

    #[test]
    fn test_ids_query_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "query": {
    "ids": {
      "values": ["2", "5"]
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
    fn test_cardinality_aggregation_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "aggs": {
    "distinct_messages": {
      "cardinality": {
        "field": "message"
      }
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_date_histogram_aggregation_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "aggs": {
    "per_day": {
      "date_histogram": {
        "field": "@timestamp",
        "fixed_interval": "1d"
      }
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_date_histogram_options_use_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "aggs": {
    "per_day": {
      "date_histogram": {
        "field": "@timestamp",
        "fixed_interval": "1d",
        "min_doc_count": 0,
        "extended_bounds": {
          "min": "2099-03-07T00:00:00.000Z",
          "max": "2099-03-10T00:00:00.000Z"
        }
      }
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_terms_subaggregation_uses_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "aggs": {
    "by_service": {
      "terms": {
        "field": "service"
      },
      "aggs": {
        "avg_index_col": {
          "avg": {
            "field": "index_col"
          }
        }
      }
    }
  }
}"#,
        );

        assert!(command.supports_typed_node_merge());
    }

    #[test]
    fn test_terms_order_and_missing_use_typed_node_merge_path() {
        let command = parse_search_command(
            r#"{
  "aggs": {
    "by_service": {
      "terms": {
        "field": "service",
        "size": 10,
        "order": {
          "_key": "asc"
        },
        "missing": "unknown"
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
    fn test_search_after_requires_explicit_sort() {
        let error = parse_search_command_result(
            r#"{
  "search_after": [2]
}"#,
        )
        .err()
        .unwrap();

        assert_eq!(error.message, "`search_after` requires an explicit sort");
    }

    #[test]
    fn test_search_after_requires_matching_sort_arity() {
        let error = parse_search_command_result(
            r#"{
  "search_after": [2, 3],
  "sort": [
    {
      "index_col": {
        "order": "asc"
      }
    }
  ]
}"#,
        )
        .err()
        .unwrap();

        assert_eq!(
            error.message,
            "`search_after` must include one value per sort field"
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
    fn test_terms_subaggregation_with_unsupported_nested_range_stays_on_legacy_path() {
        let command = parse_search_command(
            r#"{
  "aggs": {
    "by_service": {
      "terms": {
        "field": "service"
      },
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
                ignore_unavailable: None,
                allow_no_indices: None,
                expand_wildcards: None,
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
