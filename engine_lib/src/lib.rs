#![allow(dead_code)]

extern crate core;

#[path = "../../main_lib/src/compaction.rs"]
pub mod compaction;
#[path = "../../main_lib/src/data_access.rs"]
pub mod data_access;
#[path = "../../main_lib/src/data_contract.rs"]
pub mod data_contract;
#[path = "../../main_lib/src/data_fusion_functions.rs"]
mod data_fusion_functions;
#[path = "../../main_lib/src/distributed_cache.rs"]
mod distributed_cache;
#[path = "../../main_lib/src/dynamodb.rs"]
mod dynamodb;
#[path = "../../main_lib/src/dynamodb_protocol.rs"]
mod dynamodb_protocol;
#[path = "../../main_lib/src/dynamodb_service_impl.rs"]
pub mod dynamodb_service_impl;
#[path = "../../main_lib/src/dynamodb_state_provider.rs"]
mod dynamodb_state_provider;
#[path = "../../main_lib/src/elastic_search_cluster_info.rs"]
mod elastic_search_cluster_info;
#[path = "../../main_lib/src/elastic_search_commands.rs"]
mod elastic_search_commands;
#[path = "../../main_lib/src/elastic_search_common.rs"]
mod elastic_search_common;
#[path = "../../main_lib/src/elastic_search_datetime_parser.rs"]
mod elastic_search_datetime_parser;
#[path = "../../main_lib/src/elastic_search_endpoints.rs"]
mod elastic_search_endpoints;
#[path = "../../main_lib/src/elastic_search_index.rs"]
mod elastic_search_index;
#[path = "../../main_lib/src/elastic_search_ingest.rs"]
pub mod elastic_search_ingest;
#[path = "../../main_lib/src/elastic_search_lifetime_policy.rs"]
pub mod elastic_search_lifetime_policy;
#[path = "../../main_lib/src/elastic_search_parser.rs"]
mod elastic_search_parser;
#[path = "../../main_lib/src/elastic_search_pipeline.rs"]
pub mod elastic_search_pipeline;
#[path = "../../main_lib/src/elastic_search_responses.rs"]
mod elastic_search_responses;
#[path = "../../main_lib/src/elastic_search_storage_schema.rs"]
mod elastic_search_storage_schema;
#[path = "../../main_lib/src/ephemeral_fetch_tracker.rs"]
mod ephemeral_fetch_tracker;
#[path = "../../main_lib/src/ephemeral_service_impl.rs"]
pub mod ephemeral_service_impl;
#[path = "../../main_lib/src/ephemeral_state_provider.rs"]
mod ephemeral_state_provider;
#[path = "../../main_lib/src/expression_evaluator.rs"]
mod expression_evaluator;
#[path = "../../main_lib/src/lakehouse_serving.rs"]
pub mod lakehouse_serving;
#[path = "../../main_lib/src/leaderless_state_provider.rs"]
mod leaderless_state_provider;
#[path = "../../main_lib/src/metadata_store.rs"]
pub mod metadata_store;
#[path = "../../main_lib/src/mongodb_protocol.rs"]
mod mongodb_protocol;
#[path = "../../main_lib/src/painless_parser.rs"]
mod painless_parser;
pub mod peers;
#[path = "../../main_lib/src/pipeline.rs"]
pub mod pipeline;
#[path = "../../main_lib/src/prefetch.rs"]
mod prefetch;
#[path = "../../main_lib/src/private_api.rs"]
mod private_api;
#[path = "../../main_lib/src/router.rs"]
pub mod router;
#[path = "../../main_lib/src/schema_massager.rs"]
pub mod schema_massager;
#[path = "../../main_lib/src/search_executor.rs"]
mod search_executor;
#[path = "../../main_lib/src/search_plan.rs"]
pub mod search_plan;
#[path = "../../main_lib/src/search_runtime.rs"]
pub mod search_runtime;
#[path = "../../main_lib/src/serving_dataset.rs"]
pub mod serving_dataset;
#[path = "../../main_lib/src/serving_plan.rs"]
pub mod serving_plan;
#[path = "../../main_lib/src/serving_protocol.rs"]
pub mod serving_protocol;
#[path = "../../main_lib/src/state_common.rs"]
mod state_common;
#[path = "../../main_lib/src/state_provider.rs"]
pub mod state_provider;
#[path = "../../main_lib/src/test_api.rs"]
pub mod test_api;
#[path = "../../main_lib/src/util.rs"]
mod util;
