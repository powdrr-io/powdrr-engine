use crate::compaction::{CompactionCommand, compact_logs};
use crate::dynamodb_protocol;
use crate::elastic_search_endpoints::AliasPathExtractor;
use crate::elastic_search_endpoints::NameAliasPathExtractor;
use crate::elastic_search_endpoints::NameIdPathExtractor;
use crate::elastic_search_endpoints::NamePathExtractor;
use crate::elastic_search_endpoints::QueryStringAliases;
use crate::elastic_search_endpoints::QueryStringClusterHealth;
use crate::elastic_search_endpoints::QueryStringClusterSettings;
use crate::elastic_search_endpoints::QueryStringFieldCaps;
use crate::elastic_search_endpoints::QueryStringSearch;
use crate::mongodb_protocol;
use crate::peers::{
    PrivateCompactionInvocationExternal, PrivateExtensionInvocationExternal,
    PrivatePrefetchInvocationExternal, PrivateSearchInvocationExternal,
    PrivateSqlInvocationExternal,
};
use crate::private_api;
use crate::private_api::{compaction_query, extension_query, prefetch_query, search_query};
use crate::test_api::test_v1_add_checkpoint;
use crate::test_api::test_v1_create_index;
use crate::test_api::test_v1_process_work;
use crate::test_api::test_v1_set_testing_mode;
use crate::test_api::test_v1_set_testing_processing_mode;
use crate::{elastic_search_endpoints, elastic_search_lifetime_policy, lakehouse_serving};
use futures::TryFutureExt;
use futures::future;
use futures_util::future::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::StatusCode;
use gotham::hyper::{Body, body};
use gotham::middleware::Middleware;
use gotham::mime;
use gotham::pipeline::new_pipeline;
use gotham::pipeline::single_pipeline;
use gotham::prelude::NewMiddleware;
use gotham::prelude::StaticResponseExtender;
use gotham::router::Router;
use gotham::router::builder::*;
use gotham::state::FromState;
use gotham::state::State;
use gotham::state::StateData;
use http::HeaderMap;
use serde::Deserialize;
use std::pin::Pin;
use std::sync::Arc;

pub fn private_v1_sql(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let invocation_obj: PrivateSqlInvocationExternal = match serde_json::from_str(&body_content)
        {
            Ok(io) => io,
            Err(_) => panic!("This should not happen"),
        };
        let query_result = private_api::data_query(
            &invocation_obj.invocation,
            invocation_obj.index,
            invocation_obj.num,
        )
        .await;
        match query_result {
            Ok(success) => {
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    serde_json::to_string(&success.result).unwrap(),
                );
                Ok((state, res))
            }
            Err(error) => {
                let error_message = format!("{}", error);
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    error_message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
}

pub fn private_v1_search(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let invocation_obj: PrivateSearchInvocationExternal =
            match serde_json::from_str(&body_content) {
                Ok(io) => io,
                Err(_) => panic!("This should not happen"),
            };
        match search_query(
            &invocation_obj.invocation,
            invocation_obj.index,
            invocation_obj.num,
        )
        .await
        {
            Ok(success) => {
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    serde_json::to_string(&success).unwrap(),
                );
                Ok((state, res))
            }
            Err(error) => {
                let error_message = format!("{}", error);
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    error_message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
}

pub fn private_v1_extension(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let command: PrivateExtensionInvocationExternal = match serde_json::from_str(&body_content)
        {
            Ok(io) => io,
            Err(_) => panic!("This should not happen"),
        };
        match extension_query(&command.invocation, command.index, command.num).await {
            Ok(success) => {
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    serde_json::to_string(&success).unwrap(),
                );
                Ok((state, res))
            }
            Err(error) => {
                let error_message = format!("{}", error);
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    error_message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
}

pub fn private_v1_prefetch(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let command: PrivatePrefetchInvocationExternal = match serde_json::from_str(&body_content) {
            Ok(io) => io,
            Err(_) => panic!("This should not happen"),
        };
        match prefetch_query(&command.invocation, command.index, command.num).await {
            Ok(_success) => {
                let res = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, "");
                Ok((state, res))
            }
            Err(error) => {
                let error_message = format!("{}", error);
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    error_message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
}

pub fn private_v1_compact(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let command: PrivateCompactionInvocationExternal = match serde_json::from_str(&body_content)
        {
            Ok(io) => io,
            Err(_) => panic!("This should not happen"),
        };
        match compaction_query(&command.invocation, command.index, command.num).await {
            Ok(success) => {
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    serde_json::to_string(&success.result).unwrap(),
                );
                Ok((state, res))
            }
            Err(error) => {
                let error_message = format!("{}", error);
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    error_message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
}

pub fn private_v1_compact_leader(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let command: CompactionCommand = match serde_json::from_str(&body_content) {
            Ok(io) => io,
            Err(_) => panic!("This should not happen"),
        };
        let response = compact_logs(Arc::new(command)).await;
        match response {
            Ok(success) => {
                let res = success.generate_response(&state);
                Ok((state, res))
            }
            Err(error) => {
                let error_message = format!("{}", error);
                let res = create_response(
                    &state,
                    StatusCode::BAD_REQUEST,
                    mime::TEXT_PLAIN,
                    error_message,
                );
                Ok((state, res))
            }
        }
    }
    .boxed()
}

#[derive(Clone, NewMiddleware)]
pub struct RouterMiddleware;

impl Middleware for RouterMiddleware {
    fn call<Chain>(self, state: State, chain: Chain) -> Pin<Box<HandlerFuture>>
    where
        Chain: FnOnce(State) -> Pin<Box<HandlerFuture>> + Send + 'static,
        Self: Sized,
    {
        // We're finished working on the Request, so allow other components to continue processing
        // the Request.
        //
        // Alternatively we could elect to not call chain and return a Response we've created if we
        // want to prevent any further processing from occuring on the Request.
        let result = chain(state);

        // Once a Response is generated by another part of the application, in this example's case
        // the middleware_reliant_handler function, we want to do some more work.
        //
        // The syntax used here is part of the async environment in which the Gotham web framework
        // operates, you may not have encountered this before. For more details you can read about
        // the Tokio project at https://tokio.rs/docs/getting-started/hello-world/
        let f = result.and_then(move |(state, mut response)| {
            let request_headers = state.borrow::<HeaderMap>();
            let request_opaque_id = request_headers.get("X-Opaque-Id").clone();
            let is_dynamodb_request = request_headers.contains_key("x-amz-target");

            let headers = response.headers_mut();
            if !is_dynamodb_request {
                headers.insert("X-elastic-product", "Elasticsearch".parse().unwrap());
            }
            if request_opaque_id.is_some() {
                headers.insert("X-Opaque-Id", request_opaque_id.unwrap().clone());
            }
            headers.remove("x-request-id");
            headers.remove("date");
            future::ok((state, response))
        });

        f.boxed()
    }
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
struct PathExtractor {
    // This will be a Vec containing each path segment as a separate String, with no '/'s.
    #[serde(rename = "*")]
    #[allow(dead_code)]
    parts: Vec<String>,
}

/// Create a `Router`
///
/// Results in a tree of routes that that looks like:
///
/// | _private/v1/_sql              --> POST
/// | {tables}/_search --> POST

/// matching on.
pub fn router(include_test_apis: bool) -> Router {
    let (chain, pipelines) = single_pipeline(new_pipeline().add(RouterMiddleware).build());

    build_router(chain, pipelines, |route| {
        route.scope("/_private", |route| {
            route.scope("/v1", |route| {
                route.post("/_sql").to(private_v1_sql);
                route.post("/_search").to(private_v1_search);
                route.post("/_compact").to(private_v1_compact);
                route.post("/_compact_leader").to(private_v1_compact_leader);
                route.post("/_extension").to(private_v1_extension);
                route.post("/_prefetch").to(private_v1_prefetch);
            })
        });

        if include_test_apis {
            route.scope("/_test", |route| {
                route.scope("/v1", |route| {
                    route.post("/_create_index").to(test_v1_create_index);
                    route.post("/_add_checkpoint").to(test_v1_add_checkpoint);
                    route.put("/_testing_mode").to(test_v1_set_testing_mode);
                    route
                        .put("/_testing_and_processing_mode")
                        .to(test_v1_set_testing_processing_mode);
                    route.put("/_process_work").to(test_v1_process_work);
                })
            });
        }

        route.post("/").to(dynamodb_protocol::dynamodb_api);

        // ES endpoints
        route.get("/").to(elastic_search_endpoints::es_root);
        route.head("/").to(elastic_search_endpoints::es_root_head);
        route.get("/_nodes").to(elastic_search_endpoints::es_nodes);
        route
            .get("/_license")
            .to(elastic_search_endpoints::es_license);
        route.get("/_xpack").to(elastic_search_endpoints::es_xpack);
        route
            .get("/_ingest/pipeline/:name")
            .to(elastic_search_endpoints::es_unsupported_get_pipeline);
        route
            .put("/_ingest/pipeline/:name")
            .to(elastic_search_endpoints::es_unsupported_create_pipeline);
        route
            .post("/_ingest/pipeline/:name")
            .to(elastic_search_endpoints::es_unsupported_create_pipeline);
        route
            .get("/_ingest/pipeline/_simulate")
            .to(elastic_search_endpoints::es_unsupported_get_pipeline_simulate);
        route
            .post("/_ingest/pipeline/_simulate")
            .to(elastic_search_endpoints::es_simulate_pipeline);
        route
            .get("/_ingest/pipeline/:name/_simulate")
            .to(elastic_search_endpoints::es_unsupported_get_pipeline_simulate);
        route
            .post("/_ingest/pipeline/:name/_simulate")
            .to(elastic_search_endpoints::es_unsupported_named_pipeline_simulate);
        route
            .post("/_search/scroll")
            .to(elastic_search_endpoints::es_unsupported_search_scroll);
        route
            .delete("/_search/scroll")
            .to(elastic_search_endpoints::es_unsupported_search_scroll);
        route
            .post("/_search/template")
            .to(elastic_search_endpoints::es_unsupported_search_template);
        route
            .post("/:name/_search/template")
            .to(elastic_search_endpoints::es_unsupported_search_template);
        route
            .post("/_async_search")
            .to(elastic_search_endpoints::es_unsupported_async_search);
        route
            .get("/_cat/indices")
            .to(elastic_search_endpoints::es_unsupported_cat_indices);
        route
            .get("/_cat/aliases")
            .to(elastic_search_endpoints::es_unsupported_cat_aliases);

        route
            .get("_cluster/settings")
            .with_query_string_extractor::<QueryStringClusterSettings>()
            .to(elastic_search_endpoints::es_cluster_settings);
        route
            .get("/_cluster/health")
            .with_query_string_extractor::<QueryStringClusterHealth>()
            .to(elastic_search_endpoints::es_cluster_health);
        route
            .get("/_cluster/health/:name")
            .with_query_string_extractor::<QueryStringClusterHealth>()
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_cluster_health);
        route
            .get("/_alias")
            .to(elastic_search_endpoints::es_get_aliases);
        route
            .get("/_alias/:alias")
            .with_path_extractor::<AliasPathExtractor>()
            .to(elastic_search_endpoints::es_get_named_aliases);
        route
            .get("/_resolve/index/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_resolve_index);
        route
            .get("/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_index);
        route
            .head("/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_head_index);
        route
            .get("/:name/_alias")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_index_aliases);
        route
            .get("/:name/_alias/:alias")
            .with_path_extractor::<NameAliasPathExtractor>()
            .to(elastic_search_endpoints::es_get_index_named_aliases);
        route
            .head("/:name/_alias/:alias")
            .with_path_extractor::<NameAliasPathExtractor>()
            .to(elastic_search_endpoints::es_head_index_alias);
        route
            .get("/:name/_settings")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_index_settings);
        route
            .get("/:name/_mapping")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_index_mapping);
        route
            .get("/:name/_serve/config")
            .with_path_extractor::<NamePathExtractor>()
            .to(lakehouse_serving::get_serving_config);
        route
            .get("/:name/_serve/layout")
            .with_path_extractor::<NamePathExtractor>()
            .to(lakehouse_serving::get_serving_layout_advice);
        route
            .get("/:name/_dynamodb/config")
            .with_path_extractor::<NamePathExtractor>()
            .to(dynamodb_protocol::get_dynamodb_config);
        route
            .get("/:name/_mongo/config")
            .with_path_extractor::<NamePathExtractor>()
            .to(mongodb_protocol::get_mongodb_config);
        route
            .put("/:name/_serve/config")
            .with_path_extractor::<NamePathExtractor>()
            .to(lakehouse_serving::put_serving_config);
        route
            .put("/:name/_dynamodb/config")
            .with_path_extractor::<NamePathExtractor>()
            .to(dynamodb_protocol::put_dynamodb_config);
        route
            .put("/:name/_mongo/config")
            .with_path_extractor::<NamePathExtractor>()
            .to(mongodb_protocol::put_mongodb_config);
        route
            .post("/_mongo/:database/_command")
            .with_path_extractor::<mongodb_protocol::MongoDatabasePathExtractor>()
            .to(mongodb_protocol::mongodb_command);
        route
            .post("/:name/_serve")
            .with_path_extractor::<NamePathExtractor>()
            .to(lakehouse_serving::serve_query);
        route
            .post("/:name/_serve/cache_manager")
            .with_path_extractor::<NamePathExtractor>()
            .to(lakehouse_serving::manage_serving_cache);
        route
            .post("/:name/_mongo/find")
            .with_path_extractor::<NamePathExtractor>()
            .to(mongodb_protocol::mongodb_find);
        route
            .get("/_index_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_index_template);
        route
            .get("/_component_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_index_template);
        route
            .get("/_search")
            .with_query_string_extractor::<QueryStringSearch>()
            .to(elastic_search_endpoints::es_search);
        route
            .get("/_field_caps")
            .with_query_string_extractor::<QueryStringFieldCaps>()
            .to(elastic_search_endpoints::es_field_caps);
        route
            .post("/_field_caps")
            .with_query_string_extractor::<QueryStringFieldCaps>()
            .to(elastic_search_endpoints::es_field_caps);
        route
            .post("/_search")
            .with_query_string_extractor::<QueryStringSearch>()
            .to(elastic_search_endpoints::es_search);
        route
            .post("/_msearch")
            .with_query_string_extractor::<QueryStringSearch>()
            .to(elastic_search_endpoints::es_msearch);
        route.post("/_mget").to(elastic_search_endpoints::es_mget);
        route
            .get("/:name/_search")
            .with_query_string_extractor::<QueryStringSearch>()
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_search_table);
        route
            .get("/:name/_field_caps")
            .with_query_string_extractor::<QueryStringFieldCaps>()
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_field_caps_index);
        route
            .post("/:name/_field_caps")
            .with_query_string_extractor::<QueryStringFieldCaps>()
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_field_caps_index);
        route
            .get("/_count")
            .with_query_string_extractor::<QueryStringSearch>()
            .to(elastic_search_endpoints::es_count);
        route
            .post("/_count")
            .with_query_string_extractor::<QueryStringSearch>()
            .to(elastic_search_endpoints::es_count);
        route
            .post("/:name/_search")
            .with_query_string_extractor::<QueryStringSearch>()
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_search_table);
        route
            .post("/:name/_msearch")
            .with_query_string_extractor::<QueryStringSearch>()
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_msearch_table);
        route
            .post("/:name/_mget")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_mget_table);
        route
            .get("/:name/_count")
            .with_query_string_extractor::<QueryStringSearch>()
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_count_table);
        route
            .post("/:name/_count")
            .with_query_string_extractor::<QueryStringSearch>()
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_count_table);
        route
            .post("/:name/_create/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_create_with_id);
        route
            .put("/:name/_create/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_create_with_id);
        route
            .post("/:name/_doc")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_index_auto_id);
        route
            .post("/:name/_doc/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_index_with_id);
        route
            .put("/:name/_doc/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_index_with_id);
        route
            .post("/:name/_update/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_update_with_id);
        route
            .get("/:name/_doc/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_get_with_id);
        route
            .head("/:name/_doc/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_head_with_id);
        route
            .delete("/:name/_doc/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_delete_with_id);
        route
            .post("/:name/_pit")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_index_pit);
        route
            .delete("/_pit")
            .to(elastic_search_endpoints::es_delete_pit);

        route
            .put("/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_create_index);
        route
            .post("/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_create_index);
        route
            .put("/_index_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_create_index_template);
        route
            .post("/_index_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_create_index_template);
        route
            .put("/_component_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_create_index_template);
        route
            .post("/_component_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_create_index_template);
        route
            .head("/_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_head_template);
        route
            .head("/_index_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_head_template);
        route
            .get("/_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_template);

        route
            .post("/:name/_update_by_query")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_update_by_query);
        route
            .put("/_aliases")
            .with_query_string_extractor::<QueryStringAliases>()
            .to(elastic_search_endpoints::es_update_aliases);
        route
            .post("/_aliases")
            .with_query_string_extractor::<QueryStringAliases>()
            .to(elastic_search_endpoints::es_update_aliases);

        route.associate("/_bulk", |assoc| {
            assoc.post().to(elastic_search_endpoints::es_bulk_ingest);
            assoc.put().to(elastic_search_endpoints::es_bulk_ingest);
        });

        route
            .get("/_ilm/policy/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_lifetime_policy::es_get_ilm_policy);
        route
            .post("/_ilm/policy/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_lifetime_policy::es_post_ilm_policy);
        route
            .put("/_ilm/policy/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_lifetime_policy::es_post_ilm_policy);
        route
            .put("/_monitoring/bulk")
            .to(elastic_search_lifetime_policy::es_post_monitoring_bulk);
        route
            .post("/_monitoring/bulk")
            .to(elastic_search_lifetime_policy::es_post_monitoring_bulk);
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::sync::LazyLock;
    use std::{env, str};

    use crate::data_contract::{
        CreateTable, DeletesMetadata, FileSetPayload, IcebergFileStats, IcebergMetadata,
        SpeedboatMetadata, TableMetadataCheckpoint,
    };
    use crate::elastic_search_responses::{QueryResultTotal, QueryResults};
    use crate::lakehouse_serving::ServingConfigResponse;
    use crate::router::router;
    use crate::schema_massager::{
        PowdrrDataType, PowdrrField, PowdrrSchema, extract_powdrr_schema_str,
    };
    use crate::serving_plan::ServingQueryClassification;
    use crate::state_provider::STATE_PROVIDER;
    use crate::test_api::{
        CacheMode, CompactionMode, IndexingMode, PeerMode, PeerModeType, PrefetchMode, StateMode,
        StorageMode, TestProcessingMode,
    };
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use gotham::mime;
    use gotham::plain::test::AsyncTestServer;
    use gotham::test::{TestResponse, TestServer};
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tempfile::TempDir;

    pub(crate) static TEST_SERVER: LazyLock<TestServer> =
        LazyLock::new(|| TestServer::with_timeout(router(true), 1000).unwrap());

    fn set_testing_and_processing_mode(test_server: &TestServer) {
        crate::mongodb_protocol::reset_mongodb_cursor_state_for_tests();
        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_and_processing_mode",
                serde_json::to_string(&TestProcessingMode {
                    state_mode: StateMode::Testing,
                    storage_mode: StorageMode::default(),
                    cache_mode: CacheMode::Redis(None),
                    peer_mode: PeerMode::SelfOnly,
                    indexing_mode: IndexingMode::Disabled,
                    compaction_mode: CompactionMode::Disabled,
                    prefetch_mode: PrefetchMode::Disabled,
                })
                .unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
    }

    fn write_mongo_test_parquet(path: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id_seq_no", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["1_1", "2_1"])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "Login attempt failed",
                    "Login successful",
                ])) as ArrayRef,
            ],
        )
        .unwrap();

        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_serving_delete_test_parquet(path: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id_seq_no", DataType::Utf8, false),
            Field::new("snippet", DataType::Utf8, false),
            Field::new("searchTerms", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["doc_1_1", "doc_2_1", "doc_3_1"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["s1", "s2", "s3"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["term-a", "term-b", "term-c"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alpha", "bravo", "charlie"])) as ArrayRef,
            ],
        )
        .unwrap();

        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn add_mongo_parquet_checkpoint(test_server: &TestServer, table_name: &str) -> TempDir {
        let schema = PowdrrSchema::from(&vec![
            PowdrrField {
                name: "_id_seq_no".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "message".to_string(),
                data_type: PowdrrDataType::String,
            },
        ]);
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join(format!("{}.parquet", table_name));
        write_mongo_test_parquet(&parquet_path);
        let checkpoint = TableMetadataCheckpoint {
            table_name: table_name.to_string(),
            original_checkpoint_id: None,
            checkpoint_id: format!("{}_checkpoint_0", table_name),
            iceberg_metadata: Some(IcebergMetadata {
                table_schema: schema.clone(),
                snapshot_id: Some(format!("{}_snapshot_0", table_name)),
                files: FileSetPayload::single(
                    format!("file://{}", parquet_path.display()),
                    fs::metadata(&parquet_path).unwrap().len(),
                    schema.clone(),
                ),
                partition_spec: vec![],
                sort_order: vec![],
                column_names: vec![],
                column_stats: vec![],
                access_artifacts: vec![],
                file_stats: vec![IcebergFileStats {
                    file_path: format!("file://{}", parquet_path.display()),
                    record_count: Some(2),
                    columns: vec![],
                    partition_values: vec![],
                    row_groups: vec![],
                }],
            }),
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema,
        };

        test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        temp_dir
    }

    fn put_mongo_lookup_serving_config(test_server: &TestServer, table_name: &str) {
        let response = test_server
            .client()
            .put(
                &format!("http://localhost/{}/_serve/config", table_name),
                r#"{
                  "patterns": [
                    {
                      "name": "mongo_id_lookup",
                      "eq_fields": ["_id_seq_no"],
                      "max_limit": 1
                    }
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);
    }

    fn put_mongo_cursor_serving_config(test_server: &TestServer, table_name: &str) {
        let response = test_server
            .client()
            .put(
                &format!("http://localhost/{}/_serve/config", table_name),
                r#"{
                  "patterns": [
                    {
                      "name": "mongo_message_top_n",
                      "order_field": "message",
                      "descending": false,
                      "max_limit": 10,
                      "projection": ["message", "_id_seq_no"]
                    }
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);
    }

    fn put_mongo_config(test_server: &TestServer, table_name: &str, collection: &str) {
        put_mongo_config_with_options(test_server, table_name, "powdrr_mongo", collection, true);
    }

    fn put_mongo_config_with_options(
        test_server: &TestServer,
        table_name: &str,
        database: &str,
        collection: &str,
        enabled: bool,
    ) {
        let response = test_server
            .client()
            .put(
                &format!("http://localhost/{}/_mongo/config", table_name),
                json!({
                    "enabled": enabled,
                    "database": database,
                    "collection": collection,
                    "id": { "field": "_id_seq_no" }
                })
                .to_string(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);
    }

    fn perform_mongo_command(
        test_server: &TestServer,
        database: &str,
        payload: Value,
    ) -> TestResponse {
        test_server
            .client()
            .post(
                &format!("http://localhost/_mongo/{}/_command", database),
                payload.to_string(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap()
    }

    #[test]
    fn test_serving_config_and_fast_path_query() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_and_processing_mode",
                serde_json::to_string(&TestProcessingMode {
                    state_mode: StateMode::Testing,
                    storage_mode: StorageMode::default(),
                    cache_mode: CacheMode::Redis(None),
                    peer_mode: PeerMode::SelfOnly,
                    indexing_mode: IndexingMode::Disabled,
                    compaction_mode: CompactionMode::Disabled,
                    prefetch_mode: PrefetchMode::Disabled,
                })
                .unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        let schema = PowdrrSchema::from(&vec![
            PowdrrField {
                name: "_id_seq_no".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "snippet".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "searchTerms".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "title".to_string(),
                data_type: PowdrrDataType::String,
            },
        ]);

        let file_path = format!(
            "file://{}/tests/data/flights.parquet",
            env::current_dir().unwrap().to_str().unwrap()
        );

        let checkpoint = TableMetadataCheckpoint {
            table_name: "serve_flights".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "serve_checkpoint_0".to_string(),
            iceberg_metadata: Some(IcebergMetadata {
                table_schema: schema.clone(),
                snapshot_id: Some("snapshot_1".to_string()),
                files: FileSetPayload::single(file_path, 1, schema.clone()),
                partition_spec: vec![],
                sort_order: vec![],
                column_names: vec![],
                column_stats: vec![],
                access_artifacts: vec![],
                file_stats: vec![],
            }),
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema: schema.clone(),
        };

        test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        let config_response = test_server
            .client()
            .put(
                "http://localhost/serve_flights/_serve/config",
                r#"{
                  "patterns": [
                    {
                      "name": "title_top_n",
                      "order_field": "title",
                      "descending": false,
                      "max_limit": 10,
                      "projection": ["title"]
                    }
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(config_response.status(), 200);

        let get_config_response = test_server
            .client()
            .get("http://localhost/serve_flights/_serve/config")
            .perform()
            .unwrap();

        assert_eq!(get_config_response.status(), 200);
        let config_obj: ServingConfigResponse =
            serde_json::from_str(&get_config_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(config_obj.serving.patterns.len(), 1);
        assert_eq!(config_obj.serving.patterns[0].name, "title_top_n");

        let query_response = test_server
            .client()
            .post(
                "http://localhost/serve_flights/_serve",
                r#"{
                  "select": ["title"],
                  "order_by": [{ "field": "title", "descending": false }],
                  "limit": 2
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(query_response.status(), 200);
        let response_obj: serde_json::Value =
            serde_json::from_str(&query_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            serde_json::from_value::<ServingQueryClassification>(
                response_obj["classification"].clone()
            )
            .unwrap(),
            ServingQueryClassification::FastPath
        );
        assert_eq!(
            response_obj["matched_pattern"].as_str().unwrap(),
            "title_top_n"
        );
        assert_eq!(response_obj["rows"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_serving_query_honors_delete_metadata() {
        let test_server = &*TEST_SERVER;

        set_testing_and_processing_mode(test_server);

        let schema = PowdrrSchema::from(&vec![
            PowdrrField {
                name: "_id_seq_no".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "snippet".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "searchTerms".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "title".to_string(),
                data_type: PowdrrDataType::String,
            },
        ]);

        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("serve_flights_with_deletes.parquet");
        write_serving_delete_test_parquet(&parquet_path);
        let delete_path = temp_dir
            .path()
            .join("serve_flights_with_deletes.delete.json");
        fs::write(&delete_path, "{\"_id_seq_no\":\"doc_1_1\"}\n").unwrap();
        let file_path = format!("file://{}", parquet_path.display());

        let checkpoint = TableMetadataCheckpoint {
            table_name: "serve_flights_with_deletes".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "serve_checkpoint_with_deletes_0".to_string(),
            iceberg_metadata: Some(IcebergMetadata {
                table_schema: schema.clone(),
                snapshot_id: Some("snapshot_with_deletes_1".to_string()),
                files: FileSetPayload::single(
                    file_path,
                    fs::metadata(&parquet_path).unwrap().len(),
                    schema.clone(),
                ),
                partition_spec: vec![],
                sort_order: vec![],
                column_names: vec![],
                column_stats: vec![],
                access_artifacts: vec![],
                file_stats: vec![],
            }),
            speedboat_metadata: None,
            deletes_metadata: Some(DeletesMetadata {
                files: vec![format!("file://{}", delete_path.display())],
            }),
            extension_metadata: HashMap::new(),
            schema: schema.clone(),
        };

        test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        let config_response = test_server
            .client()
            .put(
                "http://localhost/serve_flights_with_deletes/_serve/config",
                r#"{
                  "patterns": [
                    {
                      "name": "title_top_n",
                      "order_field": "title",
                      "descending": false,
                      "max_limit": 10,
                      "projection": ["title"]
                    }
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(config_response.status(), 200);

        let query_response = test_server
            .client()
            .post(
                "http://localhost/serve_flights_with_deletes/_serve",
                r#"{
                  "select": ["title"],
                  "order_by": [{ "field": "title", "descending": false }],
                  "limit": 2
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(query_response.status(), 200);
        let response_obj: serde_json::Value =
            serde_json::from_str(&query_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            serde_json::from_value::<ServingQueryClassification>(
                response_obj["classification"].clone()
            )
            .unwrap(),
            ServingQueryClassification::FastPath
        );
        assert_eq!(
            response_obj["matched_pattern"].as_str().unwrap(),
            "title_top_n"
        );
        let rows = response_obj["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["title"], json!("bravo"));
        assert_eq!(rows[1]["title"], json!("charlie"));
    }

    #[test]
    fn test_mongodb_config_round_trip() {
        let test_server = &*TEST_SERVER;
        let table_name = "mongo_logs_config";

        set_testing_and_processing_mode(test_server);
        let _temp_dir = add_mongo_parquet_checkpoint(test_server, table_name);

        let put_response = test_server
            .client()
            .put(
                &format!("http://localhost/{}/_mongo/config", table_name),
                json!({
                    "enabled": true,
                    "database": "powdrr_mongo",
                    "collection": "logs_roundtrip",
                    "id": { "field": "_id_seq_no" }
                })
                .to_string(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(put_response.status(), 200);
        let put_obj: Value = serde_json::from_str(&put_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(put_obj["acknowledged"], json!(true));
        assert_eq!(put_obj["mongodb"]["collection"], json!("logs_roundtrip"));
        assert_eq!(put_obj["mongodb"]["id"]["field"], json!("_id_seq_no"));

        let get_response = test_server
            .client()
            .get(&format!("http://localhost/{}/_mongo/config", table_name))
            .perform()
            .unwrap();

        assert_eq!(get_response.status(), 200);
        let get_obj: Value = serde_json::from_str(&get_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_obj["acknowledged"], json!(true));
        assert_eq!(get_obj["table"], json!(table_name));
        assert_eq!(get_obj["mongodb"]["database"], json!("powdrr_mongo"));
        assert_eq!(get_obj["mongodb"]["collection"], json!("logs_roundtrip"));
        assert_eq!(get_obj["mongodb"]["id"]["field"], json!("_id_seq_no"));
    }

    #[test]
    fn test_mongodb_command_hello() {
        let test_server = &*TEST_SERVER;

        let response = perform_mongo_command(
            test_server,
            "admin",
            json!({
                "hello": 1,
                "$db": "admin"
            }),
        );

        assert_eq!(response.status(), 200);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(response_obj["ok"], json!(1.0));
        assert_eq!(response_obj["helloOk"], json!(true));
        assert_eq!(response_obj["readOnly"], json!(true));
        assert_eq!(response_obj["maxWireVersion"], json!(21));

        let build_info_response = perform_mongo_command(
            test_server,
            "admin",
            json!({
                "buildInfo": 1,
                "$db": "admin"
            }),
        );

        assert_eq!(build_info_response.status(), 200);
        let build_info_obj: Value =
            serde_json::from_str(&build_info_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(build_info_obj["ok"], json!(1.0));
        assert_eq!(
            build_info_obj["gitVersion"],
            json!("powdrr-mongo-http-bridge")
        );
        assert_eq!(build_info_obj["bits"], json!(64));
    }

    #[test]
    fn test_mongodb_command_list_collections_only_returns_enabled_bindings_for_database() {
        let test_server = &*TEST_SERVER;
        let database = "mongo_list_collections_db";

        set_testing_and_processing_mode(test_server);
        let _alpha_dir = add_mongo_parquet_checkpoint(test_server, "mongo_list_collections_alpha");
        let _beta_dir = add_mongo_parquet_checkpoint(test_server, "mongo_list_collections_beta");
        let _other_dir = add_mongo_parquet_checkpoint(test_server, "mongo_list_collections_other");

        put_mongo_lookup_serving_config(test_server, "mongo_list_collections_alpha");
        put_mongo_lookup_serving_config(test_server, "mongo_list_collections_beta");
        put_mongo_lookup_serving_config(test_server, "mongo_list_collections_other");

        put_mongo_config_with_options(
            test_server,
            "mongo_list_collections_alpha",
            database,
            "alpha",
            true,
        );
        put_mongo_config_with_options(
            test_server,
            "mongo_list_collections_beta",
            database,
            "beta_disabled",
            false,
        );
        put_mongo_config_with_options(
            test_server,
            "mongo_list_collections_other",
            "mongo_list_collections_other_db",
            "other",
            true,
        );

        let response = perform_mongo_command(
            test_server,
            database,
            json!({
                "listCollections": 1,
                "$db": database
            }),
        );

        assert_eq!(response.status(), 200);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            response_obj["cursor"]["ns"],
            json!(format!("{}.$cmd.listCollections", database))
        );
        let first_batch = response_obj["cursor"]["firstBatch"].as_array().unwrap();
        assert!(
            first_batch
                .iter()
                .any(|entry| entry["name"] == json!("alpha"))
        );
        assert!(
            !first_batch
                .iter()
                .any(|entry| entry["name"] == json!("beta_disabled"))
        );
        assert!(
            !first_batch
                .iter()
                .any(|entry| entry["name"] == json!("other"))
        );

        let name_only_response = perform_mongo_command(
            test_server,
            database,
            json!({
                "listCollections": 1,
                "nameOnly": true,
                "filter": { "name": "alpha" },
                "$db": database
            }),
        );

        assert_eq!(name_only_response.status(), 200);
        let name_only_obj: Value =
            serde_json::from_str(&name_only_response.read_utf8_body().unwrap()).unwrap();
        let filtered_batch = name_only_obj["cursor"]["firstBatch"].as_array().unwrap();
        assert_eq!(filtered_batch.len(), 1);
        assert_eq!(filtered_batch[0]["name"], json!("alpha"));
        assert_eq!(filtered_batch[0]["type"], json!("collection"));
        assert!(filtered_batch[0].get("options").is_none());
    }

    #[test]
    fn test_mongodb_command_list_databases_includes_configured_databases() {
        let test_server = &*TEST_SERVER;
        let database = "mongo_list_databases_unique";

        set_testing_and_processing_mode(test_server);
        let _temp_dir = add_mongo_parquet_checkpoint(test_server, "mongo_list_databases_table");
        put_mongo_lookup_serving_config(test_server, "mongo_list_databases_table");
        put_mongo_config_with_options(
            test_server,
            "mongo_list_databases_table",
            database,
            "logs",
            true,
        );

        let response = perform_mongo_command(
            test_server,
            "admin",
            json!({
                "listDatabases": 1,
                "$db": "admin"
            }),
        );

        assert_eq!(response.status(), 200);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        let databases = response_obj["databases"].as_array().unwrap();
        assert!(
            databases
                .iter()
                .any(|entry| entry["name"] == json!(database))
        );
    }

    #[test]
    fn test_mongodb_command_list_indexes_returns_default_id_index() {
        let test_server = &*TEST_SERVER;
        let table_name = "mongo_list_indexes_table";
        let database = "mongo_list_indexes_db";
        let collection = "logs_indexes";

        set_testing_and_processing_mode(test_server);
        let _temp_dir = add_mongo_parquet_checkpoint(test_server, table_name);
        put_mongo_lookup_serving_config(test_server, table_name);
        put_mongo_config_with_options(test_server, table_name, database, collection, true);

        let response = perform_mongo_command(
            test_server,
            database,
            json!({
                "listIndexes": collection,
                "$db": database
            }),
        );

        assert_eq!(response.status(), 200);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            response_obj["cursor"]["ns"],
            json!(format!("{}.$cmd.listIndexes.{}", database, collection))
        );
        let first_batch = response_obj["cursor"]["firstBatch"].as_array().unwrap();
        assert_eq!(first_batch.len(), 1);
        assert_eq!(first_batch[0]["name"], json!("_id_"));
        assert_eq!(first_batch[0]["key"], json!({ "_id": 1 }));
        assert_eq!(
            first_batch[0]["ns"],
            json!(format!("{}.{}", database, collection))
        );
    }

    #[test]
    fn test_mongodb_command_stats_use_collection_metadata() {
        let test_server = &*TEST_SERVER;
        let database = "mongo_stats_db";

        set_testing_and_processing_mode(test_server);
        let _alpha_dir = add_mongo_parquet_checkpoint(test_server, "mongo_stats_alpha_table");
        let _beta_dir = add_mongo_parquet_checkpoint(test_server, "mongo_stats_beta_table");
        let _other_dir = add_mongo_parquet_checkpoint(test_server, "mongo_stats_other_table");

        put_mongo_lookup_serving_config(test_server, "mongo_stats_alpha_table");
        put_mongo_lookup_serving_config(test_server, "mongo_stats_beta_table");
        put_mongo_lookup_serving_config(test_server, "mongo_stats_other_table");

        put_mongo_config_with_options(
            test_server,
            "mongo_stats_alpha_table",
            database,
            "alpha",
            true,
        );
        put_mongo_config_with_options(
            test_server,
            "mongo_stats_beta_table",
            database,
            "beta",
            true,
        );
        put_mongo_config_with_options(
            test_server,
            "mongo_stats_other_table",
            "mongo_stats_other_db",
            "other",
            true,
        );

        let coll_stats_response = perform_mongo_command(
            test_server,
            database,
            json!({
                "collStats": "alpha",
                "$db": database
            }),
        );

        assert_eq!(coll_stats_response.status(), 200);
        let coll_stats_obj: Value =
            serde_json::from_str(&coll_stats_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(coll_stats_obj["ns"], json!(format!("{}.alpha", database)));
        assert_eq!(coll_stats_obj["count"], json!(2));
        assert_eq!(coll_stats_obj["nindexes"], json!(1));
        assert_eq!(coll_stats_obj["indexSizes"], json!({ "_id_": 0 }));
        assert!(coll_stats_obj["storageSize"].as_u64().unwrap() > 0);
        assert!(coll_stats_obj["avgObjSize"].as_f64().unwrap() > 0.0);

        let db_stats_response = perform_mongo_command(
            test_server,
            database,
            json!({
                "dbStats": 1,
                "$db": database
            }),
        );

        assert_eq!(db_stats_response.status(), 200);
        let db_stats_obj: Value =
            serde_json::from_str(&db_stats_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(db_stats_obj["db"], json!(database));
        assert_eq!(db_stats_obj["collections"], json!(2));
        assert_eq!(db_stats_obj["objects"], json!(4));
        assert_eq!(db_stats_obj["indexes"], json!(2));
        assert!(db_stats_obj["storageSize"].as_u64().unwrap() > 0);
        assert!(db_stats_obj["avgObjSize"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn test_mongodb_command_find_resolves_collection_to_table() {
        let test_server = &*TEST_SERVER;
        let table_name = "mongo_logs_command_lookup";
        let database = "mongo_find_command_db";

        set_testing_and_processing_mode(test_server);
        let _temp_dir = add_mongo_parquet_checkpoint(test_server, table_name);
        put_mongo_lookup_serving_config(test_server, table_name);
        put_mongo_config_with_options(test_server, table_name, database, "logs", true);

        let response = perform_mongo_command(
            test_server,
            database,
            json!({
                "find": "logs",
                "filter": { "_id": "1_1" },
                "projection": { "message": 1 },
                "limit": 1,
                "$db": database
            }),
        );

        assert_eq!(response.status(), 200);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(response_obj["ok"], json!(1.0));
        assert_eq!(
            response_obj["cursor"]["ns"],
            json!(format!("{}.logs", database))
        );
        let row = response_obj["cursor"]["firstBatch"][0].as_object().unwrap();
        assert_eq!(row.get("_id"), Some(&json!("1_1")));
        assert_eq!(row.get("message"), Some(&json!("Login attempt failed")));
        assert!(row.get("_id_seq_no").is_none());
    }

    #[test]
    fn test_mongodb_command_find_get_more_and_kill_cursors() {
        let test_server = &*TEST_SERVER;
        let table_name = "mongo_logs_cursor_lookup";
        let database = "mongo_cursor_command_db";
        let collection = "logs_cursor";

        set_testing_and_processing_mode(test_server);
        let _temp_dir = add_mongo_parquet_checkpoint(test_server, table_name);
        put_mongo_cursor_serving_config(test_server, table_name);
        put_mongo_config_with_options(test_server, table_name, database, collection, true);

        let find_response = perform_mongo_command(
            test_server,
            database,
            json!({
                "find": collection,
                "sort": { "message": 1 },
                "projection": { "message": 1 },
                "limit": 2,
                "batchSize": 1,
                "$db": database
            }),
        );

        assert_eq!(find_response.status(), 200);
        let find_obj: Value =
            serde_json::from_str(&find_response.read_utf8_body().unwrap()).unwrap();
        let cursor_id = find_obj["cursor"]["id"].as_i64().unwrap();
        assert!(cursor_id > 0);
        assert_eq!(
            find_obj["cursor"]["firstBatch"].as_array().unwrap().len(),
            1
        );

        let get_more_response = perform_mongo_command(
            test_server,
            database,
            json!({
                "getMore": cursor_id,
                "collection": collection,
                "batchSize": 1,
                "$db": database
            }),
        );

        assert_eq!(get_more_response.status(), 200);
        let get_more_obj: Value =
            serde_json::from_str(&get_more_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_more_obj["cursor"]["id"], json!(0));
        assert_eq!(
            get_more_obj["cursor"]["ns"],
            json!(format!("{}.{}", database, collection))
        );
        let next_batch = get_more_obj["cursor"]["nextBatch"].as_array().unwrap();
        assert_eq!(next_batch.len(), 1);
        assert_eq!(next_batch[0]["message"], json!("Login successful"));

        let kill_find_response = perform_mongo_command(
            test_server,
            database,
            json!({
                "find": collection,
                "sort": { "message": 1 },
                "projection": { "message": 1 },
                "limit": 2,
                "batchSize": 1,
                "$db": database
            }),
        );

        let kill_find_obj: Value =
            serde_json::from_str(&kill_find_response.read_utf8_body().unwrap()).unwrap();
        let kill_cursor_id = kill_find_obj["cursor"]["id"].as_i64().unwrap();
        assert!(kill_cursor_id > 0);

        let kill_response = perform_mongo_command(
            test_server,
            database,
            json!({
                "killCursors": collection,
                "cursors": [kill_cursor_id],
                "$db": database
            }),
        );

        assert_eq!(kill_response.status(), 200);
        let kill_obj: Value =
            serde_json::from_str(&kill_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(kill_obj["cursorsKilled"], json!([kill_cursor_id]));
        assert_eq!(kill_obj["cursorsNotFound"], json!([]));

        let expiry_table = "mongo_logs_cursor_expiry";
        let expiry_database = "mongo_cursor_expiry_db";
        let expiry_collection = "logs_cursor_expiry";
        let expiry_base_time_ms = 1_000_000;

        set_testing_and_processing_mode(test_server);
        crate::mongodb_protocol::set_mongodb_cursor_time_for_tests(Some(expiry_base_time_ms));
        let _expiry_temp_dir = add_mongo_parquet_checkpoint(test_server, expiry_table);
        put_mongo_cursor_serving_config(test_server, expiry_table);
        put_mongo_config_with_options(
            test_server,
            expiry_table,
            expiry_database,
            expiry_collection,
            true,
        );

        let expiry_find_response = perform_mongo_command(
            test_server,
            expiry_database,
            json!({
                "find": expiry_collection,
                "sort": { "message": 1 },
                "projection": { "message": 1 },
                "limit": 2,
                "batchSize": 1,
                "$db": expiry_database
            }),
        );
        let expiry_find_obj: Value =
            serde_json::from_str(&expiry_find_response.read_utf8_body().unwrap()).unwrap();
        let expiry_cursor_id = expiry_find_obj["cursor"]["id"].as_i64().unwrap();
        assert!(expiry_cursor_id > 0);

        crate::mongodb_protocol::set_mongodb_cursor_time_for_tests(Some(
            expiry_base_time_ms
                + crate::mongodb_protocol::mongodb_cursor_timeout_ms_for_tests()
                + 1,
        ));

        let expiry_get_more_response = perform_mongo_command(
            test_server,
            expiry_database,
            json!({
                "getMore": expiry_cursor_id,
                "collection": expiry_collection,
                "$db": expiry_database
            }),
        );
        assert_eq!(expiry_get_more_response.status(), 404);
        let expiry_get_more_obj: Value =
            serde_json::from_str(&expiry_get_more_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(expiry_get_more_obj["ok"], json!(0.0));
        assert_eq!(expiry_get_more_obj["code"], json!(43));
        assert_eq!(expiry_get_more_obj["codeName"], json!("CursorNotFound"));

        let no_timeout_table = "mongo_logs_cursor_no_timeout";
        let no_timeout_database = "mongo_cursor_no_timeout_db";
        let no_timeout_collection = "logs_cursor_no_timeout";
        let no_timeout_base_time_ms = 2_000_000;

        set_testing_and_processing_mode(test_server);
        crate::mongodb_protocol::set_mongodb_cursor_time_for_tests(Some(no_timeout_base_time_ms));
        let _no_timeout_temp_dir = add_mongo_parquet_checkpoint(test_server, no_timeout_table);
        put_mongo_cursor_serving_config(test_server, no_timeout_table);
        put_mongo_config_with_options(
            test_server,
            no_timeout_table,
            no_timeout_database,
            no_timeout_collection,
            true,
        );

        let no_timeout_find_response = perform_mongo_command(
            test_server,
            no_timeout_database,
            json!({
                "find": no_timeout_collection,
                "sort": { "message": 1 },
                "projection": { "message": 1 },
                "limit": 2,
                "batchSize": 1,
                "noCursorTimeout": true,
                "$db": no_timeout_database
            }),
        );
        let no_timeout_find_obj: Value =
            serde_json::from_str(&no_timeout_find_response.read_utf8_body().unwrap()).unwrap();
        let no_timeout_cursor_id = no_timeout_find_obj["cursor"]["id"].as_i64().unwrap();
        assert!(no_timeout_cursor_id > 0);

        crate::mongodb_protocol::set_mongodb_cursor_time_for_tests(Some(
            no_timeout_base_time_ms
                + crate::mongodb_protocol::mongodb_cursor_timeout_ms_for_tests()
                + 1,
        ));

        let no_timeout_get_more_response = perform_mongo_command(
            test_server,
            no_timeout_database,
            json!({
                "getMore": no_timeout_cursor_id,
                "collection": no_timeout_collection,
                "$db": no_timeout_database
            }),
        );
        assert_eq!(no_timeout_get_more_response.status(), 200);
        let no_timeout_get_more_obj: Value =
            serde_json::from_str(&no_timeout_get_more_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(no_timeout_get_more_obj["cursor"]["id"], json!(0));
        let no_timeout_next_batch = no_timeout_get_more_obj["cursor"]["nextBatch"]
            .as_array()
            .unwrap();
        assert_eq!(no_timeout_next_batch.len(), 1);
        assert_eq!(
            no_timeout_next_batch[0]["message"],
            json!("Login successful")
        );
    }

    #[test]
    fn test_mongodb_config_rejects_duplicate_enabled_namespace() {
        let test_server = &*TEST_SERVER;
        let database = "mongo_duplicate_config_db";
        let collection = "duplicate_logs";

        set_testing_and_processing_mode(test_server);
        let _first_dir = add_mongo_parquet_checkpoint(test_server, "mongo_duplicate_config_first");
        let _second_dir =
            add_mongo_parquet_checkpoint(test_server, "mongo_duplicate_config_second");

        put_mongo_lookup_serving_config(test_server, "mongo_duplicate_config_first");
        put_mongo_lookup_serving_config(test_server, "mongo_duplicate_config_second");

        put_mongo_config_with_options(
            test_server,
            "mongo_duplicate_config_first",
            database,
            collection,
            true,
        );

        let response = test_server
            .client()
            .put(
                "http://localhost/mongo_duplicate_config_second/_mongo/config",
                json!({
                    "enabled": true,
                    "database": database,
                    "collection": collection,
                    "id": { "field": "_id_seq_no" }
                })
                .to_string(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response.status(), 400);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(response_obj["codeName"], json!("BadValue"));
        assert!(
            response_obj["errmsg"]
                .as_str()
                .unwrap()
                .contains("already exposed by table mongo_duplicate_config_first")
        );
    }

    #[test]
    fn test_mongodb_find_http_bridge_requires_mongo_config() {
        let test_server = &*TEST_SERVER;
        let table_name = "mongo_logs_unconfigured";

        set_testing_and_processing_mode(test_server);
        let _temp_dir = add_mongo_parquet_checkpoint(test_server, table_name);
        put_mongo_lookup_serving_config(test_server, table_name);

        let response = test_server
            .client()
            .post(
                &format!("http://localhost/{}/_mongo/find", table_name),
                json!({
                    "find": table_name,
                    "limit": 1
                })
                .to_string(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response.status(), 404);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(response_obj["ok"], json!(0.0));
        assert_eq!(response_obj["code"], json!(26));
        assert_eq!(response_obj["codeName"], json!("NamespaceNotFound"));
    }

    #[test]
    fn test_mongodb_find_http_bridge_uses_id_mapping_and_collection_namespace() {
        let test_server = &*TEST_SERVER;
        let table_name = "mongo_logs_lookup";

        set_testing_and_processing_mode(test_server);
        let _temp_dir = add_mongo_parquet_checkpoint(test_server, table_name);
        put_mongo_lookup_serving_config(test_server, table_name);
        put_mongo_config(test_server, table_name, "logs");

        let response = test_server
            .client()
            .post(
                &format!("http://localhost/{}/_mongo/find", table_name),
                json!({
                    "find": "logs",
                    "filter": { "_id": "1_1" },
                    "projection": { "message": 1 },
                    "limit": 1
                })
                .to_string(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(response_obj["ok"], json!(1.0));
        assert_eq!(response_obj["cursor"]["ns"], json!("powdrr_mongo.logs"));
        let row = response_obj["cursor"]["firstBatch"][0].as_object().unwrap();
        assert_eq!(row.get("_id"), Some(&json!("1_1")));
        assert_eq!(row.get("message"), Some(&json!("Login attempt failed")));
        assert!(row.get("_id_seq_no").is_none());
    }

    #[test]
    fn test_mongodb_find_http_bridge_respects_id_exclusion_projection() {
        let test_server = &*TEST_SERVER;
        let table_name = "mongo_logs_projection";

        set_testing_and_processing_mode(test_server);
        let _temp_dir = add_mongo_parquet_checkpoint(test_server, table_name);
        put_mongo_lookup_serving_config(test_server, table_name);
        put_mongo_config(test_server, table_name, "logs_projection");

        let response = test_server
            .client()
            .post(
                &format!("http://localhost/{}/_mongo/find", table_name),
                json!({
                    "find": "logs_projection",
                    "filter": { "_id": "1_1" },
                    "projection": { "message": 1, "_id": 0 },
                    "limit": 1
                })
                .to_string(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        let row = response_obj["cursor"]["firstBatch"][0].as_object().unwrap();
        assert_eq!(row.get("message"), Some(&json!("Login attempt failed")));
        assert!(row.get("_id").is_none());
        assert!(row.get("_id_seq_no").is_none());
    }

    #[test]
    fn test_mongodb_find_http_bridge_rejects_collection_mismatch_under_config() {
        let test_server = &*TEST_SERVER;
        let table_name = "mongo_logs_mismatch";

        set_testing_and_processing_mode(test_server);
        let _temp_dir = add_mongo_parquet_checkpoint(test_server, table_name);
        put_mongo_lookup_serving_config(test_server, table_name);
        put_mongo_config(test_server, table_name, "logs_mismatch");

        let response = test_server
            .client()
            .post(
                &format!("http://localhost/{}/_mongo/find", table_name),
                json!({
                    "find": "other_collection",
                    "filter": { "_id": "1_1" },
                    "limit": 1
                })
                .to_string(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response.status(), 400);
        let response_obj: Value =
            serde_json::from_str(&response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(response_obj["ok"], json!(0.0));
        assert_eq!(response_obj["code"], json!(2));
        assert_eq!(response_obj["codeName"], json!("BadValue"));
        assert!(
            response_obj["errmsg"]
                .as_str()
                .unwrap()
                .contains("is exposed as Mongo collection logs_mismatch")
        );
    }

    #[test]
    fn test_es_bulk_create() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_and_processing_mode",
                serde_json::to_string(&TestProcessingMode {
                    state_mode: StateMode::Testing,
                    storage_mode: StorageMode::default(),
                    cache_mode: CacheMode::Redis(None),
                    peer_mode: PeerMode::SelfOnly,
                    indexing_mode: IndexingMode::Sync,
                    compaction_mode: CompactionMode::Disabled,
                    prefetch_mode: PrefetchMode::Disabled,
                })
                .unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        let checkpoint = TableMetadataCheckpoint {
            table_name: "logs".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "0".to_string(),
            iceberg_metadata: None,
            speedboat_metadata: Some(SpeedboatMetadata {
                files: FileSetPayload::single(
                    format!(
                        "file://{}/tests/data/logs.json",
                        env::current_dir().unwrap().to_str().unwrap()
                    ),
                    include_str!("../tests/data/logs.json").len() as u64,
                    extract_powdrr_schema_str(include_str!("../tests/data/logs.json")),
                ),
            }),
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema: PowdrrSchema::minimal(),
        };

        let checkpoint_response = test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform();

        match checkpoint_response {
            Err(_) => panic!("test setup failed"),
            Ok(_) => (),
        };

        let body_create_index = r#"{
    "settings" : {
        "index": {
        "number_of_shards" : 2,
        "number_of_replicas" : 1
    } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert!(response_create_index.status() == 200 || response_create_index.status() == 208);

        let body = r#"{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:04:05.000Z", "index_col": 1, "user": { "id": "vlb44hny" }, "message": "Login attempt failed" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:06:07.000Z", "index_col": 2, "user": { "id": "8a4f500d" }, "message": "Login successful" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-09T11:07:08.000Z", "index_col": 3, "user": { "id": "l7gk7f82" }, "message": "Logout successful" }"#;

        let response = test_server
            .client()
            .post("http://localhost/_bulk", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);
    }

    #[test]
    fn test_es_read_only_metadata_and_count_subset() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_and_processing_mode",
                serde_json::to_string(&TestProcessingMode {
                    state_mode: StateMode::Testing,
                    storage_mode: StorageMode::default(),
                    cache_mode: CacheMode::Redis(None),
                    peer_mode: PeerMode::SelfOnly,
                    indexing_mode: IndexingMode::Sync,
                    compaction_mode: CompactionMode::Disabled,
                    prefetch_mode: PrefetchMode::Disabled,
                })
                .unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        let original_index_body = r#"{
          "settings": {
            "index": {
              "number_of_shards": 2,
              "number_of_replicas": 1
            }
          },
          "aliases": {
            "logs_alias": {
              "is_hidden": false
            }
          },
          "mappings": {
            "dynamic": false,
            "properties": {
              "message": {
                "type": "text"
              },
              "index_col": {
                "type": "long"
              }
            }
          }
        }"#;

        futures::executor::block_on(STATE_PROVIDER.upsert_table_metadata(&CreateTable {
            name: "logs".to_string(),
            tags: HashMap::from([("_es_original".to_string(), original_index_body.to_string())]),
            serving: None,
            dynamodb: None,
            mongodb: None,
        }))
        .unwrap();
        futures::executor::block_on(
            STATE_PROVIDER.upsert_table_metadata(&CreateTable {
                name: "logs_archive".to_string(),
                tags: HashMap::from([(
                    "_es_original".to_string(),
                    json!({
                        "aliases": {
                            "logs_alias": {},
                            "archive_alias": {}
                        },
                        "mappings": {
                            "properties": {
                                "index_col": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "settings": {
                            "index": {
                                "number_of_shards": 1,
                                "number_of_replicas": 0
                            }
                        }
                    })
                    .to_string(),
                )]),
                serving: None,
                dynamodb: None,
                mongodb: None,
            }),
        )
        .unwrap();
        futures::executor::block_on(
            STATE_PROVIDER.upsert_table_metadata(&CreateTable {
                name: "events_extra".to_string(),
                tags: HashMap::from([(
                    "_es_original".to_string(),
                    json!({
                        "mappings": {
                            "properties": {
                                "message": {
                                    "type": "text"
                                },
                                "index_col": {
                                    "type": "long"
                                }
                            }
                        },
                        "settings": {
                            "index": {
                                "number_of_shards": 1,
                                "number_of_replicas": 0
                            }
                        }
                    })
                    .to_string(),
                )]),
                serving: None,
                dynamodb: None,
                mongodb: None,
            }),
        )
        .unwrap();

        let checkpoint = TableMetadataCheckpoint {
            table_name: "logs".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "logs_checkpoint_0".to_string(),
            iceberg_metadata: None,
            speedboat_metadata: Some(SpeedboatMetadata {
                files: FileSetPayload::single(
                    format!(
                        "file://{}/tests/data/logs.json",
                        env::current_dir().unwrap().to_str().unwrap()
                    ),
                    include_str!("../tests/data/logs.json").len() as u64,
                    extract_powdrr_schema_str(include_str!("../tests/data/logs.json")),
                ),
            }),
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema: PowdrrSchema::minimal(),
        };

        test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        let events_extra_checkpoint = TableMetadataCheckpoint {
            table_name: "events_extra".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "events_extra_checkpoint_0".to_string(),
            iceberg_metadata: None,
            speedboat_metadata: Some(SpeedboatMetadata {
                files: FileSetPayload::single(
                    format!(
                        "file://{}/tests/data/events_extra.json",
                        env::current_dir().unwrap().to_str().unwrap()
                    ),
                    include_str!("../tests/data/events_extra.json").len() as u64,
                    extract_powdrr_schema_str(include_str!("../tests/data/events_extra.json")),
                ),
            }),
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema: PowdrrSchema::minimal(),
        };

        test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&events_extra_checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        let head_root = test_server
            .client()
            .head("http://localhost/")
            .perform()
            .unwrap();
        assert_eq!(head_root.status(), 200);

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();
        assert_eq!(process_work_response.status(), 200);

        let get_index_response = test_server
            .client()
            .get("http://localhost/logs")
            .perform()
            .unwrap();
        assert_eq!(get_index_response.status(), 200);
        let get_index_json: Value =
            serde_json::from_str(&get_index_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            get_index_json["logs"]["mappings"]["properties"]["message"]["type"],
            "text"
        );
        assert_eq!(
            get_index_json["logs"]["settings"]["index"]["number_of_shards"],
            "2"
        );
        assert_eq!(
            get_index_json["logs"]["settings"]["index"]["number_of_replicas"],
            "1"
        );
        assert_eq!(get_index_json["logs"]["aliases"]["logs_alias"], json!({}));

        let get_mapping_response = test_server
            .client()
            .get("http://localhost/logs/_mapping")
            .perform()
            .unwrap();
        assert_eq!(get_mapping_response.status(), 200);
        let get_mapping_json: Value =
            serde_json::from_str(&get_mapping_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            get_mapping_json["logs"]["mappings"]["properties"]["index_col"]["type"],
            "long"
        );

        let get_settings_response = test_server
            .client()
            .get("http://localhost/logs/_settings")
            .perform()
            .unwrap();
        assert_eq!(get_settings_response.status(), 200);
        let get_settings_json: Value =
            serde_json::from_str(&get_settings_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            get_settings_json["logs"]["settings"]["index"]["number_of_shards"],
            "2"
        );

        let get_wildcard_index_response = test_server
            .client()
            .get("http://localhost/logs*/_mapping")
            .perform()
            .unwrap();
        assert_eq!(get_wildcard_index_response.status(), 200);
        let get_wildcard_index_json: Value =
            serde_json::from_str(&get_wildcard_index_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            get_wildcard_index_json
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["logs".to_string(), "logs_archive".to_string()]
        );

        let cluster_health_response = test_server
            .client()
            .get("http://localhost/_cluster/health")
            .perform()
            .unwrap();
        assert_eq!(cluster_health_response.status(), 200);
        let cluster_health_json: Value =
            serde_json::from_str(&cluster_health_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(cluster_health_json["cluster_name"], "docker-cluster");
        assert_eq!(cluster_health_json["status"], "green");
        assert_eq!(cluster_health_json["number_of_nodes"], 1);

        let alias_update_response = test_server
            .client()
            .post(
                "http://localhost/_aliases",
                r#"{
                  "actions": [
                    {
                      "add": {
                        "index": "logs",
                        "alias": "logs_secondary"
                      }
                    }
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(alias_update_response.status(), 200);

        let get_alias_response = test_server
            .client()
            .get("http://localhost/logs/_alias")
            .perform()
            .unwrap();
        assert_eq!(get_alias_response.status(), 200);
        let get_alias_json: Value =
            serde_json::from_str(&get_alias_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_alias_json["logs"]["aliases"]["logs_alias"], json!({}));
        assert_eq!(
            get_alias_json["logs"]["aliases"]["logs_secondary"],
            json!({})
        );

        let get_global_aliases_response = test_server
            .client()
            .get("http://localhost/_alias")
            .perform()
            .unwrap();
        assert_eq!(get_global_aliases_response.status(), 200);
        let get_global_aliases_json: Value =
            serde_json::from_str(&get_global_aliases_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            get_global_aliases_json["logs"]["aliases"]["logs_alias"],
            json!({})
        );
        assert_eq!(
            get_global_aliases_json["logs_archive"]["aliases"]["archive_alias"],
            json!({})
        );

        let get_named_aliases_response = test_server
            .client()
            .get("http://localhost/_alias/logs_alias")
            .perform()
            .unwrap();
        assert_eq!(get_named_aliases_response.status(), 200);
        let get_named_aliases_json: Value =
            serde_json::from_str(&get_named_aliases_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            get_named_aliases_json["logs"]["aliases"]["logs_alias"],
            json!({})
        );
        assert_eq!(
            get_named_aliases_json["logs_archive"]["aliases"]["logs_alias"],
            json!({})
        );
        assert!(
            get_named_aliases_json["logs"]["aliases"]
                .get("logs_secondary")
                .is_none()
        );

        let get_index_named_alias_response = test_server
            .client()
            .get("http://localhost/logs/_alias/logs_secondary")
            .perform()
            .unwrap();
        assert_eq!(get_index_named_alias_response.status(), 200);
        let get_index_named_alias_json: Value =
            serde_json::from_str(&get_index_named_alias_response.read_utf8_body().unwrap())
                .unwrap();
        assert_eq!(
            get_index_named_alias_json["logs"]["aliases"]["logs_secondary"],
            json!({})
        );

        let head_index_alias_response = test_server
            .client()
            .head("http://localhost/logs/_alias/logs_secondary")
            .perform()
            .unwrap();
        assert_eq!(head_index_alias_response.status(), 200);

        let missing_head_index_alias_response = test_server
            .client()
            .head("http://localhost/logs/_alias/does_not_exist")
            .perform()
            .unwrap();
        assert_eq!(missing_head_index_alias_response.status(), 404);

        let resolve_index_response = test_server
            .client()
            .get("http://localhost/_resolve/index/logs")
            .perform()
            .unwrap();
        assert_eq!(resolve_index_response.status(), 200);
        let resolve_index_json: Value =
            serde_json::from_str(&resolve_index_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(resolve_index_json["indices"][0]["name"], "logs");
        assert_eq!(
            resolve_index_json["indices"][0]["attributes"],
            json!(["open"])
        );
        assert_eq!(
            resolve_index_json["indices"][0]["aliases"],
            json!(["logs_alias", "logs_secondary"])
        );
        assert_eq!(resolve_index_json["aliases"], json!([]));

        let resolve_alias_response = test_server
            .client()
            .get("http://localhost/_resolve/index/logs_alias")
            .perform()
            .unwrap();
        assert_eq!(resolve_alias_response.status(), 200);
        let resolve_alias_json: Value =
            serde_json::from_str(&resolve_alias_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(resolve_alias_json["indices"], json!([]));
        assert_eq!(
            resolve_alias_json["aliases"][0],
            json!({
                "name": "logs_alias",
                "indices": ["logs", "logs_archive"]
            })
        );

        let get_field_caps_response = test_server
            .client()
            .get("http://localhost/logs/_field_caps?fields=message,index_col")
            .perform()
            .unwrap();
        assert_eq!(get_field_caps_response.status(), 200);
        let get_field_caps_json: Value =
            serde_json::from_str(&get_field_caps_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_field_caps_json["indices"], json!(["logs"]));
        assert_eq!(
            get_field_caps_json["fields"]["message"]["text"]["searchable"],
            true
        );
        assert_eq!(
            get_field_caps_json["fields"]["message"]["text"]["aggregatable"],
            false
        );
        assert_eq!(
            get_field_caps_json["fields"]["index_col"]["long"]["indices"],
            json!(["logs"])
        );

        let post_field_caps_response = test_server
            .client()
            .post(
                "http://localhost/_field_caps",
                r#"{
                  "fields": ["index_col"]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(post_field_caps_response.status(), 200);
        let post_field_caps_json: Value =
            serde_json::from_str(&post_field_caps_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            post_field_caps_json["indices"],
            json!(["logs", "logs_archive"])
        );
        assert_eq!(
            post_field_caps_json["fields"]["index_col"]["long"]["indices"],
            json!(["logs"])
        );
        assert_eq!(
            post_field_caps_json["fields"]["index_col"]["keyword"]["indices"],
            json!(["logs_archive"])
        );

        let get_search_response = test_server
            .client()
            .get("http://localhost/logs/_search")
            .perform()
            .unwrap();
        assert_eq!(get_search_response.status(), 200);
        let get_search_json: Value =
            serde_json::from_str(&get_search_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_search_json["hits"]["total"]["value"], 6);

        let get_count_response = test_server
            .client()
            .get("http://localhost/logs/_count")
            .perform()
            .unwrap();
        assert_eq!(get_count_response.status(), 200);
        let get_count_json: Value =
            serde_json::from_str(&get_count_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_count_json["count"], 6);

        let wildcard_count_response = test_server
            .client()
            .post(
                "http://localhost/logs,does-not-exist/_count?ignore_unavailable=true",
                "{}",
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(wildcard_count_response.status(), 200);
        let wildcard_count_json: Value =
            serde_json::from_str(&wildcard_count_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(wildcard_count_json["count"], 6);

        let missing_target_count_response = test_server
            .client()
            .post(
                "http://localhost/logs,does-not-exist/_count",
                "{}",
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(missing_target_count_response.status(), 404);

        let filtered_count_response = test_server
            .client()
            .post(
                "http://localhost/logs/_count",
                r#"{
                  "query": {
                    "match": {
                      "message": {
                        "query": "Login"
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(filtered_count_response.status(), 200);
        let filtered_count_json: Value =
            serde_json::from_str(&filtered_count_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(filtered_count_json["count"], 4);

        let terms_query_response = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                r#"{
                  "query": {
                    "terms": {
                      "index_col": [2, 5]
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(terms_query_response.status(), 200);
        let terms_query_json: Value =
            serde_json::from_str(&terms_query_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(terms_query_json["hits"]["total"]["value"], 2);

        let ids_query_response = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                r#"{
                  "query": {
                    "ids": {
                      "values": ["2", "5"]
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(ids_query_response.status(), 200);
        let ids_query_json: Value =
            serde_json::from_str(&ids_query_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(ids_query_json["hits"]["total"]["value"], 2);

        let multi_match_query_response = test_server
            .client()
            .post(
                "http://localhost/logs,_events_does_not_exist/_search?ignore_unavailable=true",
                r#"{
                  "query": {
                    "multi_match": {
                      "query": "Login",
                      "fields": ["message"]
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(multi_match_query_response.status(), 200);
        let multi_match_query_json: Value =
            serde_json::from_str(&multi_match_query_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(multi_match_query_json["hits"]["total"]["value"], 4);

        let mget_table_response = test_server
            .client()
            .post(
                "http://localhost/logs/_mget",
                r#"{
                  "ids": ["1", "999"]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(mget_table_response.status(), 200);
        let mget_table_json: Value =
            serde_json::from_str(&mget_table_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(mget_table_json["docs"][0]["found"], true);
        assert_eq!(mget_table_json["docs"][0]["_id"], "1");
        assert_eq!(
            mget_table_json["docs"][0]["_source"]["message"],
            "Login attempt failed"
        );
        assert_eq!(mget_table_json["docs"][1]["found"], false);

        let mget_alias_response = test_server
            .client()
            .post(
                "http://localhost/_mget",
                r#"{
                  "docs": [
                    {"_index": "logs_secondary", "_id": "2"}
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(mget_alias_response.status(), 200);
        let mget_alias_json: Value =
            serde_json::from_str(&mget_alias_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(mget_alias_json["docs"][0]["found"], true);
        assert_eq!(
            mget_alias_json["docs"][0]["_source"]["message"],
            "Login successful"
        );

        let mget_global_response = test_server
            .client()
            .post(
                "http://localhost/_mget",
                r#"{
                  "docs": [
                    {"_index": "logs", "_id": "2"},
                    {"_index": "does-not-exist", "_id": "1"}
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(mget_global_response.status(), 200);
        let mget_global_json: Value =
            serde_json::from_str(&mget_global_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(mget_global_json["docs"][0]["found"], true);
        assert_eq!(
            mget_global_json["docs"][0]["_source"]["message"],
            "Login successful"
        );
        assert_eq!(mget_global_json["docs"][1]["found"], false);

        let msearch_table_response = test_server
            .client()
            .post(
                "http://localhost/logs/_msearch",
                "{}\n{\"query\":{\"match\":{\"message\":{\"query\":\"Login\"}}}}\n{}\n{\"query\":{\"match\":{\"message\":{\"query\":\"Logout\"}}}}\n",
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(msearch_table_response.status(), 200);
        let msearch_table_json: Value =
            serde_json::from_str(&msearch_table_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            msearch_table_json["responses"][0]["hits"]["total"]["value"],
            4
        );
        assert_eq!(
            msearch_table_json["responses"][1]["hits"]["total"]["value"],
            2
        );

        let wildcard_search_response = test_server
            .client()
            .post(
                "http://localhost/logs_secondary,does-not-exist/_search?ignore_unavailable=true",
                "{}",
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(wildcard_search_response.status(), 200);
        let wildcard_search_json: Value =
            serde_json::from_str(&wildcard_search_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(wildcard_search_json["hits"]["total"]["value"], 6);

        let wildcard_multi_index_search_response = test_server
            .client()
            .post(
                "http://localhost/logs*/_search",
                r#"{
                  "query": {
                    "match": {
                      "message": {
                        "query": "Login"
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(wildcard_multi_index_search_response.status(), 200);
        let wildcard_multi_index_search_json: Value = serde_json::from_str(
            &wildcard_multi_index_search_response
                .read_utf8_body()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            wildcard_multi_index_search_json["hits"]["total"]["value"],
            4
        );

        let msearch_global_response = test_server
            .client()
            .post(
                "http://localhost/_msearch",
                "{\"index\":\"logs\"}\n{\"query\":{\"match\":{\"message\":{\"query\":\"Login\"}}}}\n{\"index\":\"does-not-exist\"}\n{}\n",
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(msearch_global_response.status(), 200);
        let msearch_global_json: Value =
            serde_json::from_str(&msearch_global_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            msearch_global_json["responses"][0]["hits"]["total"]["value"],
            4
        );
        assert_eq!(msearch_global_json["responses"][1]["status"], 404);
        assert_eq!(
            msearch_global_json["responses"][1]["error"]["reason"],
            "Index does not exist"
        );

        let wildcard_msearch_response = test_server
            .client()
            .post(
                "http://localhost/_msearch?ignore_unavailable=true",
                "{\"index\":\"logs*,does-not-exist\"}\n{\"query\":{\"match\":{\"message\":{\"query\":\"Login\"}}}}\n",
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(wildcard_msearch_response.status(), 200);
        let wildcard_msearch_json: Value =
            serde_json::from_str(&wildcard_msearch_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            wildcard_msearch_json["responses"][0]["hits"]["total"]["value"],
            4
        );

        let get_alias_doc_response = test_server
            .client()
            .get("http://localhost/logs_secondary/_doc/2")
            .perform()
            .unwrap();
        assert_eq!(get_alias_doc_response.status(), 200);
        let get_alias_doc_json: Value =
            serde_json::from_str(&get_alias_doc_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_alias_doc_json["_id"], "2");
        assert_eq!(get_alias_doc_json["_source"]["message"], "Login successful");

        let head_alias_doc_response = test_server
            .client()
            .head("http://localhost/logs_secondary/_doc/2")
            .perform()
            .unwrap();
        assert_eq!(head_alias_doc_response.status(), 200);

        let missing_head_alias_doc_response = test_server
            .client()
            .head("http://localhost/logs_secondary/_doc/999")
            .perform()
            .unwrap();
        assert_eq!(missing_head_alias_doc_response.status(), 404);

        let sorted_search_response = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                r#"{
                  "size": 2,
                  "sort": [
                    {
                      "index_col": {
                        "order": "asc"
                      }
                    }
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(sorted_search_response.status(), 200);
        let sorted_search_json: Value =
            serde_json::from_str(&sorted_search_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(sorted_search_json["hits"]["hits"][0]["sort"], json!([1]));
        assert_eq!(sorted_search_json["hits"]["hits"][1]["sort"], json!([2]));

        let search_after_response = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                r#"{
                  "size": 2,
                  "search_after": [2],
                  "sort": [
                    {
                      "index_col": {
                        "order": "asc"
                      }
                    }
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(search_after_response.status(), 200);
        let search_after_json: Value =
            serde_json::from_str(&search_after_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(search_after_json["hits"]["hits"][0]["sort"], json!([3]));
        assert_eq!(search_after_json["hits"]["hits"][1]["sort"], json!([4]));

        let multi_index_sorted_search_response = test_server
            .client()
            .post(
                "http://localhost/logs,events_extra/_search",
                r#"{
                  "size": 3,
                  "sort": [
                    {
                      "index_col": {
                        "order": "asc"
                      }
                    }
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(multi_index_sorted_search_response.status(), 200);
        let multi_index_sorted_search_json: Value =
            serde_json::from_str(&multi_index_sorted_search_response.read_utf8_body().unwrap())
                .unwrap();
        assert_eq!(
            multi_index_sorted_search_json["hits"]["hits"][0]["_index"],
            "events_extra"
        );
        assert_eq!(
            multi_index_sorted_search_json["hits"]["hits"][0]["sort"],
            json!([0])
        );
        assert_eq!(
            multi_index_sorted_search_json["hits"]["hits"][1]["sort"],
            json!([1])
        );
        assert_eq!(
            multi_index_sorted_search_json["hits"]["hits"][2]["sort"],
            json!([2])
        );

        let multi_index_search_after_response = test_server
            .client()
            .post(
                "http://localhost/logs,events_extra/_search",
                r#"{
                  "size": 2,
                  "search_after": [6],
                  "sort": [
                    {
                      "index_col": {
                        "order": "asc"
                      }
                    }
                  ]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(multi_index_search_after_response.status(), 200);
        let multi_index_search_after_json: Value =
            serde_json::from_str(&multi_index_search_after_response.read_utf8_body().unwrap())
                .unwrap();
        assert_eq!(
            multi_index_search_after_json["hits"]["hits"][0]["sort"],
            json!([7])
        );
        assert_eq!(
            multi_index_search_after_json["hits"]["hits"][1]["sort"],
            json!([8])
        );

        let multi_index_avg_agg_response = test_server
            .client()
            .post(
                "http://localhost/logs,events_extra/_search",
                r#"{
                  "size": 0,
                  "aggs": {
                    "avg_index_col": {
                      "avg": {
                        "field": "index_col"
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(multi_index_avg_agg_response.status(), 200);
        let multi_index_avg_agg_json: Value =
            serde_json::from_str(&multi_index_avg_agg_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            multi_index_avg_agg_json["aggregations"]["avg_index_col"]["value"],
            json!(4.0)
        );

        let multi_index_cardinality_agg_response = test_server
            .client()
            .post(
                "http://localhost/logs,events_extra/_search",
                r#"{
                  "size": 0,
                  "aggs": {
                    "distinct_messages": {
                      "cardinality": {
                        "field": "message"
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(multi_index_cardinality_agg_response.status(), 200);
        let multi_index_cardinality_agg_json: Value = serde_json::from_str(
            &multi_index_cardinality_agg_response
                .read_utf8_body()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            multi_index_cardinality_agg_json["aggregations"]["distinct_messages"]["value"],
            json!(9)
        );

        let multi_index_date_histogram_response = test_server
            .client()
            .post(
                "http://localhost/logs,events_extra/_search",
                r#"{
                  "size": 0,
                  "aggs": {
                    "per_day": {
                      "date_histogram": {
                        "field": "@timestamp",
                        "fixed_interval": "1d"
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(multi_index_date_histogram_response.status(), 200);
        let multi_index_date_histogram_json: Value = serde_json::from_str(
            &multi_index_date_histogram_response
                .read_utf8_body()
                .unwrap(),
        )
        .unwrap();
        let histogram_buckets =
            multi_index_date_histogram_json["aggregations"]["per_day"]["buckets"]
                .as_array()
                .unwrap();
        assert_eq!(histogram_buckets.len(), 5);
        assert_eq!(
            histogram_buckets[0]["key_as_string"],
            json!("2099-03-07T00:00:00.000Z")
        );
        assert_eq!(histogram_buckets[0]["doc_count"], json!(1));
        assert_eq!(
            histogram_buckets[1]["key_as_string"],
            json!("2099-03-08T00:00:00.000Z")
        );
        assert_eq!(histogram_buckets[1]["doc_count"], json!(4));
        assert_eq!(
            histogram_buckets[2]["key_as_string"],
            json!("2099-03-09T00:00:00.000Z")
        );
        assert_eq!(histogram_buckets[2]["doc_count"], json!(2));

        let multi_index_query_string_response = test_server
            .client()
            .post(
                "http://localhost/logs,events_extra/_search",
                r#"{
                  "sort": [
                    {
                      "index_col": {
                        "order": "asc"
                      }
                    }
                  ],
                  "query": {
                    "query_string": {
                      "query": "Archive OR logout",
                      "fields": ["message"],
                      "default_operator": "OR"
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(multi_index_query_string_response.status(), 200);
        let multi_index_query_string_json: Value =
            serde_json::from_str(&multi_index_query_string_response.read_utf8_body().unwrap())
                .unwrap();
        assert_eq!(
            multi_index_query_string_json["hits"]["hits"][0]["_source"]["message"],
            json!("Archive login pending")
        );
        assert_eq!(
            multi_index_query_string_json["hits"]["hits"][1]["_source"]["message"],
            json!("Logout successful")
        );

        let bounded_histogram_response = test_server
            .client()
            .post(
                "http://localhost/logs,events_extra/_search",
                r#"{
                  "size": 0,
                  "aggs": {
                    "per_day": {
                      "date_histogram": {
                        "field": "@timestamp",
                        "fixed_interval": "1d",
                        "min_doc_count": 0,
                        "extended_bounds": {
                          "min": "2099-03-06T00:00:00.000Z",
                          "max": "2099-03-12T00:00:00.000Z"
                        }
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(bounded_histogram_response.status(), 200);
        let bounded_histogram_json: Value =
            serde_json::from_str(&bounded_histogram_response.read_utf8_body().unwrap()).unwrap();
        let bounded_histogram_buckets =
            bounded_histogram_json["aggregations"]["per_day"]["buckets"]
                .as_array()
                .unwrap();
        assert_eq!(bounded_histogram_buckets.len(), 7);
        assert_eq!(
            bounded_histogram_buckets[0]["key_as_string"],
            json!("2099-03-06T00:00:00.000Z")
        );
        assert_eq!(bounded_histogram_buckets[0]["doc_count"], json!(0));
        assert_eq!(
            bounded_histogram_buckets[6]["key_as_string"],
            json!("2099-03-12T00:00:00.000Z")
        );
        assert_eq!(bounded_histogram_buckets[6]["doc_count"], json!(0));

        let multi_index_terms_subagg_response = test_server
            .client()
            .post(
                "http://localhost/logs,events_extra/_search",
                r#"{
                  "size": 0,
                  "aggs": {
                    "by_boolean": {
                      "terms": {
                        "field": "boolean",
                        "size": 10
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
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(multi_index_terms_subagg_response.status(), 200);
        let multi_index_terms_subagg_json: Value =
            serde_json::from_str(&multi_index_terms_subagg_response.read_utf8_body().unwrap())
                .unwrap();
        let terms_buckets = multi_index_terms_subagg_json["aggregations"]["by_boolean"]["buckets"]
            .as_array()
            .unwrap();
        assert_eq!(terms_buckets.len(), 1);
        assert_eq!(terms_buckets[0]["key"], json!("true"));
        assert_eq!(terms_buckets[0]["doc_count"], json!(9));
        assert_eq!(terms_buckets[0]["avg_index_col"]["value"], json!(4.0));

        let ordered_terms_response = test_server
            .client()
            .post(
                "http://localhost/logs,events_extra/_search",
                r#"{
                  "size": 0,
                  "aggs": {
                    "by_message": {
                      "terms": {
                        "field": "message",
                        "size": 3,
                        "order": {
                          "_key": "asc"
                        }
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(ordered_terms_response.status(), 200);
        let ordered_terms_json: Value =
            serde_json::from_str(&ordered_terms_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(
            ordered_terms_json["aggregations"]["by_message"]["buckets"][0]["key"],
            json!("Archive login complete")
        );
        assert_eq!(
            ordered_terms_json["aggregations"]["by_message"]["buckets"][1]["key"],
            json!("Archive login pending")
        );
        assert_eq!(
            ordered_terms_json["aggregations"]["by_message"]["buckets"][2]["key"],
            json!("Archive logout successful")
        );

        let invalid_search_after_response = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                r#"{
                  "size": 2,
                  "search_after": [2]
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        assert_eq!(invalid_search_after_response.status(), 400);

        let missing_index_response = test_server
            .client()
            .get("http://localhost/does-not-exist")
            .perform()
            .unwrap();
        assert_eq!(missing_index_response.status(), 404);
    }
    /*
        #[test]
        fn test_private_api_data_query() {
            let test_server = &*TEST_SERVER;

            test_server.client().put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN
            ).perform().unwrap();

            let file_path = format!("file://{}/tests/data/flights.parquet", env::current_dir().unwrap().to_str().unwrap());

            let schema = PowdrrSchema::from(&vec!(
                PowdrrField{ name: "snippet".to_string(), data_type: PowdrrDataType::String },
                PowdrrField{ name: "searchTerms".to_string(), data_type: PowdrrDataType::String },
                PowdrrField{ name: "title".to_string(), data_type: PowdrrDataType::String },
            ));

            let checkpoint = TableMetadataCheckpoint {
                table_name: "flights".to_string(),
                checkpoint_id: "0".to_string(),
                iceberg_metadata: Some(IcebergMetadata {
                    snapshot_id: "fake_iceberg_snapshot".to_string(),
                    files: vec!(file_path),
                    column_names: vec!(),
                    column_stats: vec!(),
                    schemas: vec!(schema.clone()),
                    file_schemas: vec!(0),
                }),
                speedboat_metadata: None,
                deletes_metadata: None,
                extension_metadata: None,
                schema: schema.clone(),
            };

            let checkpoint_response = test_server.client().post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            ).perform();

            match checkpoint_response {
                Err(_) => panic!("test setup failed"),
                Ok(_) => ()
            };


            let mut builder = SqlBuilder::for_agg();
            builder.set_all_fields_testing_only();
            builder.filter(SqlExpression::Like(
                Box::new(SqlExpression::FieldRef("t".to_string(), "snippet".to_string())),
                Box::new(SqlExpression::LiteralString("%Looking%".to_string())),
            ));

            let body_obj = PrivateSqlInvocation::new(
                builder.build(),
                vec!["es".to_string()],
                vec![],
                vec!(SnapshotDescriptor { table_name: "flights".to_string(), snapshot_id: "fake_id".to_string()}),
                0,
                1,
            );

            let response = test_server.client().post(
                "http://localhost/_private/v1/_sql",
                serde_json::to_string(&body_obj).unwrap(),
                mime::APPLICATION_JSON,
            ).perform().unwrap();

            assert_eq!(response.status(), 200);
            let body = response.read_body().unwrap();
            let str_body = str::from_utf8(&body).unwrap();
            let json_body: serde_json::Value = serde_json::from_str(&str_body).unwrap();
            let num = json_body["num"].as_u64().unwrap();
            assert_eq!(num, 505);
        }
    */
    #[test]
    fn test_es_search_table_parquet() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let schema = PowdrrSchema::from(&vec![
            PowdrrField {
                name: "_id_seq_no".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "snippet".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "searchTerms".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "title".to_string(),
                data_type: PowdrrDataType::String,
            },
        ]);

        let file_path = format!(
            "file://{}/tests/data/flights.parquet",
            env::current_dir().unwrap().to_str().unwrap()
        );

        let checkpoint = TableMetadataCheckpoint {
            table_name: "flights".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "0".to_string(),
            iceberg_metadata: Some(IcebergMetadata {
                table_schema: schema.clone(),
                snapshot_id: Some("fake_iceberg_snapshot".to_string()),
                files: FileSetPayload::single(file_path, 1, schema.clone()),
                partition_spec: vec![],
                sort_order: vec![],
                column_names: vec![],
                column_stats: vec![],
                access_artifacts: vec![],
                file_stats: vec![],
            }),
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema: schema.clone(),
        };

        let checkpoint_response = test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform();

        match checkpoint_response {
            Err(_) => panic!("test setup failed"),
            Ok(_) => (),
        };

        let body_obj = r#"
        {
           "query": {
             "match": {
               "snippet": {
                 "query": "flight"
               }
             }
           }
        }"#;

        let response_result = test_server
            .client()
            .post(
                "http://localhost/flights/_search",
                body_obj,
                mime::APPLICATION_JSON,
            )
            .perform();

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                print!("{}", str_body);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_search_table_json() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let schema = PowdrrSchema::from(&vec![
            PowdrrField {
                name: "@timestamp".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "_id".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "_version".to_string(),
                data_type: PowdrrDataType::Integer,
            },
            PowdrrField {
                name: "_seq_no".to_string(),
                data_type: PowdrrDataType::Integer,
            },
            PowdrrField {
                name: "_id_seq_no".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "message".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "_source".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "index_col".to_string(),
                data_type: PowdrrDataType::Integer,
            },
        ]);

        let data_file_path = format!(
            "file://{}/tests/data/logs.json",
            env::current_dir().unwrap().to_str().unwrap()
        );

        let checkpoint = TableMetadataCheckpoint {
            table_name: "logs".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "fake_id".to_string(),
            iceberg_metadata: None,
            speedboat_metadata: Some(SpeedboatMetadata {
                files: FileSetPayload::single(data_file_path.clone(), 6, schema.clone()),
            }),
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema: schema.clone(),
        };

        let checkpoint_response = test_server
            .client()
            .post(
                "http://localhost/_test/v1/_add_checkpoint",
                serde_json::to_string(&checkpoint).unwrap(),
                mime::APPLICATION_JSON,
            )
            .perform();

        match checkpoint_response {
            Err(_) => panic!("test setup failed"),
            Ok(_) => (),
        };

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let body_obj = r#"
        {
           "query": {
             "match": {
               "message": {
                 "query": "Login"
               }
             }
           }
        }"#;

        let response_result = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                body_obj,
                mime::APPLICATION_JSON,
            )
            .perform();

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body: serde_json::Value = serde_json::from_str(&str_body).unwrap();
                let hits = json_body["hits"]["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 4);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_ingest_then_search_table() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let body = r#"{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:04:05.000Z", "index_col": 1, "user": { "id": "vlb44hny" }, "message": "Login attempt failed" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:06:07.000Z", "index_col": 2, "user": { "id": "8a4f500d" }, "message": "Login successful" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-09T11:07:08.000Z", "index_col": 3, "user": { "id": "l7gk7f82" }, "message": "Logout successful" }"#;

        let response = test_server
            .client()
            .post("http://localhost/_bulk", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let body_obj = r#"
        {
           "query": {
             "match": {
               "message": {
                 "query": "Login"
               }
             }
           }
        }"#;

        let response_result = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                body_obj,
                mime::APPLICATION_JSON,
            )
            .perform();

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body: serde_json::Value = serde_json::from_str(&str_body).unwrap();
                let hits = json_body["hits"]["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 2);
                let first_hit = hits[0].as_object().unwrap();
                let first_hit_message = first_hit["_source"]["message"].as_str().unwrap();
                let second_hit = hits[1].as_object().unwrap();
                let second_hit_message = second_hit["_source"]["message"].as_str().unwrap();
                // Annoying because order is not guaranteed.
                assert!(
                    second_hit_message.contains("Login successful")
                        || second_hit_message.contains("Login attempt failed")
                );
                assert!(
                    first_hit_message.contains("Login successful")
                        || first_hit_message.contains("Login attempt failed")
                );
                assert_ne!(first_hit_message, second_hit_message);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_ingest_then_search_table_for_nonexistent() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let body = r#"{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:04:05.000Z", "index_col": 1, "user": { "id": "vlb44hny" }, "message": "Login attempt failed" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:06:07.000Z", "index_col": 2, "user": { "id": "8a4f500d" }, "message": "Login successful" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-09T11:07:08.000Z", "index_col": 3, "user": { "id": "l7gk7f82" }, "message": "Logout successful" }"#;

        let response = test_server
            .client()
            .post("http://localhost/_bulk", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let body_obj = r#"
        {
           "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "should": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "type": "space"
                      }
                    }
                  ]
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  }
}"#;

        let response_result = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                body_obj,
                mime::APPLICATION_JSON,
            )
            .perform();

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body: serde_json::Value = serde_json::from_str(&str_body).unwrap();
                let hits = json_body["hits"]["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 0);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_ingest_then_search_table_agg() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let body = r#"{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:04:05.000Z", "index_col": 1, "user": { "id": "vlb44hny" }, "message": "Login attempt failed" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:06:07.000Z", "index_col": 2, "user": { "id": "8a4f500d" }, "message": "Login successful" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-09T11:07:08.000Z", "index_col": 3, "user": { "id": "l7gk7f82" }, "message": "Logout successful" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-10T11:08:07.000Z", "index_col": 4, "user": { "id": "8a2f500d" }, "message": "Login successful" }"#;

        let response = test_server
            .client()
            .post("http://localhost/_bulk", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let body_obj = r#"
        {
           "query": {
             "match": {
               "message": {
                 "query": "Login"
               }
             }
           },
           "aggs": {
             "messageType": {
               "terms": {
                 "field": "message"
               }           
            }
           }
        }"#;

        let response_result = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                body_obj,
                mime::APPLICATION_JSON,
            )
            .perform();

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body: serde_json::Value = serde_json::from_str(&str_body).unwrap();
                let hits = json_body["hits"]["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 3);
                let buckets = json_body["aggregations"]["messageType"]["buckets"]
                    .as_array()
                    .unwrap();
                assert_eq!(buckets.len(), 2);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_ingest_then_search_table_sub_agg() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let body = r#"{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:04:05.000Z", "index_col": 1, "user": { "id": "vlb44hny" }, "type": "t-shirt", "price": 100.00 }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:06:07.000Z", "index_col": 2, "user": { "id": "8a4f500d" }, "type": "t-shirt", "price": 80.00 }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-09T11:07:08.000Z", "index_col": 3, "user": { "id": "l7gk7f82" }, "type": "button down", "price": 120.00 }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-10T11:08:07.000Z", "index_col": 4, "user": { "id": "8a2f500d" }, "type": "button down", "price": 140.00 }"#;

        let response = test_server
            .client()
            .post("http://localhost/_bulk", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let body_obj = r#"
        {
          "aggs": {
            "avg_price": { "avg": { "field": "price" } },
            "t_shirts": {
              "filter": { "term": { "type": "t-shirt" } },
              "aggs": {
                "avg_price": { "avg": { "field": "price" } }
              }
            }
          }
        }"#;

        let response_result = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                body_obj,
                mime::APPLICATION_JSON,
            )
            .perform();

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body: serde_json::Value = serde_json::from_str(&str_body).unwrap();
                let aggregations = json_body["aggregations"].as_object().unwrap();
                assert_eq!(aggregations.len(), 2);
                assert_eq!(110.0, aggregations["avg_price"]["value"].as_f64().unwrap());
                assert_eq!(
                    aggregations["t_shirts"]["avg_price"]["value"]
                        .as_f64()
                        .unwrap(),
                    90.0
                );
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[tokio::test]
    async fn test_es_ingest_search_ingest_compact_then_search_table() {
        let test_server = AsyncTestServer::new(router(true)).await.unwrap();

        test_server
            .client()
            .put("http://localhost/_test/v1/_testing_mode")
            .body("")
            .mime(mime::TEXT_PLAIN)
            .perform()
            .await
            .unwrap();

        STATE_PROVIDER
            .set_peer_mode(&PeerModeType::Testing(test_server.clone()))
            .await;

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put("http://localhost/logs")
            .body(body_create_index)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let body = r#"{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:04:05.000Z", "index_col": 1, "user": { "id": "vlb44hny" }, "message": "Login attempt failed" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:06:07.000Z", "index_col": 2, "user": { "id": "8a4f500d" }, "message": "Login successful" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-09T11:07:08.000Z", "index_col": 3, "user": { "id": "l7gk7f82" }, "message": "Logout successful" }"#;

        let response = test_server
            .client()
            .post("http://localhost/_bulk")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let process_work_response = test_server
            .client()
            .put("http://localhost/_test/v1/_process_work")
            .body("")
            .mime(mime::TEXT_PLAIN)
            .perform()
            .await
            .unwrap();

        assert_eq!(process_work_response.status(), 200);
        let body = process_work_response.read_body().await.unwrap();
        let str_body = str::from_utf8(&body).unwrap();
        let snapshot_id = str_body.parse::<u64>().unwrap();

        let body_obj = r#"
        {
           "query": {
             "match": {
               "message": {
                 "query": "Login"
               }
             }
           }
        }"#;

        let response_result = test_server
            .client()
            .post("http://localhost/logs/_search")
            .body(body_obj)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().await.unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body: serde_json::Value = serde_json::from_str(&str_body).unwrap();
                let hits = json_body["hits"]["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 2);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let body2 = r#"{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:04:05.000Z", "index_col": 4, "user": { "id": "vlb44hny" }, "message": "2 Login attempt failed" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:06:07.000Z", "index_col": 5, "user": { "id": "8a4f500d" }, "message": "2 Login successful" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-09T11:07:08.000Z", "index_col": 6, "user": { "id": "l7gk7f82" }, "message": "2 Logout successful" }"#;

        let response = test_server
            .client()
            .post("http://localhost/_bulk")
            .body(body2)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let process_work_response = test_server
            .client()
            .put("http://localhost/_test/v1/_process_work")
            .body(snapshot_id.to_string())
            .mime(mime::TEXT_PLAIN)
            .perform()
            .await
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let response_result = test_server
            .client()
            .post("http://localhost/logs/_search")
            .body(body_obj)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().await.unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body: serde_json::Value = serde_json::from_str(&str_body).unwrap();
                let hits = json_body["hits"]["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 4);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_simulate_simple_pipeline() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let test_val = r#"{
            "pipeline" :
            {
              "description": "_description",
              "processors": [
                {
                  "set" : {
                    "field" : "field2",
                    "value" : "_value"
                  }
                }
              ]
            },
            "docs": [
              {
                "_index": "index",
                "_id": "id",
                "_source": {
                  "foo": "bar"
                }
              },
              {
                "_index": "index",
                "_id": "id",
                "_source": {
                  "foo": "rab"
                }
              }
            ]
          }"#;

        let simulate_response = test_server
            .client()
            .post(
                "http://localhost/_ingest/pipeline/_simulate",
                test_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match simulate_response {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                print!("{}", str_body);
                let value = serde_json::from_str::<Value>(str_body).unwrap();
                let value_map = value.as_object().unwrap();
                let docs = value_map.get("docs").unwrap().as_array().unwrap();
                assert_eq!(docs.len(), 2);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[tokio::test]
    async fn test_es_create_single() {
        let test_server = AsyncTestServer::new(router(true)).await.unwrap();

        test_server
            .client()
            .put("http://localhost/_test/v1/_testing_mode")
            .body("")
            .mime(mime::TEXT_PLAIN)
            .perform()
            .await
            .unwrap();

        STATE_PROVIDER
            .set_peer_mode(&PeerModeType::Testing(test_server.clone()))
            .await;

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put("http://localhost/logs")
            .body(body_create_index)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let test_val = r#"{
            "@timestamp": "2099-11-15T13:12:00",
            "message": "GET /search HTTP/1.1 200 1070000",
            "user": {
                "id": "kimchy"
            }
            }"#;

        let create_response = test_server
            .client()
            .post("http://localhost/logs/_create/my_id")
            .body(test_val)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match create_response {
            Ok(response) => {
                assert_eq!(response.status(), 201);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let process_work_response = test_server
            .client()
            .put("http://localhost/_test/v1/_process_work")
            .body("")
            .mime(mime::TEXT_PLAIN)
            .perform()
            .await
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let create_response = test_server
            .client()
            .post("http://localhost/logs/_create/my_id")
            .body(test_val)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match create_response {
            Ok(response) => {
                assert_eq!(response.status(), 409);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_index_single_auto_id() {
        let test_server = &*TEST_SERVER;

        set_testing_and_processing_mode(test_server);

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let test_val = r#"{
            "@timestamp": "2099-11-15T13:12:00",
            "message": "GET /search HTTP/1.1 200 1070000",
            "user": {
                "id": "kimchy"
            }
            }"#;

        let index_response = test_server
            .client()
            .post(
                "http://localhost/logs/_doc",
                test_val,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(index_response.status(), 201);
        let index_json: Value =
            serde_json::from_str(&index_response.read_utf8_body().unwrap()).unwrap();
        let doc_id = index_json["_id"].as_str().unwrap().to_string();
        assert_eq!(index_json["_version"], json!(1));

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let get_response = test_server
            .client()
            .get(&format!("http://localhost/logs/_doc/{}", doc_id))
            .perform()
            .unwrap();

        assert_eq!(get_response.status(), 200);
        let get_json: Value =
            serde_json::from_str(&get_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_json["_id"], json!(doc_id));
        assert_eq!(
            get_json["_source"]["message"],
            json!("GET /search HTTP/1.1 200 1070000")
        );
    }

    #[test]
    fn test_es_put_doc_with_id_replaces_existing_doc() {
        let test_server = &*TEST_SERVER;

        set_testing_and_processing_mode(test_server);

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let original_doc = r#"{
            "@timestamp": "2099-11-15T13:12:00",
            "message": "original message",
            "user": {
                "id": "kimchy"
            }
            }"#;
        let replacement_doc = r#"{
            "@timestamp": "2099-11-15T13:13:00",
            "message": "replacement message",
            "user": {
                "id": "kimchy"
            }
            }"#;

        let first_index_response = test_server
            .client()
            .put(
                "http://localhost/logs/_doc/my_id",
                original_doc,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(first_index_response.status(), 201);

        let first_process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(first_process_work_response.status(), 200);

        let second_index_response = test_server
            .client()
            .put(
                "http://localhost/logs/_doc/my_id",
                replacement_doc,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(second_index_response.status(), 200);
        let second_index_json: Value =
            serde_json::from_str(&second_index_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(second_index_json["_id"], json!("my_id"));
        assert_eq!(second_index_json["_version"], json!(2));

        let second_process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(second_process_work_response.status(), 200);

        let get_response = test_server
            .client()
            .get("http://localhost/logs/_doc/my_id")
            .perform()
            .unwrap();

        assert_eq!(get_response.status(), 200);
        let get_json: Value =
            serde_json::from_str(&get_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_json["_version"], json!(2));
        assert_eq!(get_json["_source"]["message"], json!("replacement message"));

        let old_search_response = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                r#"{
                  "query": {
                    "match": {
                      "message": {
                        "query": "original"
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        let old_search_json: Value =
            serde_json::from_str(&old_search_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(old_search_json["hits"]["total"]["value"], json!(0));

        let new_search_response = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                r#"{
                  "query": {
                    "match": {
                      "message": {
                        "query": "replacement"
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        let new_search_json: Value =
            serde_json::from_str(&new_search_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(new_search_json["hits"]["total"]["value"], json!(1));
        assert_eq!(
            new_search_json["hits"]["hits"][0]["_source"]["message"],
            json!("replacement message")
        );
    }

    #[test]
    fn test_es_bulk_index_replaces_existing_doc_after_refresh() {
        let test_server = &*TEST_SERVER;

        set_testing_and_processing_mode(test_server);

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let first_bulk_body = make_index_bulk_body(
            "logs".to_string(),
            vec![(
                "my_id".to_string(),
                r#"{
                    "@timestamp": "2099-11-15T13:12:00",
                    "message": "bulk original message",
                    "user": {
                        "id": "kimchy"
                    }
                }"#
                .to_string(),
            )],
        );
        let first_bulk_response = test_server
            .client()
            .post(
                "http://localhost/_bulk",
                first_bulk_body,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(first_bulk_response.status(), 200);

        let first_process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(first_process_work_response.status(), 200);

        let second_bulk_body = make_index_bulk_body(
            "logs".to_string(),
            vec![(
                "my_id".to_string(),
                r#"{
                    "@timestamp": "2099-11-15T13:13:00",
                    "message": "bulk replacement message",
                    "user": {
                        "id": "kimchy"
                    }
                }"#
                .to_string(),
            )],
        );
        let second_bulk_response = test_server
            .client()
            .post(
                "http://localhost/_bulk",
                second_bulk_body,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(second_bulk_response.status(), 200);

        let second_process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(second_process_work_response.status(), 200);

        let get_response = test_server
            .client()
            .get("http://localhost/logs/_doc/my_id")
            .perform()
            .unwrap();

        assert_eq!(get_response.status(), 200);
        let get_json: Value =
            serde_json::from_str(&get_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_json["_version"], json!(2));
        assert_eq!(
            get_json["_source"]["message"],
            json!("bulk replacement message")
        );

        let old_search_response = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                r#"{
                  "query": {
                    "match": {
                      "message": {
                        "query": "bulk original"
                      }
                    }
                  }
                }"#,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();
        let old_search_json: Value =
            serde_json::from_str(&old_search_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(old_search_json["hits"]["total"]["value"], json!(0));
    }

    #[test]
    fn test_es_update_single_merges_existing_doc() {
        let test_server = &*TEST_SERVER;

        set_testing_and_processing_mode(test_server);

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let original_doc = r#"{
            "@timestamp": "2099-11-15T13:12:00",
            "message": "original message",
            "user": {
                "id": "kimchy"
            }
            }"#;
        let update_doc = r#"{
            "doc": {
                "message": "patched message",
                "user": {
                    "name": "greg"
                }
            }
            }"#;

        let create_response = test_server
            .client()
            .put(
                "http://localhost/logs/_doc/my_id",
                original_doc,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(create_response.status(), 201);

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let update_response = test_server
            .client()
            .post(
                "http://localhost/logs/_update/my_id",
                update_doc,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(update_response.status(), 200);
        let update_json: Value =
            serde_json::from_str(&update_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(update_json["_version"], json!(2));

        let second_process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(second_process_work_response.status(), 200);

        let get_response = test_server
            .client()
            .get("http://localhost/logs/_doc/my_id")
            .perform()
            .unwrap();

        assert_eq!(get_response.status(), 200);
        let get_json: Value =
            serde_json::from_str(&get_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_json["_version"], json!(2));
        assert_eq!(get_json["_source"]["message"], json!("patched message"));
        assert_eq!(get_json["_source"]["user"]["id"], json!("kimchy"));
        assert_eq!(get_json["_source"]["user"]["name"], json!("greg"));
    }

    #[test]
    fn test_es_update_single_doc_as_upsert_creates_missing_doc() {
        let test_server = &*TEST_SERVER;

        set_testing_and_processing_mode(test_server);

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let update_doc = r#"{
            "doc": {
                "@timestamp": "2099-11-15T13:12:00",
                "message": "created via update",
                "user": {
                    "id": "kimchy"
                }
            },
            "doc_as_upsert": true
            }"#;

        let update_response = test_server
            .client()
            .post(
                "http://localhost/logs/_update/upsert_id",
                update_doc,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(update_response.status(), 201);
        let update_json: Value =
            serde_json::from_str(&update_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(update_json["_version"], json!(1));

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let get_response = test_server
            .client()
            .get("http://localhost/logs/_doc/upsert_id")
            .perform()
            .unwrap();

        assert_eq!(get_response.status(), 200);
        let get_json: Value =
            serde_json::from_str(&get_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_json["_version"], json!(1));
        assert_eq!(get_json["_source"]["message"], json!("created via update"));
    }

    #[test]
    fn test_es_bulk_update_merges_existing_doc_after_refresh() {
        let test_server = &*TEST_SERVER;

        set_testing_and_processing_mode(test_server);

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let original_doc = r#"{
            "@timestamp": "2099-11-15T13:12:00",
            "message": "bulk original message",
            "user": {
                "id": "kimchy"
            }
            }"#;

        let create_response = test_server
            .client()
            .put(
                "http://localhost/logs/_doc/my_id",
                original_doc,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(create_response.status(), 201);

        let first_process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(first_process_work_response.status(), 200);

        let bulk_body = make_update_bulk_body(
            "logs".to_string(),
            vec![(
                "my_id".to_string(),
                r#"{
                    "doc": {
                        "message": "bulk patched message",
                        "user": {
                            "name": "greg"
                        }
                    }
                }"#
                .to_string(),
            )],
        );
        let bulk_response = test_server
            .client()
            .post("http://localhost/_bulk", bulk_body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(bulk_response.status(), 200);

        let second_process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(second_process_work_response.status(), 200);

        let get_response = test_server
            .client()
            .get("http://localhost/logs/_doc/my_id")
            .perform()
            .unwrap();

        assert_eq!(get_response.status(), 200);
        let get_json: Value =
            serde_json::from_str(&get_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_json["_version"], json!(2));
        assert_eq!(
            get_json["_source"]["message"],
            json!("bulk patched message")
        );
        assert_eq!(get_json["_source"]["user"]["id"], json!("kimchy"));
        assert_eq!(get_json["_source"]["user"]["name"], json!("greg"));
    }

    #[test]
    fn test_es_bulk_delete_removes_existing_doc_after_refresh() {
        let test_server = &*TEST_SERVER;

        set_testing_and_processing_mode(test_server);

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let original_doc = r#"{
            "@timestamp": "2099-11-15T13:12:00",
            "message": "bulk delete message",
            "user": {
                "id": "kimchy"
            }
            }"#;

        let create_response = test_server
            .client()
            .put(
                "http://localhost/logs/_doc/my_id",
                original_doc,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(create_response.status(), 201);

        let first_process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(first_process_work_response.status(), 200);

        let bulk_body = make_delete_bulk_body("logs".to_string(), vec!["my_id".to_string()]);
        let bulk_response = test_server
            .client()
            .post("http://localhost/_bulk", bulk_body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(bulk_response.status(), 200);

        let second_process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(second_process_work_response.status(), 200);

        let get_response = test_server
            .client()
            .get("http://localhost/logs/_doc/my_id")
            .perform()
            .unwrap();

        assert_eq!(get_response.status(), 404);
    }

    #[test]
    fn test_es_create_then_delete_single() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let test_val = r#"{
            "@timestamp": "2099-11-15T13:12:00",
            "message": "GET /search HTTP/1.1 200 1070000",
            "user": {
                "id": "kimchy"
            }
            }"#;

        let create_response = test_server
            .client()
            .post(
                "http://localhost/logs/_create/my_id",
                test_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match create_response {
            Ok(response) => {
                assert_eq!(response.status(), 201);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let get_response = test_server
            .client()
            .get("http://localhost/logs/_doc/my_id")
            .perform();

        match get_response {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                print!("{}", str_body);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let delete_response = test_server
            .client()
            .delete("http://localhost/logs/_doc/my_id")
            .perform();

        match delete_response {
            Ok(response) => {
                assert_eq!(response.status(), 200);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let get_response = test_server
            .client()
            .get("http://localhost/logs/_doc/my_id")
            .perform();

        match get_response {
            Ok(response) => {
                assert_eq!(response.status(), 404);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                print!("{}", str_body);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_ingest_then_update_then_search_table() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/logs",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let body = r#"{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:04:05.000Z", "index_col": 1, "user": { "id": "vlb44hny" }, "message": "Login attempt failed" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-08T11:06:07.000Z", "index_col": 2, "user": { "id": "8a4f500d" }, "message": "Login successful" }
{"create":{ "_index": "logs" }}
{ "@timestamp": "2099-03-09T11:07:08.000Z", "index_col": 3, "user": { "id": "l7gk7f82" }, "message": "Logout successful" }"#;

        let response = test_server
            .client()
            .post("http://localhost/_bulk", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let update_body_obj = r#"
        {
           "query": {
             "match": {
               "message": {
                 "query": "Login"
               }
             }
           },
           "script": {
              "source": "ctx._source.dude = params.foo",
              "lang": "painless",
              "params" : {
                "foo": "bar"
              }
           }
        }"#;

        let update_response_result = test_server
            .client()
            .post(
                "http://localhost/logs/_update_by_query",
                update_body_obj,
                mime::APPLICATION_JSON,
            )
            .perform();

        match update_response_result {
            Ok(response) => {
                //assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                print!("{}", str_body);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let body_obj = r#"
        {
           "query": {
             "match": {
               "dude": {
                 "query": "bar"
               }
             }
           }
        }"#;

        let response_result = test_server
            .client()
            .post(
                "http://localhost/logs/_search",
                body_obj,
                mime::APPLICATION_JSON,
            )
            .perform();

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body: serde_json::Value = serde_json::from_str(&str_body).unwrap();
                let hits = json_body["hits"]["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 2);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_ingest_then_search_space() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 1,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/.kibana_8.7.1",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let doc_val = r#"{
  "space": {
    "name": "Default",
    "description": "This is your default space!",
    "color": "\\#00bfb3",
    "disabledFeatures": [],
    "_reserved": true
  },
  "type": "space",
  "references": [],
  "migrationVersion": {
    "space": "6.6.0"
  },
  "coreMigrationVersion": "8.7.1",
  "updated_at": "2025-06-29T19:26:43.469Z",
  "created_at": "2025-06-29T19:26:43.469Z"
}"#;

        let create_response = test_server
            .client()
            .post(
                "http://localhost/.kibana_8.7.1/_create/space%3Adefault",
                doc_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match create_response {
            Ok(response) => {
                assert_eq!(response.status(), 201);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let query_val = r#"{
  "size": 1000,
  "seq_no_primary_term": true,
  "from": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "should": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "type": "space"
                      }
                    }
                  ],
                  "must_not": [
                    {
                      "exists": {
                        "field": "namespace"
                      }
                    },
                    {
                      "exists": {
                        "field": "namespaces"
                      }
                    }
                  ]
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  },
  "sort": [
    {
      "space.name.keyword": {
        "unmapped_type": "keyword"
      }
    }
  ]
}"#;

        let query_response = test_server
            .client()
            .post(
                "http://localhost/.kibana_8.7.1/_search",
                query_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match query_response {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let obj_body = serde_json::from_str::<QueryResults>(str_body).unwrap();
                match obj_body.hits.total {
                    QueryResultTotal::Complex(complex) => {
                        assert_eq!(complex.value, 1);
                    }
                    _ => {
                        panic!("Failed to get total")
                    }
                }
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let query_val = r#"{
  "size": 1000,
  "seq_no_primary_term": true,
  "from": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "should": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "space._reserved": true
                      }
                    }
                  ]
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  }
}"#;

        let query_response = test_server
            .client()
            .post(
                "http://localhost/.kibana_8.7.1/_search",
                query_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match query_response {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let obj_body = serde_json::from_str::<QueryResults>(str_body).unwrap();
                match obj_body.hits.total {
                    QueryResultTotal::Complex(complex) => {
                        assert_eq!(complex.value, 1);
                    }
                    _ => {
                        panic!("Failed to get total")
                    }
                }
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_date_comparison() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 1,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/.kibana_8.7.1",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let doc_val = r#"{
  "space": {
    "name": "Default",
    "description": "This is your default space!",
    "color": "\\#00bfb3",
    "disabledFeatures": [],
    "_reserved": true
  },
  "type": "space",
  "references": [],
  "migrationVersion": {
    "space": "6.6.0"
  },
  "coreMigrationVersion": "8.7.1",
  "updated_at": "2025-06-29T19:26:43.469Z",
  "created_at": "2025-06-29T19:26:43.469Z"
}"#;

        let create_response = test_server
            .client()
            .post(
                "http://localhost/.kibana_8.7.1/_create/space%3Adefault",
                doc_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match create_response {
            Ok(response) => {
                assert_eq!(response.status(), 201);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let query_val = r#"{
  "size": 1000,
  "seq_no_primary_term": true,
  "from": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "should": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "type": "space"
                      },
                      "range": {
                        "updated_at": {
                          "lte": "now"
                        }
                      }
                    }
                  ],
                  "must_not": [
                    {
                      "exists": {
                        "field": "namespace"
                      }
                    },
                    {
                      "exists": {
                        "field": "namespaces"
                      }
                    }
                  ]
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  },
  "sort": [
    {
      "space.name.keyword": {
        "unmapped_type": "keyword"
      }
    }
  ]
}"#;

        let query_response = test_server
            .client()
            .post(
                "http://localhost/.kibana_8.7.1/_search",
                query_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match query_response {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let obj_body = serde_json::from_str::<QueryResults>(str_body).unwrap();
                match obj_body.hits.total {
                    QueryResultTotal::Complex(complex) => {
                        assert_eq!(complex.value, 1);
                    }
                    _ => {
                        panic!("Failed to get total")
                    }
                }
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    #[test]
    fn test_es_update_by_query_kibana() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 1,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/.kibana_task_manager_8.7.1",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let doc_val = r#"{
  "task": {
    "taskType": "alerts_invalidate_api_keys",
    "state": "{}",
    "params": "{}",
    "schedule": {
      "interval": "5m"
    },
    "traceparent": "00-0ab09373b4693b7c9a409be5be19845e-8eb1a3c13b4ca47a-00",
    "enabled": true,
    "attempts": 0,
    "scheduledAt": "2025-07-03T02:51:23.055Z",
    "startedAt": null,
    "retryAt": null,
    "runAt": "2025-07-03T02:51:23.055Z",
    "status": "idle"
  },
  "type": "task",
  "references": [],
  "migrationVersion": {
    "task": "8.5.0"
  },
  "coreMigrationVersion": "8.7.1",
  "updated_at": "2025-07-03T02:51:23.055Z",
  "created_at": "2025-07-03T02:51:23.055Z"
}"#;

        let create_response = test_server.client().post(
            "http://localhost/.kibana_task_manager_8.7.1/_create/task%3AAlerts-alerts_invalidate_api_keys?refresh=false&require_alias=true".to_string(),
            doc_val,
            mime::APPLICATION_JSON,
        ).perform();

        match create_response {
            Ok(response) => {
                assert!(response.status() == 201 || response.status() == 208);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let query_val = r#"{
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "type": "task"
          }
        },
        {
          "bool": {
            "must": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "task.enabled": true
                      }
                    }
                  ]
                }
              },
              {
                "bool": {
                  "should": [
                    {
                      "bool": {
                        "must": [
                          {
                            "term": {
                              "task.status": "idle"
                            }
                          },
                          {
                            "range": {
                              "task.runAt": {
                                "lte": "now"
                              }
                            }
                          }
                        ]
                      }
                    },
                    {
                      "bool": {
                        "must": [
                          {
                            "bool": {
                              "should": [
                                {
                                  "term": {
                                    "task.status": "running"
                                  }
                                },
                                {
                                  "term": {
                                    "task.status": "claiming"
                                  }
                                }
                              ]
                            }
                          },
                          {
                            "range": {
                              "task.retryAt": {
                                "lte": "now"
                              }
                            }
                          }
                        ]
                      }
                    }
                  ]
                }
              }
            ],
            "filter": [
              {
                "bool": {
                  "must_not": [
                    {
                      "bool": {
                        "should": [
                          {
                            "term": {
                              "task.status": "running"
                            }
                          },
                          {
                            "term": {
                              "task.status": "claiming"
                            }
                          }
                        ],
                        "must": {
                          "range": {
                            "task.retryAt": {
                              "gt": "now"
                            }
                          }
                        }
                      }
                    }
                  ]
                }
              }
            ]
          }
        }
      ]
    }
  },
  "script": {
    "source": "\n    if (params.claimableTaskTypes.contains(ctx._source.task.taskType)) {\n      if (ctx._source.task.schedule != null || ctx._source.task.attempts < params.taskMaxAttempts[ctx._source.task.taskType]) {\n        if(ctx._source.task.retryAt != null && ZonedDateTime.parse(ctx._source.task.retryAt).toInstant().toEpochMilli() < params.now) {\n    ctx._source.task.scheduledAt=ctx._source.task.retryAt;\n  } else {\n    ctx._source.task.scheduledAt=ctx._source.task.runAt;\n  }\n    ctx._source.task.status = \"claiming\"; ctx._source.task.ownerId=params.fieldUpdates.ownerId; ctx._source.task.retryAt=params.fieldUpdates.retryAt;\n      } else {\n        ctx._source.task.status = \"failed\";\n      }\n    } else if (params.unusedTaskTypes.contains(ctx._source.task.taskType)) {\n      ctx._source.task.status = \"unrecognized\";\n    } else {\n      ctx.op = \"noop\";\n    }",
    "lang": "painless",
    "params": {
      "now": 1751407079698,
      "fieldUpdates": {
        "ownerId": "kibana:9f414dbe-805e-4f6d-8b06-b84cc2d2b9a6",
        "retryAt": "2025-07-01T21:58:29.646Z"
      },
      "claimableTaskTypes": [
        "osquery:telemetry-saved-queries",
        "alerts_invalidate_api_keys"
      ],
      "skippedTaskTypes": [
        "session_cleanup",
        "actions_telemetry",
        "cleanup_failed_action_executions",
        "alerting_telemetry",
        "alerting_health_check",
        "report:execute",
        "reports:monitor",
        "alerting:transform_health",
        "actions:.email",
        "actions:.index",
        "actions:.pagerduty",
        "actions:.swimlane",
        "actions:.server-log",
        "actions:.slack",
        "actions:.webhook",
        "actions:.cases-webhook",
        "actions:.xmatters",
        "actions:.servicenow",
        "actions:.servicenow-sir",
        "actions:.servicenow-itom",
        "actions:.jira",
        "actions:.resilient",
        "actions:.teams",
        "actions:.torq",
        "actions:.opsgenie",
        "actions:.tines",
        "alerting:.index-threshold",
        "alerting:.geo-containment",
        "alerting:.es-query",
        "dashboard_telemetry",
        "cases-telemetry-task",
        "Fleet-Usage-Sender",
        "Fleet-Usage-Logger",
        "fleet:reassign_action:retry",
        "fleet:unenroll_action:retry",
        "fleet:upgrade_action:retry",
        "fleet:update_agent_tags:retry",
        "fleet:request_diagnostics:retry",
        "fleet:check-deleted-files-task",
        "osquery:telemetry-packs",
        "apm-source-map-migration-task",
        "osquery:telemetry-configs",
        "cloud_security_posture-stats_task",
        "ML:saved-objects-sync",
        "alerting:xpack.ml.anomaly_detection_alert",
        "alerting:xpack.ml.anomaly_detection_jobs_health",
        "UPTIME:SyntheticsService:Sync-Saved-Monitor-Objects",
        "alerting:xpack.uptime.alerts.monitorStatus",
        "alerting:xpack.uptime.alerts.tlsCertificate",
        "alerting:xpack.uptime.alerts.durationAnomaly",
        "alerting:xpack.uptime.alerts.tls",
        "alerting:xpack.synthetics.alerts.monitorStatus",
        "alerting:siem.eqlRule",
        "alerting:siem.savedQueryRule",
        "alerting:siem.indicatorRule",
        "alerting:siem.mlRule",
        "alerting:siem.queryRule",
        "alerting:siem.thresholdRule",
        "alerting:siem.newTermsRule",
        "alerting:siem.notifications",
        "endpoint:user-artifact-packager",
        "security:endpoint-diagnostics",
        "security:endpoint-meta-telemetry",
        "security:telemetry-lists",
        "security:telemetry-detection-rules",
        "security:telemetry-prebuilt-rule-alerts",
        "security:telemetry-timelines",
        "security:telemetry-configuration",
        "security:telemetry-filterlist-artifact",
        "endpoint:metadata-check-transforms-task",
        "alerting:metrics.alert.anomaly",
        "alerting:logs.alert.document.count",
        "alerting:metrics.alert.inventory.threshold",
        "alerting:metrics.alert.threshold",
        "alerting:monitoring_alert_cluster_health",
        "alerting:monitoring_alert_license_expiration",
        "alerting:monitoring_alert_cpu_usage",
        "alerting:monitoring_alert_missing_monitoring_data",
        "alerting:monitoring_alert_disk_usage",
        "alerting:monitoring_alert_thread_pool_search_rejections",
        "alerting:monitoring_alert_thread_pool_write_rejections",
        "alerting:monitoring_alert_jvm_memory_usage",
        "alerting:monitoring_alert_nodes_changed",
        "alerting:monitoring_alert_logstash_version_mismatch",
        "alerting:monitoring_alert_kibana_version_mismatch",
        "alerting:monitoring_alert_elasticsearch_version_mismatch",
        "alerting:monitoring_ccr_read_exceptions",
        "alerting:monitoring_shard_size",
        "apm-telemetry-task",
        "alerting:apm.transaction_duration",
        "alerting:apm.anomaly",
        "alerting:apm.error_rate",
        "alerting:apm.transaction_error_rate"
      ],
      "unusedTaskTypes": [
        "sampleTaskRemovedType",
        "alerting:siem.signals",
        "search_sessions_monitor",
        "search_sessions_cleanup",
        "search_sessions_expire"
      ],
      "taskMaxAttempts": {
        "apm-source-map-migration-task": 5
      }
    }
  },
  "sort": [
    {
      "_script": {
        "type": "number",
        "order": "asc",
        "script": {
          "lang": "painless",
          "source": "\nif (doc['task.retryAt'].size()!=0) {\n  return doc['task.retryAt'].value.toInstant().toEpochMilli();\n}\nif (doc['task.runAt'].size()!=0) {\n  return doc['task.runAt'].value.toInstant().toEpochMilli();\n}\n    "
        }
      }
    }
  ],
  "max_docs": 1,
  "conflicts": "proceed"
}"#;

        let update_response_result = test_server
            .client()
            .post(
                "http://localhost/.kibana_task_manager_8.7.1/_update_by_query",
                query_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match update_response_result {
            Ok(response) => {
                //assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body = serde_json::from_str::<Value>(str_body).unwrap();
                assert_eq!(json_body["updated"].as_u64().unwrap(), 1);
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let get_response_result = test_server.client().get(
            "http://localhost/.kibana_task_manager_8.7.1/_doc/task%3AAlerts-alerts_invalidate_api_keys",
        ).perform();

        match get_response_result {
            Ok(response) => {
                //assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                print!("{}", str_body);
                let json_body = serde_json::from_str::<Value>(str_body).unwrap();
                assert_eq!(
                    json_body["_source"]["task"]["status"].as_str().unwrap(),
                    "claiming"
                );
                // TODO: Check the response
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }

        let query_val = r#"{
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "type": "task"
          }
        }
      ]
    }
  }
}"#;

        let query_response = test_server
            .client()
            .post(
                "http://localhost/.kibana_task_manager_8.7.1/_search",
                query_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match query_response {
            Ok(response) => {
                //assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body = serde_json::from_str::<Value>(str_body).unwrap();
                let hits = json_body["hits"]["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 1);
                assert_eq!(
                    hits[0]["_source"]["task"]["ownerId"].as_str().unwrap(),
                    "kibana:9f414dbe-805e-4f6d-8b06-b84cc2d2b9a6"
                );
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }

    fn make_create_bulk_body(index: String, values: Vec<String>) -> String {
        let removed_newlines = values
            .iter()
            .map(|v| format!("{}\n", v.replace("\n", "")))
            .collect::<Vec<String>>();
        let create_lines = values
            .iter()
            .map(|_| format!("{{\"create\":{{\"_index\":\"{}\"}}}}\n", index))
            .collect::<Vec<String>>();
        let together = create_lines
            .iter()
            .zip(removed_newlines.iter())
            .map(|(v, c)| format!("{}{}", v, c))
            .collect::<Vec<String>>();
        together.join("")
    }

    fn make_index_bulk_body(index: String, values: Vec<(String, String)>) -> String {
        let index_lines = values
            .iter()
            .map(|(id, _)| {
                format!(
                    "{{\"index\":{{\"_index\":\"{}\",\"_id\":\"{}\"}}}}\n",
                    index, id
                )
            })
            .collect::<Vec<String>>();
        let source_lines = values
            .iter()
            .map(|(_, value)| format!("{}\n", value.replace("\n", "")))
            .collect::<Vec<String>>();
        index_lines
            .iter()
            .zip(source_lines.iter())
            .map(|(index_line, source_line)| format!("{index_line}{source_line}"))
            .collect::<Vec<String>>()
            .join("")
    }

    fn make_update_bulk_body(index: String, values: Vec<(String, String)>) -> String {
        let update_lines = values
            .iter()
            .map(|(id, _)| {
                format!(
                    "{{\"update\":{{\"_index\":\"{}\",\"_id\":\"{}\"}}}}\n",
                    index, id
                )
            })
            .collect::<Vec<String>>();
        let body_lines = values
            .iter()
            .map(|(_, value)| format!("{}\n", value.replace("\n", "")))
            .collect::<Vec<String>>();
        update_lines
            .iter()
            .zip(body_lines.iter())
            .map(|(update_line, body_line)| format!("{update_line}{body_line}"))
            .collect::<Vec<String>>()
            .join("")
    }

    fn make_delete_bulk_body(index: String, ids: Vec<String>) -> String {
        ids.iter()
            .map(|id| {
                format!(
                    "{{\"delete\":{{\"_index\":\"{}\",\"_id\":\"{}\"}}}}\n",
                    index, id
                )
            })
            .collect::<Vec<String>>()
            .join("")
    }

    #[test]
    fn test_es_search_table_json_okta_system_log() {
        let test_server = &*TEST_SERVER;

        test_server
            .client()
            .put(
                "http://localhost/_test/v1/_testing_mode",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;

        let response_create_index = test_server
            .client()
            .put(
                "http://localhost/okta_system_log",
                body_create_index,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response_create_index.status(), 200);

        let body = make_create_bulk_body(
            "okta_system_log".to_string(),
            vec![
                include_str!("../tests/data/okta_system_log_1.json").to_string(),
                include_str!("../tests/data/okta_system_log_2.json").to_string(),
                include_str!("../tests/data/okta_system_log_3.json").to_string(),
                include_str!("../tests/data/okta_system_log_4.json").to_string(),
            ],
        );

        let response = test_server
            .client()
            .post("http://localhost/_bulk", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), 200);

        let process_work_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_process_work",
                "",
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(process_work_response.status(), 200);

        let query_val = r#"{
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "debugContext.debugData.isUserSuspicious": false
          }
        }
      ]
    }
  }
}"#;

        let response_result = test_server
            .client()
            .post(
                "http://localhost/okta_system_log/_search",
                query_val,
                mime::APPLICATION_JSON,
            )
            .perform();

        match response_result {
            Ok(response) => {
                assert_eq!(response.status(), 200);
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                let json_body = serde_json::from_str::<Value>(str_body).unwrap();
                let hits = json_body["hits"]["hits"].as_array().unwrap();
                assert_eq!(hits.len(), 1);
                assert_eq!(
                    hits[0]["_source"]["debugContext"]["debugData"]["isUserSuspicious"]
                        .as_bool()
                        .unwrap(),
                    false
                );
            }
            Err(e) => {
                panic!("Failed {}", e)
            }
        }
    }
}
