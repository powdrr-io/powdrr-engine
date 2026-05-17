use crate::compaction::{compact_logs, CompactionCommand};
use crate::dynamodb_protocol;
use crate::elastic_search_endpoints::NameIdPathExtractor;
use crate::elastic_search_endpoints::NamePathExtractor;
use crate::elastic_search_endpoints::QueryStringAliases;
use crate::elastic_search_endpoints::QueryStringClusterSettings;
use crate::elastic_search_endpoints::QueryStringSearch;
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
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_create_pipeline);
        route
            .get("/_ingest/pipeline/_simulate")
            .to(elastic_search_endpoints::es_simulate_pipeline);
        route
            .post("/_ingest/pipeline/_simulate")
            .to(elastic_search_endpoints::es_simulate_pipeline);
        route
            .get("/_ingest/pipeline/:name/_simulate")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_simulate_named_pipeline);
        route
            .post("/_ingest/pipeline/:name/_simulate")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_simulate_named_pipeline);

        route
            .get("_cluster/settings")
            .with_query_string_extractor::<QueryStringClusterSettings>()
            .to(elastic_search_endpoints::es_cluster_settings);
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
            .get("/:name/_dynamodb/config")
            .with_path_extractor::<NamePathExtractor>()
            .to(dynamodb_protocol::get_dynamodb_config);
        route
            .put("/:name/_serve/config")
            .with_path_extractor::<NamePathExtractor>()
            .to(lakehouse_serving::put_serving_config);
        route
            .put("/:name/_dynamodb/config")
            .with_path_extractor::<NamePathExtractor>()
            .to(dynamodb_protocol::put_dynamodb_config);
        route
            .post("/:name/_serve")
            .with_path_extractor::<NamePathExtractor>()
            .to(lakehouse_serving::serve_query);
        route
            .get("/_index_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_index_template);
        route
            .get("/_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_index_template);
        route
            .get("/_component_template/:name")
            .with_path_extractor::<NamePathExtractor>()
            .to(elastic_search_endpoints::es_get_index_template);
        route
            .post("/_search")
            .with_query_string_extractor::<QueryStringSearch>()
            .to(elastic_search_endpoints::es_search);
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
            .post("/:name/_doc/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_update_with_id);
        route
            .post("/:name/_update/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_update_with_id);
        route
            .get("/:name/_doc/:id")
            .with_path_extractor::<NameIdPathExtractor>()
            .to(elastic_search_endpoints::es_get_with_id);
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
    use std::sync::LazyLock;
    use std::{env, str};

    use crate::data_contract::{
        CreateTable, FileSetPayload, IcebergMetadata, SpeedboatMetadata, TableMetadataCheckpoint,
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
    use gotham::mime;
    use gotham::plain::test::AsyncTestServer;
    use gotham::test::TestServer;
    use serde_json::{Value, json};

    pub(crate) static TEST_SERVER: LazyLock<TestServer> =
        LazyLock::new(|| TestServer::with_timeout(router(true), 1000).unwrap());

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
                column_names: vec![],
                column_stats: vec![],
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
        }))
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

        let get_count_response = test_server
            .client()
            .get("http://localhost/logs/_count")
            .perform()
            .unwrap();
        assert_eq!(get_count_response.status(), 200);
        let get_count_json: Value =
            serde_json::from_str(&get_count_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(get_count_json["count"], 6);

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
                column_names: vec![],
                column_stats: vec![],
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
