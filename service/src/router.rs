use std::pin::Pin;
use futures_util::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{body, Body, StatusCode};
use gotham::mime;
use gotham::pipeline::{new_pipeline, single_pipeline};
use gotham::prelude::{DefineSingleRoute, DrawRoutes, FromState, StateData, StaticResponseExtender};
use gotham::router::{build_router, Router};
use gotham::state::State;
use serde::{Deserialize};
use powdrr_lib::data_contract::ServiceMode;
use crate::service_impl_provider::SERVICE_IMPL;
use crate::v1_handlers;



#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct NamePathExtractor {
    pub(crate) name: String,
}

/// Create a `Router`
///
/// Results in a tree of routes that that looks like:
///
/// | _private/v1/_sql              --> POST
/// | {tables}/_search --> POST

/// matching on.
pub fn router(include_test_apis: bool) -> Router {
    let (chain, pipelines) = single_pipeline(new_pipeline().build());

    build_router(chain, pipelines, |route| {
        route.scope("/api", |route| {
            route.scope("/v1", |route| {
                route.post("/create_table").to(v1_handlers::create_table);
                route.get("/describe_table/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::describe_table);
                route.post("/add_alias").to(v1_handlers::add_alias);
                route.post("/remove_alias").to(v1_handlers::remove_alias);
                route.post("/create_table_template/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::create_table_template);
                route.get("/describe_table_template/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::describe_table_template);
                route.post("/create_pipeline/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::create_pipeline);
                route.get("/describe_pipeline/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::describe_pipeline);
                route.post("/create_lifetime_policy/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::create_lifetime_policy);
                route.post("/describe_lifetime_policy/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::describe_lifetime_policy);
                route.post("/speedboat_commit").to(v1_handlers::speedboat_commit);
                route.post("/iceberg_commit/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::iceberg_commit);
                route.post("/extension_commit/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::extension_commit);
                route.post("/compaction_commit/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::compaction_commit);
                route.get("/get_latest_checkpoint").to(v1_handlers::get_latest_checkpoint);
                route.get("/get_checkpoint").to(v1_handlers::get_checkpoint);
                route.get("/get_extension_work_items/:name")
                    .with_path_extractor::<NamePathExtractor>()
                    .to(v1_handlers::get_extension_work_items);
                route.get("/get_compaction_work_items").to(v1_handlers::get_compaction_work_items);
            })
        });

        if include_test_apis {
            route.scope("/_test", |route| {
                route.scope("/v1", |route| {
                    route.put("/_set_mode").to(set_mode);
                })
            });
        }

    })
}

pub fn set_mode(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
            Ok(vb) => vb,
            Err(_) => panic!("Oh no"),
        };
        let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
        let invocation_obj: ServiceMode = match serde_json::from_str(&body_content) {
            Ok(io) => io,
            Err(_) => panic!("This should not happen"),
        };

        match SERVICE_IMPL.set_mode(invocation_obj).await {
            Ok(_) => {
                let res = create_response(&state, StatusCode::OK, mime::TEXT_PLAIN, "OK");
                Ok((state, res))
            }
            Err(_) => panic!("Oh no"),
        }
    }.boxed()
}
