use futures_util::future::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::hyper::StatusCode;
use gotham::state::State;
use gotham::state::FromState;
use gotham::hyper::{body, Body};
use std::pin::Pin;
use gotham::mime;
use serde::Serialize;
use powdrr_lib::data_contract::{AddAlias, CompactionCommit, CreateTable, ExtensionCommit, GetLatestCheckpoint, IcebergCommit, SpeedboatCommit};
use powdrr_lib::elastic_search_ingest::CreateIndexTemplateBody;
use powdrr_lib::elastic_search_lifetime_policy::ILMPolicyDefinition;
use powdrr_lib::pipeline::PipelineDefinition;
use powdrr_lib::state_peers::CheckpointDescriptor;
use crate::response::GenericResponse;
use crate::router::NamePathExtractor;
use crate::service_impl_provider::{ServiceImplError, SERVICE_IMPL};

macro_rules! nothing_handler {
    ($fn_name:ident() -> $ret_type:ty $body:block) => {
        pub fn $fn_name(state: State) -> Pin<Box<HandlerFuture>> {
            async move {
                let body_result = $body; // Execute the original function's body
                let res = body_result.generate_response(&state);
                Ok((state, res))
            }.boxed()
        }
    };
}


macro_rules! body_handler {
    ($fn_name:ident($arg_name:ident: $arg_type:ty) -> $ret_type:ty $body:block) => {
        pub fn $fn_name(mut state: State) -> Pin<Box<HandlerFuture>> {
            async move {
                let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
                    Ok(vb) => vb,
                    Err(_) => panic!("Oh no"),
                };
                let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
                let $arg_name: $arg_type = match serde_json::from_str(&body_content) {
                    Ok(io) => io,
                    Err(_) => panic!("This should not happen"),
                };

                let body_result = $body; // Execute the original function's body
                let res = body_result.generate_response(&state);
                Ok((state, res))
            }.boxed()
        }
    };
}

macro_rules! body_with_name_handler {
    ($fn_name:ident($name_arg_name:ident: $name_arg_type:ty, $body_arg_name:ident: $body_arg_type:ty) -> $ret_type:ty $body:block) => {
        pub fn $fn_name(mut state: State) -> Pin<Box<HandlerFuture>> {
            async move {
                let path_extractor = NamePathExtractor::borrow_from(&state);
                let $name_arg_name = path_extractor.name.to_string();
                let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
                    Ok(vb) => vb,
                    Err(_) => panic!("Oh no"),
                };
                let body_content = String::from_utf8(valid_body.to_vec()).unwrap();
                let $body_arg_name: $body_arg_type = match serde_json::from_str(&body_content) {
                    Ok(io) => io,
                    Err(_) => panic!("This should not happen"),
                };

                let body_result = $body; // Execute the original function's body
                let res = body_result.generate_response(&state);
                Ok((state, res))
            }.boxed()
        }
    };
}

macro_rules! name_handler {
    ($fn_name:ident($arg_name:ident: $arg_type:ty) -> $ret_type:ty $body:block) => {
        pub fn $fn_name(state: State) -> Pin<Box<HandlerFuture>> {
            async move {
                let path_extractor = NamePathExtractor::borrow_from(&state);
                let $arg_name = path_extractor.name.to_string();
                let body_result = $body; // Execute the original function's body
                let res = body_result.generate_response(&state);
                Ok((state, res))
            }.boxed()
        }
    };
}


fn handle_result_none(value: Result<(), ServiceImplError>) -> GenericResponse {
    match value {
        Ok(_) => {
            GenericResponse {
                status: StatusCode::OK, mime: mime::TEXT_PLAIN, body: "Ok".to_string(), headers: vec![],
            }
        },
        Err(e) => {
            tracing::error!("Error: {:?}", e);
            GenericResponse {
                status: StatusCode::SERVICE_UNAVAILABLE, mime: mime::TEXT_PLAIN, body: "Service Unavailable".to_string(), headers: vec![],
            }
        }
    }
}


fn handle_result<T>(value: Result<T, ServiceImplError>) -> GenericResponse
    where T: Sized + Serialize,
{
    match value {
        Ok(v) => {
            GenericResponse {
                status: StatusCode::OK, mime: mime::APPLICATION_JSON, body: serde_json::to_string(&v).unwrap(), headers: vec![],
            }
        },
        Err(e) => {
            tracing::error!("Error: {:?}", e);
            GenericResponse {
                status: StatusCode::SERVICE_UNAVAILABLE, mime: mime::TEXT_PLAIN, body: "Service Unavailable".to_string(), headers: vec![],
            }
        }
    }
}

fn handle_result_option<T>(value: Result<Option<T>, ServiceImplError>) -> GenericResponse
    where T: Sized + Serialize,
{
    match value {
        Ok(v) => match v {
            Some(v) => {
                GenericResponse {
                    status: StatusCode::OK,
                    mime: mime::APPLICATION_JSON,
                    body: serde_json::to_string(&v).unwrap(),
                    headers: vec![],
                }
            },
            None => {
                GenericResponse {
                    status: StatusCode::NOT_FOUND,
                    mime: mime::TEXT_PLAIN,
                    body: "Not found".to_string(),
                    headers: vec![],
                }
            }
        },
        Err(e) => {
            tracing::error!("Error: {:?}", e);
            GenericResponse {
                status: StatusCode::SERVICE_UNAVAILABLE, mime: mime::TEXT_PLAIN, body: "Service Unavailable".to_string(), headers: vec![],
            }
        }
    }
}


body_handler! { create_table(input: CreateTable) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.create_table(&input).await)
}}

name_handler! { describe_table(name: String) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.describe_table(&name).await)
}}


body_handler! { add_alias(input: AddAlias) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.add_alias(&input.table_name, &input.alias).await)
}}

body_handler! { remove_alias(input: AddAlias) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.remove_alias(&input.table_name, &input.alias).await)
}}

body_with_name_handler! { create_table_template(name: String, input: CreateIndexTemplateBody) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.create_table_template(&name, &input).await)
}}


name_handler! { describe_table_template(name: String) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.describe_table_template(&name).await)
}}


body_with_name_handler! { create_pipeline(name: String, input: PipelineDefinition) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.create_pipeline(&name, &input).await)
}}


name_handler! { describe_pipeline(name: String) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.describe_pipeline(&name).await)
}}


body_with_name_handler! { create_lifetime_policy(name: String, input: ILMPolicyDefinition) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.create_lifetime_policy(&name, &input).await)
}}


name_handler! { describe_lifetime_policy(name: String) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.describe_lifetime_policy(&name).await)
}}


body_handler! { speedboat_commit(input: SpeedboatCommit) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.speedboat_commit(&input).await)
}}

body_with_name_handler! { iceberg_commit(name: String, input: IcebergCommit) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.iceberg_commit(&name, &input).await)
}}

body_with_name_handler! { extension_commit(name: String, input: ExtensionCommit) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.extension_commit(&name, &input).await)
}}

body_with_name_handler! { compaction_commit(name: String, input: CompactionCommit) -> GenericResponse {
    handle_result_none(SERVICE_IMPL.compaction_commit(&name, &input).await)
}}

body_handler! { get_latest_checkpoint(input: GetLatestCheckpoint) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.get_latest_checkpoint(&input.table_name, input.extension).await)
}}

body_handler! { get_checkpoint(input: CheckpointDescriptor) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.get_checkpoint(input).await)
}}

name_handler! { get_extension_work_items(name: String) -> GenericResponse {
    handle_result(SERVICE_IMPL.get_extension_work_items(&name).await)
}}


nothing_handler! { get_compaction_work_items() -> GenericResponse {
    handle_result(SERVICE_IMPL.get_compaction_work_items().await)
}}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;
    use gotham::hyper::StatusCode;
    use gotham::mime;
    use gotham::test::TestServer;
    use powdrr_lib::data_contract::{AddAlias, CreateTable, TableDescription};
    use crate::router::router;

    pub(crate) static TEST_SERVER: LazyLock<TestServer> = LazyLock::new(|| TestServer::with_timeout(router(true), 1000).unwrap());

    #[test]
    fn test_create_and_describe_table() {
        let test_server = &*TEST_SERVER;

        test_server.client().put(
            "http://localhost/_test/v1/_testing_mode",
            "",
            mime::TEXT_PLAIN
        ).perform().unwrap();

        let body = CreateTable {
            name: "the_name".to_string(),
            tags: Default::default(),
        };

        let create_response = test_server.client().post(
            "http://localhost/api/v1/create_table",
            serde_json::to_string(&body).unwrap(),
            mime::APPLICATION_JSON,
        ).perform().unwrap();

        assert_eq!(create_response.status(), 200);

        let describe_response = test_server.client().get(
            "http://localhost/api/v1/describe_table/the_name",
        ).perform().unwrap();

        assert_eq!(describe_response.status(), 200);

        let describe_obj: TableDescription = serde_json::from_str(&*describe_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(describe_obj.name, "the_name");

        let add_alias_body = AddAlias {
            table_name: "the_name".to_string(),
            alias: "the_alias".to_string(),
        };

        let add_alias_response = test_server.client().post(
            "http://localhost/api/v1/add_alias",
            serde_json::to_string(&add_alias_body).unwrap(),
            mime::APPLICATION_JSON,
        ).perform().unwrap();

        assert_eq!(add_alias_response.status(), 200);

        let alias_describe_response = test_server.client().get(
            "http://localhost/api/v1/describe_table/the_alias",
        ).perform().unwrap();

        assert_eq!(alias_describe_response.status(), 200);

        let describe_obj: TableDescription = serde_json::from_str(&*alias_describe_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(describe_obj.name, "the_name");

        let remove_alias_response = test_server.client().post(
            "http://localhost/api/v1/remove_alias",
            serde_json::to_string(&add_alias_body).unwrap(),
            mime::APPLICATION_JSON,
        ).perform().unwrap();

        assert_eq!(remove_alias_response.status(), 200);

        let no_alias_describe_response = test_server.client().get(
            "http://localhost/api/v1/describe_table/the_alias",
        ).perform().unwrap();

        assert_eq!(no_alias_describe_response.status(), StatusCode::NOT_FOUND);

    }
}
