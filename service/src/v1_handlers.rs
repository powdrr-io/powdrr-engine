use crate::response::GenericResponse;
use crate::router::NamePathExtractor;
use crate::service_impl_provider::{SERVICE_IMPL, ServiceImplError};
use futures_util::future::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::hyper::Method;
use gotham::hyper::Uri;
use gotham::hyper::{Body, body};
use gotham::hyper::{HeaderMap, StatusCode};
use gotham::mime;
use gotham::state::FromState;
use gotham::state::State;
use powdrr_service_lib::data_contract::CreateIndexTemplateBody;
use powdrr_service_lib::data_contract::{
    ACCESS_KEY_HEADER_KEY, AddAlias, CleanupCommit, CompactionCommit, CreateTable, ExtensionCommit,
    GetLatestCheckpoint, IcebergCommit, OrgInfo, OrgSettings, SECRET_KEY_HEADER_KEY,
    SpeedboatCommit,
};
use powdrr_service_lib::elastic_search_lifetime_policy::ILMPolicyDefinition;
use powdrr_service_lib::metadata_store::{ServingNodeActivationAck, ServingNodeLease};
use powdrr_service_lib::peers::CheckpointDescriptor;
use powdrr_service_lib::pipeline::PipelineDefinition;
use powdrr_service_lib::read_only_coordination::ArtifactReadinessAck;
use reqwest::Client;
use serde::Serialize;
use std::pin::Pin;

async fn validate_org_info(headers: &HeaderMap) -> Option<OrgInfo> {
    let access_key = match headers.get(ACCESS_KEY_HEADER_KEY) {
        Some(key) => key.to_str().unwrap(),
        None => return None,
    };

    let secret_key = match headers.get(SECRET_KEY_HEADER_KEY) {
        Some(key) => key.to_str().unwrap(),
        None => return None,
    };

    SERVICE_IMPL
        .lookup_org(&access_key.into(), &secret_key.into())
        .await
        .unwrap_or_else(|_| {
            tracing::error!("Unable to retrieve org info");
            None
        })
}

async fn forward_to_leader_if_needed(
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body_bytes: Option<Vec<u8>>,
) -> Option<GenericResponse> {
    let base_url = match SERVICE_IMPL.forward_base_url().await {
        Ok(base_url) => base_url,
        Err(error) => {
            tracing::error!("Failed to determine leader forwarding target: {}", error);
            return Some(GenericResponse {
                status: StatusCode::SERVICE_UNAVAILABLE,
                mime: mime::TEXT_PLAIN,
                body: "Service Unavailable".to_string(),
                headers: vec![],
            });
        }
    }?;

    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| uri.path().to_string());

    let reqwest_method =
        reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET);
    let mut request = Client::new().request(reqwest_method, format!("{base_url}{path_and_query}"));
    for (key, value) in headers.iter() {
        if key.as_str().eq_ignore_ascii_case("host")
            || key.as_str().eq_ignore_ascii_case("content-length")
        {
            continue;
        }
        if let Ok(value_str) = value.to_str() {
            request = request.header(key.as_str(), value_str);
        }
    }
    if let Some(body_bytes) = body_bytes {
        request = request.body(body_bytes);
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!("Leader forwarding request failed: {}", error);
            return Some(GenericResponse {
                status: StatusCode::SERVICE_UNAVAILABLE,
                mime: mime::TEXT_PLAIN,
                body: "Service Unavailable".to_string(),
                headers: vec![],
            });
        }
    };

    let mime_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(mime::TEXT_PLAIN);
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
    let body = match response.text().await {
        Ok(body) => body,
        Err(error) => {
            tracing::error!("Failed to read leader response body: {}", error);
            "Service Unavailable".to_string()
        }
    };

    Some(GenericResponse {
        status,
        mime: mime_type,
        body,
        headers: vec![],
    })
}

macro_rules! nothing_handler {
    ($fn_name:ident($org_info_arg_name:ident: $org_info_arg_type:ty) -> $ret_type:ty $body:block) => {
        pub fn $fn_name(state: State) -> Pin<Box<HandlerFuture>> {
            async move {
                let method = Method::borrow_from(&state).clone();
                let uri = Uri::borrow_from(&state).clone();
                let headers = HeaderMap::borrow_from(&state).clone();
                if let Some(forwarded) =
                    forward_to_leader_if_needed(method, uri, headers, None).await
                {
                    let res = forwarded.generate_response(&state);
                    return Ok((state, res));
                }
                let headers = HeaderMap::borrow_from(&state);
                let $org_info_arg_name: $org_info_arg_type = match validate_org_info(&headers).await
                {
                    Some(org) => org,
                    None => {
                        let res = GenericResponse {
                            status: StatusCode::UNAUTHORIZED,
                            mime: mime::TEXT_PLAIN,
                            body: "Unauthorized".to_string(),
                            headers: vec![],
                        }
                        .generate_response(&state);
                        return Ok((state, res));
                    }
                };
                let body_result = $body; // Execute the original function's body
                let res = body_result.generate_response(&state);
                Ok((state, res))
            }
            .boxed()
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
                let body_vec = valid_body.to_vec();
                let method = Method::borrow_from(&state).clone();
                let uri = Uri::borrow_from(&state).clone();
                let headers = HeaderMap::borrow_from(&state).clone();
                if let Some(forwarded) =
                    forward_to_leader_if_needed(method, uri, headers, Some(body_vec.clone())).await
                {
                    let res = forwarded.generate_response(&state);
                    return Ok((state, res));
                }
                let body_content = String::from_utf8(body_vec).unwrap();
                let $arg_name: $arg_type = match serde_json::from_str(&body_content) {
                    Ok(io) => io,
                    Err(_) => panic!("This should not happen"),
                };

                let body_result = $body; // Execute the original function's body
                let res = body_result.generate_response(&state);
                Ok((state, res))
            }
            .boxed()
        }
    };
}

macro_rules! body_handler_org_info {
    ($fn_name:ident($org_info_arg_name:ident: $org_info_arg_type:ty, $arg_name:ident: $arg_type:ty) -> $ret_type:ty $body:block) => {
        pub fn $fn_name(mut state: State) -> Pin<Box<HandlerFuture>> {
            async move {
                let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
                    Ok(vb) => vb,
                    Err(_) => panic!("Oh no"),
                };
                let body_vec = valid_body.to_vec();
                let method = Method::borrow_from(&state).clone();
                let uri = Uri::borrow_from(&state).clone();
                let headers = HeaderMap::borrow_from(&state).clone();
                if let Some(forwarded) =
                    forward_to_leader_if_needed(method, uri, headers, Some(body_vec.clone())).await
                {
                    let res = forwarded.generate_response(&state);
                    return Ok((state, res));
                }
                let headers = HeaderMap::borrow_from(&state);
                let $org_info_arg_name: $org_info_arg_type = match validate_org_info(&headers).await
                {
                    Some(org) => org,
                    None => {
                        let res = GenericResponse {
                            status: StatusCode::UNAUTHORIZED,
                            mime: mime::TEXT_PLAIN,
                            body: "Unauthorized".to_string(),
                            headers: vec![],
                        }
                        .generate_response(&state);
                        return Ok((state, res));
                    }
                };
                let body_content = String::from_utf8(body_vec).unwrap();
                let $arg_name: $arg_type = match serde_json::from_str(&body_content) {
                    Ok(io) => io,
                    Err(_) => panic!("This should not happen"),
                };

                let body_result = $body; // Execute the original function's body
                let res = body_result.generate_response(&state);
                Ok((state, res))
            }
            .boxed()
        }
    };
}

macro_rules! body_with_name_handler {
    ($fn_name:ident($org_info_arg_name:ident: $org_info_arg_type:ty, $name_arg_name:ident: $name_arg_type:ty, $body_arg_name:ident: $body_arg_type:ty) -> $ret_type:ty $body:block) => {
        pub fn $fn_name(mut state: State) -> Pin<Box<HandlerFuture>> {
            async move {
                let valid_body = match body::to_bytes(Body::take_from(&mut state)).await {
                    Ok(vb) => vb,
                    Err(_) => panic!("Oh no"),
                };
                let body_vec = valid_body.to_vec();
                let method = Method::borrow_from(&state).clone();
                let uri = Uri::borrow_from(&state).clone();
                let headers = HeaderMap::borrow_from(&state).clone();
                if let Some(forwarded) =
                    forward_to_leader_if_needed(method, uri, headers, Some(body_vec.clone())).await
                {
                    let res = forwarded.generate_response(&state);
                    return Ok((state, res));
                }
                let headers = HeaderMap::borrow_from(&state);
                let $org_info_arg_name: $org_info_arg_type = match validate_org_info(&headers).await
                {
                    Some(org) => org,
                    None => {
                        let res = GenericResponse {
                            status: StatusCode::UNAUTHORIZED,
                            mime: mime::TEXT_PLAIN,
                            body: "Unauthorized".to_string(),
                            headers: vec![],
                        }
                        .generate_response(&state);
                        return Ok((state, res));
                    }
                };

                let path_extractor = NamePathExtractor::borrow_from(&state);
                let $name_arg_name = path_extractor.name.to_string();
                let body_content = String::from_utf8(body_vec).unwrap();
                let $body_arg_name: $body_arg_type = match serde_json::from_str(&body_content) {
                    Ok(io) => io,
                    Err(_) => panic!("This should not happen"),
                };

                let body_result = $body; // Execute the original function's body
                let res = body_result.generate_response(&state);
                Ok((state, res))
            }
            .boxed()
        }
    };
}

macro_rules! name_handler {
    ($fn_name:ident($org_info_arg_name:ident: $org_info_arg_type:ty, $arg_name:ident: $arg_type:ty) -> $ret_type:ty $body:block) => {
        pub fn $fn_name(state: State) -> Pin<Box<HandlerFuture>> {
            async move {
                let method = Method::borrow_from(&state).clone();
                let uri = Uri::borrow_from(&state).clone();
                let headers = HeaderMap::borrow_from(&state).clone();
                if let Some(forwarded) =
                    forward_to_leader_if_needed(method, uri, headers, None).await
                {
                    let res = forwarded.generate_response(&state);
                    return Ok((state, res));
                }
                let headers = HeaderMap::borrow_from(&state);
                let $org_info_arg_name: $org_info_arg_type = match validate_org_info(&headers).await
                {
                    Some(org) => org,
                    None => {
                        let res = GenericResponse {
                            status: StatusCode::UNAUTHORIZED,
                            mime: mime::TEXT_PLAIN,
                            body: "Unauthorized".to_string(),
                            headers: vec![],
                        }
                        .generate_response(&state);
                        return Ok((state, res));
                    }
                };
                let path_extractor = NamePathExtractor::borrow_from(&state);
                let $arg_name = path_extractor.name.to_string();
                let body_result = $body; // Execute the original function's body
                let res = body_result.generate_response(&state);
                Ok((state, res))
            }
            .boxed()
        }
    };
}

#[allow(dead_code)]
fn handle_result_none(value: Result<(), ServiceImplError>) -> GenericResponse {
    match value {
        Ok(_) => GenericResponse {
            status: StatusCode::OK,
            mime: mime::TEXT_PLAIN,
            body: "Ok".to_string(),
            headers: vec![],
        },
        Err(e) => {
            tracing::error!("Error: {:?}", e);
            GenericResponse {
                status: StatusCode::SERVICE_UNAVAILABLE,
                mime: mime::TEXT_PLAIN,
                body: "Service Unavailable".to_string(),
                headers: vec![],
            }
        }
    }
}

fn handle_result<T>(value: Result<T, ServiceImplError>) -> GenericResponse
where
    T: Sized + Serialize,
{
    match value {
        Ok(v) => GenericResponse {
            status: StatusCode::OK,
            mime: mime::APPLICATION_JSON,
            body: serde_json::to_string(&v).unwrap(),
            headers: vec![],
        },
        Err(e) => {
            tracing::error!("Error: {:?}", e);
            GenericResponse {
                status: StatusCode::SERVICE_UNAVAILABLE,
                mime: mime::TEXT_PLAIN,
                body: "Service Unavailable".to_string(),
                headers: vec![],
            }
        }
    }
}

fn handle_result_option<T>(value: Result<Option<T>, ServiceImplError>) -> GenericResponse
where
    T: Sized + Serialize,
{
    match value {
        Ok(v) => match v {
            Some(v) => GenericResponse {
                status: StatusCode::OK,
                mime: mime::APPLICATION_JSON,
                body: serde_json::to_string(&v).unwrap(),
                headers: vec![],
            },
            None => GenericResponse {
                status: StatusCode::NOT_FOUND,
                mime: mime::TEXT_PLAIN,
                body: "Not found".to_string(),
                headers: vec![],
            },
        },
        Err(e) => {
            tracing::error!("Error: {:?}", e);
            GenericResponse {
                status: StatusCode::SERVICE_UNAVAILABLE,
                mime: mime::TEXT_PLAIN,
                body: "Service Unavailable".to_string(),
                headers: vec![],
            }
        }
    }
}

body_handler_org_info! { create_table(org_info: OrgInfo, input: CreateTable) -> GenericResponse {
    handle_result(SERVICE_IMPL.create_table(&org_info, &input).await)
}}

name_handler! { describe_table(org_info: OrgInfo, name: String) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.describe_table(&org_info, &name).await)
}}

body_handler_org_info! { add_alias(org_info: OrgInfo, input: AddAlias) -> GenericResponse {
    handle_result(SERVICE_IMPL.add_alias(&org_info, &input.table_name, &input.alias).await)
}}

body_handler_org_info! { remove_alias(org_info: OrgInfo, input: AddAlias) -> GenericResponse {
    handle_result(SERVICE_IMPL.remove_alias(&org_info, &input.table_name, &input.alias).await)
}}

body_with_name_handler! { create_table_template(org_info: OrgInfo, name: String, input: CreateIndexTemplateBody) -> GenericResponse {
    handle_result(SERVICE_IMPL.create_table_template(&org_info, &name, &input).await)
}}

name_handler! { describe_table_template(org_info: OrgInfo, name: String) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.describe_table_template(&org_info, &name).await)
}}

body_with_name_handler! { create_pipeline(org_info: OrgInfo, name: String, input: PipelineDefinition) -> GenericResponse {
    handle_result(SERVICE_IMPL.create_pipeline(&org_info, &name, &input).await)
}}

name_handler! { describe_pipeline(org_info: OrgInfo, name: String) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.describe_pipeline(&org_info, &name).await)
}}

body_with_name_handler! { create_lifetime_policy(org_info: OrgInfo, name: String, input: ILMPolicyDefinition) -> GenericResponse {
    handle_result(SERVICE_IMPL.create_lifetime_policy(&org_info, &name, &input).await)
}}

name_handler! { describe_lifetime_policy(org_info: OrgInfo, name: String) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.describe_lifetime_policy(&org_info, &name).await)
}}

body_handler_org_info! { speedboat_commit(org_info: OrgInfo, input: SpeedboatCommit) -> GenericResponse {
    handle_result(SERVICE_IMPL.speedboat_commit(&org_info, &input).await)
}}

body_with_name_handler! { iceberg_commit(org_info: OrgInfo, name: String, input: IcebergCommit) -> GenericResponse {
    handle_result(SERVICE_IMPL.iceberg_commit(&org_info, &name, &input).await)
}}

body_with_name_handler! { extension_commit(org_info: OrgInfo, name: String, input: ExtensionCommit) -> GenericResponse {
    handle_result(SERVICE_IMPL.extension_commit(&org_info, &name, &input).await)
}}

body_with_name_handler! { compaction_commit(org_info: OrgInfo, name: String, input: CompactionCommit) -> GenericResponse {
    handle_result(SERVICE_IMPL.compaction_commit(&org_info, &name, &input).await)
}}

body_handler_org_info! { cleanup_commit(org_info: OrgInfo, input: CleanupCommit) -> GenericResponse {
    handle_result(SERVICE_IMPL.cleanup_commit(&org_info, &input).await)
}}

body_handler_org_info! { get_latest_checkpoint(org_info: OrgInfo, input: GetLatestCheckpoint) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.get_latest_checkpoint(&org_info, &input.table_name, input.extension).await)
}}

body_handler_org_info! { get_published_active_checkpoint(org_info: OrgInfo, input: GetLatestCheckpoint) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.get_published_active_checkpoint(&org_info, &input.table_name, input.extension).await)
}}

body_handler_org_info! { get_latest_target_checkpoint(org_info: OrgInfo, input: GetLatestCheckpoint) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.get_latest_target_checkpoint(&org_info, &input.table_name, input.extension).await)
}}

body_handler_org_info! { get_checkpoint_cutover_state(org_info: OrgInfo, input: GetLatestCheckpoint) -> GenericResponse {
    handle_result(SERVICE_IMPL.get_checkpoint_cutover_state(&org_info, &input.table_name, input.extension).await)
}}

body_handler_org_info! { heartbeat_serving_node(org_info: OrgInfo, input: ServingNodeLease) -> GenericResponse {
    handle_result(SERVICE_IMPL.heartbeat_serving_node(&org_info, &input).await)
}}

body_handler_org_info! { record_serving_node_activation(org_info: OrgInfo, input: ServingNodeActivationAck) -> GenericResponse {
    handle_result(SERVICE_IMPL.record_serving_node_activation(&org_info, &input).await)
}}

body_handler_org_info! { record_artifact_readiness(org_info: OrgInfo, input: ArtifactReadinessAck) -> GenericResponse {
    handle_result(SERVICE_IMPL.record_artifact_readiness(&org_info, &input).await)
}}

body_handler_org_info! { list_artifact_readiness(org_info: OrgInfo, input: GetLatestCheckpoint) -> GenericResponse {
    handle_result(SERVICE_IMPL.list_artifact_readiness(&org_info, &input.table_name, input.extension).await)
}}

body_handler_org_info! { get_read_only_coordination_state(org_info: OrgInfo, input: GetLatestCheckpoint) -> GenericResponse {
    handle_result(SERVICE_IMPL.get_read_only_coordination_state(&org_info, &input.table_name, input.extension).await)
}}

body_handler_org_info! { get_checkpoint(org_info: OrgInfo, input: CheckpointDescriptor) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.get_checkpoint(&org_info, &input).await)
}}

name_handler! { get_extension_work_items(org_info: OrgInfo, name: String) -> GenericResponse {
    handle_result(SERVICE_IMPL.get_extension_work_items(&org_info, &name).await)
}}

nothing_handler! { get_compaction_work_items(org_info: OrgInfo) -> GenericResponse {
    handle_result(SERVICE_IMPL.get_compaction_work_items(&org_info).await)
}}

nothing_handler! { get_cleanup_work_items(org_info: OrgInfo) -> GenericResponse {
    handle_result(SERVICE_IMPL.get_cleanup_work_items(&org_info).await)
}}

body_handler! { create_org(input: OrgSettings) -> GenericResponse {
    handle_result(SERVICE_IMPL.create_org(&input).await)
}}

#[cfg(test)]
mod tests {
    use crate::router::router;
    use gotham::hyper::StatusCode;
    use gotham::mime;
    use gotham::test::TestServer;
    use powdrr_service_lib::data_contract::{
        ACCESS_KEY_HEADER_KEY, AddAlias, CreateTable, SECRET_KEY_HEADER_KEY, ServiceMode,
        TEST_ACCESS_KEY, TEST_SECRET_KEY, TableDescription,
    };
    use std::sync::LazyLock;

    pub(crate) static TEST_SERVER: LazyLock<TestServer> =
        LazyLock::new(|| TestServer::with_timeout(router(true), 1000).unwrap());

    #[test]
    fn test_create_and_describe_table() {
        let test_server = &*TEST_SERVER;

        let test_mode_response = test_server
            .client()
            .put(
                "http://localhost/_test/v1/_set_mode",
                serde_json::to_string(&ServiceMode::test()).unwrap(),
                mime::TEXT_PLAIN,
            )
            .perform()
            .unwrap();

        assert_eq!(test_mode_response.status(), StatusCode::OK);

        let body = CreateTable {
            name: "the_name".to_string(),
            tags: Default::default(),
            serving: None,
            dynamodb: None,
            mongodb: None,
            redis: None,
        };

        let client = test_server.client();

        let mut create_table = client.post(
            "http://localhost/api/v1/create_table",
            serde_json::to_string(&body).unwrap(),
            mime::APPLICATION_JSON,
        );
        create_table
            .headers_mut()
            .insert(ACCESS_KEY_HEADER_KEY, TEST_ACCESS_KEY.parse().unwrap());
        create_table
            .headers_mut()
            .insert(SECRET_KEY_HEADER_KEY, TEST_SECRET_KEY.parse().unwrap());

        let create_response = create_table.perform().unwrap();

        assert_eq!(create_response.status(), 200);

        let mut describe_request = client.get("http://localhost/api/v1/describe_table/the_name");
        describe_request
            .headers_mut()
            .insert(ACCESS_KEY_HEADER_KEY, TEST_ACCESS_KEY.parse().unwrap());
        describe_request
            .headers_mut()
            .insert(SECRET_KEY_HEADER_KEY, TEST_SECRET_KEY.parse().unwrap());

        let describe_response = describe_request.perform().unwrap();

        assert_eq!(describe_response.status(), 200);

        let describe_obj: TableDescription =
            serde_json::from_str(&*describe_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(describe_obj.name, "the_name");

        let add_alias_body = AddAlias {
            table_name: "the_name".to_string(),
            alias: "the_alias".to_string(),
        };

        let mut add_alias_request = client.post(
            "http://localhost/api/v1/add_alias",
            serde_json::to_string(&add_alias_body).unwrap(),
            mime::APPLICATION_JSON,
        );
        add_alias_request
            .headers_mut()
            .insert(ACCESS_KEY_HEADER_KEY, TEST_ACCESS_KEY.parse().unwrap());
        add_alias_request
            .headers_mut()
            .insert(SECRET_KEY_HEADER_KEY, TEST_SECRET_KEY.parse().unwrap());

        let add_alias_response = add_alias_request.perform().unwrap();

        assert_eq!(add_alias_response.status(), 200);

        let mut alias_describe_request =
            client.get("http://localhost/api/v1/describe_table/the_alias");
        alias_describe_request
            .headers_mut()
            .insert(ACCESS_KEY_HEADER_KEY, TEST_ACCESS_KEY.parse().unwrap());
        alias_describe_request
            .headers_mut()
            .insert(SECRET_KEY_HEADER_KEY, TEST_SECRET_KEY.parse().unwrap());

        let alias_describe_response = alias_describe_request.perform().unwrap();

        assert_eq!(alias_describe_response.status(), 200);

        let describe_obj: TableDescription =
            serde_json::from_str(&*alias_describe_response.read_utf8_body().unwrap()).unwrap();
        assert_eq!(describe_obj.name, "the_name");

        let mut remove_alias_request = client.post(
            "http://localhost/api/v1/remove_alias",
            serde_json::to_string(&add_alias_body).unwrap(),
            mime::APPLICATION_JSON,
        );
        remove_alias_request
            .headers_mut()
            .insert(ACCESS_KEY_HEADER_KEY, TEST_ACCESS_KEY.parse().unwrap());
        remove_alias_request
            .headers_mut()
            .insert(SECRET_KEY_HEADER_KEY, TEST_SECRET_KEY.parse().unwrap());

        let remove_alias_response = remove_alias_request.perform().unwrap();

        assert_eq!(remove_alias_response.status(), 200);

        let mut no_alias_describe_request =
            client.get("http://localhost/api/v1/describe_table/the_alias");
        no_alias_describe_request
            .headers_mut()
            .insert(ACCESS_KEY_HEADER_KEY, TEST_ACCESS_KEY.parse().unwrap());
        no_alias_describe_request
            .headers_mut()
            .insert(SECRET_KEY_HEADER_KEY, TEST_SECRET_KEY.parse().unwrap());

        let no_alias_describe_response = no_alias_describe_request.perform().unwrap();

        assert_eq!(no_alias_describe_response.status(), StatusCode::NOT_FOUND);
    }
}
