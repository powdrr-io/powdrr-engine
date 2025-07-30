use futures_util::future::FutureExt;
use gotham::handler::HandlerFuture;
use gotham::hyper::StatusCode;
use gotham::state::State;
use gotham::state::FromState;
use gotham::hyper::{body, Body};
use powdrr_lib::state_hosted_service::{CreateTable, API_SERVICE_CLIENT};
use std::pin::Pin;
use gotham::mime;
use crate::response::GenericResponse;


macro_rules! post_handler {
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


post_handler! { create_table(input: CreateTable) -> GenericResponse {
    API_SERVICE_CLIENT.create_table(&input).await;
    GenericResponse {
        status: StatusCode::OK, mime: mime::TEXT_PLAIN, body: "Ok".to_string(), headers: vec![],
    }
}}




#[cfg(test)]
mod tests {
    use std::sync::LazyLock;
    use gotham::mime;
    use gotham::test::TestServer;
    use powdrr_lib::state_hosted_service::CreateTable;
    use crate::router::router;

    pub(crate) static TEST_SERVER: LazyLock<TestServer> = LazyLock::new(|| TestServer::with_timeout(router(true), 1000).unwrap());

    #[test]
    fn test_create_table() {
        let test_server = &*TEST_SERVER;

        test_server.client().put(
            "http://localhost/_test/v1/_testing_mode",
            "",
            mime::TEXT_PLAIN
        ).perform().unwrap();

        let body = CreateTable {
            name: "the name".to_string(),
            tags: Default::default(),
        };

        let response = test_server.client().post(
            "http://localhost/api/v1/create_table",
            serde_json::to_string(&body).unwrap(),
            mime::APPLICATION_JSON,
        ).perform().unwrap();

        assert_eq!(response.status(), 200);
    }
}
