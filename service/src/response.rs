use gotham::helpers::http::response::create_response;
use gotham::hyper::header::HeaderName;
use gotham::hyper::StatusCode;
use gotham::mime::Mime;
use gotham::state::State;

pub(crate) struct GenericResponse {
    pub status: StatusCode,
    pub mime: Mime,
    pub body: String,
    pub headers: Vec<(HeaderName, String)>,
}

unsafe impl Send for GenericResponse {}
unsafe impl Sync for GenericResponse {}


impl GenericResponse {
    pub(crate) fn generate_response(&self, state: &State) -> gotham::hyper::Response<gotham::hyper::Body> {
        let mut response = create_response(state, self.status.clone(), self.mime.clone(), self.body.clone());
        if self.headers.len() != 0 {
            let response_headers = response.headers_mut();
            for (k, v) in self.headers.iter() {
                response_headers.insert(k, v.parse().unwrap());
            }
        }
        response
    }
}
