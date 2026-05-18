use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct ServiceApiError {
    pub(crate) message: String,
}

impl Error for ServiceApiError {}

impl Display for ServiceApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

unsafe impl Send for ServiceApiError {}
unsafe impl Sync for ServiceApiError {}

impl ServiceApiError {
    pub fn new(message: String) -> Self {
        assert!(!message.is_empty(), "Message must not be empty");
        ServiceApiError { message }
    }
}
