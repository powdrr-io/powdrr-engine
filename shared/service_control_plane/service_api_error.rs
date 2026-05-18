#[derive(Debug, Clone)]
pub struct ServiceApiError {
    pub(crate) message: String,
}

impl std::error::Error for ServiceApiError {}

impl std::fmt::Display for ServiceApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
