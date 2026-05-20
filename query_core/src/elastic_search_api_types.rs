use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct QueryStringSearch {
    #[allow(dead_code)]
    pub allow_partial_search_results: Option<bool>,
    #[allow(dead_code)]
    pub sort: Option<String>,
    #[allow(dead_code)]
    pub ignore_unavailable: Option<bool>,
    #[allow(dead_code)]
    pub allow_no_indices: Option<bool>,
    #[allow(dead_code)]
    pub expand_wildcards: Option<String>,
    pub rest_total_hits_as_int: Option<bool>,
}

impl QueryStringSearch {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            allow_partial_search_results: None,
            sort: None,
            ignore_unavailable: None,
            allow_no_indices: None,
            expand_wildcards: None,
            rest_total_hits_as_int: None,
        }
    }
}
