use serde::Deserialize;

use gotham::prelude::StaticResponseExtender;
use gotham::state::StateData;
use powdrr_query_lib::elastic_search_api_types::QueryStringSearch;

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct NamePathExtractor {
    pub(crate) name: String,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct NameIdPathExtractor {
    pub(crate) name: String,
    pub(crate) id: String,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct AliasPathExtractor {
    pub(crate) alias: String,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct NameAliasPathExtractor {
    pub(crate) name: String,
    pub(crate) alias: String,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub struct QueryStringSearchExtractor {
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

impl From<QueryStringSearchExtractor> for QueryStringSearch {
    fn from(value: QueryStringSearchExtractor) -> Self {
        Self {
            allow_partial_search_results: value.allow_partial_search_results,
            sort: value.sort,
            ignore_unavailable: value.ignore_unavailable,
            allow_no_indices: value.allow_no_indices,
            expand_wildcards: value.expand_wildcards,
            rest_total_hits_as_int: value.rest_total_hits_as_int,
        }
    }
}

impl QueryStringSearchExtractor {
    pub fn into_query_string_search(self) -> QueryStringSearch {
        self.into()
    }
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub(crate) struct QueryStringClusterSettings {
    pub(crate) include_defaults: Option<bool>,
    #[allow(dead_code)]
    pub(crate) flat_settings: Option<bool>,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub(crate) struct QueryStringClusterHealth {
    #[allow(dead_code)]
    pub(crate) level: Option<String>,
    #[allow(dead_code)]
    pub(crate) local: Option<bool>,
    #[allow(dead_code)]
    pub(crate) timeout: Option<String>,
    #[allow(dead_code)]
    pub(crate) wait_for_status: Option<String>,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub(crate) struct QueryStringFieldCaps {
    pub(crate) fields: Option<String>,
    #[allow(dead_code)]
    pub(crate) include_unmapped: Option<bool>,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
pub(crate) struct QueryStringAliases {
    #[allow(dead_code)]
    pub(crate) timeout: Option<String>,
}
