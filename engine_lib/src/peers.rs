use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use gotham::mime;
use gotham::plain::test::AsyncTestServer;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::{error::Error, fmt::Display};

use crate::compaction::{CompactionCommand, CompactionResponse, compact_logs};
use crate::data_contract::{ExtensionFileMetadata, FileSetPayload};
use crate::elastic_search_common::result_to_record_batch;
use crate::elastic_search_responses::QueryResultHit;
use crate::private_api::{compaction_query, extension_query, prefetch_query};
use crate::schema_massager::{PowdrrSchema, SqlQuery};
use crate::test_api::{CompactionMode, PeerModeType};
use crate::{private_api::data_query, state_common::FileFilter};

#[derive(Serialize, Deserialize, Clone)]
pub struct FieldFileFilterDescriptor {
    pub field_name: String,
    pub file_filter: FileFilter,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FileFilterDescriptor {
    pub able_name: String,
    pub filters: Vec<FieldFileFilterDescriptor>,
}

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../shared/service_control_plane/checkpoint_descriptor.rs"
));

#[derive(Serialize, Deserialize)]
pub enum PrivateInvocation {
    Sql(PrivateSqlInvocation),
    Compaction(PrivateCompactionInvocation),
    Extension(PrivateExtensionInvocation),
    Prefetch(PrivatePrefetchInvocation),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PrivateSqlInvocation {
    pub sql: SqlQuery,
    pub required_extensions: Vec<String>,
    pub file_filter: Vec<FileFilterDescriptor>,
    pub checkpoints: Vec<CheckpointDescriptor>,
}

#[derive(Serialize, Deserialize)]
pub struct PrivateSqlInvocationExternal {
    pub invocation: PrivateSqlInvocation,
    pub index: u64,
    pub num: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PrivateSearchInvocation {
    pub sql: SqlQuery,
    pub required_extensions: Vec<String>,
    pub checkpoints: Vec<CheckpointDescriptor>,
    pub table: String,
    pub size: usize,
    pub calculate_score: bool,
    pub aggregations: Vec<PrivateSearchAggregationSpec>,
    pub sorts: Vec<PrivateSearchSortSpec>,
}

#[derive(Serialize, Deserialize)]
pub struct PrivateSearchInvocationExternal {
    pub invocation: PrivateSearchInvocation,
    pub index: u64,
    pub num: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PrivateSearchResult {
    pub hits: Vec<QueryResultHit>,
    pub total_hits: usize,
    pub aggregations: Vec<PrivateSearchAggregationPartial>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PrivateSearchSortSpec {
    pub field: String,
    pub descending: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum PrivateSearchAggregationSpec {
    Terms {
        name: String,
        field: String,
        size: Option<u32>,
        sub_aggregations: Vec<PrivateSearchAggregationSpec>,
    },
    Average {
        name: String,
        field: String,
    },
    Filter {
        name: String,
        filter: PrivateSearchAggregationFilterSpec,
        sub_aggregations: Vec<PrivateSearchAggregationSpec>,
    },
}

#[derive(Serialize, Deserialize, Clone)]
pub enum PrivateSearchAggregationFilterSpec {
    Term { field: String, value: String },
}

#[derive(Serialize, Deserialize, Clone)]
pub enum PrivateSearchAggregationPartial {
    Terms {
        name: String,
        buckets: Vec<PrivateSearchTermsBucketPartial>,
    },
    Average {
        name: String,
        sum: f64,
        count: u64,
    },
    Filter {
        name: String,
        doc_count: u64,
        sub_aggregations: Vec<PrivateSearchAggregationPartial>,
    },
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PrivateSearchTermsBucketPartial {
    pub key: String,
    pub doc_count: u64,
    pub sub_aggregations: Vec<PrivateSearchAggregationPartial>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PrivateCompactionInvocation {
    pub sql: SqlQuery,
    pub speedboat_files: FileSetPayload,
    pub table_schema: PowdrrSchema,
    pub delete_files: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct PrivateCompactionInvocationExternal {
    pub invocation: PrivateCompactionInvocation,
    pub index: u64,
    pub num: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PrivateExtensionInvocation {
    pub extension_name: String,
    pub speedboat_files: FileSetPayload,
    pub iceberg_files: FileSetPayload,
}

#[derive(Serialize, Deserialize)]
pub struct PrivateExtensionInvocationExternal {
    pub invocation: PrivateExtensionInvocation,
    pub index: u64,
    pub num: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PrivatePrefetchInvocation {
    pub required_extensions: Vec<String>,
    pub checkpoints: Vec<CheckpointDescriptor>,
}

#[derive(Serialize, Deserialize)]
pub struct PrivatePrefetchInvocationExternal {
    pub invocation: PrivatePrefetchInvocation,
    pub index: u64,
    pub num: u64,
}

#[derive(Serialize)]
pub struct PrivateMetadataInvocation {
    name: String,
}

pub enum PrivateInvocationResult {
    Data(Vec<RecordBatch>),
    Extension(ExtensionFileMetadata),
    Prefetch,
}

#[derive(Debug)]
pub struct PeerClientError {
    pub message: String,
}

impl Display for PeerClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)?;
        Ok(())
    }
}

impl Error for PeerClientError {}

unsafe impl Send for PeerClientError {}
unsafe impl Sync for PeerClientError {}

#[async_trait]
pub trait PeerClient: Send + Sync + Debug {
    fn box_clone(&self) -> Box<dyn PeerClient>;

    async fn private_sql(
        &self,
        invocation: &PrivateSqlInvocation,
        index: u64,
        num: u64,
    ) -> Result<Vec<RecordBatch>, PeerClientError>;

    async fn private_search(
        &self,
        invocation: &PrivateSearchInvocation,
        index: u64,
        num: u64,
    ) -> Result<PrivateSearchResult, PeerClientError>;

    async fn private_compaction(
        &self,
        invocation: &PrivateCompactionInvocation,
        index: u64,
        num: u64,
    ) -> Result<Vec<RecordBatch>, PeerClientError>;

    async fn private_extension(
        &self,
        invocation: &PrivateExtensionInvocation,
        index: u64,
        num: u64,
    ) -> Result<ExtensionFileMetadata, PeerClientError>;

    async fn private_prefetch(
        &self,
        invocation: &PrivatePrefetchInvocation,
        index: u64,
        num: u64,
    ) -> Result<(), PeerClientError>;

    async fn private_compaction_leader(
        &self,
        invocation: &CompactionCommand,
    ) -> Result<Option<CompactionResponse>, PeerClientError>;
}

impl Clone for Box<dyn PeerClient> {
    fn clone(&self) -> Box<dyn PeerClient> {
        self.box_clone()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RemotePeer {
    address: String,
    client: Client,
}

impl RemotePeer {
    #[allow(dead_code)]
    pub fn new(address: String) -> Self {
        Self {
            address,
            client: reqwest::Client::new(),
        }
    }
}

unsafe impl Send for RemotePeer {}
unsafe impl Sync for RemotePeer {}

#[async_trait]
impl PeerClient for RemotePeer {
    fn box_clone(&self) -> Box<dyn PeerClient> {
        Box::new(self.clone())
    }

    async fn private_sql(
        &self,
        invocation: &PrivateSqlInvocation,
        index: u64,
        num: u64,
    ) -> Result<Vec<RecordBatch>, PeerClientError> {
        let invocation_obj = PrivateSqlInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };

        let response = self
            .client
            .post(format!("http://{}/_private/v1/_sql", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                let body = response.bytes().await.unwrap();
                Ok(result_to_record_batch(serde_json::from_slice(&body).unwrap()).await)
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_search(
        &self,
        invocation: &PrivateSearchInvocation,
        index: u64,
        num: u64,
    ) -> Result<PrivateSearchResult, PeerClientError> {
        let invocation_obj = PrivateSearchInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };

        let response = self
            .client
            .post(format!("http://{}/_private/v1/_search", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                let body = response.bytes().await.unwrap();
                Ok(serde_json::from_slice(&body).unwrap())
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_compaction(
        &self,
        invocation: &PrivateCompactionInvocation,
        index: u64,
        num: u64,
    ) -> Result<Vec<RecordBatch>, PeerClientError> {
        let invocation_obj = PrivateCompactionInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };

        let response = self
            .client
            .post(format!("http://{}/_private/v1/_compact", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                let body = response.bytes().await.unwrap();
                Ok(result_to_record_batch(serde_json::from_slice(&body).unwrap()).await)
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_extension(
        &self,
        invocation: &PrivateExtensionInvocation,
        index: u64,
        num: u64,
    ) -> Result<ExtensionFileMetadata, PeerClientError> {
        let invocation_obj = PrivateExtensionInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };

        let response = self
            .client
            .post(format!("http://{}/_private/v1/_extension", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                let body = response.bytes().await.unwrap();
                Ok(serde_json::from_slice(&body).unwrap())
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_prefetch(
        &self,
        invocation: &PrivatePrefetchInvocation,
        index: u64,
        num: u64,
    ) -> Result<(), PeerClientError> {
        let invocation_obj = PrivatePrefetchInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };

        let response = self
            .client
            .post(format!("http://{}/_private/v1/_prefetch", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                Ok(())
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_compaction_leader(
        &self,
        invocation: &CompactionCommand,
    ) -> Result<Option<CompactionResponse>, PeerClientError> {
        let response = self
            .client
            .post(format!(
                "http://{}/_private/v1/_compact_leader",
                self.address
            ))
            .json(&invocation)
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());

        let body = response.bytes().await.unwrap();
        let response = serde_json::from_slice(&body).unwrap();
        Ok(Some(response))
    }
}

#[derive(Clone)]
pub(crate) struct TestingRemotePeer {
    server: AsyncTestServer,
}

impl Debug for TestingRemotePeer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("TestingRemotePeer")
    }
}

impl TestingRemotePeer {
    #[allow(dead_code)]
    pub fn new(server: AsyncTestServer) -> Self {
        Self { server }
    }
}

unsafe impl Send for TestingRemotePeer {}
unsafe impl Sync for TestingRemotePeer {}

#[async_trait]
impl PeerClient for TestingRemotePeer {
    fn box_clone(&self) -> Box<dyn PeerClient> {
        Box::new(self.clone())
    }

    async fn private_sql(
        &self,
        invocation: &PrivateSqlInvocation,
        index: u64,
        num: u64,
    ) -> Result<Vec<RecordBatch>, PeerClientError> {
        let invocation_obj = PrivateSqlInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };
        let body = serde_json::to_string(&invocation_obj).unwrap();

        let response = self
            .server
            .client()
            .post("http://localhost/_private/v1/_sql")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                let body = response.read_body().await.unwrap();
                Ok(result_to_record_batch(serde_json::from_slice(&body).unwrap()).await)
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_search(
        &self,
        invocation: &PrivateSearchInvocation,
        index: u64,
        num: u64,
    ) -> Result<PrivateSearchResult, PeerClientError> {
        let invocation_obj = PrivateSearchInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };
        let body = serde_json::to_string(&invocation_obj).unwrap();

        let response = self
            .server
            .client()
            .post("http://localhost/_private/v1/_search")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                let body = response.read_body().await.unwrap();
                Ok(serde_json::from_slice(&body).unwrap())
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_compaction(
        &self,
        invocation: &PrivateCompactionInvocation,
        index: u64,
        num: u64,
    ) -> Result<Vec<RecordBatch>, PeerClientError> {
        let invocation_obj = PrivateCompactionInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };
        let body = serde_json::to_string(&invocation_obj).unwrap();

        let response = self
            .server
            .client()
            .post("http://localhost/_private/v1/_compact")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                let body = response.read_body().await.unwrap();
                Ok(result_to_record_batch(serde_json::from_slice(&body).unwrap()).await)
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_extension(
        &self,
        invocation: &PrivateExtensionInvocation,
        index: u64,
        num: u64,
    ) -> Result<ExtensionFileMetadata, PeerClientError> {
        let invocation_obj = PrivateExtensionInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };
        let body = serde_json::to_string(&invocation_obj).unwrap();

        let response = self
            .server
            .client()
            .post("http://localhost/_private/v1/_extension")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                let body = response.read_body().await.unwrap();
                Ok(serde_json::from_slice(&body).unwrap())
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_prefetch(
        &self,
        invocation: &PrivatePrefetchInvocation,
        index: u64,
        num: u64,
    ) -> Result<(), PeerClientError> {
        let invocation_obj = PrivatePrefetchInvocationExternal {
            invocation: invocation.clone(),
            index,
            num,
        };
        let body = serde_json::to_string(&invocation_obj).unwrap();

        let response = self
            .server
            .client()
            .post("http://localhost/_private/v1/_prefetch")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match response {
            Ok(response) => {
                assert!(response.status().is_success());
                Ok(())
            }
            Err(error) => Err(PeerClientError {
                message: format!("Error: {}", error),
            }),
        }
    }

    async fn private_compaction_leader(
        &self,
        invocation: &CompactionCommand,
    ) -> Result<Option<CompactionResponse>, PeerClientError> {
        let response = self
            .server
            .client()
            .post("http://localhost/_private/v1/_compact_leader")
            .body(serde_json::to_string(&invocation).unwrap())
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await
            .unwrap();

        assert!(response.status().is_success());

        let response_bytes = response.read_body().await.unwrap();
        let response_str = String::from_utf8(response_bytes).unwrap();

        let response = serde_json::from_str::<CompactionResponse>(response_str.as_str()).unwrap();
        Ok(Some(response))
    }
}

pub struct PeerProvider {
    mode: PeerModeType,
    clients: Vec<Box<dyn PeerClient>>,
}

impl PeerProvider {
    pub fn new(mode: PeerModeType) -> Self {
        Self {
            mode: mode.clone(),
            clients: Self::create_clients(&mode),
        }
    }

    fn create_clients(mode: &PeerModeType) -> Vec<Box<dyn PeerClient>> {
        match mode {
            PeerModeType::SelfOnly => {
                vec![Box::new(SelfPeer::new(CompactionMode::Disabled))]
            }
            PeerModeType::Remote(addresses) => {
                let mut addresses = addresses.clone();
                addresses.sort_unstable();
                addresses
                    .iter()
                    .map(|address| {
                        Box::new(RemotePeer::new(address.clone())) as Box<dyn PeerClient>
                    })
                    .collect()
            }
            PeerModeType::Testing(server) => {
                vec![Box::new(TestingRemotePeer::new(server.clone()))]
            }
        }
    }

    pub fn set_mode(&mut self, mode: PeerModeType) {
        self.mode = mode.clone();
        self.clients = Self::create_clients(&mode);
    }

    pub fn get_peer_clients(&self) -> Vec<Box<dyn PeerClient>> {
        self.clients.clone()
    }
}

pub async fn get_docker_peer_ips() -> Vec<String> {
    let service_names = match std::env::var("SERVICE_NAMES") {
        Ok(name) => name,
        Err(_) => {
            tracing::error!("SERVICE_NAMES is not set");
            return vec![];
        }
    };

    service_names
        .split(',')
        .map(|service_name| service_name.to_string())
        .collect()
}

#[derive(Clone, Debug)]
pub struct SelfPeer {
    pub compaction_mode: CompactionMode,
}

impl SelfPeer {
    pub fn new(compaction_mode: CompactionMode) -> Self {
        Self {
            compaction_mode: compaction_mode.clone(),
        }
    }
}

unsafe impl Send for SelfPeer {}
unsafe impl Sync for SelfPeer {}

#[async_trait]
impl PeerClient for SelfPeer {
    fn box_clone(&self) -> Box<dyn PeerClient> {
        Box::new(self.clone())
    }

    async fn private_sql(
        &self,
        invocation: &PrivateSqlInvocation,
        index: u64,
        num: u64,
    ) -> Result<Vec<RecordBatch>, PeerClientError> {
        let query_result = data_query(invocation, index, num).await;
        match query_result {
            Ok(query_result) => Ok(result_to_record_batch(query_result.result).await),
            Err(error) => Err(PeerClientError {
                message: error.message,
            }),
        }
    }

    async fn private_search(
        &self,
        invocation: &PrivateSearchInvocation,
        index: u64,
        num: u64,
    ) -> Result<PrivateSearchResult, PeerClientError> {
        match crate::private_api::search_query(invocation, index, num).await {
            Ok(result) => Ok(result),
            Err(error) => Err(PeerClientError {
                message: error.message,
            }),
        }
    }

    async fn private_compaction(
        &self,
        invocation: &PrivateCompactionInvocation,
        index: u64,
        num: u64,
    ) -> Result<Vec<RecordBatch>, PeerClientError> {
        let query_result = compaction_query(invocation, index, num).await;
        match query_result {
            Ok(query_result) => Ok(result_to_record_batch(query_result.result).await),
            Err(error) => Err(PeerClientError {
                message: error.message,
            }),
        }
    }

    async fn private_extension(
        &self,
        invocation: &PrivateExtensionInvocation,
        index: u64,
        num: u64,
    ) -> Result<ExtensionFileMetadata, PeerClientError> {
        match extension_query(invocation, index, num).await {
            Ok(result) => Ok(result),
            Err(error) => Err(PeerClientError {
                message: error.message,
            }),
        }
    }

    async fn private_prefetch(
        &self,
        invocation: &PrivatePrefetchInvocation,
        index: u64,
        num: u64,
    ) -> Result<(), PeerClientError> {
        match prefetch_query(invocation, index, num).await {
            Ok(_) => Ok(()),
            Err(error) => Err(PeerClientError {
                message: error.message,
            }),
        }
    }

    async fn private_compaction_leader(
        &self,
        invocation: &CompactionCommand,
    ) -> Result<Option<CompactionResponse>, PeerClientError> {
        match &self.compaction_mode {
            CompactionMode::Async(_threshold) => {
                match compact_logs(Arc::new(invocation.clone())).await {
                    Ok(success) => {
                        if success.status == 200 {
                            Ok(Some(serde_json::from_str(success.body.as_str()).unwrap()))
                        } else {
                            Err(PeerClientError {
                                message: success.body,
                            })
                        }
                    }
                    Err(error) => Err(PeerClientError {
                        message: error.to_string(),
                    }),
                }
            }
            CompactionMode::External(compaction_leader) => {
                let client = Client::new();
                let response = client
                    .post(format!("{}/_private/v1/_compact_leader", compaction_leader))
                    .body(serde_json::to_string(&invocation).unwrap())
                    .send()
                    .await
                    .unwrap();

                assert!(response.status().is_success());
                let response_str = response.text().await.unwrap();
                let response =
                    serde_json::from_str::<CompactionResponse>(response_str.as_str()).unwrap();
                Ok(Some(response))
            }
            CompactionMode::Disabled => Ok(None),
        }
    }
}
