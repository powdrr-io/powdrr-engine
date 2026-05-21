use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::{error::Error, fmt::Display};

use crate::compaction::{compact_logs, CompactionCommand, CompactionResponse};
use crate::data_contract::{ExtensionFileMetadata, FileSetPayload};
use crate::elastic_search_common::result_to_record_batch;
use crate::elastic_search_responses::QueryResultHit;
use crate::private_api::{
    compaction_query_batches, data_query_batches, extension_query, prefetch_query,
};
use crate::schema_massager::{PowdrrSchema, SqlQuery};
use crate::state_common::FileFilter;
use crate::test_api::{CompactionMode, PeerModeType};
use gotham::plain::test::AsyncTestServer;
use mime;
pub use powdrr_control_plane::checkpoint_descriptor::CheckpointDescriptor;

#[derive(Serialize, Deserialize, Clone)]
pub struct FieldFileFilterDescriptor {
    pub field_name: String,
    pub file_filter: FileFilter,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FileFilterDescriptor {
    pub able_name: String, // Field name to list of (operator, value)
    pub filters: Vec<FieldFileFilterDescriptor>,
}

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
    pub exact_sql: Option<SqlQuery>,
    pub exact_constraints: Vec<PrivateExactConstraintGroup>,
    pub range_constraints: Vec<PrivateSearchRangeConstraint>,
    pub required_extensions: Vec<String>,
    pub base_extension_suffixes: Vec<String>,
    pub exact_extension_suffixes: Vec<String>,
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PrivateExactConstraintGroup {
    pub field: String,
    pub values: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PrivateSearchRangeConstraint {
    pub field: String,
    pub gt: Option<Value>,
    pub gte: Option<Value>,
    pub lt: Option<Value>,
    pub lte: Option<Value>,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum PrivateSearchAggregationSpec {
    Terms {
        name: String,
        field: String,
        size: Option<u32>,
        order: Option<PrivateSearchTermsOrderSpec>,
        missing: Option<Value>,
        sub_aggregations: Vec<PrivateSearchAggregationSpec>,
    },
    Average {
        name: String,
        field: String,
    },
    Cardinality {
        name: String,
        field: String,
    },
    DateHistogram {
        name: String,
        field: String,
        fixed_interval: String,
        min_doc_count: Option<u64>,
        extended_bounds: Option<PrivateSearchDateHistogramExtendedBoundsSpec>,
        sub_aggregations: Vec<PrivateSearchAggregationSpec>,
    },
    Filter {
        name: String,
        filter: PrivateSearchAggregationFilterSpec,
        sub_aggregations: Vec<PrivateSearchAggregationSpec>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PrivateSearchTermsOrderSpec {
    CountAsc,
    CountDesc,
    KeyAsc,
    KeyDesc,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PrivateSearchDateHistogramExtendedBoundsSpec {
    pub min: Value,
    pub max: Value,
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
    Cardinality {
        name: String,
        values: Vec<String>,
    },
    DateHistogram {
        name: String,
        buckets: Vec<PrivateSearchHistogramBucketPartial>,
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
pub struct PrivateSearchHistogramBucketPartial {
    pub key: i64,
    pub key_as_string: String,
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
        RemotePeer {
            address: address,
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

        let resp = self
            .client
            .post(format!("http://{}/_private/v1/_sql", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                let body = res.bytes().await.unwrap();
                Ok(result_to_record_batch(body.as_ref()))
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
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

        let resp = self
            .client
            .post(format!("http://{}/_private/v1/_search", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                let body = res.bytes().await.unwrap();
                Ok(serde_json::from_slice(&body).unwrap())
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
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

        let resp = self
            .client
            .post(format!("http://{}/_private/v1/_compact", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                let body = res.bytes().await.unwrap();
                Ok(result_to_record_batch(body.as_ref()))
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
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

        let resp = self
            .client
            .post(format!("http://{}/_private/v1/_extension", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                let body = res.bytes().await.unwrap();
                Ok(serde_json::from_slice(&body).unwrap())
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
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

        let resp = self
            .client
            .post(format!("http://{}/_private/v1/_prefetch", self.address))
            .json(&invocation_obj)
            .send()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                Ok(())
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
            }),
        }
    }

    async fn private_compaction_leader(
        &self,
        invocation: &CompactionCommand,
    ) -> Result<Option<CompactionResponse>, PeerClientError> {
        let res = self
            .client
            .post(format!(
                "http://{}/_private/v1/_compact_leader",
                self.address
            ))
            .json(&invocation)
            .send()
            .await
            .unwrap();

        assert!(res.status().is_success());

        let body = res.bytes().await.unwrap();
        let response = serde_json::from_slice(&body).unwrap();
        Ok(Some(response))
    }

    /*
    async fn private_metadata(&self, invocation: &PrivateMetadataInvocation) -> Result<String, PeerClientError> {
        let address = &self.address;
        let body = serde_json::to_string(invocation);
        match body {
            Ok(b) => {
                let resp = futures::executor::block_on(self.client.post(format!("{address}/_private/v1/_metadata"))
                .header("Content-Type", "application/json")
                .body(b)
                .send());
                match resp {
                    Ok(r) => {
                        let text = futures::executor::block_on(r.text());
                        match text {
                            Ok(t) => Ok(t),
                            Err(e) => Err(PeerClientError{ message: "Error".to_string() }),
                        }
                    },
                    Err(e) => Err(PeerClientError{ message: "Error".to_string() })
                }
            },
            Err(e) => {
                Err(PeerClientError{ message: "Error".to_string() })
            }
        }
    }
    */
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
        TestingRemotePeer { server }
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
        let body = match serde_json::to_string(&invocation_obj) {
            Ok(b) => b,
            Err(_) => {
                panic!("Malformed request")
            }
        };

        let resp = self
            .server
            .client()
            .post("http://localhost/_private/v1/_sql")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                let body = res.read_body().await.unwrap();
                Ok(result_to_record_batch(&body))
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
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
        let body = match serde_json::to_string(&invocation_obj) {
            Ok(b) => b,
            Err(_) => {
                panic!("Malformed request")
            }
        };

        let resp = self
            .server
            .client()
            .post("http://localhost/_private/v1/_search")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                let body = res.read_body().await.unwrap();
                Ok(serde_json::from_slice(&body).unwrap())
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
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
        let body = match serde_json::to_string(&invocation_obj) {
            Ok(b) => b,
            Err(_) => {
                panic!("Malformed request")
            }
        };

        let resp = self
            .server
            .client()
            .post("http://localhost/_private/v1/_compact")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                let body = res.read_body().await.unwrap();
                Ok(result_to_record_batch(&body))
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
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
        let body = match serde_json::to_string(&invocation_obj) {
            Ok(b) => b,
            Err(_) => {
                panic!("Malformed request")
            }
        };

        let resp = self
            .server
            .client()
            .post("http://localhost/_private/v1/_extension")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                let body = res.read_body().await.unwrap();
                Ok(serde_json::from_slice(&body).unwrap())
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
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
        let body = match serde_json::to_string(&invocation_obj) {
            Ok(b) => b,
            Err(_) => {
                panic!("Malformed request")
            }
        };

        let resp = self
            .server
            .client()
            .post("http://localhost/_private/v1/_prefetch")
            .body(body)
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await;

        match resp {
            Ok(res) => {
                assert!(res.status().is_success());
                Ok(())
            }
            Err(e) => Err(PeerClientError {
                message: format!("Error: {}", e),
            }),
        }
    }

    async fn private_compaction_leader(
        &self,
        invocation: &CompactionCommand,
    ) -> Result<Option<CompactionResponse>, PeerClientError> {
        let res = self
            .server
            .client()
            .post("http://localhost/_private/v1/_compact_leader")
            .body(serde_json::to_string(&invocation).unwrap())
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await
            .unwrap();

        assert!(res.status().is_success());

        let response_bytes = res.read_body().await.unwrap();
        let response_str = String::from_utf8(response_bytes).unwrap();

        let response = serde_json::from_str::<CompactionResponse>(response_str.as_str()).unwrap();
        Ok(Some(response))
    }
    /*
    async fn private_metadata(&self, invocation: &PrivateMetadataInvocation) -> Result<String, PeerClientError> {
        let address = &self.address;
        let body = serde_json::to_string(invocation);
        match body {
            Ok(b) => {
                let resp = futures::executor::block_on(self.client.post(format!("{address}/_private/v1/_metadata"))
                .header("Content-Type", "application/json")
                .body(b)
                .send());
                match resp {
                    Ok(r) => {
                        let text = futures::executor::block_on(r.text());
                        match text {
                            Ok(t) => Ok(t),
                            Err(e) => Err(PeerClientError{ message: "Error".to_string() }),
                        }
                    },
                    Err(e) => Err(PeerClientError{ message: "Error".to_string() })
                }
            },
            Err(e) => {
                Err(PeerClientError{ message: "Error".to_string() })
            }
        }
    }
    */
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
                let peers: Vec<Box<dyn PeerClient>> =
                    vec![Box::new(SelfPeer::new(CompactionMode::Disabled))];
                peers
            }
            PeerModeType::Remote(addresses) => {
                let mut addresses = addresses.clone();
                addresses.sort_unstable();
                let mut peers: Vec<Box<dyn PeerClient>> = vec![];
                for address in addresses.iter() {
                    peers.push(Box::new(RemotePeer::new(address.clone())))
                }
                peers
            }
            PeerModeType::Testing(server) => {
                let peers: Vec<Box<dyn PeerClient>> =
                    vec![Box::new(TestingRemotePeer::new(server.clone()))];
                peers
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
        .map(|x| x.to_string())
        .collect::<Vec<String>>()
}

pub async fn get_kubernetes_peer_ips() -> Vec<String> {
    let pod_name = match std::env::var("HOSTNAME") {
        Ok(name) => name,
        Err(_) => {
            tracing::error!("HOSTNAME is not set");
            return vec![];
        }
    };

    let client = kube::Client::try_default().await.unwrap();
    let pods: Api<Pod> = Api::default_namespaced(client.clone());

    // find the app label of the own pod
    let own_pod = pods.get_metadata(&pod_name).await.unwrap();
    let app_label = own_pod
        .metadata
        .labels
        .unwrap()
        .get("app")
        .unwrap()
        .to_string();

    // get all pods with app label
    let pods = pods.list(&Default::default()).await.unwrap();
    let pods = pods
        .items
        .iter()
        .filter(|pod| pod.metadata.labels.as_ref().unwrap().get("app").unwrap() == &app_label);

    let mut ips: Vec<String> = vec![];
    for pod in pods {
        tracing::warn!("Pod: {:?}", pod.metadata.name);
        let pod_status = match pod.status.as_ref() {
            Some(status) => status,
            None => {
                tracing::error!("Pod status is None for pod: {:?}", pod.metadata.name);
                continue;
            }
        };

        let pod_ip = match pod_status.pod_ip.as_ref() {
            Some(ip) => ip,
            None => {
                tracing::error!("Pod IP is None for pod: {:?}", pod.metadata.name);
                continue;
            }
        };

        ips.push(pod_ip.clone());
    }

    //filter duplicate IPs
    ips.sort_unstable();
    ips.dedup();

    return ips;
}

#[derive(Clone, Debug)]
pub struct SelfPeer {
    pub compaction_mode: CompactionMode,
}

impl SelfPeer {
    pub fn new(compaction_mode: CompactionMode) -> Self {
        SelfPeer {
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
        let query_result = data_query_batches(invocation, index, num).await;
        match query_result {
            Ok(qr) => Ok(qr),
            Err(e) => Err(PeerClientError { message: e.message }),
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
            Err(e) => Err(PeerClientError { message: e.message }),
        }
    }

    async fn private_compaction(
        &self,
        invocation: &PrivateCompactionInvocation,
        index: u64,
        num: u64,
    ) -> Result<Vec<RecordBatch>, PeerClientError> {
        let query_result = compaction_query_batches(invocation, index, num).await;
        match query_result {
            Ok(qr) => Ok(qr),
            Err(e) => Err(PeerClientError { message: e.message }),
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
            Err(e) => Err(PeerClientError { message: e.message }),
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
            Err(e) => Err(PeerClientError { message: e.message }),
        }
    }

    async fn private_compaction_leader(
        &self,
        invocation: &CompactionCommand,
    ) -> Result<Option<CompactionResponse>, PeerClientError> {
        match &self.compaction_mode {
            CompactionMode::Async(_num_files_threshold) => {
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
                    Err(e) => Err(PeerClientError {
                        message: e.to_string(),
                    }),
                }
            }
            CompactionMode::External(compaction_leader) => {
                let client = Client::new();

                let res = match client
                    .post(format!("{}/_private/v1/_compact_leader", compaction_leader))
                    .body(serde_json::to_string(&invocation).unwrap())
                    .send()
                    .await
                {
                    Ok(res) => res,
                    Err(e) => panic!("Error: {}", e),
                };

                assert!(res.status().is_success());

                let response_str = res.text().await.unwrap().clone();

                let response =
                    serde_json::from_str::<CompactionResponse>(response_str.as_str()).unwrap();
                Ok(Some(response))
            }
            CompactionMode::Disabled => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_contract::{FileSetPayload, IcebergMetadata, TableMetadataCheckpoint};
    use crate::schema_massager::{PowdrrDataType, PowdrrField, SqlBuilder, SqlExpression};
    use gotham::mime;
    use powdrr_query_server::router::router;
    use std::collections::HashMap;
    use std::env;

    async fn setup_private_sql_invocation(test_server: &AsyncTestServer) -> PrivateSqlInvocation {
        test_server
            .client()
            .put("http://localhost/_test/v1/_testing_mode")
            .body("")
            .mime(mime::TEXT_PLAIN)
            .perform()
            .await
            .unwrap();

        let schema = PowdrrSchema::from(&vec![
            PowdrrField {
                name: "_id_seq_no".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "snippet".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "searchTerms".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "title".to_string(),
                data_type: PowdrrDataType::String,
            },
        ]);
        let file_path = format!(
            "file://{}/testdata/flights.parquet",
            env::current_dir().unwrap().to_str().unwrap()
        );
        let checkpoint = TableMetadataCheckpoint {
            table_name: "flights".to_string(),
            original_checkpoint_id: None,
            checkpoint_id: "0".to_string(),
            iceberg_metadata: Some(IcebergMetadata {
                table_schema: schema.clone(),
                snapshot_id: Some("fake_iceberg_snapshot".to_string()),
                files: FileSetPayload::single(file_path, 1, schema.clone()),
                partition_spec: vec![],
                sort_order: vec![],
                column_names: vec![],
                column_stats: vec![],
                access_artifacts: vec![],
                file_stats: vec![],
            }),
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema,
        };

        test_server
            .client()
            .post("http://localhost/_test/v1/_add_checkpoint")
            .body(serde_json::to_string(&checkpoint).unwrap())
            .mime(mime::APPLICATION_JSON)
            .perform()
            .await
            .unwrap();

        let mut builder = SqlBuilder::for_agg();
        builder.set_all_fields_testing_only();
        builder.filter(SqlExpression::Like(
            Box::new(SqlExpression::FieldRef(
                "t".to_string(),
                "snippet".to_string(),
            )),
            Box::new(SqlExpression::LiteralString("%Looking%".to_string())),
        ));

        PrivateSqlInvocation {
            sql: builder.build(),
            required_extensions: vec![],
            file_filter: vec![],
            checkpoints: vec![CheckpointDescriptor::new(
                "flights".to_string(),
                "0".to_string(),
            )],
        }
    }

    #[tokio::test]
    async fn testing_remote_peer_private_sql_decodes_arrow_stream() {
        let test_server = AsyncTestServer::new(router(true)).await.unwrap();
        let invocation = setup_private_sql_invocation(&test_server).await;
        let peer = TestingRemotePeer::new(test_server.clone());
        let batches = peer.private_sql(&invocation, 0, 1).await.unwrap();

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            505
        );
    }

    #[tokio::test]
    async fn self_peer_private_sql_uses_direct_batch_path() {
        let test_server = AsyncTestServer::new(router(true)).await.unwrap();
        let invocation = setup_private_sql_invocation(&test_server).await;
        let expected = data_query_batches(&invocation, 0, 1).await.unwrap();
        let peer = SelfPeer::new(CompactionMode::Disabled);
        let batches = peer.private_sql(&invocation, 0, 1).await.unwrap();

        assert_eq!(batches.len(), expected.len());
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            expected.iter().map(|batch| batch.num_rows()).sum::<usize>()
        );
    }
}
