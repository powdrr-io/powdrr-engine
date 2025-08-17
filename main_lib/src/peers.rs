use std::{error::Error, fmt::Display};
use std::net::IpAddr;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{private_api::data_query, state_common::FileFilter};
use crate::compaction::{compact_logs, CompactionCommand, CompactionResponse};
use crate::elastic_search_common::result_to_record_batch;
use crate::private_api::{compaction_query, extension_query, prefetch_query};
use crate::schema_massager::{PowdrrSchema, SqlQuery};
use crate::data_contract::{ExtensionFileMetadata, FileSetPayload};
use crate::test_api::CompactionMode;

#[derive(Serialize, Deserialize)]
pub struct FieldFileFilterDescriptor {
    pub field_name: String,
    pub file_filter: FileFilter,
}

#[derive(Serialize, Deserialize)]
pub struct FileFilterDescriptor {
    pub able_name: String, // Field name to list of (operator, value)
    pub filters: Vec<FieldFileFilterDescriptor>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CheckpointDescriptor {
    pub table_name: String,
    pub checkpoint_id: String,
    pub original_checkpoint_id: Option<String>,
}


impl CheckpointDescriptor {
    pub fn new(table_name: String, checkpoint_id: String) -> Self {
        CheckpointDescriptor {
            table_name,
            checkpoint_id,
            original_checkpoint_id: None,
        }
    }

    pub fn from_full_name(full_name: &str) -> Self {
        let parts: Vec<&str> = full_name.split(':').collect();
        if parts.len() == 2 {
            CheckpointDescriptor {
                table_name: parts[0].to_string(),
                checkpoint_id: parts[1].to_string(),
                original_checkpoint_id: None,
            }
        } else if parts.len() == 3 {
            CheckpointDescriptor {
                table_name: parts[0].to_string(),
                checkpoint_id: parts[2].to_string(),
                original_checkpoint_id: Some(parts[1].to_string()),
            }
        } else {
            panic!("Invalid checkpoint descriptor: {}", full_name);
        }
    }

    pub(crate) fn full_checkpoint_id(&self) -> String {
        match &self.original_checkpoint_id {
            Some(original_checkpoint_id) => format!("{}:{}", original_checkpoint_id, self.checkpoint_id),
            None => self.checkpoint_id.clone(),
        }
    }

    pub fn full_name(&self) -> String {
        match &self.original_checkpoint_id {
            Some(original_checkpoint_id) => format!("{}:{}:{}", self.table_name, original_checkpoint_id, self.checkpoint_id),
            None => format!("{}:{}", self.table_name, self.checkpoint_id),
        }
    }
}

impl Display for CheckpointDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.table_name, self.checkpoint_id)
    }
}


#[derive(Serialize, Deserialize)]
pub enum PrivateInvocation {
    Sql(PrivateSqlInvocation),
    Compaction(PrivateCompactionInvocation),
    Extension(PrivateExtensionInvocation),
    Prefetch(PrivatePrefetchInvocation),
}

#[derive(Serialize, Deserialize)]
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


#[derive(Serialize, Deserialize)]
pub struct PrivateCompactionInvocation {
    pub sql: SqlQuery,
    pub speedboat_files: FileSetPayload,
    pub table_schema: PowdrrSchema,
    pub delete_files: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct PrivateExtensionInvocation {
    pub extension_name: String,
    pub speedboat_files: FileSetPayload,
    pub iceberg_files: FileSetPayload,
}

#[derive(Serialize, Deserialize)]
pub struct PrivatePrefetchInvocation {
    pub required_extensions: Vec<String>,
    pub checkpoints: Vec<CheckpointDescriptor>,
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

impl Error for PeerClientError{}

unsafe impl Send for PeerClientError {}
unsafe impl Sync for PeerClientError {}

#[async_trait]
pub trait PeerClient: Send + Sync {
    async fn private_sql(&self, invocation: &PrivateSqlInvocation, index: u64, num: u64) -> Result<Vec<RecordBatch>, PeerClientError>;

    async fn private_compaction(&self, invocation: &PrivateCompactionInvocation, index: u64, num: u64) -> Result<Vec<RecordBatch>, PeerClientError>;

    async fn private_extension(&self, invocation: &PrivateExtensionInvocation, index: u64, num: u64) -> Result<ExtensionFileMetadata, PeerClientError>;

    async fn private_prefetch(&self, invocation: &PrivatePrefetchInvocation, index: u64, num: u64) -> Result<(), PeerClientError>;

    async fn private_compaction_leader(&self, invocation: &CompactionCommand) -> Result<Option<CompactionResponse>, PeerClientError>;
}

#[allow(dead_code)]
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
    async fn private_sql(&self, invocation: &PrivateSqlInvocation, _index: u64, _num: u64) -> Result<Vec<RecordBatch>, PeerClientError> {
        let address = &self.address;
        let body = serde_json::to_string(invocation);
        match body {
            Ok(b) => {
                let resp = self.client.post(format!("http://{address}/_private/v1/_sql"))
                    .header("Content-Type", "application/json")
                    .body(b)
                    .send().await;
                match resp {
                    Ok(r) => {
                        let text = r.text().await;
                        match text {
                            Ok(_) => Ok(vec!()),
                            Err(_) => Err(PeerClientError{ message: "Error".to_string() }),
                        }
                    },
                    Err(_) => Err(PeerClientError{ message: "Error".to_string() })
                }
            },
            Err(_) => {
                panic!("Malformed request")
            }
        }
    }

    async fn private_compaction(&self, _invocation: &PrivateCompactionInvocation, _index: u64, _num: u64) -> Result<Vec<RecordBatch>, PeerClientError> {
        todo!()
    }

    async fn private_extension(&self, _invocation: &PrivateExtensionInvocation, _index: u64, _num: u64) -> Result<ExtensionFileMetadata, PeerClientError> {
        todo!()
    }

    async fn private_prefetch(&self, _invocation: &PrivatePrefetchInvocation, _index: u64, _num: u64) -> Result<(), PeerClientError> {
        todo!()
    }

    async fn private_compaction_leader(&self, _invocation: &CompactionCommand) -> Result<Option<CompactionResponse>, PeerClientError> {
        todo!()
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

pub async fn get_peer_ips() -> Vec<IpAddr> {
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
    let app_label = own_pod.metadata.labels.unwrap().get("app").unwrap().to_string();

    // get all pods with app label
    let pods = pods.list(&Default::default()).await.unwrap();
    let pods = pods.items.iter().filter(|pod| {
        pod.metadata.labels.as_ref().unwrap().get("app").unwrap() == &app_label
    });

    let mut ips: Vec<IpAddr> = vec![];
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

        match pod_ip.parse() {
            Ok(ip) => ips.push(ip),
            Err(e) => {
                tracing::error!("Failed to parse Pod IP for pod {:?}: {}", pod.metadata.name, e);
                continue;
            }
        };
    }

    //filter duplicate IPs
    ips.sort_unstable();
    ips.dedup();

    return ips;
}


pub struct SelfPeer {
    pub compaction_mode: CompactionMode
}

impl SelfPeer {
    pub fn new(compaction_mode: CompactionMode) -> Self {
        SelfPeer { compaction_mode: compaction_mode.clone() }
    }
}

unsafe impl Send for SelfPeer {}
unsafe impl Sync for SelfPeer {}


#[async_trait]
impl PeerClient for SelfPeer {
    async fn private_sql(&self, invocation: &PrivateSqlInvocation, index: u64, num: u64) -> Result<Vec<RecordBatch>, PeerClientError> {
        let query_result = data_query(invocation, index, num).await;
        match query_result {
            Ok(qr) => {
                Ok(result_to_record_batch(qr.result).await)
            },
            Err(e) => {
                Err(PeerClientError { message: e.message })
            }
        }
    }

    async fn private_compaction(&self, invocation: &PrivateCompactionInvocation, index: u64, num: u64) -> Result<Vec<RecordBatch>, PeerClientError> {
        let query_result = compaction_query(invocation, index, num).await;
        match query_result {
            Ok(qr) => {
                Ok(result_to_record_batch(qr.result).await)
            },
            Err(e) => {
                Err(PeerClientError { message: e.message })
            }
        }
    }

    async fn private_extension(&self, invocation: &PrivateExtensionInvocation, index: u64, num: u64) -> Result<ExtensionFileMetadata, PeerClientError> {
        match extension_query(invocation, index, num).await {
            Ok(result) => {
                Ok(result)
            },
            Err(e) => {
                Err(PeerClientError { message: e.message })
            }
        }
    }

    async fn private_prefetch(&self, invocation: &PrivatePrefetchInvocation, index: u64, num: u64) -> Result<(), PeerClientError> {
        match prefetch_query(invocation, index, num).await {
            Ok(_) => {
                Ok(())
            },
            Err(e) => {
                Err(PeerClientError { message: e.message })
            }
        }
    }

    async fn private_compaction_leader(&self, invocation: &CompactionCommand) -> Result<Option<CompactionResponse>, PeerClientError> {
        match &self.compaction_mode {
            CompactionMode::Async(_num_files_threshold) => {
                match compact_logs(Arc::new(invocation.clone())).await {
                    Ok(success) => {
                        if success.status == 200 {
                            Ok(Some(serde_json::from_str(success.body.as_str()).unwrap()))
                        } else {
                            Err(PeerClientError { message: success.body })
                        }
                    },
                    Err(e) => Err(PeerClientError { message: e.to_string() })
                }
            },
            CompactionMode::External(compaction_leader) => {
                let client = Client::new();

                let res = match client.post(format!("{}/_private/v1/_compact", compaction_leader))
                    .body(serde_json::to_string(&invocation).unwrap())
                    .send().await {
                    Ok(res) => res,
                    Err(e) => panic!("Error: {}", e),
                };

                assert!(res.status().is_success());

                let response_str = res.text().await.unwrap().clone();

                let response = serde_json::from_str::<CompactionResponse>(response_str.as_str()).unwrap();
                Ok(Some(response))
            },
            CompactionMode::Disabled => {
                Ok(None)
            }
        }
    }
}
