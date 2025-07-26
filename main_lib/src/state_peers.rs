use std::{error::Error, fmt::Display};
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use gotham::test::TestServer;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{private_api::data_query, state_common::FileFilter, state_leader};
use crate::compaction::{compact_logs, CompactionCommand, CompactionResponse};
use crate::elastic_search_common::result_to_record_batch;
use crate::private_api::{compaction_query, extension_query};
use crate::schema_massager::{PowdrrSchema, SqlQuery};
use crate::state_hosted_service::{ExtensionFileMetadata, FileSetPayload};

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

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SnapshotDescriptor {
    pub table_name: String,
    pub snapshot_id: String,
}


#[derive(Serialize, Deserialize)]
pub(crate) enum PrivateInvocation {
    Sql(PrivateSqlInvocation),
    Compaction(PrivateCompactionInvocation),
    Extension(PrivateExtensionInvocation),
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PrivateSqlInvocation {
    pub sql: SqlQuery,
    pub required_extensions: Vec<String>,
    pub file_filter: Vec<FileFilterDescriptor>,
    pub snapshots: Vec<SnapshotDescriptor>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PrivateSqlInvocationExternal {
    pub invocation: PrivateSqlInvocation,
    pub index: u64,
    pub num: u64,
}


#[derive(Serialize, Deserialize)]
pub(crate) struct PrivateCompactionInvocation {
    pub sql: SqlQuery,
    pub speedboat_files: FileSetPayload,
    pub table_schema: PowdrrSchema,
    pub delete_files: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PrivateExtensionInvocation {
    pub extension_name: String,
    pub speedboat_files: FileSetPayload,
    pub iceberg_files: FileSetPayload,
}


#[derive(Serialize)]
pub struct PrivateMetadataInvocation {
    name: String,
}


pub(crate) enum PrivateInvocationResult {
    Data(Vec<RecordBatch>),
    Extension(ExtensionFileMetadata),
}


#[derive(Debug)]
pub(crate) struct PeerClientError {
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

    async fn private_compaction_leader(&self, invocation: &CompactionCommand) -> Result<CompactionResponse, PeerClientError>;
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

    async fn private_compaction_leader(&self, _invocation: &CompactionCommand) -> Result<CompactionResponse, PeerClientError> {
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


pub struct SelfPeer {
}

impl SelfPeer {
    pub fn new() -> Self {
        SelfPeer {}
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

    async fn private_compaction_leader(&self, invocation: &CompactionCommand) -> Result<CompactionResponse, PeerClientError> {
        match compact_logs(Arc::new(invocation.clone())).await {
            Ok(success) => {
                Ok(serde_json::from_str(success.body.as_str()).unwrap())
            },
            Err(e) => Err(PeerClientError{ message: e.to_string() })
        }
    }
}


pub(crate) trait PeerClientGenerator {
    fn generate(&self) -> Vec<Box<dyn PeerClient>>;

    #[allow(dead_code)]
    fn test_server(&self) -> Option<&TestServer>;
}


struct RealPeerClientGenerator {}

unsafe impl Send for RealPeerClientGenerator {}
unsafe impl Sync for RealPeerClientGenerator {}

impl RealPeerClientGenerator {
    fn new() -> Self {
        RealPeerClientGenerator{}
    }
}

impl PeerClientGenerator for RealPeerClientGenerator {
    fn generate(&self) -> Vec<Box<dyn PeerClient>> {
        state_leader::get_leader().get_peers()
    }

    fn test_server(&self) -> Option<&TestServer> {
        None
    }
}


pub static mut PEER_CLIENT_GENERATOR: std::sync::LazyLock<Box<dyn PeerClientGenerator>> = 
    std::sync::LazyLock::new(|| Box::new(RealPeerClientGenerator::new()));


pub fn get_peer_clients() -> Vec<Box<dyn PeerClient>> {
    unsafe {
        (*PEER_CLIENT_GENERATOR).generate()
    }
}

#[allow(dead_code)]
pub fn get_test_server() -> Option<&'static TestServer> {
    unsafe {
        (*PEER_CLIENT_GENERATOR).test_server()
    }
}


#[cfg(test)]
pub mod tests {
    use std::str;

    use async_trait::async_trait;
    use datafusion::arrow::array::RecordBatch;
    use gotham::{mime, test::TestServer};
    use crate::compaction::{CompactionCommand, CompactionResponse};
    use crate::router::router;
    use crate::state_hosted_service::ExtensionFileMetadata;
    use super::{PeerClient, PeerClientError, PeerClientGenerator, PrivateCompactionInvocation, PrivateExtensionInvocation, PrivateSqlInvocation};

    pub(crate) struct TestPeerClient {
        server: TestServer,
    }

    impl TestPeerClient {
        #[allow(dead_code)]
        pub fn new(server: TestServer) -> Self {
            TestPeerClient {
                server: server
            }
        }
    }

    #[async_trait]
    impl PeerClient for TestPeerClient {
        async fn private_sql(&self, invocation: &PrivateSqlInvocation, _index: u64, _num: u64) -> Result<Vec<RecordBatch>, PeerClientError> {
            let body_obj = match serde_json::to_string(invocation) {
                Ok(bo) => bo,
                Err(_) => panic!("bad format")
            };
            let response = self.server.client().post(
                "http://localhost/_private/v1/_sql",
                body_obj,
                mime::APPLICATION_JSON,
            ).perform().unwrap();

            if response.status() == 200 {
                let body = response.read_body().unwrap();
                let str_body = str::from_utf8(&body).unwrap();
                panic!("Oops, need to do something: {}", str_body);
            } else {
                Err(PeerClientError { message: "Something go boom".to_string() })
            }
        }

        async fn private_compaction(&self, _invocation: &PrivateCompactionInvocation, _index: u64, _num: u64) -> Result<Vec<RecordBatch>, PeerClientError> {
            todo!()
        }

        async fn private_extension(&self, _invocation: &PrivateExtensionInvocation, _index: u64, _num: u64) -> Result<ExtensionFileMetadata, PeerClientError> {
            todo!()
        }

        async fn private_compaction_leader(&self, _invocation: &CompactionCommand) -> Result<CompactionResponse, PeerClientError> {
            todo!()
        }
    }

    pub struct TestPeerClientGenerator {
        server: TestServer,
    }

    unsafe impl Send for TestPeerClientGenerator {}
    unsafe impl Sync for TestPeerClientGenerator {}
    
    impl TestPeerClientGenerator {
        #[allow(dead_code)]
        pub fn new() -> Self {
            TestPeerClientGenerator{
                server: TestServer::new(router(true)).unwrap()
            }
        }
    }
    
    impl PeerClientGenerator for TestPeerClientGenerator {
        fn generate(&self) -> Vec<Box<dyn PeerClient>> {
            vec!(Box::new(TestPeerClient{
                server: self.server.clone()
            }))
        }

        fn test_server(&self) -> Option<&TestServer> {
            Some(&self.server)
        }
    }    
}
