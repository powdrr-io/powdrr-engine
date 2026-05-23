use crate::data_contract::{
    CleanupCommit, CleanupWorkItem, CompactionCommit, CompactionWorkItem, CreateIndexTemplateBody,
    CreateTable, ExtensionCommit, ExtensionWorkItem, IcebergCommit, SpeedboatCommit,
    TableDescription, TableMetadataCheckpoint,
};
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::ephemeral_service_impl::{EphemeralServiceImpl, EphemeralServiceSnapshot};
use crate::metadata_store::{
    CheckpointCutoverRequest, CheckpointCutoverState, CheckpointUpdateRequest,
    ClaimedCleanupWorkItem, ClaimedCompactionWorkItem, ClaimedExtensionWorkItem, MetadataClaimKind,
    MetadataStore, PublishedCheckpointRecord, PublishedCheckpointSelector,
    ServingNodeActivationAck, ServingNodeLease,
};
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::read_only_coordination::ArtifactReadinessAck;
use crate::state_provider::ServiceApiError;
use crate::test_api::TestProcessingMode;
use async_trait::async_trait;
use openraft::Config;
use openraft::Raft;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::Adaptor;
use openraft_memstore::ClientRequest;
use openraft_memstore::MemNodeId;
use openraft_memstore::MemStore;
use openraft_memstore::TypeConfig;
use reqwest::Client;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

const SERVICE_STATE_KEY: &str = "__powdrr_service_state__";

#[derive(Debug, Clone)]
pub struct RaftServiceConfig {
    pub cluster_name: String,
    pub node_id: u64,
    pub advertise_address: String,
    pub bootstrap: bool,
    pub peers: BTreeMap<u64, String>,
}

#[derive(Clone)]
struct Network {
    peers: Arc<BTreeMap<u64, String>>,
    client: Client,
}

impl Network {
    fn new(peers: Arc<BTreeMap<u64, String>>) -> Self {
        Self {
            peers,
            client: Client::new(),
        }
    }

    async fn send_rpc<Req, Resp, Err>(
        &self,
        target: MemNodeId,
        uri: &str,
        req: Req,
    ) -> Result<Resp, RPCError<MemNodeId, (), Err>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        Err: std::error::Error + DeserializeOwned,
    {
        let addr = match self.peers.get(&target) {
            Some(addr) => addr,
            None => {
                let error = std::io::Error::new(
                    ErrorKind::NotFound,
                    format!("Unknown raft peer {}", target),
                );
                return Err(RPCError::Network(NetworkError::new(&error)));
            }
        };

        let response = self
            .client
            .post(format!("http://{addr}{uri}"))
            .json(&req)
            .send()
            .await
            .map_err(|error| {
                if error.is_connect() {
                    RPCError::Unreachable(Unreachable::new(&error))
                } else {
                    RPCError::Network(NetworkError::new(&error))
                }
            })?;

        let payload = response
            .json::<Result<Resp, Err>>()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;

        payload.map_err(|error| RPCError::RemoteError(RemoteError::new(target, error)))
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: MemNodeId, _node: &()) -> Self::Network {
        NetworkConnection {
            owner: self.clone(),
            target,
        }
    }
}

pub struct NetworkConnection {
    owner: Network,
    target: MemNodeId,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<MemNodeId>, RPCError<MemNodeId, (), RaftError<MemNodeId>>>
    {
        self.owner
            .send_rpc(self.target, "/_raft/v1/append", req)
            .await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<MemNodeId>,
        RPCError<MemNodeId, (), RaftError<MemNodeId, InstallSnapshotError>>,
    > {
        self.owner
            .send_rpc(self.target, "/_raft/v1/snapshot", req)
            .await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<MemNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<MemNodeId>, RPCError<MemNodeId, (), RaftError<MemNodeId>>> {
        self.owner
            .send_rpc(self.target, "/_raft/v1/vote", req)
            .await
    }
}

pub struct RaftServiceImpl {
    mode: TestProcessingMode,
    config: RaftServiceConfig,
    raft: Raft<TypeConfig>,
    store: Arc<MemStore>,
    write_serial: AtomicU64,
    write_lock: Mutex<()>,
    peer_addresses: Arc<BTreeMap<u64, String>>,
}

macro_rules! raft_read_state {
    ($self:expr, |$state:ident| $body:expr) => {{
        $self
            .raft
            .ensure_linearizable()
            .await
            .map_err(Self::raft_api_error)?;
        let snapshot = $self.read_snapshot_state().await?;
        let mut $state = EphemeralServiceImpl::from_snapshot($self.mode.clone(), snapshot);
        $body.await
    }};
}

macro_rules! raft_write_state {
    ($self:expr, |$state:ident| $body:expr) => {{
        let _guard = $self.write_lock.lock().await;
        let snapshot = $self.read_snapshot_state().await?;
        let mut $state = EphemeralServiceImpl::from_snapshot($self.mode.clone(), snapshot);
        let result = $body.await?;
        $self.write_snapshot_state(&$state.snapshot_state()).await?;
        Ok(result)
    }};
}

impl RaftServiceImpl {
    async fn new_with_network_factory<N>(
        mut config: RaftServiceConfig,
        mode: TestProcessingMode,
        network_factory: N,
    ) -> Result<Self, ServiceApiError>
    where
        N: RaftNetworkFactory<TypeConfig> + Send + Sync + 'static,
    {
        config
            .peers
            .entry(config.node_id)
            .or_insert_with(|| config.advertise_address.clone());
        let peer_addresses = Arc::new(config.peers.clone());

        let raft_config = Arc::new(
            Config {
                cluster_name: config.cluster_name.clone(),
                ..Default::default()
            }
            .validate()
            .map_err(|error| ServiceApiError::new(format!("Invalid Raft config: {error}")))?,
        );

        let store = MemStore::new_async().await;
        let (log_store, state_machine) = Adaptor::new(store.clone());
        let raft = Raft::new(
            config.node_id,
            raft_config,
            network_factory,
            log_store,
            state_machine,
        )
        .await
        .map_err(Self::raft_fatal_error)?;

        Ok(Self {
            mode,
            config,
            raft,
            store,
            write_serial: AtomicU64::new(0),
            write_lock: Mutex::new(()),
            peer_addresses,
        })
    }

    pub async fn new(
        mut config: RaftServiceConfig,
        mode: TestProcessingMode,
    ) -> Result<Self, ServiceApiError> {
        config
            .peers
            .entry(config.node_id)
            .or_insert_with(|| config.advertise_address.clone());
        let peer_addresses = Arc::new(config.peers.clone());
        Self::new_with_network_factory(config, mode, Network::new(peer_addresses)).await
    }

    #[cfg(test)]
    async fn new_with_test_network(
        config: RaftServiceConfig,
        mode: TestProcessingMode,
        registry: TestNodeRegistry,
    ) -> Result<Self, ServiceApiError> {
        Self::new_with_network_factory(config, mode, TestNetworkFactory::new(registry)).await
    }

    fn raft_fatal_error(error: openraft::error::Fatal<MemNodeId>) -> ServiceApiError {
        ServiceApiError::new(format!("Raft fatal error: {error}"))
    }

    fn raft_api_error<E: std::fmt::Display>(error: E) -> ServiceApiError {
        ServiceApiError::new(format!("Raft API error: {error}"))
    }

    async fn read_snapshot_state(&self) -> Result<EphemeralServiceSnapshot, ServiceApiError> {
        let state_machine = self.store.get_state_machine().await;
        match state_machine.client_status.get(SERVICE_STATE_KEY) {
            Some(serialized) => serde_json::from_str(serialized).map_err(|error| {
                ServiceApiError::new(format!(
                    "Failed to deserialize replicated service state: {error}"
                ))
            }),
            None => Ok(EphemeralServiceSnapshot::default()),
        }
    }

    async fn write_snapshot_state(
        &self,
        snapshot: &EphemeralServiceSnapshot,
    ) -> Result<(), ServiceApiError> {
        let serialized = serde_json::to_string(snapshot).map_err(|error| {
            ServiceApiError::new(format!(
                "Failed to serialize replicated service state: {error}"
            ))
        })?;
        let serial = self.write_serial.fetch_add(1, Ordering::SeqCst) + 1;
        self.raft
            .client_write(ClientRequest {
                client: SERVICE_STATE_KEY.to_string(),
                serial,
                status: serialized,
            })
            .await
            .map_err(Self::raft_api_error)?;
        Ok(())
    }

    pub async fn bootstrap_cluster_if_needed(&self) -> Result<(), ServiceApiError> {
        if !self.config.bootstrap {
            return Ok(());
        }

        let members = self
            .peer_addresses
            .keys()
            .copied()
            .map(|node_id| (node_id, ()))
            .collect::<BTreeMap<MemNodeId, ()>>();
        match self.raft.initialize(members).await {
            Ok(()) => Ok(()),
            Err(error) => match error.clone().into_api_error() {
                Some(openraft::error::InitializeError::NotAllowed(_)) => Ok(()),
                Some(other) => Err(Self::raft_api_error(other)),
                None => Err(Self::raft_fatal_error(
                    error
                        .into_fatal()
                        .expect("initialize without api error must be fatal"),
                )),
            },
        }
    }

    pub async fn current_leader_id(&self) -> Option<MemNodeId> {
        self.raft.current_leader().await
    }

    pub async fn current_leader_address(&self) -> Option<String> {
        self.current_leader_id()
            .await
            .and_then(|node_id| self.peer_addresses.get(&node_id).cloned())
    }

    pub async fn forward_base_url(&self) -> Option<String> {
        match self.current_leader_id().await {
            Some(leader_id) if leader_id != self.config.node_id => self
                .peer_addresses
                .get(&leader_id)
                .map(|address| format!("http://{address}")),
            _ => None,
        }
    }

    pub async fn is_local_leader(&self) -> bool {
        self.current_leader_id().await == Some(self.config.node_id)
    }

    pub async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<MemNodeId>, RaftError<MemNodeId>> {
        self.raft.append_entries(rpc).await
    }

    pub async fn vote(
        &self,
        rpc: VoteRequest<MemNodeId>,
    ) -> Result<VoteResponse<MemNodeId>, RaftError<MemNodeId>> {
        self.raft.vote(rpc).await
    }

    pub async fn install_snapshot(
        &self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<MemNodeId>, RaftError<MemNodeId, InstallSnapshotError>>
    {
        self.raft.install_snapshot(rpc).await
    }

    pub async fn add_checkpoint(
        &self,
        checkpoint: &TableMetadataCheckpoint,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| state.add_checkpoint(checkpoint))
    }

    pub async fn get_all_iceberg_tables(&self) -> Result<Vec<String>, ServiceApiError> {
        raft_read_state!(self, |state| state.get_all_iceberg_tables())
    }

    pub async fn create_table(&self, create_table: &CreateTable) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.create_table(create_table))
    }

    pub async fn describe_table(
        &self,
        name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError> {
        raft_read_state!(self, |state| state.describe_table(name))
    }

    pub async fn add_alias(
        &self,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.add_alias(table_name, alias))
    }

    pub async fn remove_alias(
        &self,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.remove_alias(table_name, alias))
    }

    pub async fn create_table_template(
        &self,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.create_table_template(name, template))
    }

    pub async fn describe_table_template(
        &self,
        name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        raft_read_state!(self, |state| state.describe_table_template(name))
    }

    pub async fn create_pipeline(
        &self,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.create_pipeline(name, pipeline))
    }

    pub async fn describe_pipeline(
        &self,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        raft_read_state!(self, |state| state.describe_pipeline(name))
    }

    pub async fn create_lifetime_policy(
        &self,
        name: &String,
        policy: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.create_lifetime_policy(name, policy))
    }

    pub async fn describe_lifetime_policy(
        &self,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        raft_read_state!(self, |state| state.describe_lifetime_policy(name))
    }

    pub async fn speedboat_commit(
        &self,
        commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.speedboat_commit(commit))
    }

    pub async fn iceberg_commit(
        &self,
        table_name: &String,
        commit: &IcebergCommit,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.iceberg_commit(table_name, commit))
    }

    pub async fn extension_commit(
        &self,
        table_name: &String,
        commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.extension_commit(table_name, commit))
    }

    pub async fn compaction_commit(
        &self,
        table_name: &String,
        commit: &CompactionCommit,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.compaction_commit(table_name, commit))
    }

    pub async fn cleanup_commit(&self, commit: &CleanupCommit) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.cleanup_commit(commit))
    }

    pub async fn get_latest_committed_checkpoint(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        raft_read_state!(
            self,
            |state| MetadataStore::get_latest_committed_checkpoint(
                &mut state, table_name, extension
            )
        )
    }

    pub async fn get_published_active_checkpoint(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        raft_read_state!(
            self,
            |state| MetadataStore::get_published_active_checkpoint(
                &mut state, table_name, extension
            )
        )
    }

    pub async fn get_checkpoint(
        &self,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        raft_read_state!(self, |state| state.get_checkpoint(checkpoint))
    }

    pub async fn get_latest_target_checkpoint(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        raft_read_state!(self, |state| MetadataStore::get_latest_target_checkpoint(
            &mut state, table_name, extension
        ))
    }

    pub async fn get_checkpoint_cutover_state(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceApiError> {
        raft_read_state!(self, |state| MetadataStore::get_checkpoint_cutover_state(
            &mut state, table_name, extension
        ))
    }

    pub async fn heartbeat_serving_node(
        &self,
        lease: &ServingNodeLease,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| MetadataStore::heartbeat_serving_node(
            &mut state, lease
        ))
    }

    pub async fn record_serving_node_activation(
        &self,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| MetadataStore::record_serving_node_activation(
            &mut state, ack
        ))
    }

    pub async fn list_serving_node_activations(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ServingNodeActivationAck>, ServiceApiError> {
        raft_read_state!(self, |state| MetadataStore::list_serving_node_activations(
            &mut state, table_name, extension
        ))
    }

    pub async fn record_artifact_readiness(
        &self,
        ack: &ArtifactReadinessAck,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| MetadataStore::record_artifact_readiness(
            &mut state, ack
        ))
    }

    pub async fn list_artifact_readiness(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ArtifactReadinessAck>, ServiceApiError> {
        raft_read_state!(self, |state| MetadataStore::list_artifact_readiness(
            &mut state, table_name, extension
        ))
    }

    pub async fn get_extension_work_items(
        &self,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        raft_write_state!(self, |state| state.get_extension_work_items(extension_type))
    }

    pub async fn get_compaction_work_items(
        &self,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        raft_write_state!(self, |state| state.get_compaction_work_items())
    }

    pub async fn get_cleanup_work_items(&self) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        raft_write_state!(self, |state| state.get_cleanup_work_items())
    }

    pub async fn update_all_checkpoints(&self) -> Result<bool, ServiceApiError> {
        if !self.is_local_leader().await {
            return Ok(false);
        }
        raft_write_state!(self, |state| MetadataStore::update_all_checkpoints(
            &mut state
        ))
    }
}

#[async_trait]
impl MetadataStore for RaftServiceImpl {
    async fn queue_checkpoint_publication(
        &mut self,
        request: &CheckpointUpdateRequest,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| {
            MetadataStore::queue_checkpoint_publication(&mut state, request)
        })
    }

    async fn get_latest_committed_checkpoint(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        raft_read_state!(
            self,
            |state| MetadataStore::get_latest_committed_checkpoint(
                &mut state, table_name, extension
            )
        )
    }

    async fn get_published_checkpoint_record(
        &mut self,
        selector: &PublishedCheckpointSelector,
    ) -> Result<Option<PublishedCheckpointRecord>, ServiceApiError> {
        raft_read_state!(self, |state| {
            MetadataStore::get_published_checkpoint_record(&mut state, selector)
        })
    }

    async fn get_checkpoint_metadata(
        &mut self,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        self.get_checkpoint(checkpoint).await
    }

    async fn plan_checkpoint_cutover(
        &mut self,
        request: &CheckpointCutoverRequest,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| {
            MetadataStore::plan_checkpoint_cutover(&mut state, request)
        })
    }

    async fn get_checkpoint_cutover_state(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceApiError> {
        raft_read_state!(self, |state| {
            MetadataStore::get_checkpoint_cutover_state(&mut state, table_name, extension)
        })
    }

    async fn heartbeat_serving_node(
        &mut self,
        lease: &ServingNodeLease,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| {
            MetadataStore::heartbeat_serving_node(&mut state, lease)
        })
    }

    async fn record_serving_node_activation(
        &mut self,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| {
            MetadataStore::record_serving_node_activation(&mut state, ack)
        })
    }

    async fn list_serving_node_activations(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ServingNodeActivationAck>, ServiceApiError> {
        raft_read_state!(self, |state| {
            MetadataStore::list_serving_node_activations(&mut state, table_name, extension)
        })
    }

    async fn record_artifact_readiness(
        &mut self,
        ack: &ArtifactReadinessAck,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| {
            MetadataStore::record_artifact_readiness(&mut state, ack)
        })
    }

    async fn list_artifact_readiness(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ArtifactReadinessAck>, ServiceApiError> {
        raft_read_state!(self, |state| {
            MetadataStore::list_artifact_readiness(&mut state, table_name, extension)
        })
    }
    async fn claim_extension_work_items(
        &mut self,
        extension_type: &String,
    ) -> Result<Vec<ClaimedExtensionWorkItem>, ServiceApiError> {
        Ok(self
            .get_extension_work_items(extension_type)
            .await?
            .into_iter()
            .map(|work_item| ClaimedExtensionWorkItem {
                claim: MetadataClaimKind::Leased,
                work_item,
            })
            .collect())
    }

    async fn claim_compaction_work_items(
        &mut self,
    ) -> Result<Vec<ClaimedCompactionWorkItem>, ServiceApiError> {
        Ok(self
            .get_compaction_work_items()
            .await?
            .into_iter()
            .map(|(table_name, work_item)| ClaimedCompactionWorkItem {
                claim: MetadataClaimKind::Leased,
                table_name,
                work_item,
            })
            .collect())
    }

    async fn claim_cleanup_work_items(
        &mut self,
    ) -> Result<Vec<ClaimedCleanupWorkItem>, ServiceApiError> {
        Ok(self
            .get_cleanup_work_items()
            .await?
            .into_iter()
            .map(|work_item| ClaimedCleanupWorkItem {
                claim: MetadataClaimKind::Leased,
                work_item,
            })
            .collect())
    }

    async fn advance_published_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        self.update_all_checkpoints().await
    }
}

#[cfg(test)]
type TestNodeRegistry = Arc<Mutex<BTreeMap<MemNodeId, Arc<RaftServiceImpl>>>>;

#[cfg(test)]
#[derive(Clone)]
struct TestNetworkFactory {
    registry: TestNodeRegistry,
}

#[cfg(test)]
impl TestNetworkFactory {
    fn new(registry: TestNodeRegistry) -> Self {
        Self { registry }
    }
}

#[cfg(test)]
impl RaftNetworkFactory<TypeConfig> for TestNetworkFactory {
    type Network = TestNetworkConnection;

    async fn new_client(&mut self, target: MemNodeId, _node: &()) -> Self::Network {
        TestNetworkConnection {
            registry: self.registry.clone(),
            target,
        }
    }
}

#[cfg(test)]
struct TestNetworkConnection {
    registry: TestNodeRegistry,
    target: MemNodeId,
}

#[cfg(test)]
impl TestNetworkConnection {
    async fn target_raft(&self) -> Option<Arc<RaftServiceImpl>> {
        self.registry.lock().await.get(&self.target).cloned()
    }
}

#[cfg(test)]
impl RaftNetwork<TypeConfig> for TestNetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<MemNodeId>, RPCError<MemNodeId, (), RaftError<MemNodeId>>>
    {
        let Some(target_raft) = self.target_raft().await else {
            let error = std::io::Error::new(
                ErrorKind::NotFound,
                format!("Unknown test raft peer {}", self.target),
            );
            return Err(RPCError::Network(NetworkError::new(&error)));
        };
        target_raft
            .append_entries(req)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<MemNodeId>,
        RPCError<MemNodeId, (), RaftError<MemNodeId, InstallSnapshotError>>,
    > {
        let Some(target_raft) = self.target_raft().await else {
            let error = std::io::Error::new(
                ErrorKind::NotFound,
                format!("Unknown test raft peer {}", self.target),
            );
            return Err(RPCError::Network(NetworkError::new(&error)));
        };
        target_raft
            .install_snapshot(req)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<MemNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<MemNodeId>, RPCError<MemNodeId, (), RaftError<MemNodeId>>> {
        let Some(target_raft) = self.target_raft().await else {
            let error = std::io::Error::new(
                ErrorKind::NotFound,
                format!("Unknown test raft peer {}", self.target),
            );
            return Err(RPCError::Network(NetworkError::new(&error)));
        };
        target_raft
            .vote(req)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }
}

#[cfg(test)]
mod tests {
    use super::{RaftServiceConfig, RaftServiceImpl, TestNodeRegistry};
    use crate::data_contract::{
        CompactionCommit, ExtensionCommit, ExtensionFile, FileSetPayload, IcebergCommit,
        IcebergMetadata, SpeedboatCommit, SpeedboatCommitTableInfo,
    };
    use crate::ephemeral_service_impl::EphemeralServiceImpl;
    use crate::metadata_store::{
        CutoverEpoch, MetadataStore, ServingNodeActivationAck, ServingNodeLease,
    };
    use crate::read_only_coordination::{ArtifactClass, ArtifactReadinessAck};
    use crate::schema_massager::PowdrrSchema;
    use crate::test_api::{
        ApiMode, CacheMode, CompactionMode, IndexingMode, PeerMode, PrefetchMode, StateMode,
        StorageMode, TestProcessingMode,
    };
    use idgenerator::{IdGeneratorOptions, IdInstance};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Once};
    use std::time::Duration;
    use tokio::sync::Mutex;

    static TEST_IDS_INITIALIZED: Once = Once::new();
    const WORK_CLAIM_RETRY_WAIT_MS: u64 = 100;

    fn initialize_ids() {
        TEST_IDS_INITIALIZED.call_once(|| {
            let options = IdGeneratorOptions::new().worker_id(1).worker_id_bit_len(6);
            let _ = IdInstance::init(options);
        });
    }

    fn raft_test_mode(compaction_mode: CompactionMode) -> TestProcessingMode {
        TestProcessingMode {
            state_mode: StateMode::Ephemeral,
            storage_mode: StorageMode::default(),
            cache_mode: CacheMode::Redis(None),
            api_mode: ApiMode::ReadWrite,
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Disabled,
            compaction_mode,
            prefetch_mode: PrefetchMode::Disabled,
        }
    }

    #[tokio::test]
    async fn raft_cluster_failover_preserves_committed_state() {
        initialize_ids();

        let (nodes, registry) = multi_node_raft(
            "raft-multi-node-failover",
            raft_test_mode(CompactionMode::Disabled),
            3,
        )
        .await;
        let table_name = "cluster_failover_logs".to_string();
        let first_file_path = "s3://bucket/logs/cluster-first.parquet";

        let leader = wait_for_leader(&nodes).await;
        leader
            .speedboat_commit(&speedboat_commit_for(&table_name, first_file_path))
            .await
            .unwrap();
        assert!(leader.update_all_checkpoints().await.unwrap());
        let first_target_checkpoint = leader
            .get_latest_target_checkpoint(&table_name, None)
            .await
            .unwrap()
            .unwrap();
        let first_cutover_state = leader
            .get_checkpoint_cutover_state(&table_name, None)
            .await
            .unwrap();
        leader
            .heartbeat_serving_node(&serving_lease("warm-cache"))
            .await
            .unwrap();
        record_readiness(
            &leader,
            &table_name,
            None,
            first_cutover_state.epoch,
            &first_target_checkpoint,
        )
        .await;
        leader
            .record_serving_node_activation(&ServingNodeActivationAck {
                selector: first_cutover_state.selector,
                node_id: "warm-cache".to_string(),
                epoch: first_cutover_state.epoch,
                checkpoint_id: first_target_checkpoint.clone(),
                activated_at_ms: 1,
            })
            .await
            .unwrap();
        assert!(leader.update_all_checkpoints().await.unwrap());

        let leader_id = leader.config.node_id;
        let follower = nodes
            .iter()
            .find(|node| node.config.node_id != leader_id)
            .unwrap()
            .clone();
        let mut follower_observed_committed_checkpoint = false;
        for _ in 0..100 {
            if locally_replicated_active_checkpoint(&follower, &table_name).await
                == Some(first_target_checkpoint.clone())
            {
                follower_observed_committed_checkpoint = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(follower_observed_committed_checkpoint);

        leader.raft.shutdown().await.unwrap();
        registry.lock().await.remove(&leader_id);

        let survivors: Vec<Arc<RaftServiceImpl>> = nodes
            .iter()
            .filter(|node| node.config.node_id != leader_id)
            .cloned()
            .collect();
        let new_leader = wait_for_leader(&survivors).await;
        assert_ne!(new_leader.config.node_id, leader_id);
        assert_eq!(
            new_leader
                .get_latest_committed_checkpoint(&table_name, None)
                .await
                .unwrap(),
            Some(first_target_checkpoint)
        );

        new_leader
            .speedboat_commit(&speedboat_commit_for(
                &table_name,
                "s3://bucket/logs/cluster-second.parquet",
            ))
            .await
            .unwrap();
        assert!(new_leader.update_all_checkpoints().await.unwrap());
        let second_target_checkpoint = new_leader
            .get_latest_target_checkpoint(&table_name, None)
            .await
            .unwrap()
            .unwrap();
        let second_cutover_state = new_leader
            .get_checkpoint_cutover_state(&table_name, None)
            .await
            .unwrap();
        new_leader
            .heartbeat_serving_node(&serving_lease("warm-cache"))
            .await
            .unwrap();
        record_readiness(
            &new_leader,
            &table_name,
            None,
            second_cutover_state.epoch,
            &second_target_checkpoint,
        )
        .await;
        new_leader
            .record_serving_node_activation(&ServingNodeActivationAck {
                selector: second_cutover_state.selector,
                node_id: "warm-cache".to_string(),
                epoch: second_cutover_state.epoch,
                checkpoint_id: second_target_checkpoint.clone(),
                activated_at_ms: 2,
            })
            .await
            .unwrap();
        assert!(new_leader.update_all_checkpoints().await.unwrap());

        let surviving_follower = survivors
            .into_iter()
            .find(|node| node.config.node_id != new_leader.config.node_id)
            .unwrap();
        let mut surviving_follower_observed_committed_checkpoint = false;
        for _ in 0..100 {
            if locally_replicated_active_checkpoint(&surviving_follower, &table_name).await
                == Some(second_target_checkpoint.clone())
            {
                surviving_follower_observed_committed_checkpoint = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(surviving_follower_observed_committed_checkpoint);
    }
}
