use crate::data_contract::{
    CleanupCommit, CleanupWorkItem, CompactionCommit, CompactionWorkItem, CreateIndexTemplateBody,
    CreateTable, ExtensionCommit, ExtensionWorkItem, IcebergCommit, OrgInfo, OrgSettings,
    SpeedboatCommit, TableDescription, TableMetadataCheckpoint,
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
        org_info: &OrgInfo,
        checkpoint: &TableMetadataCheckpoint,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| state.add_checkpoint(org_info, checkpoint))
    }

    pub async fn get_all_iceberg_tables(&self) -> Result<Vec<String>, ServiceApiError> {
        raft_read_state!(self, |state| state.get_all_iceberg_tables())
    }

    pub async fn create_table(
        &self,
        org_info: &OrgInfo,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.create_table(org_info, create_table))
    }

    pub async fn describe_table(
        &self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError> {
        raft_read_state!(self, |state| state.describe_table(org_info, name))
    }

    pub async fn add_alias(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.add_alias(org_info, table_name, alias))
    }

    pub async fn remove_alias(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state
            .remove_alias(org_info, table_name, alias))
    }

    pub async fn create_table_template(
        &self,
        org_info: &OrgInfo,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state
            .create_table_template(org_info, name, template))
    }

    pub async fn describe_table_template(
        &self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        raft_read_state!(self, |state| state.describe_table_template(org_info, name))
    }

    pub async fn create_pipeline(
        &self,
        org_info: &OrgInfo,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state
            .create_pipeline(org_info, name, pipeline))
    }

    pub async fn describe_pipeline(
        &self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        raft_read_state!(self, |state| state.describe_pipeline(org_info, name))
    }

    pub async fn create_lifetime_policy(
        &self,
        org_info: &OrgInfo,
        name: &String,
        policy: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state
            .create_lifetime_policy(org_info, name, policy))
    }

    pub async fn describe_lifetime_policy(
        &self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        raft_read_state!(self, |state| state.describe_lifetime_policy(org_info, name))
    }

    pub async fn speedboat_commit(
        &self,
        org_info: &OrgInfo,
        commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.speedboat_commit(org_info, commit))
    }

    pub async fn iceberg_commit(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        commit: &IcebergCommit,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state
            .iceberg_commit(org_info, table_name, commit))
    }

    pub async fn extension_commit(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state
            .extension_commit(org_info, table_name, commit))
    }

    pub async fn compaction_commit(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        commit: &CompactionCommit,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state
            .compaction_commit(org_info, table_name, commit))
    }

    pub async fn cleanup_commit(
        &self,
        org_info: &OrgInfo,
        commit: &CleanupCommit,
    ) -> Result<bool, ServiceApiError> {
        raft_write_state!(self, |state| state.cleanup_commit(org_info, commit))
    }

    pub async fn get_latest_committed_checkpoint(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        raft_read_state!(
            self,
            |state| MetadataStore::get_latest_committed_checkpoint(
                &mut state, org_info, table_name, extension
            )
        )
    }

    pub async fn get_published_active_checkpoint(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        raft_read_state!(
            self,
            |state| MetadataStore::get_published_active_checkpoint(
                &mut state, org_info, table_name, extension
            )
        )
    }

    pub async fn get_checkpoint(
        &self,
        org_info: &OrgInfo,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        raft_read_state!(self, |state| state.get_checkpoint(org_info, checkpoint))
    }

    pub async fn get_latest_target_checkpoint(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        raft_read_state!(self, |state| MetadataStore::get_latest_target_checkpoint(
            &mut state, org_info, table_name, extension
        ))
    }

    pub async fn get_checkpoint_cutover_state(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceApiError> {
        raft_read_state!(self, |state| MetadataStore::get_checkpoint_cutover_state(
            &mut state, org_info, table_name, extension
        ))
    }

    pub async fn heartbeat_serving_node(
        &self,
        org_info: &OrgInfo,
        lease: &ServingNodeLease,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| MetadataStore::heartbeat_serving_node(
            &mut state, org_info, lease
        ))
    }

    pub async fn record_serving_node_activation(
        &self,
        org_info: &OrgInfo,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| MetadataStore::record_serving_node_activation(
            &mut state, org_info, ack
        ))
    }

    pub async fn list_serving_node_activations(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ServingNodeActivationAck>, ServiceApiError> {
        raft_read_state!(self, |state| MetadataStore::list_serving_node_activations(
            &mut state, org_info, table_name, extension
        ))
    }

    pub async fn record_artifact_readiness(
        &self,
        org_info: &OrgInfo,
        ack: &ArtifactReadinessAck,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| MetadataStore::record_artifact_readiness(
            &mut state, org_info, ack
        ))
    }

    pub async fn list_artifact_readiness(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ArtifactReadinessAck>, ServiceApiError> {
        raft_read_state!(self, |state| MetadataStore::list_artifact_readiness(
            &mut state, org_info, table_name, extension
        ))
    }

    pub async fn get_extension_work_items(
        &self,
        org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        raft_write_state!(self, |state| state
            .get_extension_work_items(org_info, extension_type))
    }

    pub async fn get_compaction_work_items(
        &self,
        org_info: &OrgInfo,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        raft_write_state!(self, |state| state.get_compaction_work_items(org_info))
    }

    pub async fn get_cleanup_work_items(
        &self,
        org_info: &OrgInfo,
    ) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        raft_write_state!(self, |state| state.get_cleanup_work_items(org_info))
    }

    pub async fn update_all_checkpoints(&self) -> Result<bool, ServiceApiError> {
        if !self.is_local_leader().await {
            return Ok(false);
        }
        raft_write_state!(self, |state| MetadataStore::update_all_checkpoints(
            &mut state
        ))
    }

    pub async fn create_org(&self, settings: &OrgSettings) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| state.create_org(settings))
    }

    pub async fn lookup_org(
        &self,
        access_key: &String,
        secret_key: &String,
    ) -> Result<Option<OrgInfo>, ServiceApiError> {
        raft_read_state!(self, |state| state.lookup_org(access_key, secret_key))
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
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        raft_read_state!(
            self,
            |state| MetadataStore::get_latest_committed_checkpoint(
                &mut state, org_info, table_name, extension
            )
        )
    }

    async fn get_published_checkpoint_record(
        &mut self,
        org_info: &OrgInfo,
        selector: &PublishedCheckpointSelector,
    ) -> Result<Option<PublishedCheckpointRecord>, ServiceApiError> {
        raft_read_state!(self, |state| {
            MetadataStore::get_published_checkpoint_record(&mut state, org_info, selector)
        })
    }

    async fn get_checkpoint_metadata(
        &mut self,
        org_info: &OrgInfo,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        self.get_checkpoint(org_info, checkpoint).await
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
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceApiError> {
        raft_read_state!(self, |state| {
            MetadataStore::get_checkpoint_cutover_state(&mut state, org_info, table_name, extension)
        })
    }

    async fn heartbeat_serving_node(
        &mut self,
        org_info: &OrgInfo,
        lease: &ServingNodeLease,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| {
            MetadataStore::heartbeat_serving_node(&mut state, org_info, lease)
        })
    }

    async fn record_serving_node_activation(
        &mut self,
        org_info: &OrgInfo,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| {
            MetadataStore::record_serving_node_activation(&mut state, org_info, ack)
        })
    }

    async fn list_serving_node_activations(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ServingNodeActivationAck>, ServiceApiError> {
        raft_read_state!(self, |state| {
            MetadataStore::list_serving_node_activations(
                &mut state, org_info, table_name, extension,
            )
        })
    }

    async fn record_artifact_readiness(
        &mut self,
        org_info: &OrgInfo,
        ack: &ArtifactReadinessAck,
    ) -> Result<(), ServiceApiError> {
        raft_write_state!(self, |state| {
            MetadataStore::record_artifact_readiness(&mut state, org_info, ack)
        })
    }

    async fn list_artifact_readiness(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ArtifactReadinessAck>, ServiceApiError> {
        raft_read_state!(self, |state| {
            MetadataStore::list_artifact_readiness(&mut state, org_info, table_name, extension)
        })
    }
    async fn claim_extension_work_items(
        &mut self,
        org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ClaimedExtensionWorkItem>, ServiceApiError> {
        Ok(self
            .get_extension_work_items(org_info, extension_type)
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
        org_info: &OrgInfo,
    ) -> Result<Vec<ClaimedCompactionWorkItem>, ServiceApiError> {
        Ok(self
            .get_compaction_work_items(org_info)
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
        org_info: &OrgInfo,
    ) -> Result<Vec<ClaimedCleanupWorkItem>, ServiceApiError> {
        Ok(self
            .get_cleanup_work_items(org_info)
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
        IcebergMetadata, LicenseType, OrgInfo, SpeedboatCommit, SpeedboatCommitTableInfo,
    };
    use crate::ephemeral_service_impl::EphemeralServiceImpl;
    use crate::metadata_store::{
        CutoverEpoch, MetadataStore, ServingNodeActivationAck, ServingNodeLease,
    };
    use crate::read_only_coordination::{ArtifactClass, ArtifactReadinessAck};
    use crate::schema_massager::PowdrrSchema;
    use crate::test_api::{
        CacheMode, CompactionMode, IndexingMode, PeerMode, PrefetchMode, StateMode, StorageMode,
        TestProcessingMode,
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
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Disabled,
            compaction_mode,
            prefetch_mode: PrefetchMode::Disabled,
        }
    }

    fn org_info() -> OrgInfo {
        OrgInfo {
            org_id: "org-1".to_string(),
            license_type: LicenseType::Pro,
        }
    }

    fn speedboat_commit_for(table_name: &str, file_path: &str) -> SpeedboatCommit {
        SpeedboatCommit {
            type_files: vec![SpeedboatCommitTableInfo {
                commit_type: "commit".to_string(),
                table_name: table_name.to_string(),
                segments: vec![],
                files: vec![file_path.to_string()],
                sizes: vec![1],
                schema: Some(PowdrrSchema::minimal()),
            }],
            compaction: None,
        }
    }

    fn iceberg_commit_for(
        file_path: &str,
        snapshot_id: &str,
        compactions: Vec<String>,
    ) -> IcebergCommit {
        let schema = PowdrrSchema::minimal();
        IcebergCommit {
            metadata: IcebergMetadata {
                table_schema: schema.clone(),
                snapshot_id: Some(snapshot_id.to_string()),
                files: FileSetPayload::single(file_path.to_string(), 1, schema),
                partition_spec: vec![],
                sort_order: vec![],
                column_names: vec![],
                column_stats: vec![],
                access_artifacts: vec![],
                file_stats: vec![],
            },
            deletes_table_info: None,
            compactions,
        }
    }

    fn extension_commit_for(id: &str, file_path: &str, location: &str) -> ExtensionCommit {
        ExtensionCommit {
            id: id.to_string(),
            extension: "es".to_string(),
            files: HashMap::from([(
                file_path.to_string(),
                vec![ExtensionFile {
                    suffix: "search_index".to_string(),
                    location: location.to_string(),
                }],
            )]),
        }
    }

    fn serving_lease(node_id: &str) -> ServingNodeLease {
        ServingNodeLease {
            node_id: node_id.to_string(),
            membership_epoch: CutoverEpoch::default(),
            observed_at_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    fn stale_serving_lease(node_id: &str) -> ServingNodeLease {
        ServingNodeLease {
            node_id: node_id.to_string(),
            membership_epoch: CutoverEpoch::default(),
            observed_at_ms: chrono::Utc::now().timestamp_millis() - (2 * 60 * 1000),
        }
    }

    async fn record_readiness(
        raft: &RaftServiceImpl,
        table_name: &String,
        extension: Option<String>,
        epoch: CutoverEpoch,
        checkpoint_id: &String,
    ) {
        let artifact_classes = match extension.as_deref() {
            None => vec![
                ArtifactClass::SnapshotLookupMmap,
                ArtifactClass::SnapshotExactLookupMmap,
            ],
            Some("es") => vec![ArtifactClass::SearchProjection],
            Some(extension) => vec![ArtifactClass::Custom(extension.to_string())],
        };

        for artifact_class in artifact_classes {
            raft.record_artifact_readiness(
                &org_info(),
                &ArtifactReadinessAck {
                    selector: crate::metadata_store::PublishedCheckpointSelector::target(
                        table_name.clone(),
                        extension.clone(),
                    ),
                    checkpoint_id: checkpoint_id.clone(),
                    epoch,
                    artifact_class,
                    producer_id: "warm-cache".to_string(),
                    ready_at_ms: chrono::Utc::now().timestamp_millis(),
                },
            )
            .await
            .unwrap();
        }
    }

    async fn single_node_raft(cluster_name: &str, mode: TestProcessingMode) -> RaftServiceImpl {
        let raft = RaftServiceImpl::new(
            RaftServiceConfig {
                cluster_name: cluster_name.to_string(),
                node_id: 1,
                advertise_address: "127.0.0.1:17784".to_string(),
                bootstrap: true,
                peers: BTreeMap::from([(1, "127.0.0.1:17784".to_string())]),
            },
            mode,
        )
        .await
        .unwrap();
        raft.bootstrap_cluster_if_needed().await.unwrap();
        for _ in 0..50 {
            if raft.is_local_leader().await {
                return raft;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("single-node raft cluster never elected a leader");
    }

    async fn wait_for_leader(nodes: &[Arc<RaftServiceImpl>]) -> Arc<RaftServiceImpl> {
        for _ in 0..200 {
            for node in nodes {
                if node.is_local_leader().await {
                    return node.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("raft cluster never elected a leader");
    }

    async fn multi_node_raft(
        cluster_name: &str,
        mode: TestProcessingMode,
        node_count: u64,
    ) -> (Vec<Arc<RaftServiceImpl>>, TestNodeRegistry) {
        let registry: TestNodeRegistry = Arc::new(Mutex::new(BTreeMap::new()));
        let peers: BTreeMap<u64, String> = (1..=node_count)
            .map(|node_id| (node_id, format!("test://{cluster_name}/{node_id}")))
            .collect();
        let mut nodes = vec![];
        for node_id in 1..=node_count {
            let raft = Arc::new(
                RaftServiceImpl::new_with_test_network(
                    RaftServiceConfig {
                        cluster_name: cluster_name.to_string(),
                        node_id,
                        advertise_address: format!("test://{cluster_name}/{node_id}"),
                        bootstrap: node_id == 1,
                        peers: peers.clone(),
                    },
                    mode.clone(),
                    registry.clone(),
                )
                .await
                .unwrap(),
            );
            registry.lock().await.insert(node_id, raft.clone());
            nodes.push(raft);
        }

        nodes[0].bootstrap_cluster_if_needed().await.unwrap();
        let _ = wait_for_leader(&nodes).await;
        (nodes, registry)
    }

    async fn locally_replicated_active_checkpoint(
        node: &Arc<RaftServiceImpl>,
        table_name: &String,
    ) -> Option<String> {
        let snapshot = node.read_snapshot_state().await.unwrap();
        let mut state = EphemeralServiceImpl::from_snapshot(node.mode.clone(), snapshot);
        MetadataStore::get_published_active_checkpoint(&mut state, &org_info(), table_name, None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn raft_publication_queue_publishes_base_and_extension_frontiers() {
        initialize_ids();

        let raft = single_node_raft(
            "raft-publication-queue",
            raft_test_mode(CompactionMode::Disabled),
        )
        .await;
        let org_info = org_info();
        let table_name = "servable_logs".to_string();
        let file_path = "s3://bucket/logs/first.parquet";

        raft.speedboat_commit(&org_info, &speedboat_commit_for(&table_name, file_path))
            .await
            .unwrap();

        let committed_base_checkpoint = raft
            .get_latest_committed_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            raft.get_latest_target_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            Some(committed_base_checkpoint.clone())
        );
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            None
        );

        let extension_work_items = raft
            .get_extension_work_items(&org_info, &"es".to_string())
            .await
            .unwrap();
        assert_eq!(extension_work_items.len(), 1);
        assert_eq!(
            raft.get_extension_work_items(&org_info, &"es".to_string())
                .await
                .unwrap()
                .len(),
            0
        );

        assert!(raft.update_all_checkpoints().await.unwrap());
        let target_base_checkpoint = raft
            .get_latest_target_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            raft.get_latest_committed_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            Some(target_base_checkpoint.clone())
        );
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            None
        );
        let base_cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();
        raft.heartbeat_serving_node(&org_info, &serving_lease("warm-cache"))
            .await
            .unwrap();
        record_readiness(
            &raft,
            &table_name,
            None,
            base_cutover_state.epoch,
            &target_base_checkpoint,
        )
        .await;
        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: base_cutover_state.selector,
                node_id: "warm-cache".to_string(),
                epoch: base_cutover_state.epoch,
                checkpoint_id: target_base_checkpoint.clone(),
                activated_at_ms: 1,
            },
        )
        .await
        .unwrap();
        assert!(raft.update_all_checkpoints().await.unwrap());
        assert_eq!(
            raft.get_latest_committed_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            Some(target_base_checkpoint.clone())
        );
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            Some(target_base_checkpoint.clone())
        );
        assert_eq!(
            raft.get_latest_committed_checkpoint(&org_info, &table_name, Some("es".to_string()))
                .await
                .unwrap(),
            None
        );

        raft.extension_commit(
            &org_info,
            &table_name,
            &extension_commit_for(
                "ext-1",
                file_path,
                "s3://bucket/logs/first.parquet.search_index",
            ),
        )
        .await
        .unwrap();

        let committed_extension_checkpoint = raft
            .get_latest_committed_checkpoint(&org_info, &table_name, Some("es".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            raft.get_latest_target_checkpoint(&org_info, &table_name, Some("es".to_string()))
                .await
                .unwrap(),
            Some(committed_extension_checkpoint.clone())
        );
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, Some("es".to_string()))
                .await
                .unwrap(),
            None
        );

        assert!(raft.update_all_checkpoints().await.unwrap());
        let target_extension_checkpoint = raft
            .get_latest_target_checkpoint(&org_info, &table_name, Some("es".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            raft.get_latest_committed_checkpoint(&org_info, &table_name, Some("es".to_string()))
                .await
                .unwrap(),
            Some(target_extension_checkpoint.clone())
        );
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, Some("es".to_string()))
                .await
                .unwrap(),
            None
        );
        let extension_cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, Some("es".to_string()))
            .await
            .unwrap();
        raft.heartbeat_serving_node(&org_info, &serving_lease("warm-cache"))
            .await
            .unwrap();
        record_readiness(
            &raft,
            &table_name,
            Some("es".to_string()),
            extension_cutover_state.epoch,
            &target_extension_checkpoint,
        )
        .await;
        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: extension_cutover_state.selector.clone(),
                node_id: "warm-cache".to_string(),
                epoch: extension_cutover_state.epoch,
                checkpoint_id: target_extension_checkpoint.clone(),
                activated_at_ms: 2,
            },
        )
        .await
        .unwrap();
        let recorded_activations = raft
            .list_serving_node_activations(&org_info, &table_name, Some("es".to_string()))
            .await
            .unwrap();
        assert_eq!(recorded_activations.len(), 1);
        let promoted_extension = raft.update_all_checkpoints().await.unwrap();
        let active_extension_checkpoint = raft
            .get_published_active_checkpoint(&org_info, &table_name, Some("es".to_string()))
            .await
            .unwrap();
        assert!(
            promoted_extension
                || active_extension_checkpoint == Some(target_extension_checkpoint.clone()),
            "promoted_extension={promoted_extension}, active_extension_checkpoint={active_extension_checkpoint:?}, cutover_state={:?}, recorded_activations={recorded_activations:?}",
            extension_cutover_state,
        );
        assert_eq!(
            active_extension_checkpoint,
            Some(target_extension_checkpoint)
        );
    }

    #[tokio::test]
    async fn raft_active_checkpoint_waits_for_all_live_serving_nodes() {
        initialize_ids();

        let raft = single_node_raft(
            "raft-live-node-cutover",
            raft_test_mode(CompactionMode::Disabled),
        )
        .await;
        let org_info = org_info();
        let table_name = "strict_cutover_logs".to_string();
        let file_path = "s3://bucket/logs/strict-cutover.parquet";

        raft.speedboat_commit(&org_info, &speedboat_commit_for(&table_name, file_path))
            .await
            .unwrap();
        assert!(raft.update_all_checkpoints().await.unwrap());

        let target_checkpoint = raft
            .get_latest_target_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        let cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();
        record_readiness(
            &raft,
            &table_name,
            None,
            cutover_state.epoch,
            &target_checkpoint,
        )
        .await;

        raft.heartbeat_serving_node(&org_info, &serving_lease("node-a"))
            .await
            .unwrap();
        raft.heartbeat_serving_node(&org_info, &serving_lease("node-b"))
            .await
            .unwrap();

        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: cutover_state.selector.clone(),
                node_id: "node-a".to_string(),
                epoch: cutover_state.epoch,
                checkpoint_id: target_checkpoint.clone(),
                activated_at_ms: 1,
            },
        )
        .await
        .unwrap();

        assert!(!raft.update_all_checkpoints().await.unwrap());
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            None
        );

        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: cutover_state.selector,
                node_id: "node-b".to_string(),
                epoch: cutover_state.epoch,
                checkpoint_id: target_checkpoint.clone(),
                activated_at_ms: 2,
            },
        )
        .await
        .unwrap();

        assert!(raft.update_all_checkpoints().await.unwrap());
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            Some(target_checkpoint)
        );
    }

    #[tokio::test]
    async fn raft_cutover_ignores_nodes_that_join_after_membership_is_captured() {
        initialize_ids();

        let raft = single_node_raft(
            "raft-fixed-membership-late-joiner",
            raft_test_mode(CompactionMode::Disabled),
        )
        .await;
        let org_info = org_info();
        let table_name = "late_joiner_logs".to_string();
        let file_path = "s3://bucket/logs/late-joiner.parquet";

        raft.heartbeat_serving_node(&org_info, &serving_lease("node-a"))
            .await
            .unwrap();
        raft.speedboat_commit(&org_info, &speedboat_commit_for(&table_name, file_path))
            .await
            .unwrap();
        assert!(raft.update_all_checkpoints().await.unwrap());

        let target_checkpoint = raft
            .get_latest_target_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        let cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();

        raft.heartbeat_serving_node(&org_info, &serving_lease("node-b"))
            .await
            .unwrap();
        record_readiness(
            &raft,
            &table_name,
            None,
            cutover_state.epoch,
            &target_checkpoint,
        )
        .await;
        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: cutover_state.selector,
                node_id: "node-a".to_string(),
                epoch: cutover_state.epoch,
                checkpoint_id: target_checkpoint.clone(),
                activated_at_ms: 1,
            },
        )
        .await
        .unwrap();

        assert!(raft.update_all_checkpoints().await.unwrap());
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            Some(target_checkpoint)
        );
    }

    #[tokio::test]
    async fn raft_cutover_backfills_membership_when_target_arrives_before_live_nodes() {
        initialize_ids();

        let raft = single_node_raft(
            "raft-fixed-membership-backfill",
            raft_test_mode(CompactionMode::Disabled),
        )
        .await;
        let org_info = org_info();
        let table_name = "backfill_logs".to_string();
        let file_path = "s3://bucket/logs/backfill.parquet";

        raft.speedboat_commit(&org_info, &speedboat_commit_for(&table_name, file_path))
            .await
            .unwrap();
        assert!(raft.update_all_checkpoints().await.unwrap());

        let target_checkpoint = raft
            .get_latest_target_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        let cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();

        raft.heartbeat_serving_node(&org_info, &serving_lease("node-a"))
            .await
            .unwrap();
        record_readiness(
            &raft,
            &table_name,
            None,
            cutover_state.epoch,
            &target_checkpoint,
        )
        .await;
        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: cutover_state.selector,
                node_id: "node-a".to_string(),
                epoch: cutover_state.epoch,
                checkpoint_id: target_checkpoint.clone(),
                activated_at_ms: 1,
            },
        )
        .await
        .unwrap();

        assert!(raft.update_all_checkpoints().await.unwrap());
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            Some(target_checkpoint)
        );
    }

    #[tokio::test]
    async fn raft_cutover_reconfigures_when_a_required_node_expires() {
        initialize_ids();

        let raft = single_node_raft(
            "raft-membership-reconfiguration",
            raft_test_mode(CompactionMode::Disabled),
        )
        .await;
        let org_info = org_info();
        let table_name = "membership_reconfiguration_logs".to_string();
        let file_path = "s3://bucket/logs/reconfigure.parquet";

        raft.heartbeat_serving_node(&org_info, &serving_lease("node-a"))
            .await
            .unwrap();
        raft.heartbeat_serving_node(&org_info, &serving_lease("node-b"))
            .await
            .unwrap();
        raft.speedboat_commit(&org_info, &speedboat_commit_for(&table_name, file_path))
            .await
            .unwrap();
        assert!(raft.update_all_checkpoints().await.unwrap());

        let target_checkpoint = raft
            .get_latest_target_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        let original_cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();
        record_readiness(
            &raft,
            &table_name,
            None,
            original_cutover_state.epoch,
            &target_checkpoint,
        )
        .await;

        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: original_cutover_state.selector.clone(),
                node_id: "node-a".to_string(),
                epoch: original_cutover_state.epoch,
                checkpoint_id: target_checkpoint.clone(),
                activated_at_ms: 1,
            },
        )
        .await
        .unwrap();
        raft.heartbeat_serving_node(&org_info, &stale_serving_lease("node-b"))
            .await
            .unwrap();

        assert!(!raft.update_all_checkpoints().await.unwrap());
        let reconfigured_cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();
        assert!(reconfigured_cutover_state.epoch > original_cutover_state.epoch);
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            None
        );

        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: reconfigured_cutover_state.selector,
                node_id: "node-a".to_string(),
                epoch: reconfigured_cutover_state.epoch,
                checkpoint_id: target_checkpoint.clone(),
                activated_at_ms: 2,
            },
        )
        .await
        .unwrap();

        assert!(raft.update_all_checkpoints().await.unwrap());
        assert_eq!(
            raft.get_published_active_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            Some(target_checkpoint)
        );
    }

    #[tokio::test]
    async fn raft_claimed_work_is_reissued_after_lease_expiry() {
        initialize_ids();

        let raft = single_node_raft(
            "raft-reclaimable-work",
            raft_test_mode(CompactionMode::Async(Some(1))),
        )
        .await;
        let org_info = org_info();
        let table_name = "reclaimable_work_logs".to_string();
        let file_path = "s3://bucket/logs/reclaimable.parquet";

        raft.speedboat_commit(&org_info, &speedboat_commit_for(&table_name, file_path))
            .await
            .unwrap();

        let extension_work_item = raft
            .get_extension_work_items(&org_info, &"es".to_string())
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            raft.get_extension_work_items(&org_info, &"es".to_string())
                .await
                .unwrap()
                .len(),
            0
        );
        tokio::time::sleep(Duration::from_millis(WORK_CLAIM_RETRY_WAIT_MS)).await;
        let retried_extension_work_item = raft
            .get_extension_work_items(&org_info, &"es".to_string())
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(retried_extension_work_item.id, extension_work_item.id);

        let (_claimed_table_name, compaction_work_item) = raft
            .get_compaction_work_items(&org_info)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            raft.get_compaction_work_items(&org_info)
                .await
                .unwrap()
                .len(),
            0
        );
        tokio::time::sleep(Duration::from_millis(WORK_CLAIM_RETRY_WAIT_MS)).await;
        let retried_compaction_work_item = raft
            .get_compaction_work_items(&org_info)
            .await
            .unwrap()
            .pop()
            .unwrap()
            .1;
        assert_eq!(retried_compaction_work_item.id, compaction_work_item.id);

        raft.compaction_commit(
            &org_info,
            &table_name,
            &CompactionCommit {
                removed_speedboat_files: compaction_work_item.speedboat_files.file_paths.clone(),
                removed_delete_files: compaction_work_item.delete_files.clone(),
                parquet_file_name: "s3://bucket/logs/reclaimable-compacted.parquet".to_string(),
                compaction_id: compaction_work_item.id.clone(),
                checkpoint_id_to_replace: compaction_work_item.checkpoint_id_to_replace.clone(),
                checkpoints_to_delete: compaction_work_item.checkpoints_to_delete.clone(),
            },
        )
        .await
        .unwrap();
        raft.iceberg_commit(
            &org_info,
            &table_name,
            &iceberg_commit_for(
                "s3://bucket/logs/reclaimable-compacted.parquet",
                "snapshot-reclaim",
                vec![compaction_work_item.id.clone()],
            ),
        )
        .await
        .unwrap();

        let cleanup_work_item = raft
            .get_cleanup_work_items(&org_info)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            raft.get_cleanup_work_items(&org_info).await.unwrap().len(),
            0
        );
        tokio::time::sleep(Duration::from_millis(WORK_CLAIM_RETRY_WAIT_MS)).await;
        let retried_cleanup_work_item = raft
            .get_cleanup_work_items(&org_info)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(retried_cleanup_work_item.id, cleanup_work_item.id);
    }

    #[tokio::test]
    async fn raft_cleanup_waits_for_active_checkpoint_cutover() {
        initialize_ids();

        let raft = single_node_raft(
            "raft-cleanup-pinning",
            raft_test_mode(CompactionMode::Async(Some(1))),
        )
        .await;
        let org_info = org_info();
        let table_name = "cleanup_pinning_logs".to_string();
        let file_path = "s3://bucket/logs/pinning.parquet";

        raft.heartbeat_serving_node(&org_info, &serving_lease("warm-cache"))
            .await
            .unwrap();
        raft.speedboat_commit(&org_info, &speedboat_commit_for(&table_name, file_path))
            .await
            .unwrap();
        assert!(raft.update_all_checkpoints().await.unwrap());

        let initial_target_checkpoint = raft
            .get_latest_target_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        let initial_cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();
        record_readiness(
            &raft,
            &table_name,
            None,
            initial_cutover_state.epoch,
            &initial_target_checkpoint,
        )
        .await;
        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: initial_cutover_state.selector,
                node_id: "warm-cache".to_string(),
                epoch: initial_cutover_state.epoch,
                checkpoint_id: initial_target_checkpoint,
                activated_at_ms: 1,
            },
        )
        .await
        .unwrap();
        assert!(raft.update_all_checkpoints().await.unwrap());

        let (_claimed_table_name, compaction_work_item) = raft
            .get_compaction_work_items(&org_info)
            .await
            .unwrap()
            .pop()
            .unwrap();
        raft.compaction_commit(
            &org_info,
            &table_name,
            &CompactionCommit {
                removed_speedboat_files: compaction_work_item.speedboat_files.file_paths.clone(),
                removed_delete_files: compaction_work_item.delete_files.clone(),
                parquet_file_name: "s3://bucket/logs/pinning-compacted.parquet".to_string(),
                compaction_id: compaction_work_item.id.clone(),
                checkpoint_id_to_replace: compaction_work_item.checkpoint_id_to_replace.clone(),
                checkpoints_to_delete: compaction_work_item.checkpoints_to_delete.clone(),
            },
        )
        .await
        .unwrap();
        raft.iceberg_commit(
            &org_info,
            &table_name,
            &iceberg_commit_for(
                "s3://bucket/logs/pinning-compacted.parquet",
                "snapshot-pinning",
                vec![compaction_work_item.id.clone()],
            ),
        )
        .await
        .unwrap();

        assert_eq!(
            raft.get_cleanup_work_items(&org_info).await.unwrap().len(),
            0
        );

        assert!(raft.update_all_checkpoints().await.unwrap());
        let next_target_checkpoint = raft
            .get_latest_target_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        let next_cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();
        record_readiness(
            &raft,
            &table_name,
            None,
            next_cutover_state.epoch,
            &next_target_checkpoint,
        )
        .await;
        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: next_cutover_state.selector,
                node_id: "warm-cache".to_string(),
                epoch: next_cutover_state.epoch,
                checkpoint_id: next_target_checkpoint,
                activated_at_ms: 2,
            },
        )
        .await
        .unwrap();
        assert!(raft.update_all_checkpoints().await.unwrap());

        assert_eq!(
            raft.get_cleanup_work_items(&org_info).await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn raft_work_claims_update_replicated_snapshot_state() {
        initialize_ids();

        let raft = single_node_raft(
            "raft-work-claims",
            raft_test_mode(CompactionMode::Async(Some(1))),
        )
        .await;
        let org_info = org_info();
        let table_name = "compaction_logs".to_string();
        let file_path = "s3://bucket/logs/first.parquet";

        raft.speedboat_commit(&org_info, &speedboat_commit_for(&table_name, file_path))
            .await
            .unwrap();

        let compaction_work_items = raft.get_compaction_work_items(&org_info).await.unwrap();
        assert_eq!(compaction_work_items.len(), 1);
        assert_eq!(
            raft.get_compaction_work_items(&org_info)
                .await
                .unwrap()
                .len(),
            0
        );

        let (_claimed_table_name, compaction_work_item) = compaction_work_items[0].clone();
        let compaction_id = compaction_work_item.id.clone();
        raft.compaction_commit(
            &org_info,
            &table_name,
            &CompactionCommit {
                removed_speedboat_files: compaction_work_item.speedboat_files.file_paths.clone(),
                removed_delete_files: compaction_work_item.delete_files.clone(),
                parquet_file_name: "s3://bucket/logs/compacted.parquet".to_string(),
                compaction_id: compaction_id.clone(),
                checkpoint_id_to_replace: compaction_work_item.checkpoint_id_to_replace.clone(),
                checkpoints_to_delete: compaction_work_item.checkpoints_to_delete.clone(),
            },
        )
        .await
        .unwrap();

        raft.iceberg_commit(
            &org_info,
            &table_name,
            &iceberg_commit_for(
                "s3://bucket/logs/compacted.parquet",
                "snapshot-1",
                vec![compaction_id],
            ),
        )
        .await
        .unwrap();

        let cleanup_work_items = raft.get_cleanup_work_items(&org_info).await.unwrap();
        assert_eq!(cleanup_work_items.len(), 1);
        assert_eq!(
            raft.get_cleanup_work_items(&org_info).await.unwrap().len(),
            0
        );
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
        let org_info = org_info();
        let table_name = "cluster_failover_logs".to_string();
        let first_file_path = "s3://bucket/logs/cluster-first.parquet";

        let leader = wait_for_leader(&nodes).await;
        leader
            .speedboat_commit(
                &org_info,
                &speedboat_commit_for(&table_name, first_file_path),
            )
            .await
            .unwrap();
        assert!(leader.update_all_checkpoints().await.unwrap());
        let first_target_checkpoint = leader
            .get_latest_target_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        let first_cutover_state = leader
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();
        leader
            .heartbeat_serving_node(&org_info, &serving_lease("warm-cache"))
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
            .record_serving_node_activation(
                &org_info,
                &ServingNodeActivationAck {
                    selector: first_cutover_state.selector,
                    node_id: "warm-cache".to_string(),
                    epoch: first_cutover_state.epoch,
                    checkpoint_id: first_target_checkpoint.clone(),
                    activated_at_ms: 1,
                },
            )
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
                .get_latest_committed_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            Some(first_target_checkpoint)
        );

        new_leader
            .speedboat_commit(
                &org_info,
                &speedboat_commit_for(&table_name, "s3://bucket/logs/cluster-second.parquet"),
            )
            .await
            .unwrap();
        assert!(new_leader.update_all_checkpoints().await.unwrap());
        let second_target_checkpoint = new_leader
            .get_latest_target_checkpoint(&org_info, &table_name, None)
            .await
            .unwrap()
            .unwrap();
        let second_cutover_state = new_leader
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();
        new_leader
            .heartbeat_serving_node(&org_info, &serving_lease("warm-cache"))
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
            .record_serving_node_activation(
                &org_info,
                &ServingNodeActivationAck {
                    selector: second_cutover_state.selector,
                    node_id: "warm-cache".to_string(),
                    epoch: second_cutover_state.epoch,
                    checkpoint_id: second_target_checkpoint.clone(),
                    activated_at_ms: 2,
                },
            )
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
