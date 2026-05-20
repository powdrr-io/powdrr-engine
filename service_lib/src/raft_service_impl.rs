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
    pub async fn new(
        mut config: RaftServiceConfig,
        mode: TestProcessingMode,
    ) -> Result<Self, ServiceApiError> {
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
            Network::new(peer_addresses.clone()),
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
        raft_read_state!(self, |state| {
            state.get_latest_committed_checkpoint(org_info, table_name, extension)
        })
    }

    pub async fn get_checkpoint(
        &self,
        org_info: &OrgInfo,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        raft_read_state!(self, |state| state.get_checkpoint(org_info, checkpoint))
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
                claim: MetadataClaimKind::ProcessLocal,
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
                claim: MetadataClaimKind::ProcessLocal,
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
                claim: MetadataClaimKind::ProcessLocal,
                work_item,
            })
            .collect())
    }

    async fn advance_published_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        self.update_all_checkpoints().await
    }
}

#[cfg(test)]
mod tests {
    use super::{RaftServiceConfig, RaftServiceImpl};
    use crate::data_contract::{
        CompactionCommit, ExtensionCommit, ExtensionFile, FileSetPayload, IcebergCommit,
        IcebergMetadata, LicenseType, OrgInfo, SpeedboatCommit, SpeedboatCommitTableInfo,
    };
    use crate::metadata_store::{MetadataStore, ServingNodeActivationAck};
    use crate::schema_massager::PowdrrSchema;
    use crate::test_api::{
        CacheMode, CompactionMode, IndexingMode, PeerMode, PrefetchMode, StateMode, StorageMode,
        TestProcessingMode,
    };
    use idgenerator::{IdGeneratorOptions, IdInstance};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Once;
    use std::time::Duration;

    static TEST_IDS_INITIALIZED: Once = Once::new();

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
                column_names: vec![],
                column_stats: vec![],
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

    #[tokio::test]
    async fn raft_publication_queue_publishes_base_and_extension_frontiers() {
        initialize_ids();

        let mut raft = single_node_raft(
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

        assert_eq!(
            raft.get_latest_committed_checkpoint(&org_info, &table_name, None)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            raft.get_latest_target_checkpoint(&org_info, &table_name, None)
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
            None
        );
        let base_cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, None)
            .await
            .unwrap();
        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: base_cutover_state.selector,
                node_id: "warm-base".to_string(),
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

        assert_eq!(
            raft.get_latest_committed_checkpoint(&org_info, &table_name, Some("es".to_string()))
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
            None
        );
        let extension_cutover_state = raft
            .get_checkpoint_cutover_state(&org_info, &table_name, Some("es".to_string()))
            .await
            .unwrap();
        raft.record_serving_node_activation(
            &org_info,
            &ServingNodeActivationAck {
                selector: extension_cutover_state.selector,
                node_id: "warm-es".to_string(),
                epoch: extension_cutover_state.epoch,
                checkpoint_id: target_extension_checkpoint.clone(),
                activated_at_ms: 2,
            },
        )
        .await
        .unwrap();
        assert!(raft.update_all_checkpoints().await.unwrap());
        assert_eq!(
            raft.get_latest_committed_checkpoint(&org_info, &table_name, Some("es".to_string()))
                .await
                .unwrap(),
            Some(target_extension_checkpoint)
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
        let compaction_id = "cmp-1".to_string();
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
}
