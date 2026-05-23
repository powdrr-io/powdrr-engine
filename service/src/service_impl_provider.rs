use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft_memstore::{MemNodeId, TypeConfig};
use powdrr_service_lib::data_contract::{
    CleanupCommit, CleanupWorkItem, CreateIndexTemplateBody, LicenseType, OrgCreds, OrgInfo,
    OrgSettings, ServiceImplType, ServiceMode, TEST_ACCESS_KEY, TEST_SECRET_KEY,
};
use powdrr_service_lib::data_contract::{
    CompactionCommit, CompactionWorkItem, CreateTable, ExtensionCommit, ExtensionWorkItem,
    IcebergCommit, SpeedboatCommit, TableDescription, TableMetadataCheckpoint,
};
use powdrr_service_lib::dynamodb_service_impl::DynamoDBServiceImpl;
use powdrr_service_lib::elastic_search_lifetime_policy::ILMPolicyDefinition;
use powdrr_service_lib::ephemeral_service_impl::EphemeralServiceImpl;
use powdrr_service_lib::metadata_store::MetadataStore;
use powdrr_service_lib::metadata_store::{
    CheckpointCutoverState, ServingNodeActivationAck, ServingNodeLease,
};
use powdrr_service_lib::peers::CheckpointDescriptor;
use powdrr_service_lib::pipeline::PipelineDefinition;
use powdrr_service_lib::raft_service_impl::{RaftServiceConfig, RaftServiceImpl};
use powdrr_service_lib::read_only_coordination::{
    ArtifactReadinessAck, ReadOnlyCheckpointCoordinationState, ReadOnlyCoordinationStore,
};
use powdrr_service_lib::state_provider::ServiceApiError;
use powdrr_service_lib::test_api::TestProcessingMode;
use std::error::Error;
use std::fmt::{Display, Formatter};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub struct ServiceImplError {
    pub(crate) message: String,
}

impl Error for ServiceImplError {}

impl Display for ServiceImplError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

unsafe impl Send for ServiceImplError {}
unsafe impl Sync for ServiceImplError {}

impl ServiceImplError {
    pub(crate) fn new(message: String) -> Self {
        assert!(message.len() > 0, "Message must not be empty");
        ServiceImplError { message }
    }

    pub(crate) fn from(e: ServiceApiError) -> Self {
        Self::new(format!("Service API error: {}", e))
    }
}

enum ServiceImpl {
    Ephemeral(EphemeralServiceImpl),
    DynamoDb(DynamoDBServiceImpl),
    Raft(RaftServiceImpl),
}

enum ServiceImplProviderActorMessage {
    SetMode {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        mode: ServiceMode,
    },
    ConfigureRaft {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        config: RaftServiceConfig,
    },
    BootstrapRaft {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
    },
    GetForwardBaseUrl {
        respond_to: oneshot::Sender<Result<Option<String>, ServiceImplError>>,
    },
    RaftAppendEntries {
        respond_to: oneshot::Sender<Result<AppendEntriesResponse<MemNodeId>, RaftError<MemNodeId>>>,
        request: AppendEntriesRequest<TypeConfig>,
    },
    RaftVote {
        respond_to: oneshot::Sender<Result<VoteResponse<MemNodeId>, RaftError<MemNodeId>>>,
        request: VoteRequest<MemNodeId>,
    },
    RaftInstallSnapshot {
        respond_to: oneshot::Sender<
            Result<InstallSnapshotResponse<MemNodeId>, RaftError<MemNodeId, InstallSnapshotError>>,
        >,
        request: InstallSnapshotRequest<TypeConfig>,
    },
    CreatePipeline {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        name: String,
        pipeline: PipelineDefinition,
    },
    DescribePipeline {
        respond_to: oneshot::Sender<Result<Option<PipelineDefinition>, ServiceImplError>>,
        org_info: OrgInfo,
        name: String,
    },
    CreateLifetimePolicy {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        name: String,
        policy: ILMPolicyDefinition,
    },
    DescribeLifetimePolicy {
        respond_to: oneshot::Sender<Result<Option<ILMPolicyDefinition>, ServiceImplError>>,
        org_info: OrgInfo,
        name: String,
    },
    CreateTable {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        create_table: CreateTable,
    },
    UpsertTableMetadata {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        create_table: CreateTable,
    },
    DescribeTable {
        respond_to: oneshot::Sender<Result<Option<TableDescription>, ServiceImplError>>,
        org_info: OrgInfo,
        name: String,
    },
    AddAlias {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        alias: String,
    },
    RemoveAlias {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        alias: String,
    },
    CreateTableTemplate {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        name: String,
        template: CreateIndexTemplateBody,
    },
    DescribeTableTemplate {
        respond_to: oneshot::Sender<Result<Option<CreateIndexTemplateBody>, ServiceImplError>>,
        org_info: OrgInfo,
        name: String,
    },
    AddCheckpoint {
        respond_to: oneshot::Sender<()>,
        org_info: OrgInfo,
        checkpoint: TableMetadataCheckpoint,
    },
    IcebergCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        iceberg_commit: IcebergCommit,
    },
    SpeedboatCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        speedboat_commit: SpeedboatCommit,
    },
    ExtensionCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        extension_commit: ExtensionCommit,
    },
    CompactionCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        compaction_commit: CompactionCommit,
    },
    CleanupCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
        org_info: OrgInfo,
        cleanup_commit: CleanupCommit,
    },
    GetPublishedActiveCheckpoint {
        respond_to: oneshot::Sender<Result<Option<String>, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        extensions: Option<String>,
    },
    GetLatestTargetCheckpoint {
        respond_to: oneshot::Sender<Result<Option<String>, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        extensions: Option<String>,
    },
    GetCheckpointCutoverState {
        respond_to: oneshot::Sender<Result<CheckpointCutoverState, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        extensions: Option<String>,
    },
    HeartbeatServingNode {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        lease: ServingNodeLease,
    },
    RecordServingNodeActivation {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        ack: ServingNodeActivationAck,
    },
    RecordArtifactReadiness {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        ack: ArtifactReadinessAck,
    },
    ListArtifactReadiness {
        respond_to: oneshot::Sender<Result<Vec<ArtifactReadinessAck>, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        extensions: Option<String>,
    },
    GetReadOnlyCoordinationState {
        respond_to: oneshot::Sender<Result<ReadOnlyCheckpointCoordinationState, ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        extensions: Option<String>,
    },
    GetCheckpoint {
        respond_to: oneshot::Sender<Result<Option<TableMetadataCheckpoint>, ServiceImplError>>,
        org_info: OrgInfo,
        checkpoint: CheckpointDescriptor,
    },
    GetExtensionWorkItems {
        respond_to: oneshot::Sender<Result<Vec<ExtensionWorkItem>, ServiceImplError>>,
        org_info: OrgInfo,
        extension_type: String,
    },
    GetCompactionWorkItems {
        respond_to: oneshot::Sender<Result<Vec<(String, CompactionWorkItem)>, ServiceImplError>>,
        org_info: OrgInfo,
    },
    GetCleanupWorkItems {
        respond_to: oneshot::Sender<Result<Vec<CleanupWorkItem>, ServiceImplError>>,
        org_info: OrgInfo,
    },
    UpdateAllCheckpoints {
        respond_to: oneshot::Sender<Result<bool, ServiceImplError>>,
    },
    CreateOrg {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        settings: OrgSettings,
    },
    LookupOrg {
        respond_to: oneshot::Sender<Result<Option<OrgInfo>, ServiceImplError>>,
        access_key: String,
        secret_key: String,
    },
}

unsafe impl Send for ServiceImplProviderActorMessage {}

struct ServiceImplProviderActor {
    service_impl: ServiceImpl,
    receiver: mpsc::Receiver<ServiceImplProviderActorMessage>,
}

macro_rules! handle_message_impl {
    ($self:expr, $respond_to:expr, $func:ident($($args:expr),*)) => {
        let _ = $respond_to.send($self.service_impl.$func($($args),*).await);
    };
}

impl ServiceImplProviderActor {
    fn new(receiver: mpsc::Receiver<ServiceImplProviderActorMessage>) -> Self {
        ServiceImplProviderActor {
            service_impl: ServiceImpl::Ephemeral(EphemeralServiceImpl::new(
                TestProcessingMode::ephemeral_default(),
            )),
            receiver,
        }
    }

    async fn handle_message(&mut self, msg: ServiceImplProviderActorMessage) -> () {
        match msg {
            ServiceImplProviderActorMessage::SetMode { respond_to, mode } => {
                self.service_impl = match mode.impl_type {
                    ServiceImplType::Ephemeral => {
                        ServiceImpl::Ephemeral(EphemeralServiceImpl::new(mode.as_testing_mode()))
                    }
                    ServiceImplType::DynamoDb(_) => {
                        ServiceImpl::DynamoDb(DynamoDBServiceImpl::new(mode.as_testing_mode()))
                    }
                    ServiceImplType::TestingDynamoDb(_) => {
                        let mut service_impl =
                            DynamoDBServiceImpl::test(mode.as_testing_mode()).await;
                        service_impl
                            .create_org(&OrgSettings {
                                org_id: "fake_test_org".to_string(),
                                license_type: LicenseType::Free,
                                creds: vec![OrgCreds {
                                    access_key_id: TEST_ACCESS_KEY.to_string(),
                                    secret_access_key: TEST_SECRET_KEY.to_string(),
                                    nickname: None,
                                }],
                            })
                            .await
                            .unwrap();
                        ServiceImpl::DynamoDb(service_impl)
                    }
                };
                respond_to.send(Ok(())).unwrap();
            }
            ServiceImplProviderActorMessage::ConfigureRaft { respond_to, config } => {
                let raft = RaftServiceImpl::new(config, TestProcessingMode::ephemeral_default())
                    .await
                    .map_err(ServiceImplError::from);
                match raft {
                    Ok(raft) => {
                        self.service_impl = ServiceImpl::Raft(raft);
                        respond_to.send(Ok(())).unwrap();
                    }
                    Err(error) => {
                        respond_to.send(Err(error)).unwrap();
                    }
                }
            }
            ServiceImplProviderActorMessage::BootstrapRaft { respond_to } => {
                handle_message_impl!(self, respond_to, bootstrap_raft_if_needed());
            }
            ServiceImplProviderActorMessage::GetForwardBaseUrl { respond_to } => {
                handle_message_impl!(self, respond_to, forward_base_url());
            }
            ServiceImplProviderActorMessage::RaftAppendEntries {
                respond_to,
                request,
            } => {
                handle_message_impl!(self, respond_to, raft_append_entries(request));
            }
            ServiceImplProviderActorMessage::RaftVote {
                respond_to,
                request,
            } => {
                handle_message_impl!(self, respond_to, raft_vote(request));
            }
            ServiceImplProviderActorMessage::RaftInstallSnapshot {
                respond_to,
                request,
            } => {
                handle_message_impl!(self, respond_to, raft_install_snapshot(request));
            }
            ServiceImplProviderActorMessage::CreatePipeline {
                respond_to,
                org_info,
                name,
                pipeline,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    create_pipeline(&org_info, &name, &pipeline)
                );
            }
            ServiceImplProviderActorMessage::DescribePipeline {
                respond_to,
                org_info,
                name,
            } => {
                handle_message_impl!(self, respond_to, describe_pipeline(&org_info, &name));
            }
            ServiceImplProviderActorMessage::CreateLifetimePolicy {
                respond_to,
                org_info,
                name,
                policy,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    create_lifetime_policy(&org_info, &name, &policy)
                );
            }
            ServiceImplProviderActorMessage::DescribeLifetimePolicy {
                respond_to,
                org_info,
                name,
            } => {
                handle_message_impl!(self, respond_to, describe_lifetime_policy(&org_info, &name));
            }
            ServiceImplProviderActorMessage::CreateTable {
                respond_to,
                org_info,
                create_table,
            } => {
                handle_message_impl!(self, respond_to, create_table(&org_info, &create_table));
            }
            ServiceImplProviderActorMessage::UpsertTableMetadata {
                respond_to,
                org_info,
                create_table,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    upsert_table_metadata(&org_info, &create_table)
                );
            }
            ServiceImplProviderActorMessage::DescribeTable {
                respond_to,
                org_info,
                name,
            } => {
                handle_message_impl!(self, respond_to, describe_table(&org_info, &name));
            }
            ServiceImplProviderActorMessage::AddAlias {
                respond_to,
                org_info,
                table_name,
                alias,
            } => {
                handle_message_impl!(self, respond_to, add_alias(&org_info, &table_name, &alias));
            }
            ServiceImplProviderActorMessage::RemoveAlias {
                respond_to,
                org_info,
                table_name,
                alias,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    remove_alias(&org_info, &table_name, &alias)
                );
            }
            ServiceImplProviderActorMessage::CreateTableTemplate {
                respond_to,
                org_info,
                name,
                template,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    create_table_template(&org_info, &name, &template)
                );
            }
            ServiceImplProviderActorMessage::DescribeTableTemplate {
                respond_to,
                org_info,
                name,
            } => {
                handle_message_impl!(self, respond_to, describe_table_template(&org_info, &name));
            }
            ServiceImplProviderActorMessage::AddCheckpoint {
                checkpoint,
                respond_to,
                org_info,
            } => {
                handle_message_impl!(self, respond_to, add_checkpoint(&org_info, &checkpoint));
            }
            ServiceImplProviderActorMessage::IcebergCommit {
                respond_to,
                org_info,
                table_name,
                iceberg_commit,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    iceberg_commit(&org_info, &table_name, &iceberg_commit)
                );
            }
            ServiceImplProviderActorMessage::SpeedboatCommit {
                respond_to,
                org_info,
                speedboat_commit,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    speedboat_commit(&org_info, &speedboat_commit)
                );
            }
            ServiceImplProviderActorMessage::ExtensionCommit {
                respond_to,
                org_info,
                table_name,
                extension_commit,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    extension_commit(&org_info, &table_name, &extension_commit)
                );
            }
            ServiceImplProviderActorMessage::CompactionCommit {
                respond_to,
                org_info,
                table_name,
                compaction_commit,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    compaction_commit(&org_info, &table_name, &compaction_commit)
                );
            }
            ServiceImplProviderActorMessage::CleanupCommit {
                respond_to,
                org_info,
                cleanup_commit,
            } => {
                handle_message_impl!(self, respond_to, cleanup_commit(&org_info, &cleanup_commit));
            }
            ServiceImplProviderActorMessage::GetPublishedActiveCheckpoint {
                org_info,
                table_name,
                extensions,
                respond_to,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    get_published_active_checkpoint(&org_info, &table_name, extensions)
                );
            }
            ServiceImplProviderActorMessage::GetLatestTargetCheckpoint {
                org_info,
                table_name,
                extensions,
                respond_to,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    get_latest_target_checkpoint(&org_info, &table_name, extensions)
                );
            }
            ServiceImplProviderActorMessage::GetCheckpointCutoverState {
                org_info,
                table_name,
                extensions,
                respond_to,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    get_checkpoint_cutover_state(&org_info, &table_name, extensions)
                );
            }
            ServiceImplProviderActorMessage::HeartbeatServingNode {
                org_info,
                lease,
                respond_to,
            } => {
                handle_message_impl!(self, respond_to, heartbeat_serving_node(&org_info, &lease));
            }
            ServiceImplProviderActorMessage::RecordServingNodeActivation {
                org_info,
                ack,
                respond_to,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    record_serving_node_activation(&org_info, &ack)
                );
            }
            ServiceImplProviderActorMessage::RecordArtifactReadiness {
                org_info,
                ack,
                respond_to,
            } => {
                handle_message_impl!(self, respond_to, record_artifact_readiness(&org_info, &ack));
            }
            ServiceImplProviderActorMessage::ListArtifactReadiness {
                org_info,
                table_name,
                extensions,
                respond_to,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    list_artifact_readiness(&org_info, &table_name, extensions)
                );
            }
            ServiceImplProviderActorMessage::GetReadOnlyCoordinationState {
                org_info,
                table_name,
                extensions,
                respond_to,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    get_read_only_coordination_state(&org_info, &table_name, extensions)
                );
            }
            ServiceImplProviderActorMessage::GetCheckpoint {
                checkpoint,
                respond_to,
                org_info,
            } => {
                handle_message_impl!(self, respond_to, get_checkpoint(&org_info, &checkpoint));
            }
            ServiceImplProviderActorMessage::GetExtensionWorkItems {
                extension_type,
                respond_to,
                org_info,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    get_extension_work_items(&org_info, &extension_type)
                );
            }
            ServiceImplProviderActorMessage::GetCompactionWorkItems {
                respond_to,
                org_info,
            } => {
                handle_message_impl!(self, respond_to, get_compaction_work_items(&org_info));
            }
            ServiceImplProviderActorMessage::GetCleanupWorkItems {
                respond_to,
                org_info,
            } => {
                handle_message_impl!(self, respond_to, get_cleanup_work_items(&org_info));
            }
            ServiceImplProviderActorMessage::UpdateAllCheckpoints { respond_to } => {
                handle_message_impl!(self, respond_to, update_all_checkpoints());
            }
            ServiceImplProviderActorMessage::CreateOrg {
                respond_to,
                settings,
            } => {
                handle_message_impl!(self, respond_to, create_org(&settings));
            }
            ServiceImplProviderActorMessage::LookupOrg {
                respond_to,
                access_key,
                secret_key,
            } => {
                handle_message_impl!(self, respond_to, lookup_org(&access_key, &secret_key));
            }
        }
    }
}

async fn run_api_service_client_actor_message_pump(mut actor: ServiceImplProviderActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg).await;
    }
}

macro_rules! state_provider_func_impl {
    ($self:expr, $func:ident($($args:tt),*)) => {
        match $self {
            ServiceImpl::Ephemeral(eph) => eph.$func($($args),*).await.map_err(|e|ServiceImplError::from(e)),
            ServiceImpl::DynamoDb(dynamo) => dynamo.$func($($args),*).await.map_err(|e|ServiceImplError::from(e)),
            ServiceImpl::Raft(raft) => raft.$func($($args),*).await.map_err(|e|ServiceImplError::from(e)),
        }
    };
}

macro_rules! metadata_store_func_impl {
    ($self:expr, $func:ident($($args:tt),*)) => {
        match $self {
            ServiceImpl::Ephemeral(eph) => MetadataStore::$func(eph, $($args),*).await.map_err(|e|ServiceImplError::from(e)),
            ServiceImpl::DynamoDb(dynamo) => MetadataStore::$func(dynamo, $($args),*).await.map_err(|e|ServiceImplError::from(e)),
            ServiceImpl::Raft(raft) => MetadataStore::$func(raft, $($args),*).await.map_err(|e|ServiceImplError::from(e)),
        }
    };
}

macro_rules! read_only_coordination_func_impl {
    ($self:expr, $func:ident($($args:tt),*)) => {
        match $self {
            ServiceImpl::Ephemeral(eph) => ReadOnlyCoordinationStore::$func(eph, $($args),*).await.map_err(|e|ServiceImplError::from(e)),
            ServiceImpl::DynamoDb(dynamo) => ReadOnlyCoordinationStore::$func(dynamo, $($args),*).await.map_err(|e|ServiceImplError::from(e)),
            ServiceImpl::Raft(raft) => ReadOnlyCoordinationStore::$func(raft, $($args),*).await.map_err(|e|ServiceImplError::from(e)),
        }
    };
}

impl ServiceImpl {
    async fn add_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        checkpoint: &TableMetadataCheckpoint,
    ) -> () {
        state_provider_func_impl!(self, add_checkpoint(org_info, checkpoint)).unwrap()
    }

    #[allow(dead_code)]
    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceImplError> {
        state_provider_func_impl!(self, get_all_iceberg_tables())
    }

    pub async fn create_table(
        &mut self,
        org_info: &OrgInfo,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, create_table(org_info, create_table))
    }

    pub async fn upsert_table_metadata(
        &mut self,
        org_info: &OrgInfo,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, upsert_table_metadata(org_info, create_table))
    }

    pub async fn describe_table(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<TableDescription>, ServiceImplError> {
        state_provider_func_impl!(self, describe_table(org_info, name))
    }

    pub async fn add_alias(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, add_alias(org_info, table_name, alias))
    }

    pub async fn remove_alias(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, remove_alias(org_info, table_name, alias))
    }

    pub async fn create_table_template(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, create_table_template(org_info, name, template))
    }

    pub async fn describe_table_template(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceImplError> {
        state_provider_func_impl!(self, describe_table_template(org_info, name))
    }

    pub async fn create_pipeline(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, create_pipeline(org_info, name, pipeline))
    }

    pub async fn describe_pipeline(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceImplError> {
        state_provider_func_impl!(self, describe_pipeline(org_info, name))
    }

    pub async fn create_lifetime_policy(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
        policy: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, create_lifetime_policy(org_info, name, policy))
    }

    pub async fn describe_lifetime_policy(
        &mut self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceImplError> {
        state_provider_func_impl!(self, describe_lifetime_policy(org_info, name))
    }

    pub async fn speedboat_commit(
        &mut self,
        org_info: &OrgInfo,
        commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, speedboat_commit(org_info, commit))
    }

    pub async fn iceberg_commit(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        iceberg_commit: &IcebergCommit,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, iceberg_commit(org_info, table_name, iceberg_commit))
    }

    pub async fn extension_commit(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        commit: &ExtensionCommit,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, extension_commit(org_info, table_name, commit))
    }

    pub async fn compaction_commit(
        &mut self,
        org_info: &OrgInfo,
        _table_name: &String,
        commit: &CompactionCommit,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, compaction_commit(org_info, _table_name, commit))
    }

    pub async fn cleanup_commit(
        &mut self,
        org_info: &OrgInfo,
        commit: &CleanupCommit,
    ) -> Result<bool, ServiceImplError> {
        state_provider_func_impl!(self, cleanup_commit(org_info, commit))
    }

    pub async fn get_published_active_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceImplError> {
        read_only_coordination_func_impl!(
            self,
            get_published_active_checkpoint(org_info, table_name, extensions)
        )
    }

    pub async fn get_latest_target_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceImplError> {
        read_only_coordination_func_impl!(
            self,
            get_latest_target_checkpoint(org_info, table_name, extensions)
        )
    }

    pub async fn get_checkpoint_cutover_state(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceImplError> {
        read_only_coordination_func_impl!(
            self,
            get_checkpoint_cutover_state(org_info, table_name, extensions)
        )
    }

    pub async fn heartbeat_serving_node(
        &mut self,
        org_info: &OrgInfo,
        lease: &ServingNodeLease,
    ) -> Result<(), ServiceImplError> {
        read_only_coordination_func_impl!(self, heartbeat_serving_node(org_info, lease))
    }

    pub async fn record_serving_node_activation(
        &mut self,
        org_info: &OrgInfo,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceImplError> {
        read_only_coordination_func_impl!(self, record_serving_node_activation(org_info, ack))
    }

    pub async fn record_artifact_readiness(
        &mut self,
        org_info: &OrgInfo,
        ack: &ArtifactReadinessAck,
    ) -> Result<(), ServiceImplError> {
        read_only_coordination_func_impl!(self, record_artifact_readiness(org_info, ack))
    }

    pub async fn list_artifact_readiness(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Vec<ArtifactReadinessAck>, ServiceImplError> {
        read_only_coordination_func_impl!(
            self,
            list_artifact_readiness(org_info, table_name, extensions)
        )
    }

    pub async fn get_read_only_coordination_state(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<ReadOnlyCheckpointCoordinationState, ServiceImplError> {
        read_only_coordination_func_impl!(
            self,
            get_read_only_coordination_state(org_info, table_name, extensions)
        )
    }
    pub async fn get_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        snapshot: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceImplError> {
        metadata_store_func_impl!(self, get_checkpoint(org_info, snapshot))
    }

    pub async fn get_extension_work_items(
        &mut self,
        org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceImplError> {
        metadata_store_func_impl!(self, get_extension_work_items(org_info, extension_type))
    }

    pub async fn get_compaction_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceImplError> {
        metadata_store_func_impl!(self, get_compaction_work_items(org_info))
    }

    pub async fn get_cleanup_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<CleanupWorkItem>, ServiceImplError> {
        metadata_store_func_impl!(self, get_cleanup_work_items(org_info))
    }

    pub async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceImplError> {
        metadata_store_func_impl!(self, update_all_checkpoints())
    }

    pub async fn create_org(&mut self, settings: &OrgSettings) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_org(settings))
    }

    pub async fn lookup_org(
        &mut self,
        access_key: &String,
        secret_key: &String,
    ) -> Result<Option<OrgInfo>, ServiceImplError> {
        state_provider_func_impl!(self, lookup_org(access_key, secret_key))
    }

    pub async fn bootstrap_raft_if_needed(&mut self) -> Result<(), ServiceImplError> {
        match self {
            ServiceImpl::Raft(raft) => raft
                .bootstrap_cluster_if_needed()
                .await
                .map_err(ServiceImplError::from),
            _ => Ok(()),
        }
    }

    pub async fn forward_base_url(&mut self) -> Result<Option<String>, ServiceImplError> {
        match self {
            ServiceImpl::Raft(raft) => Ok(raft.forward_base_url().await),
            _ => Ok(None),
        }
    }

    pub async fn raft_append_entries(
        &mut self,
        request: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<MemNodeId>, RaftError<MemNodeId>> {
        match self {
            ServiceImpl::Raft(raft) => raft.append_entries(request).await,
            _ => panic!("Raft append requested while service is not in raft mode"),
        }
    }

    pub async fn raft_vote(
        &mut self,
        request: VoteRequest<MemNodeId>,
    ) -> Result<VoteResponse<MemNodeId>, RaftError<MemNodeId>> {
        match self {
            ServiceImpl::Raft(raft) => raft.vote(request).await,
            _ => panic!("Raft vote requested while service is not in raft mode"),
        }
    }

    pub async fn raft_install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<MemNodeId>, RaftError<MemNodeId, InstallSnapshotError>>
    {
        match self {
            ServiceImpl::Raft(raft) => raft.install_snapshot(request).await,
            _ => panic!("Raft snapshot requested while service is not in raft mode"),
        }
    }
}

#[derive(Clone)]
pub struct ServiceImplHandle {
    sender: mpsc::Sender<ServiceImplProviderActorMessage>,
}

macro_rules! send_message {
    ($self:expr, $message_type:tt) => {{
        let (send, recv) = oneshot::channel();
        let msg = ServiceImplProviderActorMessage::$message_type { respond_to: send };
        let _ = $self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }};

    ($self:expr, $message_type:tt, $field:ident = $value:expr) => {{
        let (send, recv) = oneshot::channel();
        let msg = ServiceImplProviderActorMessage::$message_type {
            respond_to: send,
            $field: $value,
        };
        let _ = $self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }};

    ($self:expr, $message_type:tt, $field1:ident = $value1:expr, $field2:ident = $value2:expr) => {{
        let (send, recv) = oneshot::channel();
        let _ = $self
            .sender
            .send(ServiceImplProviderActorMessage::$message_type {
                respond_to: send,
                $field1: $value1,
                $field2: $value2,
            })
            .await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }};

    ($self:expr, $message_type:tt, $field1:ident = $value1:expr, $field2:ident = $value2:expr, $field3:ident = $value3:expr) => {{
        let (send, recv) = oneshot::channel();
        let _ = $self
            .sender
            .send(ServiceImplProviderActorMessage::$message_type {
                respond_to: send,
                $field1: $value1,
                $field2: $value2,
                $field3: $value3,
            })
            .await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }};
}

impl ServiceImplHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let actor = ServiceImplProviderActor::new(receiver);
        tokio::spawn(run_api_service_client_actor_message_pump(actor));

        Self { sender }
    }

    pub async fn set_mode(&self, mode: ServiceMode) -> Result<(), ServiceImplError> {
        send_message!(self, SetMode, mode = mode)
    }

    pub async fn configure_raft(&self, config: RaftServiceConfig) -> Result<(), ServiceImplError> {
        send_message!(self, ConfigureRaft, config = config)
    }

    pub async fn bootstrap_raft_if_needed(&self) -> Result<(), ServiceImplError> {
        send_message!(self, BootstrapRaft)
    }

    pub async fn forward_base_url(&self) -> Result<Option<String>, ServiceImplError> {
        send_message!(self, GetForwardBaseUrl)
    }

    pub async fn raft_append_entries(
        &self,
        request: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<MemNodeId>, RaftError<MemNodeId>> {
        send_message!(self, RaftAppendEntries, request = request)
    }

    pub async fn raft_vote(
        &self,
        request: VoteRequest<MemNodeId>,
    ) -> Result<VoteResponse<MemNodeId>, RaftError<MemNodeId>> {
        send_message!(self, RaftVote, request = request)
    }

    pub async fn raft_install_snapshot(
        &self,
        request: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<MemNodeId>, RaftError<MemNodeId, InstallSnapshotError>>
    {
        send_message!(self, RaftInstallSnapshot, request = request)
    }

    pub async fn create_pipeline(
        &self,
        org_info: &OrgInfo,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            CreatePipeline,
            org_info = org_info.clone(),
            name = name.clone(),
            pipeline = pipeline.clone()
        )
    }

    pub async fn describe_pipeline(
        &self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceImplError> {
        send_message!(
            self,
            DescribePipeline,
            org_info = org_info.clone(),
            name = name.clone()
        )
    }

    pub async fn create_lifetime_policy(
        &self,
        org_info: &OrgInfo,
        name: &String,
        policy: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            CreateLifetimePolicy,
            org_info = org_info.clone(),
            name = name.clone(),
            policy = policy.clone()
        )
    }

    pub async fn describe_lifetime_policy(
        &self,
        org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceImplError> {
        send_message!(
            self,
            DescribeLifetimePolicy,
            org_info = org_info.clone(),
            name = name.clone()
        )
    }

    pub async fn create_table(
        &self,
        org_info: &OrgInfo,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            CreateTable,
            org_info = org_info.clone(),
            create_table = create_table.clone()
        )
    }

    pub async fn upsert_table_metadata(
        &self,
        org_info: &OrgInfo,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            UpsertTableMetadata,
            org_info = org_info.clone(),
            create_table = create_table.clone()
        )
    }

    pub async fn describe_table(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
    ) -> Result<Option<TableDescription>, ServiceImplError> {
        send_message!(
            self,
            DescribeTable,
            org_info = org_info.clone(),
            name = table_name.clone()
        )
    }

    pub async fn add_alias(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            AddAlias,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            alias = alias.clone()
        )
    }

    pub async fn remove_alias(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            RemoveAlias,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            alias = alias.clone()
        )
    }

    pub async fn create_table_template(
        &self,
        org_info: &OrgInfo,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            CreateTableTemplate,
            org_info = org_info.clone(),
            name = name.clone(),
            template = template.clone()
        )
    }

    pub async fn describe_table_template(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceImplError> {
        send_message!(
            self,
            DescribeTableTemplate,
            org_info = org_info.clone(),
            name = table_name.clone()
        )
    }

    #[allow(dead_code)]
    pub async fn add_checkpoint(
        &self,
        org_info: &OrgInfo,
        checkpoint: &TableMetadataCheckpoint,
    ) -> () {
        send_message!(
            self,
            AddCheckpoint,
            org_info = org_info.clone(),
            checkpoint = checkpoint.clone()
        )
    }

    pub async fn iceberg_commit(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        iceberg_commit: &IcebergCommit,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            IcebergCommit,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            iceberg_commit = iceberg_commit.clone()
        )
    }

    pub async fn speedboat_commit(
        &self,
        org_info: &OrgInfo,
        speedboat_commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            SpeedboatCommit,
            org_info = org_info.clone(),
            speedboat_commit = speedboat_commit.clone()
        )
    }

    pub async fn extension_commit(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension_commit: &ExtensionCommit,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            ExtensionCommit,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            extension_commit = extension_commit.clone()
        )
    }

    pub async fn compaction_commit(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        compaction_commit: &CompactionCommit,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            CompactionCommit,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            compaction_commit = compaction_commit.clone()
        )
    }

    pub async fn cleanup_commit(
        &self,
        org_info: &OrgInfo,
        cleanup_commit: &CleanupCommit,
    ) -> Result<bool, ServiceImplError> {
        send_message!(
            self,
            CleanupCommit,
            org_info = org_info.clone(),
            cleanup_commit = cleanup_commit.clone()
        )
    }

    pub async fn get_latest_checkpoint(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceImplError> {
        send_message!(
            self,
            GetPublishedActiveCheckpoint,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }

    pub async fn get_published_active_checkpoint(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceImplError> {
        send_message!(
            self,
            GetPublishedActiveCheckpoint,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }

    pub async fn get_latest_target_checkpoint(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceImplError> {
        send_message!(
            self,
            GetLatestTargetCheckpoint,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }

    pub async fn get_checkpoint_cutover_state(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceImplError> {
        send_message!(
            self,
            GetCheckpointCutoverState,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }

    pub async fn heartbeat_serving_node(
        &self,
        org_info: &OrgInfo,
        lease: &ServingNodeLease,
    ) -> Result<(), ServiceImplError> {
        send_message!(
            self,
            HeartbeatServingNode,
            org_info = org_info.clone(),
            lease = lease.clone()
        )
    }

    pub async fn record_serving_node_activation(
        &self,
        org_info: &OrgInfo,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceImplError> {
        send_message!(
            self,
            RecordServingNodeActivation,
            org_info = org_info.clone(),
            ack = ack.clone()
        )
    }

    pub async fn record_artifact_readiness(
        &self,
        org_info: &OrgInfo,
        ack: &ArtifactReadinessAck,
    ) -> Result<(), ServiceImplError> {
        send_message!(
            self,
            RecordArtifactReadiness,
            org_info = org_info.clone(),
            ack = ack.clone()
        )
    }

    pub async fn list_artifact_readiness(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ArtifactReadinessAck>, ServiceImplError> {
        send_message!(
            self,
            ListArtifactReadiness,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }

    pub async fn get_read_only_coordination_state(
        &self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<ReadOnlyCheckpointCoordinationState, ServiceImplError> {
        send_message!(
            self,
            GetReadOnlyCoordinationState,
            org_info = org_info.clone(),
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }
    pub async fn get_checkpoint(
        &self,
        org_info: &OrgInfo,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceImplError> {
        send_message!(
            self,
            GetCheckpoint,
            org_info = org_info.clone(),
            checkpoint = checkpoint.clone()
        )
    }

    pub async fn get_extension_work_items(
        &self,
        org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceImplError> {
        send_message!(
            self,
            GetExtensionWorkItems,
            org_info = org_info.clone(),
            extension_type = extension_type.clone()
        )
    }
    pub async fn get_compaction_work_items(
        &self,
        org_info: &OrgInfo,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceImplError> {
        send_message!(self, GetCompactionWorkItems, org_info = org_info.clone())
    }

    pub async fn get_cleanup_work_items(
        &self,
        org_info: &OrgInfo,
    ) -> Result<Vec<CleanupWorkItem>, ServiceImplError> {
        send_message!(self, GetCleanupWorkItems, org_info = org_info.clone())
    }

    pub async fn update_all_checkpoints(&self) -> Result<bool, ServiceImplError> {
        send_message!(self, UpdateAllCheckpoints)
    }

    pub async fn create_org(&self, settings: &OrgSettings) -> Result<(), ServiceImplError> {
        send_message!(self, CreateOrg, settings = settings.clone())
    }

    pub async fn lookup_org(
        &self,
        access_key: &String,
        secret_key: &String,
    ) -> Result<Option<OrgInfo>, ServiceImplError> {
        send_message!(
            self,
            LookupOrg,
            access_key = access_key.clone(),
            secret_key = secret_key.clone()
        )
    }
}

pub static SERVICE_IMPL: std::sync::LazyLock<ServiceImplHandle> =
    std::sync::LazyLock::new(|| ServiceImplHandle::new());
