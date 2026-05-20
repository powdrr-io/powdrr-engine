use crate::data_contract::{
    CleanupCommit, CleanupWorkItem, CompactionCommit, CompactionWorkItem, CreateIndexTemplateBody,
    CreateTable, ExtensionCommit, ExtensionWorkItem, IcebergCommit, OrgSettings, SpeedboatCommit,
    TableDescription, TableMetadataCheckpoint,
};
use crate::distributed_cache::set_redis_address;
use crate::dynamodb_state_provider::DynamoDbStateProvider;
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::ephemeral_state_provider::EphemeralStateProvider;
use crate::leaderless_state_provider::LeaderlessStateProvider;
use crate::peers::{PeerClient, PeerProvider};
use crate::test_api::{
    CacheMode, CompactionMode, PeerModeType, StateMode, StorageMode, TestProcessingMode,
};
use crate::{
    data_access, distributed_cache, peers::CheckpointDescriptor, pipeline::PipelineDefinition,
};
use tokio::sync::{mpsc, oneshot};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../shared/service_control_plane/service_api_error.rs"
));

const DEFAULT_SERVABLE_EXTENSION: &str = "es";

impl ServiceApiError {
    pub fn from_reqwest(error: reqwest::Error) -> Self {
        Self::new(format!("Reqwest: {}", error))
    }
}

enum StateProviderActorMessage {
    SetMode {
        respond_to: oneshot::Sender<()>,
        mode: TestProcessingMode,
    },
    SetPeerMode {
        respond_to: oneshot::Sender<()>,
        mode: PeerModeType,
    },
    CreatePipeline {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        name: String,
        pipeline: PipelineDefinition,
    },
    DescribePipeline {
        respond_to: oneshot::Sender<Result<Option<PipelineDefinition>, ServiceApiError>>,
        name: String,
    },
    CreateLifetimePolicy {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        name: String,
        policy: ILMPolicyDefinition,
    },
    DescribeLifetimePolicy {
        respond_to: oneshot::Sender<Result<Option<ILMPolicyDefinition>, ServiceApiError>>,
        name: String,
    },
    CreateTable {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        create_table: CreateTable,
    },
    CreateOrg {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        settings: OrgSettings,
    },
    UpsertTableMetadata {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        create_table: CreateTable,
    },
    DescribeTable {
        respond_to: oneshot::Sender<Result<Option<TableDescription>, ServiceApiError>>,
        name: String,
    },
    LookupSecretAccessKey {
        respond_to: oneshot::Sender<Result<Option<String>, ServiceApiError>>,
        access_key_id: String,
    },
    GetAllIcebergTables {
        respond_to: oneshot::Sender<Result<Vec<String>, ServiceApiError>>,
    },
    AddAlias {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        table_name: String,
        alias: String,
    },
    RemoveAlias {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        table_name: String,
        alias: String,
    },
    CreateTableTemplate {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        name: String,
        template: CreateIndexTemplateBody,
    },
    DescribeTableTemplate {
        respond_to: oneshot::Sender<Result<Option<CreateIndexTemplateBody>, ServiceApiError>>,
        name: String,
    },
    AddCheckpoint {
        respond_to: oneshot::Sender<()>,
        checkpoint: TableMetadataCheckpoint,
    },
    IcebergCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        table_name: String,
        iceberg_commit: IcebergCommit,
    },
    SpeedboatCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        speedboat_commit: SpeedboatCommit,
    },
    ExtensionCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        table_name: String,
        extension_commit: ExtensionCommit,
    },
    CompactionCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        table_name: String,
        compaction_commit: CompactionCommit,
    },
    CleanupCommit {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
        cleanup_commit: CleanupCommit,
    },
    GetLatestCommittedCheckpoint {
        respond_to: oneshot::Sender<Result<Option<String>, ServiceApiError>>,
        table_name: String,
        extensions: Option<String>,
    },
    GetPublishedActiveCheckpoint {
        respond_to: oneshot::Sender<Result<Option<String>, ServiceApiError>>,
        table_name: String,
        extensions: Option<String>,
    },
    GetLatestTargetCheckpoint {
        respond_to: oneshot::Sender<Result<Option<String>, ServiceApiError>>,
        table_name: String,
        extensions: Option<String>,
    },
    GetCheckpoint {
        respond_to: oneshot::Sender<Result<Option<TableMetadataCheckpoint>, ServiceApiError>>,
        checkpoint: CheckpointDescriptor,
    },
    UpdateAllCheckpoints {
        respond_to: oneshot::Sender<Result<bool, ServiceApiError>>,
    },
    GetExtensionWorkItems {
        respond_to: oneshot::Sender<Result<Vec<ExtensionWorkItem>, ServiceApiError>>,
        extension_type: String,
    },
    GetCompactionWorkItems {
        respond_to: oneshot::Sender<Result<Vec<(String, CompactionWorkItem)>, ServiceApiError>>,
    },
    GetCleanupWorkItems {
        respond_to: oneshot::Sender<Result<Vec<CleanupWorkItem>, ServiceApiError>>,
    },
    GetPeerClients {
        respond_to: oneshot::Sender<Vec<Box<dyn PeerClient>>>,
    },
    GetNextPrefetchCheckpoints {
        respond_to: oneshot::Sender<Result<Vec<CheckpointDescriptor>, ServiceApiError>>,
        extensions: Option<String>,
    },
    SetTargetCheckpoints {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        checkpoints: Vec<CheckpointDescriptor>,
        extensions: Option<String>,
    },
}

unsafe impl Send for StateProviderActorMessage {}

struct StateProviderActor {
    state_provider: StateProvider,
    peer_provider: PeerProvider,
    receiver: mpsc::Receiver<StateProviderActorMessage>,
    #[allow(dead_code)]
    compaction_mode: CompactionMode,
}

macro_rules! handle_message_impl {
    ($self:expr, $respond_to:expr, $func:ident($($args:expr),*)) => {
        let _ = $respond_to.send($self.state_provider.$func($($args),*).await);
    };
}

impl StateProviderActor {
    fn new(receiver: mpsc::Receiver<StateProviderActorMessage>) -> Self {
        StateProviderActor {
            state_provider: StateProvider::Ephemeral(EphemeralStateProvider::new(
                TestProcessingMode::default(),
            )),
            peer_provider: PeerProvider::new(PeerModeType::SelfOnly),
            receiver,
            compaction_mode: CompactionMode::Disabled,
        }
    }

    async fn handle_message(&mut self, msg: StateProviderActorMessage) -> () {
        match msg {
            StateProviderActorMessage::SetMode { respond_to, mode } => {
                match &mode.cache_mode {
                    CacheMode::Redis(address) => {
                        set_redis_address(address);
                    }
                    CacheMode::Native => {
                        panic!("Native cache mode is not supported");
                    }
                }
                match &mode.state_mode {
                    StateMode::Testing => {
                        self.state_provider =
                            StateProvider::Ephemeral(EphemeralStateProvider::new(mode.clone()))
                    }
                    StateMode::Ephemeral => {
                        self.state_provider =
                            StateProvider::Ephemeral(EphemeralStateProvider::new(mode.clone()))
                    }
                    StateMode::TestingDynamoDb(_) => {
                        let provider = DynamoDbStateProvider::test(mode.clone()).await;
                        self.state_provider = StateProvider::DynamoDb(provider);
                    }
                    StateMode::Leaderless {
                        server_address,
                        access_key,
                        secret_key,
                    } => {
                        self.state_provider =
                            StateProvider::Leaderless(LeaderlessStateProvider::new(
                                mode.clone(),
                                server_address.clone(),
                                access_key.clone(),
                                secret_key.clone(),
                            ))
                    }
                }
                match &mode.storage_mode {
                    StorageMode::S3 {
                        rest_endpoint,
                        s3_endpoint,
                    } => data_access::set_s3_endpoint(rest_endpoint, s3_endpoint).await,
                }

                self.peer_provider
                    .set_mode(mode.peer_mode.to_peer_mode_type());
                respond_to.send(()).unwrap();
            }
            StateProviderActorMessage::SetPeerMode { respond_to, mode } => {
                self.peer_provider.set_mode(mode);
                respond_to.send(()).unwrap();
            }
            StateProviderActorMessage::CreatePipeline {
                respond_to,
                name,
                pipeline,
            } => {
                handle_message_impl!(self, respond_to, create_pipeline(&name, &pipeline));
            }
            StateProviderActorMessage::DescribePipeline { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_pipeline(&name));
            }
            StateProviderActorMessage::CreateLifetimePolicy {
                respond_to,
                name,
                policy,
            } => {
                handle_message_impl!(self, respond_to, create_lifetime_policy(&name, &policy));
            }
            StateProviderActorMessage::DescribeLifetimePolicy { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_lifetime_policy(&name));
            }
            StateProviderActorMessage::CreateTable {
                respond_to,
                create_table,
            } => {
                match distributed_cache::create_table(&create_table.name) {
                    Ok(_) => (),
                    Err(e) => panic!("Unable to create table = {}", e),
                };
                handle_message_impl!(self, respond_to, create_table(&create_table));
            }
            StateProviderActorMessage::CreateOrg {
                respond_to,
                settings,
            } => {
                handle_message_impl!(self, respond_to, create_org(&settings));
            }
            StateProviderActorMessage::UpsertTableMetadata {
                respond_to,
                create_table,
            } => {
                match distributed_cache::create_table(&create_table.name) {
                    Ok(_) => (),
                    Err(e) => panic!("Unable to create table = {}", e),
                };
                handle_message_impl!(self, respond_to, upsert_table_metadata(&create_table));
            }
            StateProviderActorMessage::DescribeTable { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_table(&name));
            }
            StateProviderActorMessage::LookupSecretAccessKey {
                respond_to,
                access_key_id,
            } => {
                handle_message_impl!(self, respond_to, lookup_secret_access_key(&access_key_id));
            }
            StateProviderActorMessage::GetAllIcebergTables { respond_to } => {
                handle_message_impl!(self, respond_to, get_all_iceberg_tables());
            }
            StateProviderActorMessage::AddAlias {
                respond_to,
                table_name,
                alias,
            } => {
                handle_message_impl!(self, respond_to, add_alias(&table_name, &alias));
            }
            StateProviderActorMessage::RemoveAlias {
                respond_to,
                table_name,
                alias,
            } => {
                handle_message_impl!(self, respond_to, remove_alias(&table_name, &alias));
            }
            StateProviderActorMessage::CreateTableTemplate {
                respond_to,
                name,
                template,
            } => {
                handle_message_impl!(self, respond_to, create_table_template(&name, &template));
            }
            StateProviderActorMessage::DescribeTableTemplate { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_table_template(&name));
            }
            StateProviderActorMessage::AddCheckpoint {
                checkpoint,
                respond_to,
            } => {
                handle_message_impl!(self, respond_to, add_checkpoint(&checkpoint));
            }
            StateProviderActorMessage::IcebergCommit {
                respond_to,
                table_name,
                iceberg_commit,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    iceberg_commit(&table_name, &iceberg_commit)
                );
            }
            StateProviderActorMessage::SpeedboatCommit {
                respond_to,
                speedboat_commit,
            } => {
                handle_message_impl!(self, respond_to, speedboat_commit(&speedboat_commit));
            }
            StateProviderActorMessage::ExtensionCommit {
                respond_to,
                table_name,
                extension_commit,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    extension_commit(&table_name, &extension_commit)
                );
            }
            StateProviderActorMessage::CompactionCommit {
                respond_to,
                table_name,
                compaction_commit,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    compaction_commit(&table_name, &compaction_commit)
                );
            }
            StateProviderActorMessage::CleanupCommit {
                respond_to,
                cleanup_commit,
            } => {
                handle_message_impl!(self, respond_to, cleanup_commit(&cleanup_commit));
            }
            StateProviderActorMessage::GetLatestCommittedCheckpoint {
                table_name,
                extensions,
                respond_to,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    get_latest_committed_checkpoint(&table_name, extensions)
                );
            }
            StateProviderActorMessage::GetPublishedActiveCheckpoint {
                table_name,
                extensions,
                respond_to,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    get_published_active_checkpoint(&table_name, extensions)
                );
            }
            StateProviderActorMessage::GetLatestTargetCheckpoint {
                table_name,
                extensions,
                respond_to,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    get_latest_target_checkpoint(&table_name, extensions)
                );
            }
            StateProviderActorMessage::UpdateAllCheckpoints { respond_to } => {
                handle_message_impl!(self, respond_to, update_all_checkpoints());
            }
            StateProviderActorMessage::GetCheckpoint {
                checkpoint,
                respond_to,
            } => {
                handle_message_impl!(self, respond_to, get_checkpoint(&checkpoint));
            }
            StateProviderActorMessage::GetExtensionWorkItems {
                extension_type,
                respond_to,
            } => {
                handle_message_impl!(self, respond_to, get_extension_work_items(&extension_type));
            }
            StateProviderActorMessage::GetCompactionWorkItems { respond_to } => {
                handle_message_impl!(self, respond_to, get_compaction_work_items());
            }
            StateProviderActorMessage::GetCleanupWorkItems { respond_to } => {
                handle_message_impl!(self, respond_to, get_cleanup_work_items());
            }
            StateProviderActorMessage::GetPeerClients { respond_to } => {
                let peers = self.peer_provider.get_peer_clients();
                respond_to.send(peers).unwrap();
            }
            StateProviderActorMessage::GetNextPrefetchCheckpoints {
                respond_to,
                extensions,
            } => {
                handle_message_impl!(self, respond_to, get_next_prefetch_checkpoints(extensions));
            }
            StateProviderActorMessage::SetTargetCheckpoints {
                respond_to,
                checkpoints,
                extensions,
            } => {
                handle_message_impl!(
                    self,
                    respond_to,
                    set_target_checkpoints(&checkpoints, extensions)
                );
            }
        }
    }
}

async fn run_state_provider_actor_message_pump(mut actor: StateProviderActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg).await;
    }
}

enum StateProvider {
    Ephemeral(EphemeralStateProvider),
    DynamoDb(DynamoDbStateProvider),
    #[allow(dead_code)]
    Leaderless(LeaderlessStateProvider),
}

macro_rules! state_provider_func_impl {
    ($self:expr, $func:ident($($args:tt),*)) => {
        match $self {
            StateProvider::Ephemeral(eph) => eph.$func($($args),*).await,
            StateProvider::DynamoDb(dynamo) => dynamo.$func($($args),*).await,
            StateProvider::Leaderless(lead) => lead.$func($($args),*).await,
        }
    };
}

impl StateProvider {
    pub(crate) async fn set_target_checkpoints(
        &mut self,
        descriptors: &Vec<CheckpointDescriptor>,
        extension: Option<String>,
    ) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, set_target_checkpoints(descriptors, extension))
    }

    async fn get_latest_target_checkpoint(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        state_provider_func_impl!(self, get_latest_target_checkpoint(table_name, extension))
    }

    async fn add_checkpoint(&mut self, checkpoint: &TableMetadataCheckpoint) -> () {
        state_provider_func_impl!(self, add_checkpoint(checkpoint))
    }

    #[allow(dead_code)]
    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        state_provider_func_impl!(self, get_all_iceberg_tables())
    }

    pub async fn create_table(
        &mut self,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, create_table(create_table))
    }

    pub async fn create_org(&mut self, settings: &OrgSettings) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, create_org(settings))
    }

    #[allow(dead_code)]
    pub async fn upsert_table_metadata(
        &mut self,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, upsert_table_metadata(create_table))
    }

    pub async fn describe_table(
        &mut self,
        name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError> {
        state_provider_func_impl!(self, describe_table(name))
    }

    pub async fn lookup_secret_access_key(
        &mut self,
        access_key_id: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        state_provider_func_impl!(self, lookup_secret_access_key(access_key_id))
    }

    pub async fn add_alias(
        &mut self,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, add_alias(table_name, alias))
    }

    pub async fn remove_alias(
        &mut self,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, remove_alias(table_name, alias))
    }

    pub async fn create_table_template(
        &mut self,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, create_table_template(name, template))
    }

    pub async fn describe_table_template(
        &mut self,
        name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        state_provider_func_impl!(self, describe_table_template(name))
    }

    pub async fn create_pipeline(
        &mut self,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, create_pipeline(name, pipeline))
    }

    pub async fn describe_pipeline(
        &mut self,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        state_provider_func_impl!(self, describe_pipeline(name))
    }

    pub async fn create_lifetime_policy(
        &mut self,
        name: &String,
        policy: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, create_lifetime_policy(name, policy))
    }

    pub async fn describe_lifetime_policy(
        &mut self,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        state_provider_func_impl!(self, describe_lifetime_policy(name))
    }

    pub async fn speedboat_commit(
        &mut self,
        commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, speedboat_commit(commit))
    }

    pub async fn iceberg_commit(
        &mut self,
        table_name: &String,
        iceberg_commit: &IcebergCommit,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, iceberg_commit(table_name, iceberg_commit))
    }

    pub async fn extension_commit(
        &mut self,
        table_name: &String,
        commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, extension_commit(table_name, commit))
    }

    pub async fn compaction_commit(
        &mut self,
        _table_name: &String,
        commit: &CompactionCommit,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, compaction_commit(_table_name, commit))
    }

    pub async fn cleanup_commit(
        &mut self,
        commit: &CleanupCommit,
    ) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, cleanup_commit(commit))
    }

    pub async fn get_latest_committed_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        state_provider_func_impl!(
            self,
            get_latest_committed_checkpoint(table_name, extensions)
        )
    }

    pub async fn get_published_active_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        state_provider_func_impl!(
            self,
            get_published_active_checkpoint(table_name, extensions)
        )
    }

    pub async fn get_checkpoint(
        &mut self,
        snapshot: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        state_provider_func_impl!(self, get_checkpoint(snapshot))
    }

    pub async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        state_provider_func_impl!(self, update_all_checkpoints())
    }

    pub async fn get_extension_work_items(
        &mut self,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        state_provider_func_impl!(self, get_extension_work_items(extension_type))
    }

    pub async fn get_compaction_work_items(
        &mut self,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        state_provider_func_impl!(self, get_compaction_work_items())
    }

    pub async fn get_cleanup_work_items(
        &mut self,
    ) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        state_provider_func_impl!(self, get_cleanup_work_items())
    }

    pub async fn get_next_prefetch_checkpoints(
        &mut self,
        extensions: Option<String>,
    ) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        state_provider_func_impl!(self, get_next_prefetch_checkpoints(extensions))
    }
}

#[derive(Clone)]
pub struct StateProviderHandle {
    sender: mpsc::Sender<StateProviderActorMessage>,
}

macro_rules! send_message {
    ($self:expr, $message_type:tt) => {{
        let (send, recv) = oneshot::channel();
        let msg = StateProviderActorMessage::$message_type { respond_to: send };
        let _ = $self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }};

    ($self:expr, $message_type:tt, $field:ident = $value:expr) => {{
        let (send, recv) = oneshot::channel();
        let msg = StateProviderActorMessage::$message_type {
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
            .send(StateProviderActorMessage::$message_type {
                respond_to: send,
                $field1: $value1,
                $field2: $value2,
            })
            .await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }};
}

impl StateProviderHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let actor = StateProviderActor::new(receiver);
        tokio::spawn(run_state_provider_actor_message_pump(actor));

        Self { sender }
    }

    pub async fn set_testing_mode(&self, mode: &TestProcessingMode) -> () {
        send_message!(self, SetMode, mode = mode.clone());
    }

    pub async fn set_peer_mode(&self, mode: &PeerModeType) -> () {
        send_message!(self, SetPeerMode, mode = mode.clone());
    }

    pub async fn create_pipeline(
        &self,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            CreatePipeline,
            name = name.clone(),
            pipeline = pipeline.clone()
        )
    }

    pub async fn describe_pipeline(
        &self,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        send_message!(self, DescribePipeline, name = name.clone())
    }

    pub async fn create_lifetime_policy(
        &self,
        name: &String,
        policy: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            CreateLifetimePolicy,
            name = name.clone(),
            policy = policy.clone()
        )
    }

    pub async fn describe_lifetime_policy(
        &self,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        send_message!(self, DescribeLifetimePolicy, name = name.clone())
    }

    pub async fn create_table(&self, create_table: &CreateTable) -> Result<bool, ServiceApiError> {
        send_message!(self, CreateTable, create_table = create_table.clone())
    }

    pub async fn create_org(&self, settings: &OrgSettings) -> Result<(), ServiceApiError> {
        send_message!(self, CreateOrg, settings = settings.clone())
    }

    pub async fn upsert_table_metadata(
        &self,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            UpsertTableMetadata,
            create_table = create_table.clone()
        )
    }

    pub async fn describe_table(
        &self,
        table_name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError> {
        send_message!(self, DescribeTable, name = table_name.clone())
    }

    pub async fn lookup_secret_access_key(
        &self,
        access_key_id: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        send_message!(
            self,
            LookupSecretAccessKey,
            access_key_id = access_key_id.clone()
        )
    }

    pub async fn get_all_iceberg_tables(&self) -> Result<Vec<String>, ServiceApiError> {
        send_message!(self, GetAllIcebergTables)
    }

    pub async fn add_alias(
        &self,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            AddAlias,
            table_name = table_name.clone(),
            alias = alias.clone()
        )
    }

    pub async fn remove_alias(
        &self,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            RemoveAlias,
            table_name = table_name.clone(),
            alias = alias.clone()
        )
    }

    pub async fn create_table_template(
        &self,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            CreateTableTemplate,
            name = name.clone(),
            template = template.clone()
        )
    }

    pub async fn describe_table_template(
        &self,
        table_name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        send_message!(self, DescribeTableTemplate, name = table_name.clone())
    }

    pub async fn add_checkpoint(&self, checkpoint: &TableMetadataCheckpoint) -> () {
        send_message!(self, AddCheckpoint, checkpoint = checkpoint.clone())
    }

    pub async fn iceberg_commit(
        &self,
        table_name: &String,
        iceberg_commit: &IcebergCommit,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            IcebergCommit,
            table_name = table_name.clone(),
            iceberg_commit = iceberg_commit.clone()
        )
    }

    pub async fn speedboat_commit(
        &self,
        speedboat_commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            SpeedboatCommit,
            speedboat_commit = speedboat_commit.clone()
        )
    }

    pub async fn extension_commit(
        &self,
        table_name: &String,
        extension_commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            ExtensionCommit,
            table_name = table_name.clone(),
            extension_commit = extension_commit.clone()
        )
    }

    pub async fn compaction_commit(
        &self,
        table_name: &String,
        compaction_commit: &CompactionCommit,
    ) -> Result<bool, ServiceApiError> {
        send_message!(
            self,
            CompactionCommit,
            table_name = table_name.clone(),
            compaction_commit = compaction_commit.clone()
        )
    }

    pub async fn cleanup_commit(
        &self,
        cleanup_commit: &CleanupCommit,
    ) -> Result<bool, ServiceApiError> {
        send_message!(self, CleanupCommit, cleanup_commit = cleanup_commit.clone())
    }

    pub async fn get_latest_committed_checkpoint(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        send_message!(
            self,
            GetLatestCommittedCheckpoint,
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }

    pub async fn get_published_active_checkpoint(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        send_message!(
            self,
            GetPublishedActiveCheckpoint,
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }

    pub async fn get_latest_checkpoint(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        self.get_published_active_checkpoint(table_name, extension)
            .await
    }

    pub async fn get_published_active_servable_checkpoint(
        &self,
        table_name: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        match self
            .get_published_active_checkpoint(
                table_name,
                Some(DEFAULT_SERVABLE_EXTENSION.to_string()),
            )
            .await?
        {
            Some(checkpoint_id) => Ok(Some(checkpoint_id)),
            None => self.get_published_active_checkpoint(table_name, None).await,
        }
    }

    pub async fn get_active_servable_checkpoint(
        &self,
        table_name: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        self.get_published_active_servable_checkpoint(table_name)
            .await
    }

    pub async fn get_latest_servable_checkpoint(
        &self,
        table_name: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        self.get_published_active_servable_checkpoint(table_name)
            .await
    }

    pub async fn get_published_target_checkpoint(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        send_message!(
            self,
            GetLatestTargetCheckpoint,
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }

    pub async fn get_published_target_servable_checkpoint(
        &self,
        table_name: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        match self
            .get_published_target_checkpoint(
                table_name,
                Some(DEFAULT_SERVABLE_EXTENSION.to_string()),
            )
            .await?
        {
            Some(checkpoint_id) => Ok(Some(checkpoint_id)),
            None => match self
                .get_published_target_checkpoint(table_name, None)
                .await?
            {
                Some(checkpoint_id) => Ok(Some(checkpoint_id)),
                None => {
                    self.get_published_active_servable_checkpoint(table_name)
                        .await
                }
            },
        }
    }

    pub async fn get_target_servable_checkpoint(
        &self,
        table_name: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        self.get_published_target_servable_checkpoint(table_name)
            .await
    }

    pub async fn get_checkpoint(
        &self,
        checkpoint: CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        send_message!(self, GetCheckpoint, checkpoint = checkpoint.clone())
    }

    pub async fn advance_published_checkpoints(&self) -> Result<bool, ServiceApiError> {
        send_message!(self, UpdateAllCheckpoints)
    }

    pub async fn update_all_checkpoints(&self) -> Result<bool, ServiceApiError> {
        self.advance_published_checkpoints().await
    }

    pub async fn get_extension_work_items(
        &self,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        send_message!(
            self,
            GetExtensionWorkItems,
            extension_type = extension_type.clone()
        )
    }

    pub async fn get_compaction_work_items(
        &self,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        send_message!(self, GetCompactionWorkItems)
    }

    pub async fn get_cleanup_work_items(&self) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        send_message!(self, GetCleanupWorkItems)
    }

    pub async fn get_peer_clients(&self) -> Vec<Box<dyn PeerClient>> {
        send_message!(self, GetPeerClients)
    }

    pub async fn get_latest_target_checkpoint(
        &self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        send_message!(
            self,
            GetLatestTargetCheckpoint,
            table_name = table_name.clone(),
            extensions = extension.clone()
        )
    }

    pub async fn get_next_prefetch_checkpoints(
        &self,
        extension: Option<String>,
    ) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        send_message!(
            self,
            GetNextPrefetchCheckpoints,
            extensions = extension.clone()
        )
    }

    pub async fn set_prefetch_checkpoints(
        &self,
        checkpoints: &Vec<CheckpointDescriptor>,
        extension: Option<String>,
    ) -> Result<(), ServiceApiError> {
        send_message!(
            self,
            SetTargetCheckpoints,
            checkpoints = checkpoints.clone(),
            extensions = extension.clone()
        )
    }
}

pub static STATE_PROVIDER: std::sync::LazyLock<StateProviderHandle> =
    std::sync::LazyLock::new(|| StateProviderHandle::new());

#[cfg(test)]
mod tests {
    use super::STATE_PROVIDER;
    use crate::data_contract::{
        ExtensionCommit, ExtensionFile, FileSetPayload, IcebergCommit, IcebergMetadata,
    };
    use crate::peers::CheckpointDescriptor;
    use crate::schema_massager::PowdrrSchema;
    use crate::test_api::{
        CacheMode, CompactionMode, IndexingMode, PeerMode, PrefetchMode, StateMode, StorageMode,
        TestProcessingMode,
    };
    use idgenerator::{IdGeneratorOptions, IdInstance};
    use std::collections::HashMap;
    use std::sync::Once;

    static TEST_IDS_INITIALIZED: Once = Once::new();

    fn initialize_ids() {
        TEST_IDS_INITIALIZED.call_once(|| {
            let options = IdGeneratorOptions::new().worker_id(1).worker_id_bit_len(6);
            let _ = IdInstance::init(options);
        });
    }

    fn ephemeral_test_mode() -> TestProcessingMode {
        TestProcessingMode {
            state_mode: StateMode::Ephemeral,
            storage_mode: StorageMode::default(),
            cache_mode: CacheMode::Redis(None),
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Disabled,
            compaction_mode: CompactionMode::Disabled,
            prefetch_mode: PrefetchMode::Disabled,
        }
    }

    fn iceberg_commit_for(file_path: &str, snapshot_id: &str) -> IcebergCommit {
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
            compactions: vec![],
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

    #[tokio::test]
    async fn published_servable_checkpoint_helpers_fall_back_and_prefer_extension_frontier() {
        initialize_ids();

        STATE_PROVIDER
            .set_testing_mode(&ephemeral_test_mode())
            .await;

        let table_name = "servable_logs".to_string();
        let first_file = "s3://bucket/logs/first.parquet";
        let second_file = "s3://bucket/logs/second.parquet";

        STATE_PROVIDER
            .iceberg_commit(&table_name, &iceberg_commit_for(first_file, "snapshot_1"))
            .await
            .unwrap();

        let first_checkpoint = STATE_PROVIDER
            .get_latest_committed_checkpoint(&table_name, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            STATE_PROVIDER
                .get_latest_committed_checkpoint(&table_name, Some("es".to_string()))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            STATE_PROVIDER
                .advance_published_checkpoints()
                .await
                .unwrap(),
            true
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );

        STATE_PROVIDER
            .extension_commit(
                &table_name,
                &extension_commit_for("ext_1", first_file, "s3://bucket/logs/first.search"),
            )
            .await
            .unwrap();

        assert_eq!(
            STATE_PROVIDER
                .get_latest_committed_checkpoint(&table_name, Some("es".to_string()))
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_checkpoint(&table_name, Some("es".to_string()))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .advance_published_checkpoints()
                .await
                .unwrap(),
            true
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_checkpoint(&table_name, Some("es".to_string()))
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );

        STATE_PROVIDER
            .iceberg_commit(&table_name, &iceberg_commit_for(second_file, "snapshot_2"))
            .await
            .unwrap();

        let second_checkpoint = STATE_PROVIDER
            .get_latest_committed_checkpoint(&table_name, None)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(second_checkpoint, first_checkpoint);
        assert_eq!(
            STATE_PROVIDER
                .get_latest_committed_checkpoint(&table_name, Some("es".to_string()))
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_checkpoint(&table_name, None)
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .advance_published_checkpoints()
                .await
                .unwrap(),
            true
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_checkpoint(&table_name, None)
                .await
                .unwrap(),
            Some(second_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );

        STATE_PROVIDER
            .extension_commit(
                &table_name,
                &extension_commit_for("ext_2", second_file, "s3://bucket/logs/second.search"),
            )
            .await
            .unwrap();

        assert_eq!(
            STATE_PROVIDER
                .get_latest_committed_checkpoint(&table_name, Some("es".to_string()))
                .await
                .unwrap(),
            Some(second_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_checkpoint(&table_name, Some("es".to_string()))
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_target_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            Some(first_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .advance_published_checkpoints()
                .await
                .unwrap(),
            true
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_target_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            Some(second_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            Some(second_checkpoint.clone())
        );

        let target_checkpoint = "prefetched-cutover-checkpoint".to_string();
        STATE_PROVIDER
            .set_prefetch_checkpoints(
                &vec![CheckpointDescriptor::new(
                    table_name.clone(),
                    target_checkpoint.clone(),
                )],
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            STATE_PROVIDER
                .get_published_target_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            Some(second_checkpoint.clone())
        );
        assert_eq!(
            STATE_PROVIDER
                .get_published_active_servable_checkpoint(&table_name)
                .await
                .unwrap(),
            Some(second_checkpoint)
        );
    }
}
