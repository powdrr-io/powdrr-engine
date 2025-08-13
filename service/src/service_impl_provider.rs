use std::{error::Error};
use std::fmt::{Display, Formatter};
use powdrr_lib::data_contract::{CleanupCommit, CleanupWorkItem, CreateIndexTemplateBody, LicenseType, OrgCreds, OrgInfo, OrgSettings, ServiceImplType, ServiceMode};
use powdrr_lib::elastic_search_lifetime_policy::ILMPolicyDefinition;
use powdrr_lib::ephemeral_service_impl::EphemeralServiceImpl;
use powdrr_lib::pipeline::PipelineDefinition;
use powdrr_lib::data_contract::{CompactionCommit, CompactionWorkItem, CreateTable, ExtensionCommit, ExtensionWorkItem, IcebergCommit, SpeedboatCommit, TableDescription, TableMetadataCheckpoint};
use powdrr_lib::peers::CheckpointDescriptor;
use tokio::sync::{mpsc, oneshot};
use powdrr_lib::state_provider::ServiceApiError;
use powdrr_lib::dynamodb_service_impl::DynamoDBServiceImpl;
use powdrr_lib::test_api::TestProcessingMode;


pub(crate) const ACCESS_KEY: &str = "access_key";
pub(crate) const SECRET_KEY: &str = "secret_key";


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
        ServiceImplError {
            message,
        }
    }

    pub(crate) fn from(e: ServiceApiError) -> Self {
        Self::new(format!("Service API error: {}", e))
    }
}


enum ServiceImpl {
    Ephemeral(EphemeralServiceImpl),
    DynamoDb(DynamoDBServiceImpl),
}

enum ServiceImplProviderActorMessage {
    SetMode {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        mode: ServiceMode,
    },
    CreatePipeline {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
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
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
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
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        create_table: CreateTable,
    },
    DescribeTable {
        respond_to: oneshot::Sender<Result<Option<TableDescription>, ServiceImplError>>,
        org_info: OrgInfo,
        name: String,
    },
    AddAlias {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        alias: String,
    },
    RemoveAlias {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        alias: String,
    },
    CreateTableTemplate {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
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
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        iceberg_commit: IcebergCommit,
    },
    SpeedboatCommit {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        speedboat_commit: SpeedboatCommit,
    },
    ExtensionCommit {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        extension_commit: ExtensionCommit,
    },
    CompactionCommit {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        table_name: String,
        compaction_commit: CompactionCommit,
    },
    CleanupCommit {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        org_info: OrgInfo,
        cleanup_commit: CleanupCommit,
    },
    GetLatestCommittedCheckpoint {
        respond_to: oneshot::Sender<Result<Option<String>, ServiceImplError>>,
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
            service_impl: ServiceImpl::Ephemeral(EphemeralServiceImpl::new(TestProcessingMode::default())),
            receiver,
        }
    }

    async fn handle_message(&mut self, msg: ServiceImplProviderActorMessage) -> () {
        match msg {
            ServiceImplProviderActorMessage::SetMode { respond_to, mode } => {
                self.service_impl = match mode.impl_type {
                    ServiceImplType::Ephemeral => ServiceImpl::Ephemeral(EphemeralServiceImpl::new(mode.as_testing_mode())),
                    ServiceImplType::DynamoDb => ServiceImpl::DynamoDb(DynamoDBServiceImpl::new(mode.as_testing_mode())),
                    ServiceImplType::TestingDynamoDb => {
                        let mut service_impl = DynamoDBServiceImpl::test(mode.as_testing_mode()).await;
                        service_impl.create_org(&OrgSettings {
                            org_id: "fake_test_org".to_string(),
                            license_type: LicenseType::Free,
                            creds: vec![
                                OrgCreds {
                                    access_key_id: ACCESS_KEY.to_string(),
                                    secret_access_key: SECRET_KEY.to_string(),
                                    nickname: None,
                                }
                            ],
                        }).await.unwrap();
                        ServiceImpl::DynamoDb(service_impl)
                    }
                };
                respond_to.send(Ok(())).unwrap();
            }
            ServiceImplProviderActorMessage::CreatePipeline { respond_to, org_info, name, pipeline } => {
                handle_message_impl!(self, respond_to, create_pipeline(&org_info, &name, &pipeline));
            },
            ServiceImplProviderActorMessage::DescribePipeline { respond_to, org_info, name } => {
                handle_message_impl!(self, respond_to, describe_pipeline(&org_info, &name));
            },
            ServiceImplProviderActorMessage::CreateLifetimePolicy { respond_to, org_info, name, policy } => {
                handle_message_impl!(self, respond_to, create_lifetime_policy(&org_info, &name, &policy));
            },
            ServiceImplProviderActorMessage::DescribeLifetimePolicy { respond_to, org_info, name } => {
                handle_message_impl!(self, respond_to, describe_lifetime_policy(&org_info, &name));
            },
            ServiceImplProviderActorMessage::CreateTable { respond_to, org_info, create_table } => {
                handle_message_impl!(self, respond_to, create_table(&org_info, &create_table));
            },
            ServiceImplProviderActorMessage::DescribeTable { respond_to, org_info, name } => {
                handle_message_impl!(self, respond_to, describe_table(&org_info, &name));
            },
            ServiceImplProviderActorMessage::AddAlias { respond_to, org_info, table_name, alias } => {
                handle_message_impl!(self, respond_to, add_alias(&org_info, &table_name, &alias));
            },
            ServiceImplProviderActorMessage::RemoveAlias { respond_to, org_info, table_name, alias } => {
                handle_message_impl!(self, respond_to, remove_alias(&org_info, &table_name, &alias));
            },
            ServiceImplProviderActorMessage::CreateTableTemplate { respond_to, org_info, name, template } => {
                handle_message_impl!(self, respond_to, create_table_template(&org_info, &name, &template));
            },
            ServiceImplProviderActorMessage::DescribeTableTemplate { respond_to, org_info, name } => {
                handle_message_impl!(self, respond_to, describe_table_template(&org_info, &name));
            },
            ServiceImplProviderActorMessage::AddCheckpoint { checkpoint, respond_to, org_info } => {
                handle_message_impl!(self, respond_to, add_checkpoint(&org_info, &checkpoint));
            },
            ServiceImplProviderActorMessage::IcebergCommit { respond_to, org_info, table_name, iceberg_commit } => {
                handle_message_impl!(self, respond_to, iceberg_commit(&org_info, &table_name, &iceberg_commit));
            },
            ServiceImplProviderActorMessage::SpeedboatCommit { respond_to, org_info, speedboat_commit } => {
                handle_message_impl!(self, respond_to, speedboat_commit(&org_info, &speedboat_commit));
            },
            ServiceImplProviderActorMessage::ExtensionCommit { respond_to, org_info, table_name, extension_commit } => {
                handle_message_impl!(self, respond_to, extension_commit(&org_info, &table_name, &extension_commit));
            },
            ServiceImplProviderActorMessage::CompactionCommit { respond_to, org_info, table_name, compaction_commit } => {
                handle_message_impl!(self, respond_to, compaction_commit(&org_info, &table_name, &compaction_commit));
            },
            ServiceImplProviderActorMessage::CleanupCommit { respond_to, org_info, cleanup_commit } => {
                handle_message_impl!(self, respond_to, cleanup_commit(&org_info, &cleanup_commit));
            },
            ServiceImplProviderActorMessage::GetLatestCommittedCheckpoint { org_info, table_name, extensions, respond_to } => {
                handle_message_impl!(self, respond_to, get_latest_committed_checkpoint(&org_info, &table_name, extensions));
            },
            ServiceImplProviderActorMessage::GetCheckpoint { checkpoint, respond_to, org_info } => {
                handle_message_impl!(self, respond_to, get_checkpoint(&org_info, &checkpoint));
            },
            ServiceImplProviderActorMessage::GetExtensionWorkItems { extension_type, respond_to, org_info } => {
                handle_message_impl!(self, respond_to, get_extension_work_items(&org_info, &extension_type));
            },
            ServiceImplProviderActorMessage::GetCompactionWorkItems { respond_to, org_info } => {
                handle_message_impl!(self, respond_to, get_compaction_work_items(&org_info));
            },
            ServiceImplProviderActorMessage::GetCleanupWorkItems { respond_to, org_info } => {
                handle_message_impl!(self, respond_to, get_cleanup_work_items(&org_info));
            },
            ServiceImplProviderActorMessage::CreateOrg { respond_to, settings } => {
                handle_message_impl!(self, respond_to, create_org(&settings));
            },
            ServiceImplProviderActorMessage::LookupOrg { respond_to, access_key, secret_key } => {
                handle_message_impl!(self, respond_to, lookup_org(&access_key, &secret_key));
            },
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
        }
    };
}


impl ServiceImpl {
    async fn add_checkpoint(&mut self, org_info: &OrgInfo, checkpoint: &TableMetadataCheckpoint) -> () {
        state_provider_func_impl!(self, add_checkpoint(org_info, checkpoint)).unwrap()
    }

    #[allow(dead_code)]
    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceImplError> {
        state_provider_func_impl!(self, get_all_iceberg_tables())
    }

    pub async fn create_table(&mut self, org_info: &OrgInfo, create_table: &CreateTable) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_table(org_info, create_table))
    }

    pub async fn describe_table(&mut self, org_info: &OrgInfo, name: &String) -> Result<Option<TableDescription>, ServiceImplError> {
        state_provider_func_impl!(self, describe_table(org_info, name))
    }

    pub async fn add_alias(&mut self, org_info: &OrgInfo, table_name: &String, alias: &String) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, add_alias(org_info, table_name, alias))
    }

    pub async fn remove_alias(&mut self, org_info: &OrgInfo, table_name: &String, alias: &String) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, remove_alias(org_info, table_name, alias))
    }

    pub async fn create_table_template(&mut self, org_info: &OrgInfo, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_table_template(org_info, name, template))
    }

    pub async fn describe_table_template(&mut self, org_info: &OrgInfo, name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceImplError> {
        state_provider_func_impl!(self, describe_table_template(org_info, name))
    }

    pub async fn create_pipeline(&mut self, org_info: &OrgInfo, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_pipeline(org_info, name, pipeline))
    }

    pub async fn describe_pipeline(&mut self, org_info: &OrgInfo, name: &String) -> Result<Option<PipelineDefinition>, ServiceImplError> {
        state_provider_func_impl!(self, describe_pipeline(org_info, name))
    }

    pub async fn create_lifetime_policy(&mut self, org_info: &OrgInfo, name: &String, policy: &ILMPolicyDefinition) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_lifetime_policy(org_info, name, policy))
    }

    pub async fn describe_lifetime_policy(&mut self, org_info: &OrgInfo, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceImplError> {
        state_provider_func_impl!(self, describe_lifetime_policy(org_info, name))
    }

    pub async fn speedboat_commit(&mut self, org_info: &OrgInfo, commit: &SpeedboatCommit) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, speedboat_commit(org_info, commit))
    }

    pub async fn iceberg_commit(&mut self, org_info: &OrgInfo, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, iceberg_commit(org_info, table_name, iceberg_commit))
    }

    pub async fn extension_commit(&mut self, org_info: &OrgInfo, table_name: &String, commit: &ExtensionCommit) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, extension_commit(org_info, table_name, commit))
    }

    pub async fn compaction_commit(&mut self, org_info: &OrgInfo, _table_name: &String, commit: &CompactionCommit) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, compaction_commit(org_info, _table_name, commit))
    }

    pub async fn cleanup_commit(&mut self, org_info: &OrgInfo, commit: &CleanupCommit) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, cleanup_commit(org_info, commit))
    }    

    pub async fn get_latest_committed_checkpoint(&mut self, org_info: &OrgInfo, table_name: &String, extensions: Option<String>) -> Result<Option<String>, ServiceImplError> {
        state_provider_func_impl!(self, get_latest_committed_checkpoint(org_info, table_name, extensions))
    }

    pub async fn get_checkpoint(&mut self, org_info: &OrgInfo, snapshot: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceImplError> {
        state_provider_func_impl!(self, get_checkpoint(org_info, snapshot))
    }

    pub async fn get_extension_work_items(&mut self, org_info: &OrgInfo, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceImplError> {
        state_provider_func_impl!(self, get_extension_work_items(org_info, extension_type))
    }

    pub async fn get_compaction_work_items(&mut self, org_info: &OrgInfo) -> Result<Vec<(String, CompactionWorkItem)>, ServiceImplError> {
        state_provider_func_impl!(self, get_compaction_work_items(org_info))
    }

    pub async fn get_cleanup_work_items(&mut self, org_info: &OrgInfo) -> Result<Vec<CleanupWorkItem>, ServiceImplError> {
        state_provider_func_impl!(self, get_cleanup_work_items(org_info))
    }    

    pub async fn create_org(&mut self, settings: &OrgSettings) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_org(settings))
    }

    pub async fn lookup_org(&mut self, access_key: &String, secret_key: &String) -> Result<Option<OrgInfo>, ServiceImplError> {
        state_provider_func_impl!(self, lookup_org(access_key, secret_key))
    }
}

#[derive(Clone)]
pub struct ServiceImplHandle {
    sender: mpsc::Sender<ServiceImplProviderActorMessage>,
}


macro_rules! send_message {
    ($self:expr, $message_type:tt) => {
        {
            let (send, recv) = oneshot::channel();
            let msg = ServiceImplProviderActorMessage::$message_type {
                respond_to: send,
            };
            let _ = $self.sender.send(msg).await;
            // TODO: deal with errors
            recv.await.expect("Actor task has been killed")
        }
    };

    ($self:expr, $message_type:tt, $field:ident = $value:expr) => {
        {
            let (send, recv) = oneshot::channel();
            let msg = ServiceImplProviderActorMessage::$message_type {
                respond_to: send,
                $field: $value
            };
            let _ = $self.sender.send(msg).await;
            // TODO: deal with errors
            recv.await.expect("Actor task has been killed")
        }
    };

    ($self:expr, $message_type:tt, $field1:ident = $value1:expr, $field2:ident = $value2:expr) => {
        {
            let (send, recv) = oneshot::channel();
            let _ = $self.sender.send(ServiceImplProviderActorMessage::$message_type {
                respond_to: send,
                $field1: $value1,
                $field2: $value2
            }).await;
            // TODO: deal with errors
            recv.await.expect("Actor task has been killed")
        }
    };

    ($self:expr, $message_type:tt, $field1:ident = $value1:expr, $field2:ident = $value2:expr, $field3:ident = $value3:expr) => {
        {
            let (send, recv) = oneshot::channel();
            let _ = $self.sender.send(ServiceImplProviderActorMessage::$message_type {
                respond_to: send,
                $field1: $value1,
                $field2: $value2,
                $field3: $value3,
            }).await;
            // TODO: deal with errors
            recv.await.expect("Actor task has been killed")
        }
    };

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


    pub async fn create_pipeline(&self, org_info: &OrgInfo, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceImplError> {
        send_message!(self, CreatePipeline, org_info = org_info.clone(), name = name.clone(), pipeline = pipeline.clone())
    }

    pub async fn describe_pipeline(&self, org_info: &OrgInfo, name: &String) -> Result<Option<PipelineDefinition>, ServiceImplError> {
        send_message!(self, DescribePipeline, org_info = org_info.clone(), name = name.clone())
    }

    pub async fn create_lifetime_policy(&self, org_info: &OrgInfo, name: &String, policy: &ILMPolicyDefinition) -> Result<(), ServiceImplError> {
        send_message!(self, CreateLifetimePolicy, org_info = org_info.clone(), name = name.clone(), policy = policy.clone())
    }

    pub async fn describe_lifetime_policy(&self, org_info: &OrgInfo, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceImplError> {
        send_message!(self, DescribeLifetimePolicy, org_info = org_info.clone(), name = name.clone())
    }

    pub async fn create_table(&self, org_info: &OrgInfo, create_table: &CreateTable) -> Result<(), ServiceImplError> {
        send_message!(self, CreateTable, org_info = org_info.clone(), create_table = create_table.clone())
    }

    pub async fn describe_table(&self, org_info: &OrgInfo, table_name: &String) -> Result<Option<TableDescription>, ServiceImplError> {
        send_message!(self, DescribeTable, org_info = org_info.clone(), name = table_name.clone())
    }

    pub async fn add_alias(&self, org_info: &OrgInfo, table_name: &String, alias: &String) -> Result<(), ServiceImplError> {
        send_message!(self, AddAlias, org_info = org_info.clone(), table_name = table_name.clone(), alias = alias.clone())
    }

    pub async fn remove_alias(&self, org_info: &OrgInfo, table_name: &String, alias: &String) -> Result<(), ServiceImplError> {
        send_message!(self, RemoveAlias, org_info = org_info.clone(), table_name = table_name.clone(), alias = alias.clone())
    }

    pub async fn create_table_template(&self, org_info: &OrgInfo, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceImplError> {
        send_message!(self, CreateTableTemplate, org_info = org_info.clone(), name = name.clone(), template = template.clone())
    }

    pub async fn describe_table_template(&self, org_info: &OrgInfo, table_name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceImplError> {
        send_message!(self, DescribeTableTemplate, org_info = org_info.clone(), name = table_name.clone())
    }

    #[allow(dead_code)]
    pub async fn add_checkpoint(&self, org_info: &OrgInfo, checkpoint: &TableMetadataCheckpoint) -> () {
        send_message!(self, AddCheckpoint, org_info = org_info.clone(), checkpoint = checkpoint.clone())
    }

    pub async fn iceberg_commit(&self, org_info: &OrgInfo, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceImplError> {
        send_message!(self, IcebergCommit, org_info = org_info.clone(), table_name = table_name.clone(), iceberg_commit = iceberg_commit.clone())
    }

    pub async fn speedboat_commit(&self, org_info: &OrgInfo, speedboat_commit: &SpeedboatCommit) -> Result<(), ServiceImplError> {
        send_message!(self, SpeedboatCommit, org_info = org_info.clone(), speedboat_commit = speedboat_commit.clone())
    }

    pub async fn extension_commit(&self, org_info: &OrgInfo, table_name: &String, extension_commit: &ExtensionCommit) -> Result<(), ServiceImplError> {
        send_message!(self, ExtensionCommit, org_info = org_info.clone(), table_name = table_name.clone(), extension_commit = extension_commit.clone())
    }

    pub async fn compaction_commit(&self, org_info: &OrgInfo, table_name: &String, compaction_commit: &CompactionCommit) -> Result<(), ServiceImplError> {
        send_message!(self, CompactionCommit, org_info = org_info.clone(), table_name = table_name.clone(), compaction_commit = compaction_commit.clone())
    }

    pub async fn cleanup_commit(&self, org_info: &OrgInfo, cleanup_commit: &CleanupCommit) -> Result<(), ServiceImplError> {
        send_message!(self, CleanupCommit, org_info = org_info.clone(), cleanup_commit = cleanup_commit.clone())
    }

    pub async fn get_latest_checkpoint(&self, org_info: &OrgInfo, table_name: &String, extension: Option<String>) -> Result<Option<String>, ServiceImplError> {
        send_message!(self, GetLatestCommittedCheckpoint, org_info = org_info.clone(), table_name = table_name.clone(), extensions = extension.clone())
    }

    pub async fn get_checkpoint(&self, org_info: &OrgInfo, checkpoint: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceImplError> {
        send_message!(self, GetCheckpoint, org_info = org_info.clone(), checkpoint = checkpoint.clone())
    }

    pub async fn get_extension_work_items(&self, org_info: &OrgInfo, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceImplError> {
        send_message!(self, GetExtensionWorkItems, org_info = org_info.clone(), extension_type = extension_type.clone())
    }

    pub async fn get_compaction_work_items(&self, org_info: &OrgInfo) -> Result<Vec<(String, CompactionWorkItem)>, ServiceImplError> {
        send_message!(self, GetCompactionWorkItems, org_info = org_info.clone())
    }

    pub async fn get_cleanup_work_items(&self, org_info: &OrgInfo) -> Result<Vec<CleanupWorkItem>, ServiceImplError> {
        send_message!(self, GetCleanupWorkItems, org_info = org_info.clone())
    }

    pub async fn create_org(&self, settings: &OrgSettings) -> Result<(), ServiceImplError> {
        send_message!(self, CreateOrg, settings = settings.clone())
    }

    pub async fn lookup_org(&self, access_key: &String, secret_key: &String) -> Result<Option<OrgInfo>, ServiceImplError> {
        send_message!(self, LookupOrg, access_key = access_key.clone(), secret_key = secret_key.clone())
    }
}

pub static SERVICE_IMPL: std::sync::LazyLock<ServiceImplHandle> = std::sync::LazyLock::new(|| ServiceImplHandle::new());
