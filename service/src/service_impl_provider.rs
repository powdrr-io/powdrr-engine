use std::{error::Error};
use std::fmt::{Display, Formatter};
use powdrr_lib::data_contract::{CreateIndexTemplateBody, ServiceImplType, ServiceMode};
use powdrr_lib::elastic_search_lifetime_policy::ILMPolicyDefinition;
use powdrr_lib::ephemeral_service_impl::EphemeralServiceImpl;
use powdrr_lib::pipeline::PipelineDefinition;
use powdrr_lib::data_contract::{CompactionCommit, CompactionWorkItem, CreateTable, ExtensionCommit, ExtensionWorkItem, IcebergCommit, SpeedboatCommit, TableDescription, TableMetadataCheckpoint};
use powdrr_lib::peers::CheckpointDescriptor;
use tokio::sync::{mpsc, oneshot};
use powdrr_lib::state_provider::ServiceApiError;
use crate::dynamodb_service_impl::DynamoDBServiceImpl;

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
        name: String,
        pipeline: PipelineDefinition,
    },
    DescribePipeline {
        respond_to: oneshot::Sender<Result<Option<PipelineDefinition>, ServiceImplError>>,
        name: String,
    },
    CreateLifetimePolicy {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        name: String,
        policy: ILMPolicyDefinition,
    },
    DescribeLifetimePolicy {
        respond_to: oneshot::Sender<Result<Option<ILMPolicyDefinition>, ServiceImplError>>,
        name: String,
    },
    CreateTable {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        create_table: CreateTable,
    },
    DescribeTable {
        respond_to: oneshot::Sender<Result<Option<TableDescription>, ServiceImplError>>,
        name: String,
    },
    AddAlias {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        table_name: String,
        alias: String,
    },
    RemoveAlias {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        table_name: String,
        alias: String,
    },
    CreateTableTemplate {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        name: String,
        template: CreateIndexTemplateBody,
    },
    DescribeTableTemplate {
        respond_to: oneshot::Sender<Result<Option<CreateIndexTemplateBody>, ServiceImplError>>,
        name: String,
    },
    AddCheckpoint {
        respond_to: oneshot::Sender<()>,
        checkpoint: TableMetadataCheckpoint,
    },
    IcebergCommit {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        table_name: String,
        iceberg_commit: IcebergCommit,
    },
    SpeedboatCommit {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        speedboat_commit: SpeedboatCommit,
    },
    ExtensionCommit {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        table_name: String,
        extension_commit: ExtensionCommit,
    },
    CompactionCommit {
        respond_to: oneshot::Sender<Result<(), ServiceImplError>>,
        table_name: String,
        compaction_commit: CompactionCommit,
    },
    GetLatestCommittedCheckpoint {
        respond_to: oneshot::Sender<Result<Option<String>, ServiceImplError>>,
        table_name: String,
        extensions: Option<String>,
    },
    GetCheckpoint {
        respond_to: oneshot::Sender<Result<Option<TableMetadataCheckpoint>, ServiceImplError>>,
        checkpoint: CheckpointDescriptor,
    },
    GetExtensionWorkItems {
        respond_to: oneshot::Sender<Result<Vec<ExtensionWorkItem>, ServiceImplError>>,
        extension_type: String,
    },
    GetCompactionWorkItems {
        respond_to: oneshot::Sender<Result<Vec<(String, CompactionWorkItem)>, ServiceImplError>>,
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
            service_impl: ServiceImpl::Ephemeral(EphemeralServiceImpl::new()),
            receiver,
        }
    }

    async fn handle_message(&mut self, msg: ServiceImplProviderActorMessage) -> () {
        match msg {
            ServiceImplProviderActorMessage::SetMode { respond_to, mode } => {
                self.service_impl = match mode.impl_type {
                    ServiceImplType::Ephemeral => ServiceImpl::Ephemeral(EphemeralServiceImpl::new()),
                    ServiceImplType::DynamoDb => ServiceImpl::DynamoDb(DynamoDBServiceImpl::new()),
                };
                respond_to.send(Ok(())).unwrap();
            }
            ServiceImplProviderActorMessage::CreatePipeline { respond_to, name, pipeline } => {
                handle_message_impl!(self, respond_to, create_pipeline(&name, &pipeline));
            },
            ServiceImplProviderActorMessage::DescribePipeline { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_pipeline(&name));
            },
            ServiceImplProviderActorMessage::CreateLifetimePolicy { respond_to, name, policy } => {
                handle_message_impl!(self, respond_to, create_lifetime_policy(&name, &policy));
            },
            ServiceImplProviderActorMessage::DescribeLifetimePolicy { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_lifetime_policy(&name));
            },
            ServiceImplProviderActorMessage::CreateTable { respond_to, create_table } => {
                handle_message_impl!(self, respond_to, create_table(&create_table));
            },
            ServiceImplProviderActorMessage::DescribeTable { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_table(&name));
            },
            ServiceImplProviderActorMessage::AddAlias { respond_to, table_name, alias } => {
                handle_message_impl!(self, respond_to, add_alias(&table_name, &alias));
            },
            ServiceImplProviderActorMessage::RemoveAlias { respond_to, table_name, alias } => {
                handle_message_impl!(self, respond_to, remove_alias(&table_name, &alias));
            },
            ServiceImplProviderActorMessage::CreateTableTemplate { respond_to, name, template } => {
                handle_message_impl!(self, respond_to, create_table_template(&name, &template));
            },
            ServiceImplProviderActorMessage::DescribeTableTemplate { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_table_template(&name));
            },
            ServiceImplProviderActorMessage::AddCheckpoint { checkpoint, respond_to } => {
                handle_message_impl!(self, respond_to, add_checkpoint(&checkpoint));
            },
            ServiceImplProviderActorMessage::IcebergCommit { respond_to, table_name, iceberg_commit } => {
                handle_message_impl!(self, respond_to, iceberg_commit(&table_name, &iceberg_commit));
            },
            ServiceImplProviderActorMessage::SpeedboatCommit { respond_to, speedboat_commit } => {
                handle_message_impl!(self, respond_to, speedboat_commit(&speedboat_commit));
            },
            ServiceImplProviderActorMessage::ExtensionCommit { respond_to, table_name, extension_commit } => {
                handle_message_impl!(self, respond_to, extension_commit(&table_name, &extension_commit));
            },
            ServiceImplProviderActorMessage::CompactionCommit { respond_to, table_name, compaction_commit } => {
                handle_message_impl!(self, respond_to, compaction_commit(&table_name, &compaction_commit));
            },
            ServiceImplProviderActorMessage::GetLatestCommittedCheckpoint { table_name, extensions, respond_to } => {
                handle_message_impl!(self, respond_to, get_latest_committed_checkpoint(&table_name, extensions));
            },
            ServiceImplProviderActorMessage::GetCheckpoint { checkpoint, respond_to } => {
                handle_message_impl!(self, respond_to, get_checkpoint(&checkpoint));
            },
            ServiceImplProviderActorMessage::GetExtensionWorkItems { extension_type, respond_to } => {
                handle_message_impl!(self, respond_to, get_extension_work_items(&extension_type));
            },
            ServiceImplProviderActorMessage::GetCompactionWorkItems { respond_to } => {
                handle_message_impl!(self, respond_to, get_compaction_work_items());
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
    async fn add_checkpoint(&mut self, checkpoint: &TableMetadataCheckpoint) -> () {
        state_provider_func_impl!(self, add_checkpoint(checkpoint)).unwrap()
    }

    #[allow(dead_code)]
    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceImplError> {
        state_provider_func_impl!(self, get_all_iceberg_tables())
    }

    pub async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_table(create_table))
    }

    pub async fn describe_table(&mut self, name: &String) -> Result<Option<TableDescription>, ServiceImplError> {
        state_provider_func_impl!(self, describe_table(name))
    }

    pub async fn add_alias(&mut self, table_name: &String, alias: &String) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, add_alias(table_name, alias))
    }

    pub async fn remove_alias(&mut self, table_name: &String, alias: &String) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, remove_alias(table_name, alias))
    }

    pub async fn create_table_template(&mut self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_table_template(name, template))
    }

    pub async fn describe_table_template(&mut self, name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceImplError> {
        state_provider_func_impl!(self, describe_table_template(name))
    }

    pub async fn create_pipeline(&mut self, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_pipeline(name, pipeline))
    }

    pub async fn describe_pipeline(&mut self, name: &String) -> Result<Option<PipelineDefinition>, ServiceImplError> {
        state_provider_func_impl!(self, describe_pipeline(name))
    }

    pub async fn create_lifetime_policy(&mut self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, create_lifetime_policy(name, policy))
    }

    pub async fn describe_lifetime_policy(&mut self, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceImplError> {
        state_provider_func_impl!(self, describe_lifetime_policy(name))
    }

    pub async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, speedboat_commit(commit))
    }

    pub async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, iceberg_commit(table_name, iceberg_commit))
    }

    pub async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, extension_commit(table_name, commit))
    }

    pub async fn compaction_commit(&mut self, _table_name: &String, commit: &CompactionCommit) -> Result<(), ServiceImplError> {
        state_provider_func_impl!(self, compaction_commit(_table_name, commit))
    }

    pub async fn get_latest_committed_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, ServiceImplError> {
        state_provider_func_impl!(self, get_latest_committed_checkpoint(table_name, extensions))
    }

    pub async fn get_checkpoint(&mut self, snapshot: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceImplError> {
        state_provider_func_impl!(self, get_checkpoint(snapshot))
    }

    pub async fn get_extension_work_items(&mut self, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceImplError> {
        state_provider_func_impl!(self, get_extension_work_items(extension_type))
    }

    pub async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, ServiceImplError> {
        state_provider_func_impl!(self, get_compaction_work_items())
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


    pub async fn create_pipeline(&self, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceImplError> {
        send_message!(self, CreatePipeline, name = name.clone(), pipeline = pipeline.clone())
    }

    pub async fn describe_pipeline(&self, name: &String) -> Result<Option<PipelineDefinition>, ServiceImplError> {
        send_message!(self, DescribePipeline, name = name.clone())
    }

    pub async fn create_lifetime_policy(&self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), ServiceImplError> {
        send_message!(self, CreateLifetimePolicy, name = name.clone(), policy = policy.clone())
    }

    pub async fn describe_lifetime_policy(&self, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceImplError> {
        send_message!(self, DescribeLifetimePolicy, name = name.clone())
    }

    pub async fn create_table(&self, create_table: &CreateTable) -> Result<(), ServiceImplError> {
        send_message!(self, CreateTable, create_table = create_table.clone())
    }

    pub async fn describe_table(&self, table_name: &String) -> Result<Option<TableDescription>, ServiceImplError> {
        send_message!(self, DescribeTable, name = table_name.clone())
    }

    pub async fn add_alias(&self, table_name: &String, alias: &String) -> Result<(), ServiceImplError> {
        send_message!(self, AddAlias, table_name = table_name.clone(), alias = alias.clone())
    }

    pub async fn remove_alias(&self, table_name: &String, alias: &String) -> Result<(), ServiceImplError> {
        send_message!(self, RemoveAlias, table_name = table_name.clone(), alias = alias.clone())
    }

    pub async fn create_table_template(&self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceImplError> {
        send_message!(self, CreateTableTemplate, name = name.clone(), template = template.clone())
    }

    pub async fn describe_table_template(&self, table_name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceImplError> {
        send_message!(self, DescribeTableTemplate, name = table_name.clone())
    }

    #[allow(dead_code)]
    pub async fn add_checkpoint(&self, checkpoint: &TableMetadataCheckpoint) -> () {
        send_message!(self, AddCheckpoint, checkpoint = checkpoint.clone())
    }

    pub async fn iceberg_commit(&self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceImplError> {
        send_message!(self, IcebergCommit, table_name = table_name.clone(), iceberg_commit = iceberg_commit.clone())
    }

    pub async fn speedboat_commit(&self, speedboat_commit: &SpeedboatCommit) -> Result<(), ServiceImplError> {
        send_message!(self, SpeedboatCommit, speedboat_commit = speedboat_commit.clone())
    }

    pub async fn extension_commit(&self, table_name: &String, extension_commit: &ExtensionCommit) -> Result<(), ServiceImplError> {
        send_message!(self, ExtensionCommit, table_name = table_name.clone(), extension_commit = extension_commit.clone())
    }

    pub async fn compaction_commit(&self, table_name: &String, compaction_commit: &CompactionCommit) -> Result<(), ServiceImplError> {
        send_message!(self, CompactionCommit, table_name = table_name.clone(), compaction_commit = compaction_commit.clone())
    }

    pub async fn get_latest_checkpoint(&self, table_name: &String, extension: Option<String>) -> Result<Option<String>, ServiceImplError> {
        send_message!(self, GetLatestCommittedCheckpoint, table_name = table_name.clone(), extensions = extension.clone())
    }

    pub async fn get_checkpoint(&self, checkpoint: CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceImplError> {
        send_message!(self, GetCheckpoint, checkpoint = checkpoint.clone())
    }

    pub async fn get_extension_work_items(&self, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceImplError> {
        send_message!(self, GetExtensionWorkItems, extension_type = extension_type.clone())
    }

    pub async fn get_compaction_work_items(&self) -> Result<Vec<(String, CompactionWorkItem)>, ServiceImplError> {
        send_message!(self, GetCompactionWorkItems)
    }

}

pub static SERVICE_IMPL: std::sync::LazyLock<ServiceImplHandle> = std::sync::LazyLock::new(|| ServiceImplHandle::new());
