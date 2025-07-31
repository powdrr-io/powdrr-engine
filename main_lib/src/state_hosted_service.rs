use std::{collections::HashMap, error::Error};
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::{elastic_search_ingest::CreateIndexTemplateBody, pipeline::PipelineDefinition, state_peers::CheckpointDescriptor};

use crate::elastic_search_index::IndexError;
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::ephemeral_state_provider::EphemeralStateProvider;
use crate::leaderless_state_provider::LeaderlessStateProvider;
use crate::schema_massager::PowdrrSchema;
use crate::state_peers::{PeerClient};
use crate::test_api::{TestProcessingMode};


#[derive(Debug, Clone)]
pub struct ServiceApiError {
    pub(crate) message: String,
}

impl Error for ServiceApiError {}

impl Display for ServiceApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

unsafe impl Send for ServiceApiError {}
unsafe impl Sync for ServiceApiError {}


impl ServiceApiError {
    pub(crate) fn new(message: String) -> Self {
        assert!(message.len() > 0, "Message must not be empty");
        ServiceApiError {
            message,
        }
    }

    pub(crate) fn from_index_error(index_error: &IndexError) -> Self {
        Self::new(format!("Index Error: {}", index_error))
    }

    pub fn from_reqwest(error: reqwest::Error) -> Self {
        Self::new(format!("Reqwest: {}", error))
    }
    
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpeedboatCommitTableInfo {
    pub commit_type: String,
    pub table_name: String,
    pub files: Vec<String>,
    pub sizes: Vec<u64>,
    pub schema: Option<PowdrrSchema>,
}

impl SpeedboatCommitTableInfo {
    pub(crate) fn as_file_set_payload(&self) -> FileSetPayload {
        FileSetPayload {
            file_paths: self.files.clone(),
            sizes: self.sizes.clone(),
            schemas: vec!(self.schema.as_ref().unwrap().clone()),
            file_schemas: self.files.iter().map(|_| 0).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GetLatestCheckpoint {
    pub table_name: String,
    pub extension: Option<String>,
}


#[derive(Serialize, Deserialize, Clone)]
pub struct SpeedboatCommit {
    pub type_files: Vec<SpeedboatCommitTableInfo>,
    pub compactions: Vec<String>,    
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileSetPayload {
    pub file_paths: Vec<String>,
    pub schemas: Vec<PowdrrSchema>,
    pub file_schemas: Vec<u64>,
    pub sizes: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct FileDescriptor {
    pub file_path: String,
    pub schema: PowdrrSchema,
    pub size: u64,
}


#[derive(Serialize, Deserialize, Clone)]
pub struct IcebergMetadata {
    pub table_schema: PowdrrSchema,
    pub snapshot_id: String,
    pub files: FileSetPayload,
    pub column_names: Vec<String>,
    // per file, per column lower and upper bounds
    // TODO: this needs to be generalized to support bloom filters
    pub column_stats: Vec<(String, String)>,
}


#[derive(Serialize, Deserialize, Clone)]
pub struct IcebergCommit {
    pub metadata: IcebergMetadata,
    pub compactions: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SpeedboatMetadata {
    pub files: FileSetPayload
}


#[derive(Serialize, Deserialize, Clone)]
pub struct DeletesMetadata {
    pub files: Vec<String>,
}


#[derive(Serialize, Deserialize, Clone)]
pub struct ExtensionFile {
    pub suffix: String,
    pub location: String,
}


pub type ExtensionFileMetadata = HashMap<String, Vec<ExtensionFile>>;


#[derive(Serialize, Deserialize, Clone)]
pub struct ExtensionCommit {
    pub extension: String,
    pub files: ExtensionFileMetadata
}


#[derive(Serialize, Deserialize, Clone)]
pub struct CompactionCommit {
    pub removed_speedboat_files: Vec<String>,
    pub removed_delete_files: Vec<String>,
    pub compaction_id: String
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TableMetadataCheckpoint {
    pub table_name: String,
    pub checkpoint_id: String,
    pub iceberg_metadata: Option<IcebergMetadata>,
    pub speedboat_metadata: Option<SpeedboatMetadata>,
    pub deletes_metadata: Option<DeletesMetadata>,
    pub extension_metadata: HashMap<String, HashMap<String, Vec<ExtensionFile>>>,
    pub schema: PowdrrSchema,
}


impl TableMetadataCheckpoint {
    pub(crate) fn fully_covered_for_extension(&self, extension_name: &String) -> bool {
        let total_num_files =
            self.speedboat_metadata.as_ref().map_or(0, |x| x.files.file_paths.len()) +
            self.iceberg_metadata.as_ref().map_or(0, |x| x.files.file_paths.len());

        let total_num_extension_files = self.extension_metadata.get(extension_name).map_or(0, |x| x.len());

        let size_check_method = total_num_files == total_num_extension_files;

        assert_eq!(size_check_method, self.validate_fully_covered_for_extension(extension_name));

        size_check_method
    }

    fn validate_fully_covered_for_extension(&self, extension_name: &String) -> bool {
        let extension_metadata_map = self.extension_metadata.get(extension_name).map_or(HashMap::new(), |x| x.clone());

        match &self.iceberg_metadata {
            Some(im) => {
                for file_path in im.files.file_paths.iter() {
                    if !extension_metadata_map.contains_key(file_path) {
                        return false;
                    }
                }
            },
            None => {}
        };

        match &self.speedboat_metadata {
            Some(im) => {
                for file_path in im.files.file_paths.iter() {
                    if !extension_metadata_map.contains_key(file_path) {
                        return false;
                    }
                }
            },
            None => {}
        };

        true
    }

    pub(crate) fn add_coverage(&mut self, extension_commit: &ExtensionCommit) -> () {
        assert!(!self.fully_covered_for_extension(&extension_commit.extension), "Already fully covered");

        let existing_extension_metadata_map = self.extension_metadata.get(&extension_commit.extension).map_or(HashMap::new(), |x| x.clone());

        if !self.extension_metadata.contains_key(&extension_commit.extension) {
            self.extension_metadata.insert(extension_commit.extension.clone(), HashMap::new());
        }

        match &self.iceberg_metadata {
            Some(im) => {
                for file_path in im.files.file_paths.iter() {
                    if extension_commit.files.contains_key(file_path) && !existing_extension_metadata_map.contains_key(file_path) {
                        self.extension_metadata.get_mut(&extension_commit.extension).unwrap().insert(file_path.clone(), extension_commit.files[file_path].clone());
                    }
                }
            },
            None => {}
        };

        match &self.speedboat_metadata {
            Some(im) => {
                for file_path in im.files.file_paths.iter() {
                    if extension_commit.files.contains_key(file_path) && !existing_extension_metadata_map.contains_key(file_path) {
                        self.extension_metadata.get_mut(&extension_commit.extension).unwrap().insert(file_path.clone(), extension_commit.files[file_path].clone());

                    }
                }
            },
            None => {}
        };
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ProposedCompaction {
    pub table_name: String,
    pub checkpoint_id: String,
    pub iceberg_metadata: Option<IcebergMetadata>,
    pub speedboat_metadata: Option<SpeedboatMetadata>,
    pub extension_metadata: Option<Vec<(String, Vec<ExtensionFileMetadata>)>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateTable {
    pub name: String,
    pub tags: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TableDescription {
    pub name: String,
    pub tags: HashMap<String, String>
}

impl TableDescription {
    pub(crate) fn from_create_table(create_table: &CreateTable) -> Self {
        TableDescription {
            name: create_table.name.clone(),
            tags: create_table.tags.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AddAlias {
    pub table_name: String,
    pub alias: String,
}


impl FileSetPayload {
    pub fn new() -> Self {
        FileSetPayload {
            file_paths: vec!(),
            sizes: vec!(),
            file_schemas: vec!(),
            schemas: vec!(),
        }
    }

    #[cfg(test)]
    pub fn single(file_path: String, size: u64, schema: PowdrrSchema) -> Self {
        FileSetPayload {
            file_paths: vec!(file_path),
            sizes: vec!(size),
            file_schemas: vec!(0),
            schemas: vec!(schema),
        }
    }

    pub fn len(&self) -> usize {
        self.file_paths.len()
    }

    pub fn clear(&mut self) -> () {
        self.file_paths.clear();
        self.sizes.clear();
        self.file_schemas.clear();
        self.schemas.clear();
    }

    pub fn remove(&mut self, file_paths_to_remove: &Vec<String>) -> () {
        let mut i = 0;
        while i < self.file_paths.len() {
            let file_name = self.file_paths.get(i).unwrap();
            if file_paths_to_remove.contains(file_name) {
                self.file_paths.remove(i);
                self.sizes.remove(i);
                self.file_schemas.remove(i);
            } else {
                i += 1;
            }
        }

        // TODO: find dangling schemas and remove them
    }

    pub fn as_file_tuples(&self) -> Vec<FileDescriptor> {
        self.file_paths.iter().zip(self.sizes.iter()).zip(self.file_schemas.iter()).map(
            |((path, size), schema_index)|
                FileDescriptor{ file_path: path.clone(), schema: self.schemas[*schema_index as usize].clone(), size: *size }
        ).collect()
    }

    fn selected_file(file_path: &String, index: u64, num: u64) -> bool {
        // TODO: validate this is a stable hash (aka it will give the same value on every machine on every run)
        let mut hasher = DefaultHasher::new();
        file_path.hash(&mut hasher);
        let hash_val = hasher.finish();
        hash_val % num == index
    }

    pub fn as_selected_tuples(&self, index: u64, num: u64) -> Vec<FileDescriptor> {
        self.as_file_tuples().iter().filter(|x|Self::selected_file(&x.file_path, index, num)).cloned().collect()
    }

    pub fn merge(&self, other: &FileSetPayload) -> Self {
        // Clone the larger one, merge in the smaller one
        let (mut cloned, to_merge) = if self.file_paths.len() > other.file_paths.len() {
            (self.clone(), other)
        } else {
            (other.clone(), self)
        };

        for file_desc in to_merge.as_file_tuples().iter() {
            cloned.add(file_desc);
        }

        cloned
    }

    pub(crate) fn add(&mut self, file_descriptor: &FileDescriptor) -> () {
        if self.file_paths.contains(&file_descriptor.file_path) {
            return;
        }
        self.file_paths.push(file_descriptor.file_path.clone());
        self.sizes.push(file_descriptor.size);
        if let Some(index) = self.schemas.iter().position(|item| item == &file_descriptor.schema) {
            self.file_schemas.push(index as u64);
        } else {
            self.file_schemas.push(self.schemas.len() as u64);
            self.schemas.push(file_descriptor.schema.clone());
        }
    }
}

enum ApiServiceClientActorMessage {
    Testing {
        respond_to: oneshot::Sender<()>,
        mode: TestProcessingMode,
    },
    CreatePipeline {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        name: String,
        pipeline: PipelineDefinition,
    },
    DescribePipeline {
        respond_to: oneshot::Sender<Result<Option<PipelineDefinition>, ServiceApiError>>,
        name: String,
    },
    CreateLifetimePolicy {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        name: String,
        policy: ILMPolicyDefinition,
    },
    DescribeLifetimePolicy {
        respond_to: oneshot::Sender<Result<Option<ILMPolicyDefinition>, ServiceApiError>>,
        name: String,
    },
    CreateTable {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        create_table: CreateTable,
    },
    DescribeTable {
        respond_to: oneshot::Sender<Result<Option<TableDescription>, ServiceApiError>>,
        name: String,
    },
    AddAlias {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        table_name: String,
        alias: String,
    },
    RemoveAlias {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        table_name: String,
        alias: String,
    },         
    CreateTableTemplate {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
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
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        table_name: String,
        iceberg_commit: IcebergCommit,
    },
    SpeedboatCommit {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        speedboat_commit: SpeedboatCommit,
    },
    ExtensionCommit {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        table_name: String,
        extension_commit: ExtensionCommit,        
    },
    CompactionCommit {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        table_name: String,
        compaction_commit: CompactionCommit,        
    },    
    GetLatestCommittedCheckpoint {
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
    GetExtensionWorkItems {
        respond_to: oneshot::Sender<Result<Vec<ExtensionWorkItem>, ServiceApiError>>,
        extension_type: String,
    },
    GetCompactionWorkItems {
        respond_to: oneshot::Sender<Result<Vec<(String, CompactionWorkItem)>, ServiceApiError>>,
    },
    GetPeerClients {
        respond_to: oneshot::Sender<Vec<Box<dyn PeerClient>>>,
    },
    GetNextPrefetchCheckpoints {
        respond_to: oneshot::Sender<Result<Vec<CheckpointDescriptor>, ServiceApiError>>,
        extensions: Option<String>,
    },
    SetPrefetchCheckpoints {
        respond_to: oneshot::Sender<Result<(), ServiceApiError>>,
        checkpoints: Vec<CheckpointDescriptor>,
        extensions: Option<String>,
    },
}

unsafe impl Send for ApiServiceClientActorMessage {}


struct ApiServiceClientActor {
    state_provider: StateProvider,
    receiver: mpsc::Receiver<ApiServiceClientActorMessage>,
}


macro_rules! handle_message_impl {
    ($self:expr, $respond_to:expr, $func:ident($($args:expr),*)) => {
        let _ = $respond_to.send($self.state_provider.$func($($args),*).await);
    };
}

impl ApiServiceClientActor {
    fn new(receiver: mpsc::Receiver<ApiServiceClientActorMessage>, _base_address: String) -> Self {
        ApiServiceClientActor {
            state_provider: StateProvider::Ephemeral(EphemeralStateProvider::new()),
            receiver,
        }
    }

    async fn handle_message(&mut self, msg: ApiServiceClientActorMessage) -> () {
        match msg {
            ApiServiceClientActorMessage::Testing { respond_to, mode } => {
                handle_message_impl!(self, respond_to, clear_and_set(mode));
            },
            ApiServiceClientActorMessage::CreatePipeline { respond_to, name, pipeline } => {
                handle_message_impl!(self, respond_to, create_pipeline(&name, &pipeline));
            },
            ApiServiceClientActorMessage::DescribePipeline { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_pipeline(&name));
            },
            ApiServiceClientActorMessage::CreateLifetimePolicy { respond_to, name, policy } => {
                handle_message_impl!(self, respond_to, create_lifetime_policy(&name, &policy));
            },
            ApiServiceClientActorMessage::DescribeLifetimePolicy { respond_to, name } => {
                    handle_message_impl!(self, respond_to, describe_lifetime_policy(&name));
            },
            ApiServiceClientActorMessage::CreateTable { respond_to, create_table } => {
                handle_message_impl!(self, respond_to, create_table(&create_table));
            },
            ApiServiceClientActorMessage::DescribeTable { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_table(&name));
            },
            ApiServiceClientActorMessage::AddAlias { respond_to, table_name, alias } => {
                handle_message_impl!(self, respond_to, add_alias(&table_name, &alias));
            },
            ApiServiceClientActorMessage::RemoveAlias { respond_to, table_name, alias } => {
                handle_message_impl!(self, respond_to, remove_alias(&table_name, &alias));
            },            
            ApiServiceClientActorMessage::CreateTableTemplate { respond_to, name, template } => {
                handle_message_impl!(self, respond_to, create_table_template(&name, &template));
            },
            ApiServiceClientActorMessage::DescribeTableTemplate { respond_to, name } => {
                handle_message_impl!(self, respond_to, describe_table_template(&name));
            },                       
            ApiServiceClientActorMessage::AddCheckpoint { checkpoint, respond_to } => {
                handle_message_impl!(self, respond_to, add_checkpoint(&checkpoint));
            },
            ApiServiceClientActorMessage::IcebergCommit { respond_to, table_name, iceberg_commit } => {
                handle_message_impl!(self, respond_to, iceberg_commit(&table_name, &iceberg_commit));
            },            
            ApiServiceClientActorMessage::SpeedboatCommit { respond_to, speedboat_commit } => {
                handle_message_impl!(self, respond_to, speedboat_commit(&speedboat_commit));
            },
            ApiServiceClientActorMessage::ExtensionCommit { respond_to, table_name, extension_commit } => {
                handle_message_impl!(self, respond_to, extension_commit(&table_name, &extension_commit));
            }, 
            ApiServiceClientActorMessage::CompactionCommit { respond_to, table_name, compaction_commit } => {
                handle_message_impl!(self, respond_to, compaction_commit(&table_name, &compaction_commit));
            },                        
            ApiServiceClientActorMessage::GetLatestCommittedCheckpoint { table_name, extensions, respond_to } => {
                handle_message_impl!(self, respond_to, get_latest_committed_checkpoint(&table_name, extensions));
            },
            ApiServiceClientActorMessage::GetLatestTargetCheckpoint { table_name, extensions, respond_to } => {
                handle_message_impl!(self, respond_to, get_latest_target_checkpoint(&table_name, extensions));
            },
            ApiServiceClientActorMessage::GetCheckpoint { checkpoint, respond_to } => {
                handle_message_impl!(self, respond_to, get_checkpoint(&checkpoint));
            },
            ApiServiceClientActorMessage::GetExtensionWorkItems { extension_type, respond_to } => {
                handle_message_impl!(self, respond_to, get_extension_work_items(&extension_type));
            },
            ApiServiceClientActorMessage::GetCompactionWorkItems { respond_to } => {
                handle_message_impl!(self, respond_to, get_compaction_work_items());
            },
            ApiServiceClientActorMessage::GetPeerClients { respond_to } => {
                handle_message_impl!(self, respond_to, get_peer_clients());
            },
            ApiServiceClientActorMessage::GetNextPrefetchCheckpoints { respond_to, extensions } => {
                handle_message_impl!(self, respond_to, get_next_prefetch_checkpoints(extensions));
            },
            ApiServiceClientActorMessage::SetPrefetchCheckpoints { respond_to, checkpoints, extensions } => {
                handle_message_impl!(self, respond_to, set_prefetch_checkpoints(&checkpoints, extensions));
            },
        }
    }
}


async fn run_api_service_client_actor_message_pump(mut actor: ApiServiceClientActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg).await;
    }
}


enum StateProvider {
    Ephemeral(EphemeralStateProvider),
    #[allow(dead_code)]
    Leaderless(LeaderlessStateProvider)
}

macro_rules! state_provider_func_impl {
    ($self:expr, $func:ident($($args:tt),*)) => {
        match $self {
            StateProvider::Ephemeral(eph) => eph.$func($($args),*).await,
            StateProvider::Leaderless(lead) => lead.$func($($args),*).await,
        }
    };
}


impl StateProvider {
    async fn clear_and_set(&mut self, mode: TestProcessingMode) -> () {
        state_provider_func_impl!(self, clear_and_set(mode))
    }

    pub(crate) async fn set_prefetch_checkpoints(&self, descriptors: &Vec<CheckpointDescriptor>, extension: Option<String>) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, set_prefetch_checkpoints(descriptors, extension))
    }

    async fn get_latest_target_checkpoint(&self, table_name: &String, extension: Option<String>) -> Result<Option<String>, ServiceApiError> {
        state_provider_func_impl!(self, get_latest_target_checkpoint(table_name, extension))
    }

    async fn add_checkpoint(&mut self, checkpoint: &TableMetadataCheckpoint) -> () {
        state_provider_func_impl!(self, add_checkpoint(checkpoint))
    }

    #[allow(dead_code)]
    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        state_provider_func_impl!(self, get_all_iceberg_tables())
    }

    pub async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, create_table(create_table))
    }

    pub async fn describe_table(&mut self, name: &String) -> Result<Option<TableDescription>, ServiceApiError> {
        state_provider_func_impl!(self, describe_table(name))
    }

    pub async fn add_alias(&mut self, table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, add_alias(table_name, alias))
    }

    pub async fn remove_alias(&mut self, table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, remove_alias(table_name, alias))
    }

    pub async fn create_table_template(&mut self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, create_table_template(name, template))
    }

    pub async fn describe_table_template(&mut self, name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        state_provider_func_impl!(self, describe_table_template(name))
    }

    pub async fn create_pipeline(&mut self, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, create_pipeline(name, pipeline))
    }

    pub async fn describe_pipeline(&mut self, name: &String) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        state_provider_func_impl!(self, describe_pipeline(name))
    }

    pub async fn create_lifetime_policy(&mut self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, create_lifetime_policy(name, policy))
    }

    pub async fn describe_lifetime_policy(&mut self, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        state_provider_func_impl!(self, describe_lifetime_policy(name))
    }

    pub async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, speedboat_commit(commit))
    }

    pub async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, iceberg_commit(table_name, iceberg_commit))
    }

    pub async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, extension_commit(table_name, commit))
    }

    pub async fn compaction_commit(&mut self, _table_name: &String, commit: &CompactionCommit) -> Result<(), ServiceApiError> {
        state_provider_func_impl!(self, compaction_commit(_table_name, commit))
    }

    pub async fn get_latest_committed_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, ServiceApiError> {
        state_provider_func_impl!(self, get_latest_committed_checkpoint(table_name, extensions))
    }

    pub async fn get_checkpoint(&mut self, snapshot: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        state_provider_func_impl!(self, get_checkpoint(snapshot))
    }

    pub async fn get_extension_work_items(&mut self, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        state_provider_func_impl!(self, get_extension_work_items(extension_type))
    }

    pub async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        state_provider_func_impl!(self, get_compaction_work_items())
    }

    pub async fn get_peer_clients(&mut self) -> Vec<Box<dyn PeerClient>> {
        state_provider_func_impl!(self, get_peer_clients())
    }

    pub async fn get_next_prefetch_checkpoints(&mut self, extensions: Option<String>) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        state_provider_func_impl!(self, get_next_prefetch_checkpoints(extensions))
    }
}


#[derive(Serialize, Deserialize, Clone)]
pub struct ExtensionWorkItem {
    pub extension_type: String,
    pub table_name: String,
    pub table_schema: PowdrrSchema,
    pub speedboat_files: FileSetPayload,
    pub iceberg_files: FileSetPayload,
}

impl ExtensionWorkItem {
    pub(crate) fn clear(&mut self) -> () {
        self.speedboat_files.clear();
        self.iceberg_files.clear();
    }

    pub(crate) fn has_work(&self) -> bool {
        self.speedboat_files.len() > 0 || self.iceberg_files.len() > 0
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompactionWorkItem {
    pub table_schema: PowdrrSchema,
    pub speedboat_files: FileSetPayload,
    pub delete_files: Vec<String>,
}


#[derive(Clone)]
pub struct ApiServiceClientHandle {
    sender: mpsc::Sender<ApiServiceClientActorMessage>,
}


macro_rules! send_message {
    ($self:expr, $message_type:tt) => {
        {
            let (send, recv) = oneshot::channel();
            let msg = ApiServiceClientActorMessage::$message_type {
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
            let msg = ApiServiceClientActorMessage::$message_type {
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
            let _ = $self.sender.send(ApiServiceClientActorMessage::$message_type {
                respond_to: send,
                $field1: $value1,
                $field2: $value2
            }).await;
            // TODO: deal with errors
            recv.await.expect("Actor task has been killed")
        }
    };

}

impl ApiServiceClientHandle {
    pub fn new(address: String) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let actor = ApiServiceClientActor::new(receiver, address);
        tokio::spawn(run_api_service_client_actor_message_pump(actor));

        Self { sender }
    }

    pub async fn set_testing_mode(&self, mode: &TestProcessingMode) -> () {
        send_message!(self, Testing, mode = mode.clone());
    }

    pub async fn create_pipeline(&self, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceApiError> {
        send_message!(self, CreatePipeline, name = name.clone(), pipeline = pipeline.clone())
    }

    pub async fn describe_pipeline(&self, name: &String) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        send_message!(self, DescribePipeline, name = name.clone())
    }

    pub async fn create_lifetime_policy(&self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), ServiceApiError> {
        send_message!(self, CreateLifetimePolicy, name = name.clone(), policy = policy.clone())
    }

    pub async fn describe_lifetime_policy(&self, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        send_message!(self, DescribeLifetimePolicy, name = name.clone())
    }

    pub async fn create_table(&self, create_table: &CreateTable) -> Result<(), ServiceApiError> {
        send_message!(self, CreateTable, create_table = create_table.clone())
    }  

    pub async fn describe_table(&self, table_name: &String) -> Result<Option<TableDescription>, ServiceApiError> {
        send_message!(self, DescribeTable, name = table_name.clone())
    } 

    pub async fn add_alias(&self, table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        send_message!(self, AddAlias, table_name = table_name.clone(), alias = alias.clone())
    }  

    pub async fn remove_alias(&self, table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        send_message!(self, RemoveAlias, table_name = table_name.clone(), alias = alias.clone())
    }         

    pub async fn create_table_template(&self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceApiError> {
        send_message!(self, CreateTableTemplate, name = name.clone(), template = template.clone())
    }  

    pub async fn describe_table_template(&self, table_name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        send_message!(self, DescribeTableTemplate, name = table_name.clone())
    }     

    pub async fn add_checkpoint(&self, checkpoint: &TableMetadataCheckpoint) -> () {
        send_message!(self, AddCheckpoint, checkpoint = checkpoint.clone())
    }

    pub async fn iceberg_commit(&self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceApiError> {
        send_message!(self, IcebergCommit, table_name = table_name.clone(), iceberg_commit = iceberg_commit.clone())
    }

    pub async fn speedboat_commit(&self, speedboat_commit: &SpeedboatCommit) -> Result<(), ServiceApiError> {
        send_message!(self, SpeedboatCommit, speedboat_commit = speedboat_commit.clone())
    }

    pub async fn extension_commit(&self, table_name: &String, extension_commit: &ExtensionCommit) -> Result<(), ServiceApiError> {
        send_message!(self, ExtensionCommit, table_name = table_name.clone(), extension_commit = extension_commit.clone())
    }

    pub async fn compaction_commit(&self, table_name: &String, compaction_commit: &CompactionCommit) -> Result<(), ServiceApiError> {
        send_message!(self, CompactionCommit, table_name = table_name.clone(), compaction_commit = compaction_commit.clone())
    }

    pub async fn get_latest_checkpoint(&self, table_name: &String, extension: Option<String>) -> Result<Option<String>, ServiceApiError> {
        send_message!(self, GetLatestCommittedCheckpoint, table_name = table_name.clone(), extensions = extension.clone())
    }

    pub async fn get_checkpoint(&self, checkpoint: CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        send_message!(self, GetCheckpoint, checkpoint = checkpoint.clone())
    }

    pub async fn get_extension_work_items(&self, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        send_message!(self, GetExtensionWorkItems, extension_type = extension_type.clone())
    }

    pub async fn get_compaction_work_items(&self) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        send_message!(self, GetCompactionWorkItems)
    }

    pub async fn get_peer_clients(&self) -> Vec<Box<dyn PeerClient>> {
        send_message!(self, GetPeerClients)
    }

    pub async fn get_latest_target_checkpoint(&self, table_name: &String, extension: Option<String>) -> Result<Option<String>, ServiceApiError> {
        send_message!(self, GetLatestTargetCheckpoint, table_name = table_name.clone(), extensions = extension.clone())
    }

    pub async fn get_next_prefetch_checkpoints(&self, extension: Option<String>) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        send_message!(self, GetNextPrefetchCheckpoints, extensions = extension.clone())
    }

    pub async fn set_prefetch_checkpoints(&self, checkpoints: &Vec<CheckpointDescriptor>, extension: Option<String>) -> Result<(), ServiceApiError> {
        send_message!(self, SetPrefetchCheckpoints, checkpoints = checkpoints.clone(), extensions = extension.clone())
    }
}

pub static API_SERVICE_CLIENT: std::sync::LazyLock<ApiServiceClientHandle> = std::sync::LazyLock::new(|| ApiServiceClientHandle::new("http://localhost:7784".to_string()));
