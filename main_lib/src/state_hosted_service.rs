use std::{collections::HashMap, error::Error};
use std::hash::{DefaultHasher, Hash, Hasher};
use async_trait::async_trait;
use idgenerator::IdInstance;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot::{self, error::RecvError}};

use crate::{distributed_cache, elastic_search_ingest::CreateIndexTemplateBody, pipeline::PipelineDefinition, state_peers::CheckpointDescriptor};
use crate::compaction::drop_all_tables;
use crate::elastic_search_index::create_index;
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::schema_massager::PowdrrSchema;
use crate::state_peers::{PeerClient, SelfPeer};
use crate::test_api::{IndexingMode, PrefetchMode, TestProcessingMode};

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SpeedboatCSpeedInfo {
    pub table_name: String,
    pub files: Vec<String>,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct SpeedboatCommitTableInfo {
    pub commit_type: String,
    pub table_name: String,
    pub files: Vec<String>,
    pub sizes: Vec<u64>,
    pub schema: Option<PowdrrSchema>,
}

impl SpeedboatCommitTableInfo {
    fn as_file_set_payload(&self) -> FileSetPayload {
        FileSetPayload {
            file_paths: self.files.clone(),
            sizes: self.sizes.clone(),
            schemas: vec!(self.schema.as_ref().unwrap().clone()),
            file_schemas: self.files.iter().map(|_| 0).collect(),
        }
    }
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SpeedboatCommit {
    pub type_files: Vec<SpeedboatCommitTableInfo>,
    pub compactions: Vec<String>,    
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct FileSetPayload {
    pub file_paths: Vec<String>,
    pub schemas: Vec<PowdrrSchema>,
    pub file_schemas: Vec<u64>,
    pub sizes: Vec<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct FileDescriptor {
    pub(crate) file_path: String,
    pub(crate) schema: PowdrrSchema,
    pub(crate) size: u64,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct IcebergMetadata {
    pub table_schema: PowdrrSchema,
    pub snapshot_id: String,
    pub files: FileSetPayload,
    pub column_names: Vec<String>,
    // per file, per column lower and upper bounds
    // TODO: this needs to be generalized to support bloom filters
    pub column_stats: Vec<(String, String)>,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct IcebergCommit {
    pub metadata: IcebergMetadata,
    pub compactions: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SpeedboatMetadata {
    pub files: FileSetPayload
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct DeletesMetadata {
    pub files: Vec<String>,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ExtensionFile {
    pub suffix: String,
    pub location: String,
}


pub type ExtensionFileMetadata = HashMap<String, Vec<ExtensionFile>>;


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ExtensionCommit {
    pub extension: String,
    pub files: ExtensionFileMetadata
}


#[derive(Serialize, Clone)]
pub(crate) struct CompactionCommit {
    pub removed_speedboat_files: Vec<String>,
    pub removed_delete_files: Vec<String>,
    pub compaction_id: String
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct TableMetadataCheckpoint {
    pub table_name: String,
    pub checkpoint_id: String,
    pub iceberg_metadata: Option<IcebergMetadata>,
    pub speedboat_metadata: Option<SpeedboatMetadata>,
    pub deletes_metadata: Option<DeletesMetadata>,
    pub extension_metadata: HashMap<String, HashMap<String, Vec<ExtensionFile>>>,
    pub schema: PowdrrSchema,
}


impl TableMetadataCheckpoint {
    fn fully_covered_for_extension(&self, extension_name: &String) -> bool {
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

    fn add_coverage(&mut self, extension_commit: &ExtensionCommit) -> () {
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

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct CreateTable {
    pub name: String,
    pub tags: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct TableDescription {
    pub name: String,
    pub tags: HashMap<String, String>
}

impl TableDescription {
    fn from_create_table(create_table: &CreateTable) -> Self {
        TableDescription {
            name: create_table.name.clone(),
            tags: create_table.tags.clone(),
        }
    }
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

    fn add(&mut self, file_descriptor: &FileDescriptor) -> () {
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



#[async_trait]
pub(crate) trait ApiServiceClient : Send + Sync {
    #[allow(dead_code)]
    async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, Box<dyn Error>>;

    async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), Box<dyn Error>>;

    async fn describe_table(&mut self, name: &String) -> Result<Option<TableDescription>, Box<dyn Error>>;
 
    async fn add_alias(&mut self, table_name: &String, alias: &String) -> Result<(), Box<dyn Error>>;

    async fn remove_alias(&mut self, table_name: &String, alias: &String) -> Result<(), Box<dyn Error>>;

    async fn create_table_template(&mut self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), Box<dyn Error>>;

    async fn describe_table_template(&mut self, name: &String) -> Result<Option<CreateIndexTemplateBody>, Box<dyn Error>>;

    async fn create_pipeline(&mut self, name: &String, pipeline: &PipelineDefinition) -> Result<(), Box<dyn Error>>;

    async fn describe_pipeline(&mut self, name: &String) -> Result<Option<PipelineDefinition>, Box<dyn Error>>;

    async fn create_lifetime_policy(&mut self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), Box<dyn Error>>;

    async fn describe_lifetime_policy(&mut self, name: &String) -> Result<Option<ILMPolicyDefinition>, Box<dyn Error>>;
    
    async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), Box<dyn Error>>;

    async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), Box<dyn Error>>;

    async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), Box<dyn Error>>;

    async fn compaction_commit(&mut self, table_name: &String, commit: &CompactionCommit) -> Result<(), Box<dyn Error>>;

    async fn get_latest_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, Box<dyn Error>>;

    async fn get_checkpoint(&mut self, snapshot: &CheckpointDescriptor) -> Result<TableMetadataCheckpoint, Box<dyn Error>>;

    async fn get_extension_work_items(&mut self, extension_name: &String) -> Result<Vec<ExtensionWorkItem>, Box<dyn Error>>;

    async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, Box<dyn Error>>;

    async fn get_peer_clients(&mut self) -> Result<Vec<Box<dyn PeerClient>>, Box<dyn Error>>;

    async fn get_next_prefetch_checkpoints(&mut self, extension: Option<String>) -> Result<Vec<CheckpointDescriptor>, Box<dyn Error>>;
}


struct ApiServiceClientActor {
    real: RealApiServiceClient,
    test: TestApiServiceClient,
    test_mode: bool,
    receiver: mpsc::Receiver<ApiServiceClientActorMessage>,
}

enum ApiServiceClientActorMessage {
    Testing {
        respond_to: oneshot::Sender<()>,
        mode: TestProcessingMode,
    },
    CreatePipeline {
        respond_to: oneshot::Sender<()>,
        name: String,
        pipeline: PipelineDefinition,
    },
    DescribePipeline {
        respond_to: oneshot::Sender<Option<PipelineDefinition>>,
        name: String,
    },
    CreateLifetimePolicy {
        respond_to: oneshot::Sender<()>,
        name: String,
        policy: ILMPolicyDefinition,
    },
    DescribeLifetimePolicy {
        respond_to: oneshot::Sender<Option<ILMPolicyDefinition>>,
        name: String,
    },
    CreateTable {
        respond_to: oneshot::Sender<()>,
        create_table: CreateTable,
    },
    DescribeTable {
        respond_to: oneshot::Sender<Option<TableDescription>>,
        name: String,
    },
    AddAlias {
        respond_to: oneshot::Sender<()>,
        table_name: String,
        alias: String,
    },
    RemoveAlias {
        respond_to: oneshot::Sender<()>,
        table_name: String,
        alias: String,
    },         
    CreateTableTemplate {
        respond_to: oneshot::Sender<()>,
        name: String,
        template: CreateIndexTemplateBody,
    },
    DescribeTableTemplate {
        respond_to: oneshot::Sender<Option<CreateIndexTemplateBody>>,
        name: String,
    },    
    AddCheckpoint {
        respond_to: oneshot::Sender<()>,
        checkpoint: TableMetadataCheckpoint,
    },
    #[allow(dead_code)]
    IcebergCommit {
        respond_to: oneshot::Sender<()>,
        table_name: String,
        iceberg_commit: IcebergCommit,
    },
    SpeedboatCommit {
        respond_to: oneshot::Sender<()>,
        speedboat_commit: SpeedboatCommit,
    },
    ExtensionCommit {
        respond_to: oneshot::Sender<()>,
        table_name: String,
        extension_commit: ExtensionCommit,        
    },
    CompactionCommit {
        respond_to: oneshot::Sender<()>,
        table_name: String,
        compaction_commit: CompactionCommit,        
    },    
    GetLatestCheckpoint {
        respond_to: oneshot::Sender<Option<String>>,
        table_name: String,
        extensions: Option<String>,
    },
    GetCheckpoint {
        respond_to: oneshot::Sender<TableMetadataCheckpoint>,
        snapshot: CheckpointDescriptor,
    },
    GetExtensionWorkItems {
        respond_to: oneshot::Sender<Vec<ExtensionWorkItem>>,
        extension_type: String,
    },
    GetCompactionWorkItems {
        respond_to: oneshot::Sender<Vec<(String, CompactionWorkItem)>>,
    },
    GetPeerClients {
        respond_to: oneshot::Sender<Vec<Box<dyn PeerClient>>>,
    },
    GetNextPrefetchCheckpoints {
        respond_to: oneshot::Sender<Vec<CheckpointDescriptor>>,
        extensions: Option<String>,
    },
}

unsafe impl Send for ApiServiceClientActorMessage {}


impl ApiServiceClientActor {
    fn new(receiver: mpsc::Receiver<ApiServiceClientActorMessage>, base_address: String) -> Self {
        ApiServiceClientActor {
            real: RealApiServiceClient::new(base_address),
            test: TestApiServiceClient::new(),
            test_mode: false,
            receiver: receiver,
        }
    }

    async fn handle_message(&mut self, msg: ApiServiceClientActorMessage) {
        match msg {
            ApiServiceClientActorMessage::Testing { respond_to, mode } => {
                self.test_mode = true;
                self.test.clear_and_set(mode).await;
                let _ = respond_to.send(());
            },
            ApiServiceClientActorMessage::CreatePipeline { respond_to, name, pipeline } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.create_pipeline(&name, &pipeline).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.create_pipeline(&name, &pipeline).await.expect("nope"));
                }                
            }, 
            ApiServiceClientActorMessage::DescribePipeline { respond_to, name } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.describe_pipeline(&name).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.describe_pipeline(&name).await.expect("nope"));
                }                
            },
            ApiServiceClientActorMessage::CreateLifetimePolicy { respond_to, name, policy } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.create_lifetime_policy(&name, &policy).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.create_lifetime_policy(&name, &policy).await.expect("nope"));
                }
            },
            ApiServiceClientActorMessage::DescribeLifetimePolicy { respond_to, name } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.describe_lifetime_policy(&name).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.describe_lifetime_policy(&name).await.expect("nope"));
                }
            },
            ApiServiceClientActorMessage::CreateTable { respond_to, create_table } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.create_table(&create_table).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.create_table(&create_table).await.expect("nope"));
                }                
            },
            ApiServiceClientActorMessage::DescribeTable { respond_to, name } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.describe_table(&name).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.describe_table(&name).await.expect("nope"));
                }                
            },
            ApiServiceClientActorMessage::AddAlias { respond_to, table_name, alias } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.add_alias(&table_name, &alias).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.add_alias(&table_name, &alias).await.expect("nope"));
                }  
            },
            ApiServiceClientActorMessage::RemoveAlias { respond_to, table_name, alias } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.remove_alias(&table_name, &alias).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.remove_alias(&table_name, &alias).await.expect("nope"));
                }  
            },            
            ApiServiceClientActorMessage::CreateTableTemplate { respond_to, name, template } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.create_table_template(&name, &template).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.create_table_template(&name, &template).await.expect("nope"));
                }                
            },
            ApiServiceClientActorMessage::DescribeTableTemplate { respond_to, name } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.describe_table_template(&name).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.describe_table_template(&name).await.expect("nope"));
                }                
            },                       
            ApiServiceClientActorMessage::AddCheckpoint { checkpoint, respond_to } => {
                self.test_mode = true;
                let _ = respond_to.send(self.test.add_checkpoint(&checkpoint));
            },
            ApiServiceClientActorMessage::IcebergCommit { respond_to, table_name, iceberg_commit } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.iceberg_commit(&table_name, &iceberg_commit).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.iceberg_commit(&table_name, &iceberg_commit).await.expect("nope"));
                }                
            },            
            ApiServiceClientActorMessage::SpeedboatCommit { respond_to, speedboat_commit } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.speedboat_commit(&speedboat_commit).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.speedboat_commit(&speedboat_commit).await.expect("nope"));
                }                
            },
            ApiServiceClientActorMessage::ExtensionCommit { respond_to, table_name, extension_commit } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.extension_commit(&table_name, &extension_commit).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.extension_commit(&table_name, &extension_commit).await.expect("nope"));
                }                
            }, 
            ApiServiceClientActorMessage::CompactionCommit { respond_to, table_name, compaction_commit } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.compaction_commit(&table_name, &compaction_commit).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.compaction_commit(&table_name, &compaction_commit).await.expect("nope"));
                }                
            },                        
            ApiServiceClientActorMessage::GetLatestCheckpoint { table_name, extensions, respond_to } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.get_latest_checkpoint(&table_name, extensions).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.get_latest_checkpoint(&table_name, extensions).await.expect("nope"));
                }                 
            },
            ApiServiceClientActorMessage::GetCheckpoint { snapshot, respond_to } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.get_checkpoint(&snapshot).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.get_checkpoint(&snapshot).await.expect("nope"));
                }
            },
            ApiServiceClientActorMessage::GetExtensionWorkItems { extension_type, respond_to } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.get_extension_work_items(&extension_type).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.get_extension_work_items(&extension_type).await.expect("nope"));
                }
            },
            ApiServiceClientActorMessage::GetCompactionWorkItems { respond_to } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.get_compaction_work_items().await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.get_compaction_work_items().await.expect("nope"));
                }
            },
            ApiServiceClientActorMessage::GetPeerClients { respond_to } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.get_peer_clients().await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.get_peer_clients().await.expect("nope"));
                }
            },
            ApiServiceClientActorMessage::GetNextPrefetchCheckpoints { respond_to, extensions } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.get_next_prefetch_checkpoints(extensions).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.get_next_prefetch_checkpoints(extensions).await.expect("nope"));
                }
            },
        }
    }
}


async fn run_api_service_client_actor_message_pump(mut actor: ApiServiceClientActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg).await;
    }
}


struct RealApiServiceClient {
    base_address: String,
    client: Client,
}

impl RealApiServiceClient {
    fn new(address: String) -> Self {
        RealApiServiceClient {
            base_address: address,
            client: reqwest::Client::new(),
        }
    }
}

unsafe impl Send for RealApiServiceClient {}
unsafe impl Sync for RealApiServiceClient {}

#[async_trait]
impl ApiServiceClient for RealApiServiceClient {
    async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, Box<dyn Error>> {
        let base_address = &self.base_address;
        let resp = self.client.get(format!("{base_address}/api/v1/iceberg_tables")).send().await;
        match resp {
            Ok(r) => {
                let json = r.json::<Vec<String>>().await;
                match json {
                    Ok(j) => Ok(j),
                    Err(e) => Err(Box::new(e)),
                }
            },
            Err(e) => {
                Err(Box::new(e))
            }
        }  
    }

    async fn create_pipeline(&mut self, _name: &String, _pipeline: &PipelineDefinition) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    async fn describe_pipeline(&mut self, _name: &String) -> Result<Option<PipelineDefinition>, Box<dyn Error>> {
        todo!()
    }

    async fn create_lifetime_policy(&mut self, _name: &String, _pipeline: &ILMPolicyDefinition) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    async fn describe_lifetime_policy(&mut self, _name: &String) -> Result<Option<ILMPolicyDefinition>, Box<dyn Error>> {
        todo!()
    }    

    async fn add_alias(&mut self, _table_name: &String, _alias: &String) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    async fn remove_alias(&mut self, _table_name: &String, _alias: &String) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    async fn create_table(&mut self, _create_table: &CreateTable) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    async fn describe_table(&mut self, _name: &String) -> Result<Option<TableDescription>, Box<dyn Error>> {
        todo!()
    }  

    async fn create_table_template(&mut self, _name: &String, _template: &CreateIndexTemplateBody) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    async fn describe_table_template(&mut self, _name: &String) -> Result<Option<CreateIndexTemplateBody>, Box<dyn Error>> {
        todo!()
    }          

    async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), Box<dyn Error>> {
        let base_address = &self.base_address;
        let body = serde_json::to_string(commit);
        match body {
            Ok(b) => {
                let resp = self.client.post(format!("{base_address}/api/v1/speedboat_commit"))
                    .header("Content-Type", "application/json")
                    .body(b)
                    .send().await;
                match resp {
                    Ok(_r) => Ok(()),
                    Err(e) => Err(Box::new(e)),
                }
            },
            Err(_e) => {
                panic!("Unable to serialize SpeedboatCommit to json")
            }
        }
    }

    async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), Box<dyn Error>> {
        let base_address = &self.base_address;
        let body = serde_json::to_string(iceberg_commit);
        match body {
            Ok(b) => {
                let resp = self.client.post(format!("{base_address}/api/v1/iceberg_commit/{table_name}"))
                    .header("Content-Type", "application/json")
                    .body(b)
                    .send().await;
                match resp {
                    Ok(_r) => Ok(()),
                    Err(e) => Err(Box::new(e)),
                }
            },
            Err(_e) => {
                panic!("Unable to serialize IcebergMetadata to json")
            }
        }   
    }

    async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), Box<dyn Error>> {
        let base_address = &self.base_address;
        let body = serde_json::to_string(commit);
        match body {
            Ok(b) => {
                let resp = self.client.post(format!("{base_address}/api/v1/extension_commit/{table_name}"))
                    .header("Content-Type", "application/json")
                    .body(b)
                    .send().await;
                match resp {
                    Ok(_r) => Ok(()),
                    Err(e) => Err(Box::new(e)),
                }
            },
            Err(_e) => {
                panic!("Unable to serialize ExtensionCommit to json")
            }
        }       
    }

    async fn compaction_commit(&mut self, table_name: &String, commit: &CompactionCommit) -> Result<(), Box<dyn Error>> {
        let base_address = &self.base_address;
        let body = serde_json::to_string(commit);
        match body {
            Ok(b) => {
                let resp = self.client.post(format!("{base_address}/api/v1/compaction_commit/{table_name}"))
                    .header("Content-Type", "application/json")
                    .body(b)
                    .send().await;
                match resp {
                    Ok(_r) => Ok(()),
                    Err(e) => Err(Box::new(e)),
                }
            },
            Err(_e) => {
                panic!("Unable to serialize CompactionCommit to json")
            }
        }          
    }

    async fn get_latest_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, Box<dyn Error>> {
        let base_address = &self.base_address;        
        let url = match extensions {
            Some(e) => format!("{base_address}/api/v1/get_latest/{table_name}/{e}"),
            None => format!("{base_address}/api/v1/get_latest/{table_name}"),
        };
        let resp = self.client.get(url).send().await;
        match resp {
            Ok(r) => {
                let text = r.text().await;
                match text {
                    Ok(t) => match t.len() {
                        0 => Ok(None),
                        _ => Ok(Some(t)),
                    },
                    Err(e) => Err(Box::new(e))
                }
            },
            Err(e) => Err(Box::new(e))
        }                     
    }

    async fn get_checkpoint(&mut self, snapshot: &CheckpointDescriptor) -> Result<TableMetadataCheckpoint, Box<dyn Error>> {
        let base_address = &self.base_address;     
        let url = format!("{base_address}/api/v1/get_checkpoint/{}/{}", snapshot.table_name, snapshot.checkpoint_id);
        let resp = self.client.get(url).send().await;
        match resp {
            Ok(r) => {
                let json = r.json::<TableMetadataCheckpoint>().await;
                match json {
                    Ok(j) => Ok(j),
                    Err(e) => Err(Box::new(e))
                }
            },
            Err(e) => Err(Box::new(e))
        }        
    }

    async fn get_extension_work_items(&mut self, _extension_name: &String) -> Result<Vec<ExtensionWorkItem>, Box<dyn Error>> {
        todo!("nope")
    }

    async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, Box<dyn Error>> {
        todo!("nope")
    }

    async fn get_peer_clients(&mut self) -> Result<Vec<Box<dyn PeerClient>>, Box<dyn Error>> {
        todo!("nope")
    }

    async fn get_next_prefetch_checkpoints(&mut self, _extension: Option<String>) -> Result<Vec<CheckpointDescriptor>, Box<dyn Error>> {
        todo!()
    }
}


#[derive(Clone)]
pub(crate) struct ExtensionWorkItem {
    pub extension_type: String,
    pub table_name: String,
    pub table_schema: PowdrrSchema,
    pub speedboat_files: FileSetPayload,
    pub iceberg_files: FileSetPayload,
}

impl ExtensionWorkItem {
    fn clear(&mut self) -> () {
        self.speedboat_files.clear();
        self.iceberg_files.clear();
    }

    fn has_work(&self) -> bool {
        self.speedboat_files.len() > 0 || self.iceberg_files.len() > 0
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct CompactionWorkItem {
    pub table_schema: PowdrrSchema,
    pub speedboat_files: FileSetPayload,
    pub delete_files: Vec<String>,
}


struct TestApiServiceClient {
    mode: TestProcessingMode,
    tables: HashMap<String, TableDescription>,
    // alias name -> table name
    table_aliases: HashMap<String, String>,
    table_templates: HashMap<String, CreateIndexTemplateBody>,
    pipelines: HashMap<String, PipelineDefinition>,
    lifetime_policies: HashMap<String, ILMPolicyDefinition>,
    latest_committed_checkpoint_id: HashMap<String, String>,
    latest_fetched_checkpoint_id: HashMap<String, String>,
    compaction_work_items: HashMap<String, CompactionWorkItem>,
    extension_work_items: HashMap<String, HashMap<String, ExtensionWorkItem>>,
    compactions: HashMap<String, CompactionCommit>,
    checkpoints: HashMap<String, TableMetadataCheckpoint>,
    checkpoints_needing_extension_work: HashMap<String, Vec<String>>,
    recent_file_extension_metadata: HashMap<String, Vec<ExtensionFile>>
}

impl TestApiServiceClient {
    fn new() -> Self {
        TestApiServiceClient{
            mode: TestProcessingMode::default(),
            tables: HashMap::new(),
            table_aliases: HashMap::new(),
            table_templates: HashMap::new(),
            pipelines: HashMap::new(),
            lifetime_policies: HashMap::new(),
            latest_committed_checkpoint_id: HashMap::new(),
            latest_fetched_checkpoint_id: HashMap::new(),
            compaction_work_items: HashMap::new(),
            compactions: HashMap::new(),
            checkpoints: HashMap::new(),
            checkpoints_needing_extension_work: HashMap::new(),
            extension_work_items: HashMap::from([("es".to_string(), HashMap::new())]),
            recent_file_extension_metadata: HashMap::new(),
        }
    }

    async fn clear_and_set(&mut self, mode: TestProcessingMode) -> () {
        distributed_cache::clear(&self.tables.keys().into_iter().map(|x|x.clone()).collect()).unwrap();
        self.mode = mode;
        self.tables.clear();
        self.table_aliases.clear();
        self.table_templates.clear();
        self.pipelines.clear();
        self.lifetime_policies.clear();
        self.latest_committed_checkpoint_id.clear();
        self.latest_fetched_checkpoint_id.clear();
        self.compaction_work_items.clear();
        self.compactions.clear();
        self.checkpoints.clear();
        self.checkpoints_needing_extension_work.clear();
        self.extension_work_items = HashMap::from([("es".to_string(), HashMap::new())]);
        self.recent_file_extension_metadata.clear();
        drop_all_tables(&"default".to_string()).await.expect("Failed while dropping all tables");
    }

    fn checkpoints_needing_extension_work(&self, table_name: &String, extension_name: &String) -> Option<Vec<String>> {
        self.checkpoints_needing_extension_work.get(&format!("{}_{}", table_name, extension_name)).cloned()
    }

    fn recent_file_extension_metadata(&self, table_name: &String, extension_name: &String, file_name: &String) -> Option<Vec<ExtensionFile>> {
        self.recent_file_extension_metadata.get(&format!("{}_{}_{}", table_name, extension_name, file_name)).cloned()
    }

    fn add_recent_extension_files(&mut self, table_name: &String, commit: &ExtensionCommit) -> () {
        for (file_name, extension_files) in commit.files.iter() {
            self.recent_file_extension_metadata.insert(
                format!("{}_{}_{}", table_name, commit.extension, file_name),
                extension_files.clone()
            );
        }
    }

    fn try_fill_checkpoint_extension_metadata(&mut self, extension_name: &String, metadata: &mut TableMetadataCheckpoint) -> (FileSetPayload, FileSetPayload) {
        if !metadata.extension_metadata.contains_key(extension_name) {
            metadata.extension_metadata.insert(extension_name.clone(), HashMap::new());
        }

        let extension_metadata = metadata.extension_metadata.get_mut(extension_name).unwrap();

        let mut iceberg_file_set = FileSetPayload::new();
        match metadata.iceberg_metadata.as_ref() {
            Some(im) => {
                for file_desc in im.files.as_file_tuples() {
                    match self.recent_file_extension_metadata(&metadata.table_name, extension_name, &file_desc.file_path) {
                        Some(metadata) => {
                            extension_metadata.insert(file_desc.file_path.clone(), metadata);
                        },
                        None => {
                            iceberg_file_set.add(&file_desc);
                        }
                    }
                }
            },
            None => ()
        };

        let mut speedboat_file_set = FileSetPayload::new();
        match metadata.speedboat_metadata.as_ref() {
            Some(im) => {
                for file_desc in im.files.as_file_tuples() {
                    match self.recent_file_extension_metadata(&metadata.table_name, extension_name, &file_desc.file_path) {
                        Some(metadata) => {
                            extension_metadata.insert(file_desc.file_path.clone(), metadata);
                        },
                        None => {
                            speedboat_file_set.add(&file_desc);
                        }
                    }
                }
            },
            None => ()
        };

        (speedboat_file_set, iceberg_file_set)
    }

    fn add_checkpoint(&mut self, metadata: &TableMetadataCheckpoint) -> () {
        // To make testing a little easier, we'll just magic up a table as necessary
        if !self.tables.contains_key(&metadata.table_name) {
            self.tables.insert(
                metadata.table_name.clone(), 
                TableDescription{ name: metadata.table_name.clone(), tags: Default::default() }
            );
        }
        let key = format!("{}_{}", &metadata.table_name, &metadata.checkpoint_id);
        if !self.checkpoints.contains_key(&key) {
            self.checkpoints.insert(key, metadata.clone());
        }
        self.latest_committed_checkpoint_id.insert(metadata.table_name.clone(), metadata.checkpoint_id.clone());
        if metadata.extension_metadata.len() > 0 {
            for extension in metadata.extension_metadata.keys() {
                let key = format!("{}_{}", &metadata.table_name, extension);
                if !self.latest_committed_checkpoint_id.contains_key(&key) {
                    self.latest_committed_checkpoint_id.insert(key, metadata.checkpoint_id.clone());
                }
            }
        } else {
            self.fill_extension_work_item(
                &metadata.table_name,
                &"es".to_string(),
                &metadata.checkpoint_id,
                &metadata.schema,
                metadata.speedboat_metadata.as_ref().map_or(&FileSetPayload::new(), |m|&m.files),
                metadata.iceberg_metadata.as_ref().map_or(&FileSetPayload::new(), |m|&m.files)
            )
        }
    }

    fn fill_extension_work_item(
        &mut self,
        table_name: &String,
        extension: &String,
        checkpoint_id: &String,
        schema: &PowdrrSchema,
        speedboat_files: &FileSetPayload,
        iceberg_files: &FileSetPayload,
    ) -> () {
        if speedboat_files.len() == 0 && iceberg_files.len() == 0 {
            return;
        }

        let es_work_items = self.extension_work_items.get_mut(&"es".to_string()).unwrap();
        if !es_work_items.contains_key(table_name) {
            es_work_items.insert(
                table_name.clone(),
                ExtensionWorkItem {
                    extension_type: extension.clone(),
                    table_name: table_name.clone(),
                    table_schema: schema.clone(),
                    speedboat_files: speedboat_files.clone(),
                    iceberg_files: iceberg_files.clone()
                }
            );
        } else {
            let table_work_item = es_work_items.get_mut(table_name).unwrap();
            table_work_item.table_schema =schema.clone();
            table_work_item.speedboat_files = table_work_item.speedboat_files.merge(speedboat_files);
            table_work_item.iceberg_files = table_work_item.iceberg_files.merge(&iceberg_files);
        }
        let key = format!("{}_{}", table_name, extension);
        if !self.checkpoints_needing_extension_work.contains_key(&key) {
            self.checkpoints_needing_extension_work.insert(key, vec![checkpoint_id.clone()]);
        } else {
            self.checkpoints_needing_extension_work.get_mut(&key).unwrap().push(checkpoint_id.clone());
        }
    }

    fn handle_compaction(&mut self, compactions: &Vec<String>, checkpoint: &mut TableMetadataCheckpoint) -> () {
        let (removed_speedboat, removed_deletes) = self.get_removed_files(compactions);

        match checkpoint.speedboat_metadata.as_mut() {
            Some(speedboat) => {
                speedboat.files.remove(&removed_speedboat);
            },
            None => ()
        };

        match checkpoint.deletes_metadata.as_mut() {
            Some(deletes) => {
                deletes.files.retain(|x|!removed_deletes.contains(x));
            },
            None => ()
        };

        for metadata in checkpoint.extension_metadata.values_mut() {
            metadata.retain(|key, _|!removed_speedboat.contains(key))
        }

        // TODO: cleanup compactions
        // self.compactions.retain(|x, _|!compactions.contains(x));
    }

    fn get_removed_files(&self, compactions: &Vec<String>) -> (Vec<String>, Vec<String>) {
        let compactions_data: Vec<&CompactionCommit> = compactions.iter().map(|x| self.compactions.get(x).unwrap()).collect();
        (
            compactions_data.iter().map(|x| x.removed_speedboat_files.clone()).flatten().collect(),
            compactions_data.iter().map(|x| x.removed_delete_files.clone()).flatten().collect(),
        )
    } 

    fn get_latest_committed_checkpoint_sync(&mut self, table_name: &String, extensions: Option<String>) -> Option<String> {
        let real_table_name = self.table_aliases.get(table_name).unwrap_or(table_name);
        let key = match extensions {
            Some(e) => &format!("{}_{}", real_table_name, e).to_string(),
            None => table_name,
        };
        match self.latest_committed_checkpoint_id.get(key) {
            Some(c) => Some(c.to_string()),
            None => None
        }
    }     

    fn set_latest_committed_checkpoint(&mut self, table_name: &String, extensions: Option<&String>, checkpoint_id: &String) {
        let real_table_name = self.table_aliases.get(table_name).unwrap_or(table_name);
        let key = match extensions {
            Some(e) => &format!("{}_{}", real_table_name, e).to_string(),
            None => table_name,
        };
        assert!(self.latest_committed_checkpoint_id.get(key).is_none() || self.latest_committed_checkpoint_id.get(key).unwrap() < checkpoint_id);
        self.latest_committed_checkpoint_id.insert(key.clone(), checkpoint_id.clone());
    }

    fn get_latest_fetched_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Option<String> {
        let real_table_name = self.table_aliases.get(table_name).unwrap_or(table_name);
        let key = match extensions {
            Some(e) => &format!("{}_{}", real_table_name, e).to_string(),
            None => table_name,
        };
        match self.latest_fetched_checkpoint_id.get(key) {
            Some(c) => Some(c.to_string()),
            None => None
        }
    }


    async fn speedboat_commit_type_commit(&mut self, table_info: &SpeedboatCommitTableInfo, compactions: &Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
        let latest_checkpoint = match self.get_latest_committed_checkpoint_sync(&table_info.table_name, None) {
            Some(checkpoint_id) => {
                let key = format!("{}_{}", &table_info.table_name, checkpoint_id);
                match self.checkpoints.get(&key) {
                    Some(c) => c,
                    None => panic!("Found latest checkpoint id but checkpoint missing = {}", key)
                }
            },
            None => {
                &TableMetadataCheckpoint {
                    table_name: table_info.table_name.clone(),
                    checkpoint_id: "".to_string(),
                    iceberg_metadata: None,
                    speedboat_metadata: None,
                    deletes_metadata: None,
                    extension_metadata: HashMap::new(),
                    schema: table_info.schema.as_ref().unwrap().clone()
                }
            },
        };

        let new_checkpoint_id = IdInstance::next_id().to_string();
        let new_speedboat_metadata = match &latest_checkpoint.speedboat_metadata {
            None => SpeedboatMetadata {
                files: FileSetPayload {
                    file_paths: table_info.files.clone(),
                    sizes: table_info.sizes.clone(),
                    schemas: vec!(table_info.schema.as_ref().unwrap().clone()),
                    file_schemas: table_info.files.iter().map(|_| 0).collect(),
                }
            },
            Some(existing) => {
                SpeedboatMetadata {
                    files: existing.files.merge(&table_info.as_file_set_payload())
                }
            },
        };

        let mut merged_schema = latest_checkpoint.schema.clone();
        if table_info.schema.is_some() {
            merged_schema.merge_from(table_info.schema.as_ref().unwrap());
        }

        let mut new_latest_checkpoint = TableMetadataCheckpoint {
            table_name: table_info.table_name.clone(),
            checkpoint_id: new_checkpoint_id.clone(),
            iceberg_metadata: latest_checkpoint.iceberg_metadata.clone(),
            speedboat_metadata: Some(new_speedboat_metadata.clone()),
            deletes_metadata: latest_checkpoint.deletes_metadata.clone(),
            extension_metadata: latest_checkpoint.extension_metadata.clone(),
            schema: merged_schema.clone(),
        };

        self.handle_compaction(compactions, &mut new_latest_checkpoint);
        let (speedboat_files, iceberg_files) = self.try_fill_checkpoint_extension_metadata(&"es".to_string(), &mut new_latest_checkpoint);

        self.checkpoints.insert(format!("{}_{}", &table_info.table_name, &new_checkpoint_id), new_latest_checkpoint.clone());

        match self.mode.indexing_mode {
            IndexingMode::Sync => {
                self.fill_extension_work_item(
                    &table_info.table_name,
                    &"es".to_string(),
                    &new_checkpoint_id,
                    &merged_schema,
                    &speedboat_files,
                    &iceberg_files
                )

            },
            IndexingMode::Async => {
                self.fill_extension_work_item(
                    &table_info.table_name,
                    &"es".to_string(),
                    &new_checkpoint_id,
                    &merged_schema,
                    &speedboat_files,
                    &iceberg_files
                )
            },
            IndexingMode::Disabled => ()
        };

        self.set_latest_committed_checkpoint(&table_info.table_name, None, &new_checkpoint_id);

        // TODO: apply some policy here based on sizes to split up compaction work items
        match self.compaction_work_items.get_mut(&table_info.table_name) {
            Some(compaction) => {
                compaction.table_schema = new_latest_checkpoint.schema.clone();
                compaction.speedboat_files = compaction.speedboat_files.merge(&table_info.as_file_set_payload())
            },
            None => {
                self.compaction_work_items.insert(
                    table_info.table_name.clone(),
                    CompactionWorkItem {
                        table_schema: new_latest_checkpoint.schema.clone(),
                        speedboat_files: table_info.as_file_set_payload(),
                        delete_files: new_latest_checkpoint.deletes_metadata.as_ref().map_or_else(|| vec!(), |m|m.files.clone()),
                    }
                );
            }
        }
        Ok(())
    }    

    async fn speedboat_commit_type_delete(&mut self, table_info: &SpeedboatCommitTableInfo, compactions: &Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
        let latest_checkpoint = match self.get_latest_committed_checkpoint_sync(&table_info.table_name, None) {
            Some(checkpoint_id) => {
                let key = format!("{}_{}", &table_info.table_name, checkpoint_id);
                match self.checkpoints.get(&key) {
                    Some(c) => c,
                    None => panic!("Found latest checkpoint id but checkpoint missing = {}", key)
                }
            },
            None => {
                &TableMetadataCheckpoint {
                    table_name: table_info.table_name.clone(),
                    checkpoint_id: "".to_string(),
                    iceberg_metadata: None,
                    speedboat_metadata: None,
                    deletes_metadata: None,
                    extension_metadata: HashMap::new(),
                    schema: PowdrrSchema{ fields: vec!() },
                }
            },
        };

        let new_checkpoint_id = IdInstance::next_id().to_string();
        let new_deletes_metadata = match &latest_checkpoint.deletes_metadata {
            None => DeletesMetadata {
                files: table_info.files.clone(),
            },
            Some(existing) => {
                let mut files = existing.files.clone();
                files.extend(table_info.files.clone());
                DeletesMetadata {
                    files
                }
            }
        };

        tracing::info!("Delete commit has {} files", new_deletes_metadata.files.len());
        for file in &new_deletes_metadata.files {
            tracing::info!("Delete file {}", file)
        }

        let mut new_latest_checkpoint = TableMetadataCheckpoint {
            table_name: table_info.table_name.clone(),
            checkpoint_id: new_checkpoint_id.clone(),
            iceberg_metadata: latest_checkpoint.iceberg_metadata.clone(),
            speedboat_metadata: latest_checkpoint.speedboat_metadata.clone(),
            deletes_metadata: Some(new_deletes_metadata),
            extension_metadata: latest_checkpoint.extension_metadata.clone(),
            schema: latest_checkpoint.schema.clone(),
        };
        self.handle_compaction(&compactions, &mut new_latest_checkpoint);
        self.try_fill_checkpoint_extension_metadata(&"es".to_string(), &mut new_latest_checkpoint);

        self.checkpoints.insert(format!("{}_{}", &table_info.table_name, &new_checkpoint_id), new_latest_checkpoint.clone());
        self.set_latest_committed_checkpoint(&table_info.table_name, None, &new_checkpoint_id);
        match self.compaction_work_items.get_mut(&table_info.table_name) {
            Some(work_item) => {
                work_item.delete_files.extend(table_info.files.clone());
            },
            None => {}
        }
        Ok(())
    }    

    async fn create_index(&mut self) -> Result<(), Box<dyn Error>> {
        let work_items = self.get_extension_work_items(&"es".to_string()).await?;
        for work_item in work_items {
            match create_index(&work_item).await {
                Ok(_) => (),
                Err(e) => {
                    tracing::error!("Failed to create index for table {}: {}", work_item.table_name, e);
                    return Err(Box::new(e));
                }
            }
        }
        Ok(())
    }

    fn get_checkpoint_sync(&self, table_name: &String, checkpoint_id: &String) -> Option<TableMetadataCheckpoint> {
        let key = format!("{}_{}", table_name, checkpoint_id);
        self.checkpoints.get(&key).cloned()
    }

    fn add_coverage_for(&mut self, table_name: &String, checkpoint_id: &String, extension_commit: &ExtensionCommit) -> Option<String> {
        let key = format!("{}_{}", table_name, checkpoint_id);

        let checkpoint = self.checkpoints.get_mut(&key).unwrap();

        checkpoint.add_coverage(extension_commit);

        if checkpoint.fully_covered_for_extension(&extension_commit.extension) {
            let key = format!("{}_{}", table_name, extension_commit.extension);
            if self.checkpoints_needing_extension_work.contains_key(&key) {
                self.checkpoints_needing_extension_work.get_mut(&key).unwrap().retain(|x|x != checkpoint_id);
            }
            Some(checkpoint_id.clone())
        } else {
            None
        }
    }

}

unsafe impl Sync for TestApiServiceClient {}
unsafe impl Send for TestApiServiceClient {}

#[async_trait]
impl ApiServiceClient for TestApiServiceClient {
    async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        todo!()
    }

    async fn create_pipeline(&mut self, name: &String, pipeline: &PipelineDefinition) -> Result<(), Box<dyn std::error::Error>> {
        match self.pipelines.get(name) {
            Some(_) => panic!("Need to do a real error path now"),
            None => {
                self.pipelines.insert(name.clone(), pipeline.clone());
                Ok(())
            }
        }        
    }

    async fn describe_pipeline(&mut self, name: &String) -> Result<Option<PipelineDefinition>, Box<dyn std::error::Error>> {
        match self.pipelines.get(name) {
            Some(p) => Ok(Some(p.clone())),
            None => Ok(None)
        }      
    }

    async fn create_lifetime_policy(&mut self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), Box<dyn Error>> {
        match self.lifetime_policies.get(name) {
            Some(_) => panic!("Need to do a real error path now"),
            None => {
                self.lifetime_policies.insert(name.clone(), policy.clone());
                Ok(())
            }
        }
    }

    async fn describe_lifetime_policy(&mut self, name: &String) -> Result<Option<ILMPolicyDefinition>, Box<dyn Error>> {
        match self.lifetime_policies.get(name) {
            Some(p) => Ok(Some(p.clone())),
            None => Ok(None)
        }
    }

    async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), Box<dyn Error>> {
        match distributed_cache::create_table(&create_table.name) {
            Ok(_) => (),
            Err(e) => panic!("Unable to create table = {}", e),
        };
        match self.tables.get(&create_table.name) {
            Some(_) => {
                self.tables.remove(&create_table.name);
                self.tables.insert(create_table.name.clone(), TableDescription::from_create_table(create_table));
            }
            None => {
                self.tables.insert(create_table.name.clone(), TableDescription::from_create_table(create_table)); 
            }
        }
        Ok(())
    } 

    async fn describe_table(&mut self, name: &String) -> Result<Option<TableDescription>, Box<dyn Error>> {
        let final_name = self.table_aliases.get(name).unwrap_or_else(|| name);
        match self.tables.get(final_name) {
            Some(d) => Ok(Some(d.clone())),
            None => Ok(None)
        }
    } 

    async fn add_alias(&mut self, table_name: &String, alias: &String) -> Result<(), Box<dyn Error>> {
        // TODO: check if something exists
        self.table_aliases.insert(alias.clone(), table_name.clone());
        Ok(())
    }

    async fn remove_alias(&mut self, _table_name: &String, alias: &String) -> Result<(), Box<dyn Error>> {
        // TODO: check if something exists
        self.table_aliases.remove(alias);
        Ok(())
    }    

    async fn create_table_template(&mut self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), Box<dyn Error>> {
        match self.table_templates.get(name) {
            Some(_) => {
                self.table_templates.remove(name);
                self.table_templates.insert(name.clone(), template.clone());
                Ok(())
            }
            None => {
                self.table_templates.insert(name.clone(), template.clone());
                Ok(())
            }
        }
    }   

    async fn describe_table_template(&mut self, name: &String) -> Result<Option<CreateIndexTemplateBody>, Box<dyn Error>> {
        match self.table_templates.get(name) {
            Some(d) => Ok(Some(d.clone())),
            None => Ok(None)
        }      
    }


    async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), Box<dyn std::error::Error>> {
        for table_info in commit.type_files.iter() {
            if table_info.commit_type == "commit" || table_info.commit_type == "compact" {
                self.speedboat_commit_type_commit(table_info, &commit.compactions).await?;
            } else if table_info.commit_type == "delete" {
                self.speedboat_commit_type_delete(table_info, &commit.compactions).await?;
            } else {
                panic!("Unknown Speedboat commit type")
            }
        }
        Ok(())
    }

    async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), Box<dyn std::error::Error>> {
        let latest_checkpoint = match self.get_latest_committed_checkpoint_sync(table_name, None) {
            Some(checkpoint_id) => {
                let key = format!("{}_{}", table_name, checkpoint_id);
                match self.checkpoints.get(&key) {
                    Some(c) => c,
                    None => panic!("Found latest checkpoint id but checkpoint missing = {}", key)
                }
            },
            None => {
                &TableMetadataCheckpoint {
                    table_name: table_name.clone(),
                    checkpoint_id: "".to_string(),
                    iceberg_metadata: None,
                    speedboat_metadata: None,
                    deletes_metadata: None,
                    extension_metadata: HashMap::new(),
                    schema: iceberg_commit.metadata.table_schema.clone()
                }
            },
        };

        let new_checkpoint_id = IdInstance::next_id().to_string();

        let mut merged_schema = latest_checkpoint.schema.clone();
        merged_schema.merge_from(&iceberg_commit.metadata.table_schema);

        let mut new_latest_checkpoint = TableMetadataCheckpoint {
            table_name: table_name.clone(),
            checkpoint_id: new_checkpoint_id.clone(),
            iceberg_metadata: Some(iceberg_commit.metadata.clone()),
            speedboat_metadata: latest_checkpoint.speedboat_metadata.clone(),
            deletes_metadata: latest_checkpoint.deletes_metadata.clone(),
            extension_metadata: latest_checkpoint.extension_metadata.clone(),
            schema: merged_schema.clone(),
        };
        self.handle_compaction(&iceberg_commit.compactions, &mut new_latest_checkpoint);
        let (speedboat_files, iceberg_files) = self.try_fill_checkpoint_extension_metadata(&"es".to_string(), &mut new_latest_checkpoint);

        self.checkpoints.insert(format!("{}_{}", &table_name, &new_checkpoint_id), new_latest_checkpoint.clone());
        self.set_latest_committed_checkpoint(&table_name, None, &new_checkpoint_id);

        match self.mode.indexing_mode {
            IndexingMode::Sync => {
                self.fill_extension_work_item(
                    &table_name,
                    &"es".to_string(),
                    &new_checkpoint_id,
                    &merged_schema,
                    &speedboat_files,
                    &iceberg_files
                );
                self.create_index().await?;
            },
            IndexingMode::Async => {
                self.fill_extension_work_item(
                    &table_name,
                    &"es".to_string(),
                    &new_checkpoint_id,
                    &merged_schema,
                    &speedboat_files,
                    &iceberg_files
                );
            },
            IndexingMode::Disabled => ()
        };

        Ok(())
    }

    async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), Box<dyn std::error::Error>> {
        let waiting_checkpoint_ids = self.checkpoints_needing_extension_work(table_name, &commit.extension).unwrap_or_else(|| vec!());
        assert!(waiting_checkpoint_ids.len() > 0);

        let removed_checkpoint_ids: Vec<String> = waiting_checkpoint_ids.iter().map(|x|self.add_coverage_for(table_name, x, commit)).flatten().collect();
        assert!(removed_checkpoint_ids.len() > 0);

        let max_id = removed_checkpoint_ids.iter().max().unwrap();

        self.add_recent_extension_files(table_name, commit);

        match self.get_latest_committed_checkpoint_sync(table_name, Some("es".to_string())) {
            Some(latest) => {
                if max_id > &latest {
                    self.set_latest_committed_checkpoint(table_name, Some(&"es".to_string()), max_id);
                }
            },
            None => {
                self.set_latest_committed_checkpoint(table_name, Some(&"es".to_string()), max_id);
            },
        };
        Ok(())
    }

    async fn compaction_commit(&mut self, _table_name: &String, commit: &CompactionCommit) -> Result<(), Box<dyn std::error::Error>> {
        // NOTE: this just notes what the compactor is saying. We don't generate the new checkpoint
        // until we see an iceberg or speedboat commit with the new info.
        self.compactions.insert(commit.compaction_id.clone(), commit.clone());
        Ok(())
    }

    async fn get_latest_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, Box<dyn std::error::Error>> {
        match self.mode.prefetch_mode {
            PrefetchMode::Disabled => {
                Ok(self.get_latest_committed_checkpoint_sync(table_name, extensions))
            },
            PrefetchMode::Enabled => {
                Ok(self.get_latest_fetched_checkpoint(table_name, extensions))
            }
        }
    }

    async fn get_checkpoint(&mut self, snapshot: &CheckpointDescriptor) -> Result<TableMetadataCheckpoint, Box<dyn std::error::Error>> {
        match self.get_checkpoint_sync(&snapshot.table_name, &snapshot.checkpoint_id) {
            Some(v) => Ok(v.clone()),
            None => panic!("Oh no")
        }
    }

    async fn get_extension_work_items(&mut self, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, Box<dyn std::error::Error>> {
        if extension_type == "es" {
            // TODO: priority by index? allow index filtering?
            let mut collected_work_items= vec!();
            match self.extension_work_items.get_mut(extension_type) {
                Some(items) => {
                    for (_, work_items) in items.iter_mut() {
                        if work_items.has_work() {
                            collected_work_items.push(work_items.clone());
                            work_items.clear();
                        }
                    }
                },
                None => ()
            };
            Ok(collected_work_items)
        } else {
            Ok(vec!())
        }
    }

    async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, Box<dyn std::error::Error>> {
        let mut work_items = vec!();
        for (table_name, compaction) in self.compaction_work_items.iter_mut() {
            tracing::info!("Compaction work item stats: size = {}/{}, files = {}/200",
                compaction.speedboat_files.sizes.iter().sum::<u64>(),
                100 * 1024 * 1024,
                compaction.speedboat_files.sizes.len()
            );
            let do_compaction = compaction.speedboat_files.sizes.iter().sum::<u64>() > 100 * 1024 * 1024 || compaction.speedboat_files.sizes.len() > 200;
            //let do_compaction = true;
            if do_compaction {
                work_items.push((table_name.clone(), compaction.clone()));
                compaction.speedboat_files.clear();
            }
        }
        Ok(work_items)
    }

    async fn get_peer_clients(&mut self) -> Result<Vec<Box<dyn PeerClient>>, Box<dyn Error>> {
        Ok(vec!(Box::new(SelfPeer::new(self.mode.compaction_mode.clone()))))
    }

    async fn get_next_prefetch_checkpoints(&mut self, extension: Option<String>) -> Result<Vec<CheckpointDescriptor>, Box<dyn Error>> {
        match self.mode.prefetch_mode {
            PrefetchMode::Enabled => {
                let mut checkpoints = vec!();
                let all_table_names: Vec<String> = self.tables.keys().cloned().collect();
                for table_name in all_table_names.iter() {
                    match self.get_latest_committed_checkpoint_sync(table_name, extension.clone()) {
                        Some(checkpoint_id) => {
                            checkpoints.push(CheckpointDescriptor {
                                table_name: table_name.clone(),
                                checkpoint_id,
                            });
                        },
                        None => ()
                    }
                }
                Ok(checkpoints)
            },
            PrefetchMode::Disabled => {
                Ok(vec!())
            }
        }
    }
}

#[derive(Clone)]
pub struct ApiServiceClientHandle {
    sender: mpsc::Sender<ApiServiceClientActorMessage>,
}

impl ApiServiceClientHandle {
    pub fn new(address: String) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let actor = ApiServiceClientActor::new(receiver, address);
        tokio::spawn(run_api_service_client_actor_message_pump(actor));

        Self { sender }
    }

    pub async fn set_testing_mode(&self, mode: &TestProcessingMode) -> () {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::Testing { 
            respond_to: send,
            mode: mode.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")         
    }

    pub async fn create_pipeline(&self, name: &String, pipeline: &PipelineDefinition) -> () {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::CreatePipeline { 
            respond_to: send,
            name: name.clone(),
            pipeline: pipeline.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")          
    }

    pub async fn describe_pipeline(&self, name: &String) -> Option<PipelineDefinition> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::DescribePipeline { 
            respond_to: send,
            name: name.clone()
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")          
    }

    pub async fn create_lifetime_policy(&self, name: &String, pipeline: &ILMPolicyDefinition) -> () {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::CreateLifetimePolicy {
            respond_to: send,
            name: name.clone(),
            policy: pipeline.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    pub async fn describe_lifetime_policy(&self, name: &String) -> Option<ILMPolicyDefinition> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::DescribeLifetimePolicy {
            respond_to: send,
            name: name.clone()
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    pub async fn create_table(&self, create_table: &CreateTable) -> () {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::CreateTable { 
            respond_to: send,
            create_table: create_table.clone()
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")        
    }  

    pub async fn describe_table(&self, table_name: &String) -> Option<TableDescription> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::DescribeTable { 
            respond_to: send,
            name: table_name.clone()
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")          
    } 

    pub async fn add_alias(&self, table_name: &String, alias: &String) -> () {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::AddAlias { 
            respond_to: send,
            table_name: table_name.clone(),
            alias: alias.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")        
    }  

    pub async fn remove_alias(&self, table_name: &String, alias: &String) -> () {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::RemoveAlias { 
            respond_to: send,
            table_name: table_name.clone(),
            alias: alias.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")        
    }         

    pub async fn create_table_template(&self, name: &String, template: &CreateIndexTemplateBody) -> () {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::CreateTableTemplate { 
            respond_to: send,
            name: name.clone(),
            template: template.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")        
    }  

    pub async fn describe_table_template(&self, table_name: &String) -> Option<CreateIndexTemplateBody> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::DescribeTableTemplate { 
            respond_to: send,
            name: table_name.clone()
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")          
    }     

    pub async fn add_checkpoint(&self, checkpoint: &TableMetadataCheckpoint) -> () {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::AddCheckpoint { 
            respond_to: send,
            checkpoint: checkpoint.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")        
    }

    #[allow(dead_code)]
    pub async fn iceberg_commit(&self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::IcebergCommit { 
            respond_to: send,
            table_name: table_name.clone(),
            iceberg_commit: iceberg_commit.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await
    }    

    pub async fn speedboat_commit(&self, speedboat_commit: &SpeedboatCommit) -> Result<(), RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::SpeedboatCommit { 
            respond_to: send,
            speedboat_commit: speedboat_commit.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await
    }

    pub async fn extension_commit(&self, table_name: &String, extension_commit: &ExtensionCommit) -> Result<(), RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::ExtensionCommit { 
            respond_to: send,
            table_name: table_name.clone(),
            extension_commit: extension_commit.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await
    }

    pub async fn compaction_commit(&self, table_name: &String, compaction_commit: &CompactionCommit) -> Result<(), RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::CompactionCommit { 
            respond_to: send,
            table_name: table_name.clone(),
            compaction_commit: compaction_commit.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await
    }    

    pub async fn get_latest_checkpoint(&self, table_name: &String, extension: Option<String>) -> Result<Option<String>, RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::GetLatestCheckpoint { 
            respond_to: send,
            table_name: table_name.clone(),
            extensions: extension.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await        
    }   

    pub async fn get_checkpoint(&self, snapshot: CheckpointDescriptor) -> Result<TableMetadataCheckpoint, RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::GetCheckpoint { 
            respond_to: send,
            snapshot: snapshot,
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await
    }

    pub async fn get_extension_work_items(&self, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::GetExtensionWorkItems {
            respond_to: send,
            extension_type: extension_type.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await        
    }

    pub async fn get_compaction_work_items(&self) -> Result<Vec<(String, CompactionWorkItem)>, RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::GetCompactionWorkItems {
            respond_to: send,
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await
    }

    pub async fn get_peer_clients(&self) -> Result<Vec<Box<dyn PeerClient>>, RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::GetPeerClients {
            respond_to: send,
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await
    }
    
    pub async fn get_next_prefetch_checkpoints(&self, extension: Option<String>) -> Result<Vec<CheckpointDescriptor>, RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::GetNextPrefetchCheckpoints {
            respond_to: send,
            extensions: extension.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await
    }
}

pub(crate) static API_SERVICE_CLIENT: std::sync::LazyLock<ApiServiceClientHandle> = std::sync::LazyLock::new(|| ApiServiceClientHandle::new("localhost:7783".to_string()));
