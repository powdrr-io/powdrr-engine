use std::{collections::HashMap, error::Error};

use async_trait::async_trait;
use idgenerator::IdInstance;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot::{self, error::RecvError}};

use crate::{distributed_cache, elastic_search_ingest::CreateIndexTemplateBody, pipeline::PipelineDefinition, state_peers::SnapshotDescriptor};
use crate::elastic_search_index::create_index_inner;
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::schema_massager::PowdrrSchema;

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SpeedboatCSpeedInfo {
    pub table_name: String,
    pub files: Vec<String>,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SpeedboatCommitTableInfo {
    pub table_name: String,
    pub files: Vec<String>,
    pub sizes: Vec<u32>,
    pub schema: Option<PowdrrSchema>,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SpeedboatCommit {
    pub commit_type: String,
    pub type_files: Vec<SpeedboatCommitTableInfo>,
    pub compactions: Vec<String>,    
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct IcebergMetadata {
    pub snapshot_id: String,
    pub files: Vec<String>,
    pub column_names: Vec<String>,
    // per file, per column lower and upper bounds
    // TODO: this needs to be generalized to support bloom filters
    pub column_stats: Vec<(String, String)>,
    pub schemas: Vec<PowdrrSchema>,
    pub file_schemas: Vec<u64>,    
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct IcebergCommit {
    pub metadata: IcebergMetadata,
    pub compactions: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SpeedboatMetadata {
    pub files: Vec<String>,
    pub sizes: Vec<u32>,
    pub schemas: Vec<PowdrrSchema>,
    pub file_schemas: Vec<u64>,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct DeletesMetadata {
    pub files: Vec<String>,
    // TODO: bloom filters here?
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ExtensionFile {
    pub suffix: String,
    pub location: String,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ExtensionFileMetadata {
    pub data_file_location: String,
    pub extension_file_locations: Vec<ExtensionFile>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ExtensionMetadata {
    pub files: Vec<ExtensionFileMetadata>
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ExtensionCommit {
    pub extension: String,
    pub checkpoint_id: String,
    pub partial_metadata: ExtensionMetadata,
}


#[derive(Serialize, Clone)]
pub(crate) struct CompactionCommit {
    pub removed_file_locations: Vec<String>,
    pub snapshot_ids: Vec<String>,
    pub compaction_id: String
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct TableMetadataCheckpoint {
    pub table_name: String,
    pub checkpoint_id: String,
    pub iceberg_metadata: Option<IcebergMetadata>,
    pub speedboat_metadata: Option<SpeedboatMetadata>,
    pub deletes_metadata: Option<DeletesMetadata>,
    pub extension_metadata: Option<Vec<(String, ExtensionMetadata)>>,
    pub schema: PowdrrSchema,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ProposedCompaction {
    pub table_name: String,
    pub checkpoint_id: String,
    pub iceberg_metadata: Option<IcebergMetadata>,
    pub speedboat_metadata: Option<SpeedboatMetadata>,
    pub extension_metadata: Option<Vec<(String, ExtensionMetadata)>>,
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
    
    async fn speedboat_commit(&mut self, commit: &SpeedboatCommit, sync_index: bool) -> Result<(), Box<dyn Error>>;

    async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), Box<dyn Error>>;

    async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), Box<dyn Error>>;

    async fn compaction_commit(&mut self, table_name: &String, commit: &CompactionCommit) -> Result<(), Box<dyn Error>>;

    async fn get_latest_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, Box<dyn Error>>;

    async fn get_checkpoint(&mut self, snapshot: &SnapshotDescriptor) -> Result<TableMetadataCheckpoint, Box<dyn Error>>;

    async fn get_workable_tables(&mut self, work_type: &String) -> Result<Vec<TableMetadataCheckpoint>, Box<dyn Error>>;
}


struct ApiServiceClientActor {
    real: RealApiServiceClient,
    test: TestApiServiceClient,
    test_mode: bool,
    sync_index: bool,
    receiver: mpsc::Receiver<ApiServiceClientActorMessage>,
}

enum ApiServiceClientActorMessage {
    Testing {
        respond_to: oneshot::Sender<()>,
        sync_index: bool,
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
        snapshot: SnapshotDescriptor,
    },
    GetWorkableTables {
        respond_to: oneshot::Sender<Vec<TableMetadataCheckpoint>>,
        work_type: String,
    },    
}

unsafe impl Send for ApiServiceClientActorMessage {}


impl ApiServiceClientActor {
    fn new(receiver: mpsc::Receiver<ApiServiceClientActorMessage>, base_address: String) -> Self {
        ApiServiceClientActor {
            real: RealApiServiceClient::new(base_address),
            test: TestApiServiceClient::new(),
            test_mode: false,
            sync_index: false,           
            receiver: receiver,
        }
    }

    async fn handle_message(&mut self, msg: ApiServiceClientActorMessage) {
        match msg {
            ApiServiceClientActorMessage::Testing { respond_to, sync_index } => {
                self.test_mode = true;
                self.sync_index = sync_index;
                self.test.clear();
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
                    let _ = respond_to.send(self.test.speedboat_commit(&speedboat_commit, self.sync_index).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.speedboat_commit(&speedboat_commit, self.sync_index).await.expect("nope"));
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
            ApiServiceClientActorMessage::GetWorkableTables { work_type, respond_to } => {
                if self.test_mode {
                    let _ = respond_to.send(self.test.get_workable_tables(&work_type).await.expect("nope"));
                } else {
                    let _ = respond_to.send(self.real.get_workable_tables(&work_type).await.expect("nope"));
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

    async fn speedboat_commit(&mut self, commit: &SpeedboatCommit, _sync_index: bool) -> Result<(), Box<dyn Error>> {
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

    async fn get_checkpoint(&mut self, snapshot: &SnapshotDescriptor) -> Result<TableMetadataCheckpoint, Box<dyn Error>> {
        let base_address = &self.base_address;     
        let url = format!("{base_address}/api/v1/get_checkpoint/{}/{}", snapshot.table_name, snapshot.snapshot_id);
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

    async fn get_workable_tables(&mut self, work_type: &String) -> Result<Vec<TableMetadataCheckpoint>, Box<dyn Error>> {
        let base_address = &self.base_address;       
        let url = format!("{base_address}/api/v1/get_workable_tables/{work_type}"); 
        let resp = self.client.get(url).send().await;
        match resp {
            Ok(r) => {
                let json = r.json::<Vec<TableMetadataCheckpoint>>().await;
                match json {
                    Ok(j) => Ok(j),
                    Err(e) => Err(Box::new(e))
                }
            },
            Err(e) => Err(Box::new(e))
        }        
    }
}


fn do_remove(removed_files: &Vec<String>, files: &mut Vec<String>, sizes: &mut Vec<u32>) -> () {
    assert!(files.len() == sizes.len());
    let mut i = 0;
    while i < files.len() {
        if removed_files.contains(files.get(i).unwrap()) {
            files.remove(i);
            sizes.remove(i);
        } else {
            i += 1;
        }
    }
}


struct TestApiServiceClient {
    tables: HashMap<String, TableDescription>,
    // alias name -> table name
    table_aliases: HashMap<String, String>,
    table_templates: HashMap<String, CreateIndexTemplateBody>,
    pipelines: HashMap<String, PipelineDefinition>,
    lifetime_policies: HashMap<String, ILMPolicyDefinition>,
    latest_checkpoint_id: HashMap<String, String>,
    index_work_items: Vec<TableMetadataCheckpoint>,
    compact_work_items: Vec<TableMetadataCheckpoint>,
    compactions: HashMap<String, CompactionCommit>,
    checkpoints: HashMap<String, TableMetadataCheckpoint>,
}

impl TestApiServiceClient {
    fn new() -> Self {
        TestApiServiceClient{
            tables: HashMap::new(),
            table_aliases: HashMap::new(),
            table_templates: HashMap::new(),
            pipelines: HashMap::new(),
            lifetime_policies: HashMap::new(),
            latest_checkpoint_id: HashMap::new(),
            index_work_items: vec!(),
            compact_work_items: vec!(),
            compactions: HashMap::new(),
            checkpoints: HashMap::new(),
        }
    }

    fn clear(&mut self) -> () {
        distributed_cache::clear(&self.tables.keys().into_iter().map(|x|x.clone()).collect()).unwrap();
        self.tables.clear();
        self.table_aliases.clear();
        self.table_templates.clear();
        self.pipelines.clear();
        self.lifetime_policies.clear();
        self.latest_checkpoint_id.clear();
        self.index_work_items.clear();
        self.compact_work_items.clear();
        self.compactions.clear();
        self.checkpoints.clear();
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
        if !self.latest_checkpoint_id.contains_key(&metadata.table_name) {
            self.latest_checkpoint_id.insert(metadata.table_name.clone(), metadata.checkpoint_id.clone());            
        }
        self.checkpoints.insert(key, metadata.clone());
    }

    fn get_removed_files(&self, compactions: &Vec<String>) -> Vec<String> {
        let compactions_data: Vec<&CompactionCommit> = compactions.iter().map(|x| self.compactions.get(x).unwrap()).collect();
        compactions_data.iter().map(|x| x.removed_file_locations.clone()).flatten().collect()
    } 

    fn get_latest_checkpoint_sync(&mut self, table_name: &String, extensions: Option<String>) -> Option<String> {
        let key = match extensions {
            Some(e) => &format!("{}_{}", table_name, e).to_string(),
            None => table_name,
        };

        match self.latest_checkpoint_id.get(key) {
            Some(c) => Some(c.to_string()),
            None => None
        }
    }     

    fn set_latest_checkpoint(&mut self, table_name: &String, extensions: Option<&String>, checkpoint_id: &String) {
        let key = match extensions {
            Some(e) => &format!("{}_{}", table_name, e).to_string(),
            None => table_name,
        };        
        self.latest_checkpoint_id.insert(key.clone(), checkpoint_id.clone());        
    }

    async fn speedboat_commit_type_commit(&mut self, commit: &SpeedboatCommit, sync_index: bool) -> Result<(), Box<dyn std::error::Error>> {
        for table_info in commit.type_files.iter() {
            let latest_checkpoint = match self.get_latest_checkpoint_sync(&table_info.table_name, None) {
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
                        extension_metadata: None,
                        schema: PowdrrSchema{ fields: vec!() },
                    }
                },
            };

            let new_checkpoint_id = IdInstance::next_id().to_string();
            let new_speedboat_metadata = match &latest_checkpoint.speedboat_metadata {
                None => SpeedboatMetadata {
                    files: table_info.files.clone(),
                    sizes: table_info.sizes.clone(),
                    schemas: vec!(),
                    file_schemas: vec!(),
                },
                Some(existing) => {
                    let mut files = existing.files.clone();
                    let mut sizes = existing.sizes.clone();
                    let all_removed_files = self.get_removed_files(&commit.compactions);
                    do_remove(&all_removed_files, &mut files, &mut sizes);
                    files.extend(table_info.files.clone());
                    sizes.extend(table_info.sizes.clone());
                    SpeedboatMetadata {
                        files: files,
                        sizes: sizes,
                        schemas: vec!(),
                        file_schemas: vec!(),
                    }
                },
            };

            let total_records: u32 = new_speedboat_metadata.sizes.iter().sum();
            tracing::info!("Speedboat commit {} has {} files with {} records", new_checkpoint_id, new_speedboat_metadata.files.len(), total_records);
            for file in &new_speedboat_metadata.files {
                tracing::info!("Speedboat file {}", file)
            }

            let new_latest_checkpoint = TableMetadataCheckpoint {
                table_name: table_info.table_name.clone(),
                checkpoint_id: new_checkpoint_id.clone(),
                iceberg_metadata: latest_checkpoint.iceberg_metadata.clone(),
                speedboat_metadata: Some(new_speedboat_metadata.clone()),
                deletes_metadata: latest_checkpoint.deletes_metadata.clone(),
                extension_metadata: latest_checkpoint.extension_metadata.clone(),   
                schema: latest_checkpoint.schema.clone(),
            };

            self.checkpoints.insert(format!("{}_{}", &table_info.table_name, &new_checkpoint_id), new_latest_checkpoint.clone());

            if sync_index {
                self.create_index(&new_latest_checkpoint).await?;
            } else {
                self.index_work_items.push(new_latest_checkpoint.clone());
            }
            self.set_latest_checkpoint(&table_info.table_name, None, &new_checkpoint_id);

            // TODO: Re-enable compaction
            //if new_speedboat_metadata.files.len() >= 2 {
            //    self.compact_work_items.push(new_latest_checkpoint.clone());
            //}
        }
        Ok(())
    }    

    async fn speedboat_commit_type_delete(&mut self, commit: &SpeedboatCommit) -> Result<(), Box<dyn std::error::Error>> {
        for table_info in commit.type_files.iter() {
            let latest_checkpoint = match self.get_latest_checkpoint_sync(&table_info.table_name, None) {
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
                        extension_metadata: None,
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
                        files: files
                    }
                }
            };

            tracing::info!("Delete commit has {} files", new_deletes_metadata.files.len());
            for file in &new_deletes_metadata.files {
                tracing::info!("Delete file {}", file)
            }

            let new_latest_checkpoint = TableMetadataCheckpoint {
                table_name: table_info.table_name.clone(),
                checkpoint_id: new_checkpoint_id.clone(),
                iceberg_metadata: latest_checkpoint.iceberg_metadata.clone(),
                speedboat_metadata: latest_checkpoint.speedboat_metadata.clone(),
                deletes_metadata: Some(new_deletes_metadata),
                extension_metadata: latest_checkpoint.extension_metadata.clone(),  
                schema: latest_checkpoint.schema.clone(),
            };

            self.checkpoints.insert(format!("{}_{}", &table_info.table_name, &new_checkpoint_id), new_latest_checkpoint.clone());
            self.index_work_items.push(new_latest_checkpoint.clone());
            self.set_latest_checkpoint(&table_info.table_name, None, &new_checkpoint_id);
            // TODO: as we get more sophisticated compaction, we need these to trigger and remove things
            // completely.
        }
        Ok(())
    }    

    async fn create_index(&mut self, new_latest_checkpoint: &TableMetadataCheckpoint) -> Result<(), Box<dyn Error>> {
        let files = create_index_inner(new_latest_checkpoint).await?;
        self.extension_commit(
            &new_latest_checkpoint.table_name,
            &ExtensionCommit {
                extension: "es".to_string(),
                checkpoint_id: new_latest_checkpoint.checkpoint_id.clone(),
                partial_metadata: ExtensionMetadata {
                    files: files,
                },
            }
        ).await
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


    async fn speedboat_commit(&mut self, commit: &SpeedboatCommit, sync_index: bool) -> Result<(), Box<dyn std::error::Error>> {
        if commit.commit_type == "commit" {
            self.speedboat_commit_type_commit(commit, sync_index).await
        } else if commit.commit_type == "delete" {
            self.speedboat_commit_type_delete(commit).await
        } else {
            panic!("Unknown Speedboat commit type")
        }
    }

    async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), Box<dyn std::error::Error>> {
        let latest_checkpoint = match self.get_latest_checkpoint_sync(table_name, None) {
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
                    extension_metadata: None,
                    schema: PowdrrSchema{ fields: vec!() },
                }
            },
        };

        let new_checkpoint_id = IdInstance::next_id().to_string();
        let new_speedboat_metadata = match &latest_checkpoint.speedboat_metadata {
            None => None,
            Some(existing) => {
                let mut files = existing.files.clone();
                let mut sizes = existing.sizes.clone();
                let all_removed_files = self.get_removed_files(&iceberg_commit.compactions);
                do_remove(&all_removed_files, &mut files, &mut sizes);
                Some(SpeedboatMetadata {
                    files: files,
                    sizes: sizes,
                    schemas: vec!(),
                    file_schemas: vec!(),
                })
            },
        };     

        let new_latest_checkpoint = TableMetadataCheckpoint {
            table_name: table_name.clone(),
            checkpoint_id: new_checkpoint_id.clone(),
            iceberg_metadata: Some(iceberg_commit.metadata.clone()),
            speedboat_metadata: new_speedboat_metadata,
            deletes_metadata: latest_checkpoint.deletes_metadata.clone(),
            extension_metadata: latest_checkpoint.extension_metadata.clone(), 
            schema: latest_checkpoint.schema.clone(),
        };

        self.checkpoints.insert(format!("{}_{}", &table_name, &new_checkpoint_id), new_latest_checkpoint.clone());
        self.set_latest_checkpoint(&table_name, None, &new_checkpoint_id);
        Ok(())   
    }

    async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), Box<dyn std::error::Error>> {
        self.index_work_items.retain(|x| &x.table_name != table_name && x.checkpoint_id != commit.checkpoint_id);
        match self.get_latest_checkpoint_sync(table_name, Some("es".to_string())) {
            Some(latest) => {
                if commit.checkpoint_id > latest {
                    self.set_latest_checkpoint(table_name, Some(&"es".to_string()), &commit.checkpoint_id);
                }
            },
            None => {
                self.set_latest_checkpoint(table_name, Some(&"es".to_string()), &commit.checkpoint_id);
            },
        };
        Ok(())
    }

    async fn compaction_commit(&mut self, table_name: &String, commit: &CompactionCommit) -> Result<(), Box<dyn std::error::Error>> {
        self.compact_work_items.retain(|x| &x.table_name != table_name && !commit.snapshot_ids.contains(&x.checkpoint_id));
        self.compactions.insert(commit.compaction_id.clone(), commit.clone());
        // NOTE: this just notes what the compactor is saying. We don't generate the new checkpoint
        // until we see an iceberg commit with the new info.
        Ok(())
    }

    async fn get_latest_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, Box<dyn std::error::Error>> {
        Ok(self.get_latest_checkpoint_sync(table_name, extensions))
    }

    async fn get_checkpoint(&mut self, snapshot: &crate::state_peers::SnapshotDescriptor) -> Result<TableMetadataCheckpoint, Box<dyn std::error::Error>> {
        let key = format!("{}_{}", snapshot.table_name, snapshot.snapshot_id);
        match self.checkpoints.get(&key) {
            Some(v) => Ok(v.clone()),
            None => panic!("Oh no")
        }
    }

    async fn get_workable_tables(&mut self, work_type: &String) -> Result<Vec<TableMetadataCheckpoint>, Box<dyn std::error::Error>> {
        if work_type == "index" {
            Ok(self.index_work_items.clone())
        } else if work_type == "compact" {
            Ok(self.compact_work_items.clone())
        } else {
            Ok(vec!())
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

    pub async fn set_testing_mode(&self, sync_index: bool) -> () {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::Testing { 
            respond_to: send,
            sync_index: sync_index,
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

    pub async fn get_checkpoint(&self, snapshot: SnapshotDescriptor) -> Result<TableMetadataCheckpoint, RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::GetCheckpoint { 
            respond_to: send,
            snapshot: snapshot,
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await
    }

    pub async fn get_workable_tables(&self, work_type: &String) -> Result<Vec<TableMetadataCheckpoint>, RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = ApiServiceClientActorMessage::GetWorkableTables { 
            respond_to: send,
            work_type: work_type.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await        
    }
}

pub(crate) static API_SERVICE_CLIENT: std::sync::LazyLock<ApiServiceClientHandle> = std::sync::LazyLock::new(|| ApiServiceClientHandle::new("localhost:7783".to_string()));
