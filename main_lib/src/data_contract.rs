use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use idgenerator::IdInstance;
use serde::{Deserialize, Serialize};
use crate::peers::CheckpointDescriptor;
use crate::schema_massager::PowdrrSchema;
use crate::test_api::TestProcessingMode;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpeedboatCommitTableInfo {
    pub commit_type: String,
    pub table_name: String,
    pub files: Vec<String>,
    pub sizes: Vec<u64>,
    pub schema: Option<PowdrrSchema>,
}

impl SpeedboatCommitTableInfo {
    pub fn as_file_set_payload(&self) -> FileSetPayload {
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


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpeedboatCommit {
    pub type_files: Vec<SpeedboatCommitTableInfo>,
    pub compaction: Option<String,>
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


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IcebergMetadata {
    pub table_schema: PowdrrSchema,
    pub snapshot_id: Option<String>,
    pub files: FileSetPayload,
    pub column_names: Vec<String>,
    // per file, per column lower and upper bounds
    // TODO: this needs to be generalized to support bloom filters
    pub column_stats: Vec<(String, String)>,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IcebergCommit {
    pub metadata: IcebergMetadata,
    pub deletes_table_info: Option<SpeedboatCommitTableInfo>,
    pub compactions: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpeedboatMetadata {
    pub files: FileSetPayload
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeletesMetadata {
    pub files: Vec<String>,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExtensionFile {
    pub suffix: String,
    pub location: String,
}


pub type ExtensionFileMetadata = HashMap<String, Vec<ExtensionFile>>;


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExtensionCommit {
    pub id: String,
    pub extension: String,
    pub files: ExtensionFileMetadata
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CompactionCommit {
    pub removed_speedboat_files: Vec<String>,
    pub removed_delete_files: Vec<String>,
    pub parquet_file_name: String,
    pub compaction_id: String,
    pub checkpoint_id_to_replace: String,
    pub checkpoints_to_delete: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CleanupCommit {
    pub id: String,
    pub table_name: String,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableMetadataCheckpoint {
    pub table_name: String,
    pub original_checkpoint_id: Option<String>,
    pub checkpoint_id: String,
    pub iceberg_metadata: Option<IcebergMetadata>,
    pub speedboat_metadata: Option<SpeedboatMetadata>,
    pub deletes_metadata: Option<DeletesMetadata>,
    pub extension_metadata: HashMap<String, HashMap<String, Vec<ExtensionFile>>>,
    pub schema: PowdrrSchema,
}


impl TableMetadataCheckpoint {
    pub fn new(table_name: String, checkpoint_id: String, schema: PowdrrSchema) -> Self {
        TableMetadataCheckpoint {
            table_name,
            original_checkpoint_id: None,
            checkpoint_id,
            iceberg_metadata: None,
            speedboat_metadata: None,
            deletes_metadata: None,
            extension_metadata: HashMap::new(),
            schema,
        }
    }

    pub fn get_descriptor(&self) -> CheckpointDescriptor {
        CheckpointDescriptor {
            table_name: self.table_name.to_string(),
            checkpoint_id: self.checkpoint_id.to_string(),
            original_checkpoint_id: self.original_checkpoint_id.clone(),
        }
    }

    pub fn clone_and_apply(
        &self,
        speedboat_commits: &Vec<SpeedboatCommit>,
        iceberg_commits: &Vec<IcebergCommit>,
        extension_commits: &Vec<ExtensionCommit>,
        compactions_lookup: &HashMap<String, CompactionCommit>
    ) -> (Self, bool) {
        let mut new_table_metadata_checkpoint = self.clone();
        new_table_metadata_checkpoint.checkpoint_id = IdInstance::next_id().to_string();

        let mut changed = false;
        for speedboat_commit in speedboat_commits {
            changed = changed | new_table_metadata_checkpoint.apply_speedboat(speedboat_commit, compactions_lookup);
        }
        for iceberg_commit in iceberg_commits {
            changed = changed | new_table_metadata_checkpoint.apply_iceberg(iceberg_commit, compactions_lookup);
        }
        for extension_commit in extension_commits {
            changed = changed | new_table_metadata_checkpoint.add_coverage(extension_commit);
        }

        (new_table_metadata_checkpoint, changed)
    }

    pub fn apply_speedboat(&mut self, speedboat_commit: &SpeedboatCommit, compactions_lookup: &HashMap<String, CompactionCommit>) -> bool {
        if self.speedboat_metadata.is_none() {
            self.speedboat_metadata = Some(SpeedboatMetadata{ files: FileSetPayload::new() });
        }
        if self.deletes_metadata.is_none() {
            self.deletes_metadata = Some(DeletesMetadata{ files: vec!() });
        }
        for speedboat_commit_table_info in speedboat_commit.type_files.iter() {
            if speedboat_commit_table_info.commit_type == "delete" {
                self.deletes_metadata.as_mut().unwrap().files.extend(speedboat_commit_table_info.files.clone());
            } else if speedboat_commit_table_info.commit_type == "commit" || speedboat_commit_table_info.commit_type == "compaction" {
                self.speedboat_metadata.as_mut().unwrap().files.merge_inplace(&speedboat_commit_table_info.as_file_set_payload());
                if speedboat_commit_table_info.schema.is_some() {
                    self.schema.merge_from(speedboat_commit_table_info.schema.as_ref().unwrap());
                }
            } else {
                panic!("Unknown commit type");
            }
        }
        if speedboat_commit.compaction.is_some() {
            self.apply_compactions(&vec!(speedboat_commit.compaction.as_ref().unwrap().clone()), compactions_lookup);
        }

        true
    }

    pub fn apply_iceberg(&mut self, iceberg_commit: &IcebergCommit, compactions_lookup: &HashMap<String, CompactionCommit>) -> bool {
        self.iceberg_metadata = Some(iceberg_commit.metadata.clone());
        if iceberg_commit.deletes_table_info.is_some() {
            if self.deletes_metadata.is_none() {
                self.deletes_metadata = Some(DeletesMetadata{ files: vec!() });
            }
            self.deletes_metadata.as_mut().unwrap().files.extend(iceberg_commit.deletes_table_info.as_ref().unwrap().files.clone());
        }
        self.schema.merge_from(&self.iceberg_metadata.as_mut().unwrap().table_schema);
        self.apply_compactions(&iceberg_commit.compactions, compactions_lookup);

        true
    }

    fn apply_compactions(&mut self, compactions: &Vec<String>, compactions_lookup: &HashMap<String, CompactionCommit>) -> () {
        let (removed_speedboat, removed_deletes) = Self::get_removed_files(compactions, compactions_lookup);

        match self.speedboat_metadata.as_mut() {
            Some(speedboat) => {
                speedboat.files.remove(&removed_speedboat);
            },
            None => ()
        };

        match self.deletes_metadata.as_mut() {
            Some(deletes) => {
                deletes.files.retain(|x|!removed_deletes.contains(x));
            },
            None => ()
        };

        for metadata in self.extension_metadata.values_mut() {
            metadata.retain(|key, _|!removed_speedboat.contains(key))
        }
    }

    pub(crate) fn apply_compaction_for_replacement(&mut self, compaction: &CompactionCommit, iceberg_metadata: &IcebergMetadata) -> () {
        assert!(self.speedboat_metadata.is_some());
        self.speedboat_metadata.as_mut().unwrap().files.remove(&compaction.removed_speedboat_files);
        if self.deletes_metadata.is_some() {
            self.deletes_metadata.as_mut().unwrap().files.retain(|x| !compaction.removed_delete_files.contains(x));
        }
        self.extension_metadata.retain(|key, _|!compaction.removed_speedboat_files.contains(key));
        let file_payload = iceberg_metadata.files.select(&compaction.parquet_file_name);
        if self.iceberg_metadata.is_none() {
            self.iceberg_metadata = Some(IcebergMetadata {
                table_schema: iceberg_metadata.table_schema.clone(),
                snapshot_id: None,
                files: file_payload,
                column_names: vec![],
                column_stats: vec![],
            });
        } else {
            self.iceberg_metadata.as_mut().unwrap().files.merge(&file_payload);
        }
    }

    fn get_removed_files(compactions: &Vec<String>, compactions_lookup: &HashMap<String, CompactionCommit>) -> (Vec<String>, Vec<String>) {
        let compactions_data: Vec<&CompactionCommit> = compactions.iter().map(|x| compactions_lookup.get(x).unwrap()).collect();
        (
            compactions_data.iter().map(|x| x.removed_speedboat_files.clone()).flatten().collect(),
            compactions_data.iter().map(|x| x.removed_delete_files.clone()).flatten().collect(),
        )
    }

    pub fn fully_covered_for_extension(&self, extension_name: &String) -> bool {
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

    pub fn add_coverage(&mut self, extension_commit: &ExtensionCommit) -> bool {
        let existing_extension_metadata_map = self.extension_metadata.get(&extension_commit.extension).map_or(HashMap::new(), |x| x.clone());

        if !self.extension_metadata.contains_key(&extension_commit.extension) {
            self.extension_metadata.insert(extension_commit.extension.clone(), HashMap::new());
        }

        let mut changed = false;
        match &self.iceberg_metadata {
            Some(im) => {
                for file_path in im.files.file_paths.iter() {
                    if extension_commit.files.contains_key(file_path) && !existing_extension_metadata_map.contains_key(file_path) {
                        self.extension_metadata.get_mut(&extension_commit.extension).unwrap().insert(file_path.clone(), extension_commit.files[file_path].clone());
                        changed = true;
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
                        changed = true;
                    }
                }
            },
            None => {}
        };

        changed
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableDescription {
    pub name: String,
    pub tags: HashMap<String, String>
}

impl TableDescription {
    pub fn from_create_table(create_table: &CreateTable) -> Self {
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

    pub fn validate(&self) -> () {
        let max_index = self.schemas.len() as u64;
        for index in self.file_schemas.iter() {
            assert!(index < &max_index);
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

        cloned.merge_inplace(to_merge);
        cloned
    }

    pub fn merge_inplace(&mut self, other: &FileSetPayload) -> () {
        for file_desc in other.as_file_tuples().iter() {
            self.add(file_desc);
        }
    }

    pub fn add(&mut self, file_descriptor: &FileDescriptor) -> () {
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

    pub(crate) fn select(&self, file_name: &String) -> FileSetPayload {
        // Note: the file name is not the full path of the file

        for (i, file_path) in self.file_paths.iter().enumerate() {
            if file_path.contains(file_name) {
                return FileSetPayload {
                    file_paths: vec!(file_path.clone()),
                    sizes: vec!(self.sizes[i]),
                    file_schemas: vec!(0),
                    schemas: vec!(self.schemas[self.file_schemas[i] as usize].clone()),
                }
            }
        }
        assert!(false, "File not found");
        // Not reached
        FileSetPayload::new()
    }
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExtensionWorkItem {
    pub id: String,
    pub extension_type: String,
    pub table_name: String,
    pub table_schema: PowdrrSchema,
    pub speedboat_files: FileSetPayload,
    pub iceberg_files: FileSetPayload,
}

impl ExtensionWorkItem {
    pub fn clear(&mut self) -> () {
        self.speedboat_files.clear();
        self.iceberg_files.clear();
    }

    pub fn has_work(&self) -> bool {
        self.speedboat_files.len() > 0 || self.iceberg_files.len() > 0
    }

    pub fn merge_speedboat(&mut self, commit: &SpeedboatCommit) -> () {
        for speedboat_commit_table_info in commit.type_files.iter() {
            if speedboat_commit_table_info.commit_type == "commit" || speedboat_commit_table_info.commit_type == "compaction" {
                self.speedboat_files.merge_inplace(&speedboat_commit_table_info.as_file_set_payload());
            }
        }
    }

    pub fn merge_iceberg(&mut self, commit: &IcebergCommit) -> () {
        self.iceberg_files.merge_inplace(&commit.metadata.files);
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompactionWorkItem {
    pub id: String,
    pub table_schema: PowdrrSchema,
    pub speedboat_files: FileSetPayload,
    pub delete_files: Vec<String>,
    pub checkpoint_id_to_replace: String,
    pub checkpoints_to_delete: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct CompactionWorkItemTracker {
    pub(crate) in_progress: bool,
    pub(crate) work_item: CompactionWorkItem
}


impl CompactionWorkItem {
    pub fn from_checkpoint(checkpoint: &TableMetadataCheckpoint, checkpoints_to_delete: &Vec<String>) -> Self {
        assert!(checkpoint.speedboat_metadata.is_some());
        CompactionWorkItem {
            id: IdInstance::next_id().to_string(),
            table_schema: checkpoint.schema.clone(),
            speedboat_files: checkpoint.speedboat_metadata.as_ref().unwrap().files.clone(),
            delete_files: checkpoint.deletes_metadata.as_ref().map_or(vec![], |x| x.files.clone()),
            checkpoint_id_to_replace: checkpoint.checkpoint_id.clone(),
            checkpoints_to_delete: checkpoints_to_delete.clone(),
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CleanupWorkItem {
    pub id: String,
    pub table_name: String,
    pub files_to_delete: Vec<String>,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AliasInfo {
    pub is_hidden: bool,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum StringOrBool {
    Bool(bool),
    String(String),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetaInfo {
    #[serde(rename = "migrationMappingPropertyHashes")]
    migration_mapping_property_hashes: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PropertyInfo {
    #[serde(rename = "type")]
    pub type_name: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    pub dynamic: Option<StringOrBool>,
    pub properties: Option<HashMap<String, PropertyInfo>>,
    pub fields: Option<HashMap<String, PropertyInfo>>,
    #[serde(default)]
    pub ignore_above: u32,
    pub scaling_factor: Option<u32>,
}



#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Mappings {
    pub dynamic: StringOrBool,
    pub _meta: Option<MetaInfo>,
    pub properties: HashMap<String, PropertyInfo>,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexMappingSettings {
    pub total_fields: IndexMappingFieldSettings,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexMappingFieldSettings {
    pub limit: Option<u32>
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexSettings {
    pub number_of_shards: Option<u32>,
    pub number_of_replicas: Option<u32>,
    pub auto_expand_replicas: Option<String>,
    pub refresh_interval: Option<String>,
    pub priority: Option<u32>,
    pub mapping: Option<IndexMappingSettings>,
}

#[derive(Serialize, Debug)]
pub struct CreateIndexResult {
    pub acknowledged: bool,
    pub shards_acknowledged: bool,
    pub index: String,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateIndexSettings {
    pub index: IndexSettings
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum CreateIndexSettingsOption {
    Indirect(CreateIndexSettings),
    Direct(IndexSettings),
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateIndexBody {
    pub aliases: Option<HashMap<String, AliasInfo>>,
    pub mappings: Option<Mappings>,
    pub settings: Option<CreateIndexSettingsOption>,
}

impl CreateIndexBody {
    pub(crate) fn parse(content: &String) -> Result<Self, serde_json::Error> {
        if content.len() == 0 {
            Ok(CreateIndexBody{ aliases: None, mappings: None, settings: None })
        } else {
            serde_json::from_str(content)
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateIndexTemplateBody {
    #[serde(default)]
    pub index_patterns: Vec<String>,
    pub priority: Option<u32>,
    pub version: Option<u32>,
    pub template: CreateIndexBody,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ServiceImplType {
    Ephemeral,
    DynamoDb,
    TestingDynamoDb,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceMode {
    pub impl_type: ServiceImplType,
}


impl ServiceMode {
    pub fn as_testing_mode(&self) -> TestProcessingMode {
        // TODO: does this need to be configurable?
        TestProcessingMode::default()
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LicenseType {
    Free,
    Pro,
}


pub const ACCESS_KEY_HEADER_KEY: &str = "ACCESS_KEY";
pub const SECRET_KEY_HEADER_KEY: &str = "SECRET_KEY";


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrgCreds {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub nickname: Option<String>,
}


impl OrgCreds {
    #[allow(dead_code)]
    fn new(nickname: Option<String>) -> Self {
        // TODO: Make these cryptographic random
        OrgCreds {
            access_key_id: IdInstance::next_id().to_string(),
            secret_access_key: IdInstance::next_id().to_string(),
            nickname,
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrgSettings {
    pub org_id: String,
    pub license_type: LicenseType,
    pub creds: Vec<OrgCreds>,
}

impl OrgSettings {
    pub(crate) fn to_org_info(&self) -> OrgInfo {
        OrgInfo {
            org_id: self.org_id.clone(),
            license_type: self.license_type.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrgInfo {
    pub org_id: String,
    pub license_type: LicenseType,
}
