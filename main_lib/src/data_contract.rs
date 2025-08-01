use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use serde::{Deserialize, Serialize};
use crate::schema_massager::PowdrrSchema;

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

#[derive(Serialize, Deserialize, Clone)]
struct AliasInfo {
    is_hidden: bool
}


#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum StringOrBool {
    Bool(bool),
    String(String),
}

#[derive(Serialize, Deserialize, Clone)]
struct MetaInfo {
    #[serde(rename = "migrationMappingPropertyHashes")]
    migration_mapping_property_hashes: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct PropertyInfo {
    #[serde(rename = "type")]
    type_name: Option<String>,
    #[serde(default)]
    enabled: bool,
    dynamic: Option<StringOrBool>,
    properties: Option<HashMap<String, PropertyInfo>>,
    fields: Option<HashMap<String, PropertyInfo>>,
    #[serde(default)]
    ignore_above: u32,
    scaling_factor: Option<u32>,
}



#[derive(Serialize, Deserialize, Clone)]
struct Mappings {
    dynamic: StringOrBool,
    _meta: Option<MetaInfo>,
    properties: HashMap<String, PropertyInfo>,
}


#[derive(Serialize, Deserialize, Clone)]
struct IndexMappingSettings {
    total_fields: IndexMappingFieldSettings,
}

#[derive(Serialize, Deserialize, Clone)]
struct IndexMappingFieldSettings {
    limit: Option<u32>
}


#[derive(Serialize, Deserialize, Clone)]
struct IndexSettings {
    number_of_shards: Option<u32>,
    number_of_replicas: Option<u32>,
    auto_expand_replicas: Option<String>,
    refresh_interval: Option<String>,
    priority: Option<u32>,
    mapping: Option<IndexMappingSettings>,
}

#[derive(Serialize, Deserialize, Clone)]
struct CreateIndexSettings {
    index: IndexSettings
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum CreateIndexSettingsOption {
    Indirect(CreateIndexSettings),
    Direct(IndexSettings),
}


#[derive(Serialize, Deserialize, Clone)]
pub struct CreateIndexBody {
    aliases: Option<HashMap<String, AliasInfo>>,
    mappings: Option<Mappings>,
    settings: Option<CreateIndexSettingsOption>,
}

impl CreateIndexBody {
    fn parse(content: &String) -> Result<Self, serde_json::Error> {
        if content.len() == 0 {
            Ok(CreateIndexBody{ aliases: None, mappings: None, settings: None })
        } else {
            serde_json::from_str(content)
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CreateIndexTemplateBody {
    #[serde(default)]
    index_patterns: Vec<String>,
    priority: Option<u32>,
    version: Option<u32>,
    template: CreateIndexBody,
}
