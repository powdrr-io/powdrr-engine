use crate::checkpoint_descriptor::CheckpointDescriptor;
use crate::schema_massager::PowdrrSchema;
use crate::test_api::TestProcessingMode;
use idgenerator::IdInstance;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpeedboatSegmentFile {
    pub segment_id: String,
    pub file_path: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpeedboatCommitTableInfo {
    pub commit_type: String,
    pub table_name: String,
    #[serde(default)]
    pub segments: Vec<SpeedboatSegmentFile>,
    pub files: Vec<String>,
    pub sizes: Vec<u64>,
    pub schema: Option<PowdrrSchema>,
}

impl SpeedboatCommitTableInfo {
    fn synthetic_legacy_segment_id(file_path: &str) -> String {
        let mut hasher = DefaultHasher::new();
        file_path.hash(&mut hasher);
        format!("legacy-{:x}", hasher.finish())
    }

    pub fn from_segments(
        commit_type: String,
        table_name: String,
        segments: Vec<SpeedboatSegmentFile>,
        schema: Option<PowdrrSchema>,
    ) -> Self {
        Self {
            commit_type,
            table_name,
            files: segments
                .iter()
                .map(|segment| segment.file_path.clone())
                .collect(),
            sizes: segments.iter().map(|segment| segment.size).collect(),
            segments,
            schema,
        }
    }

    pub fn segment_files(&self) -> Vec<SpeedboatSegmentFile> {
        if !self.segments.is_empty() {
            return self.segments.clone();
        }

        self.files
            .iter()
            .zip(self.sizes.iter())
            .map(|(file_path, size)| SpeedboatSegmentFile {
                segment_id: Self::synthetic_legacy_segment_id(file_path),
                file_path: file_path.clone(),
                size: *size,
            })
            .collect()
    }

    pub fn as_file_set_payload(&self) -> FileSetPayload {
        let segments = self.segment_files();
        FileSetPayload {
            file_paths: segments
                .iter()
                .map(|segment| segment.file_path.clone())
                .collect(),
            sizes: segments.iter().map(|segment| segment.size).collect(),
            schemas: vec![self.schema.as_ref().unwrap().clone()],
            file_schemas: segments.iter().map(|_| 0).collect(),
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
    pub compaction: Option<String>,
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IcebergColumnStats {
    pub field_id: i32,
    pub field_name: String,
    #[serde(default)]
    pub null_count: Option<u64>,
    #[serde(default)]
    pub lower_bound: Option<Value>,
    #[serde(default)]
    pub upper_bound: Option<Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IcebergPartitionField {
    pub source_field_id: i32,
    pub source_field_name: String,
    pub field_id: i32,
    pub field_name: String,
    pub transform: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IcebergPartitionValue {
    pub source_field_name: String,
    pub field_name: String,
    pub transform: String,
    #[serde(default)]
    pub value: Option<Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IcebergSortField {
    pub source_field_id: i32,
    pub source_field_name: String,
    pub transform: String,
    #[serde(default)]
    pub descending: bool,
    #[serde(default)]
    pub nulls_first: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IcebergAccessArtifact {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub exact: bool,
    #[serde(default)]
    pub supports_eq: bool,
    #[serde(default)]
    pub supports_range: bool,
    #[serde(default)]
    pub supports_order: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IcebergRowGroupStats {
    pub row_group_index: usize,
    #[serde(default)]
    pub record_count: Option<u64>,
    #[serde(default)]
    pub compressed_bytes: u64,
    #[serde(default)]
    pub page_index_present: bool,
    #[serde(default)]
    pub bloom_filter_present: bool,
    #[serde(default)]
    pub columns: Vec<IcebergColumnStats>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IcebergFileStats {
    pub file_path: String,
    #[serde(default)]
    pub record_count: Option<u64>,
    #[serde(default)]
    pub columns: Vec<IcebergColumnStats>,
    #[serde(default)]
    pub partition_values: Vec<IcebergPartitionValue>,
    #[serde(default)]
    pub row_groups: Vec<IcebergRowGroupStats>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IcebergMetadata {
    pub table_schema: PowdrrSchema,
    pub snapshot_id: Option<String>,
    pub files: FileSetPayload,
    #[serde(default)]
    pub partition_spec: Vec<IcebergPartitionField>,
    #[serde(default)]
    pub sort_order: Vec<IcebergSortField>,
    pub column_names: Vec<String>,
    // per file, per column lower and upper bounds
    // TODO: this needs to be generalized to support bloom filters
    pub column_stats: Vec<(String, String)>,
    #[serde(default)]
    pub access_artifacts: Vec<IcebergAccessArtifact>,
    #[serde(default)]
    pub file_stats: Vec<IcebergFileStats>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IcebergCommit {
    pub metadata: IcebergMetadata,
    pub deletes_table_info: Option<SpeedboatCommitTableInfo>,
    pub compactions: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpeedboatMetadata {
    pub files: FileSetPayload,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeletesMetadata {
    pub files: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ExtensionFile {
    pub suffix: String,
    pub location: String,
}

pub type ExtensionFileMetadata = HashMap<String, Vec<ExtensionFile>>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExtensionCommit {
    pub id: String,
    pub extension: String,
    pub files: ExtensionFileMetadata,
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
        compactions_lookup: &HashMap<String, CompactionCommit>,
    ) -> (Self, bool) {
        let mut new_table_metadata_checkpoint = self.clone();
        new_table_metadata_checkpoint.checkpoint_id = IdInstance::next_id().to_string();

        let mut changed = false;
        for speedboat_commit in speedboat_commits {
            changed = changed
                | new_table_metadata_checkpoint
                    .apply_speedboat(speedboat_commit, compactions_lookup);
        }
        for iceberg_commit in iceberg_commits {
            changed = changed
                | new_table_metadata_checkpoint.apply_iceberg(iceberg_commit, compactions_lookup);
        }
        for extension_commit in extension_commits {
            changed = changed | new_table_metadata_checkpoint.add_coverage(extension_commit);
        }

        (new_table_metadata_checkpoint, changed)
    }

    pub fn apply_speedboat(
        &mut self,
        speedboat_commit: &SpeedboatCommit,
        compactions_lookup: &HashMap<String, CompactionCommit>,
    ) -> bool {
        if self.speedboat_metadata.is_none() {
            self.speedboat_metadata = Some(SpeedboatMetadata {
                files: FileSetPayload::new(),
            });
        }
        if self.deletes_metadata.is_none() {
            self.deletes_metadata = Some(DeletesMetadata { files: vec![] });
        }
        for speedboat_commit_table_info in speedboat_commit.type_files.iter() {
            if speedboat_commit_table_info.commit_type == "delete" {
                self.deletes_metadata
                    .as_mut()
                    .unwrap()
                    .files
                    .extend(speedboat_commit_table_info.files.clone());
            } else if speedboat_commit_table_info.commit_type == "commit"
                || speedboat_commit_table_info.commit_type == "compaction"
            {
                self.speedboat_metadata
                    .as_mut()
                    .unwrap()
                    .files
                    .merge_inplace(&speedboat_commit_table_info.as_file_set_payload());
                if speedboat_commit_table_info.schema.is_some() {
                    self.schema
                        .merge_from(speedboat_commit_table_info.schema.as_ref().unwrap());
                }
            } else {
                panic!("Unknown commit type");
            }
        }
        if speedboat_commit.compaction.is_some() {
            self.apply_compactions(
                &vec![speedboat_commit.compaction.as_ref().unwrap().clone()],
                compactions_lookup,
            );
        }

        true
    }

    pub fn apply_iceberg(
        &mut self,
        iceberg_commit: &IcebergCommit,
        compactions_lookup: &HashMap<String, CompactionCommit>,
    ) -> bool {
        self.iceberg_metadata = Some(iceberg_commit.metadata.clone());
        if iceberg_commit.deletes_table_info.is_some() {
            if self.deletes_metadata.is_none() {
                self.deletes_metadata = Some(DeletesMetadata { files: vec![] });
            }
            self.deletes_metadata.as_mut().unwrap().files.extend(
                iceberg_commit
                    .deletes_table_info
                    .as_ref()
                    .unwrap()
                    .files
                    .clone(),
            );
        }
        self.schema
            .merge_from(&self.iceberg_metadata.as_mut().unwrap().table_schema);
        self.apply_compactions(&iceberg_commit.compactions, compactions_lookup);
        self.retain_current_extension_metadata();

        true
    }

    fn apply_compactions(
        &mut self,
        compactions: &Vec<String>,
        compactions_lookup: &HashMap<String, CompactionCommit>,
    ) -> () {
        let (removed_speedboat, removed_deletes) =
            Self::get_removed_files(compactions, compactions_lookup);

        match self.speedboat_metadata.as_mut() {
            Some(speedboat) => {
                speedboat.files.remove(&removed_speedboat);
            }
            None => (),
        };

        match self.deletes_metadata.as_mut() {
            Some(deletes) => {
                deletes.files.retain(|x| !removed_deletes.contains(x));
            }
            None => (),
        };

        for metadata in self.extension_metadata.values_mut() {
            metadata.retain(|key, _| !removed_speedboat.contains(key))
        }
    }

    pub fn apply_compaction_for_replacement(
        &mut self,
        compaction: &CompactionCommit,
        iceberg_metadata: &IcebergMetadata,
    ) -> () {
        assert!(self.speedboat_metadata.is_some());
        self.speedboat_metadata
            .as_mut()
            .unwrap()
            .files
            .remove(&compaction.removed_speedboat_files);
        if self.deletes_metadata.is_some() {
            self.deletes_metadata
                .as_mut()
                .unwrap()
                .files
                .retain(|x| !compaction.removed_delete_files.contains(x));
        }
        let file_payload = iceberg_metadata.files.select(&compaction.parquet_file_name);
        let file_stats = iceberg_metadata.select_file_stats(&compaction.parquet_file_name);
        if self.iceberg_metadata.is_none() {
            self.iceberg_metadata = Some(IcebergMetadata {
                table_schema: iceberg_metadata.table_schema.clone(),
                snapshot_id: None,
                files: file_payload,
                partition_spec: iceberg_metadata.partition_spec.clone(),
                sort_order: iceberg_metadata.sort_order.clone(),
                column_names: vec![],
                column_stats: vec![],
                access_artifacts: iceberg_metadata.access_artifacts.clone(),
                file_stats,
            });
        } else {
            let metadata = self.iceberg_metadata.as_mut().unwrap();
            metadata.files.merge_inplace(&file_payload);
            if metadata.partition_spec.is_empty() {
                metadata.partition_spec = iceberg_metadata.partition_spec.clone();
            }
            if metadata.sort_order.is_empty() {
                metadata.sort_order = iceberg_metadata.sort_order.clone();
            }
            if metadata.access_artifacts.is_empty() {
                metadata.access_artifacts = iceberg_metadata.access_artifacts.clone();
            }
            for stat in file_stats {
                if !metadata
                    .file_stats
                    .iter()
                    .any(|existing| existing.file_path == stat.file_path)
                {
                    metadata.file_stats.push(stat);
                }
            }
        }
        self.retain_current_extension_metadata();
    }

    fn get_removed_files(
        compactions: &Vec<String>,
        compactions_lookup: &HashMap<String, CompactionCommit>,
    ) -> (Vec<String>, Vec<String>) {
        let compactions_data: Vec<&CompactionCommit> = compactions
            .iter()
            .map(|x| compactions_lookup.get(x).unwrap())
            .collect();
        (
            compactions_data
                .iter()
                .map(|x| x.removed_speedboat_files.clone())
                .flatten()
                .collect(),
            compactions_data
                .iter()
                .map(|x| x.removed_delete_files.clone())
                .flatten()
                .collect(),
        )
    }

    pub fn fully_covered_for_extension(&self, extension_name: &String) -> bool {
        let current_file_paths = self.current_file_paths();
        let total_num_files = current_file_paths.len();

        let total_num_extension_files =
            self.extension_metadata.get(extension_name).map_or(0, |x| {
                x.keys()
                    .filter(|file_path| current_file_paths.contains(*file_path))
                    .count()
            });

        let size_check_method = total_num_files == total_num_extension_files;

        assert_eq!(
            size_check_method,
            self.validate_fully_covered_for_extension(extension_name)
        );

        size_check_method
    }

    pub fn retain_current_extension_metadata(&mut self) {
        let current_file_paths = self.current_file_paths();
        for metadata in self.extension_metadata.values_mut() {
            metadata.retain(|file_path, _| current_file_paths.contains(file_path));
        }
    }

    fn validate_fully_covered_for_extension(&self, extension_name: &String) -> bool {
        let extension_metadata_map = self
            .extension_metadata
            .get(extension_name)
            .map_or(HashMap::new(), |x| x.clone());

        match &self.iceberg_metadata {
            Some(im) => {
                for file_path in im.files.file_paths.iter() {
                    if !extension_metadata_map.contains_key(file_path) {
                        return false;
                    }
                }
            }
            None => {}
        };

        match &self.speedboat_metadata {
            Some(im) => {
                for file_path in im.files.file_paths.iter() {
                    if !extension_metadata_map.contains_key(file_path) {
                        return false;
                    }
                }
            }
            None => {}
        };

        true
    }

    fn current_file_paths(&self) -> HashSet<String> {
        let mut file_paths = HashSet::new();

        if let Some(iceberg_metadata) = &self.iceberg_metadata {
            file_paths.extend(iceberg_metadata.files.file_paths.iter().cloned());
        }

        if let Some(speedboat_metadata) = &self.speedboat_metadata {
            file_paths.extend(speedboat_metadata.files.file_paths.iter().cloned());
        }

        file_paths
    }

    pub fn add_coverage(&mut self, extension_commit: &ExtensionCommit) -> bool {
        let existing_extension_metadata_map = self
            .extension_metadata
            .get(&extension_commit.extension)
            .map_or(HashMap::new(), |x| x.clone());

        if !self
            .extension_metadata
            .contains_key(&extension_commit.extension)
        {
            self.extension_metadata
                .insert(extension_commit.extension.clone(), HashMap::new());
        }

        let mut changed = false;
        match &self.iceberg_metadata {
            Some(im) => {
                for file_path in im.files.file_paths.iter() {
                    if extension_commit.files.contains_key(file_path)
                        && !existing_extension_metadata_map.contains_key(file_path)
                    {
                        self.extension_metadata
                            .get_mut(&extension_commit.extension)
                            .unwrap()
                            .insert(file_path.clone(), extension_commit.files[file_path].clone());
                        changed = true;
                    }
                }
            }
            None => {}
        };

        match &self.speedboat_metadata {
            Some(im) => {
                for file_path in im.files.file_paths.iter() {
                    if extension_commit.files.contains_key(file_path)
                        && !existing_extension_metadata_map.contains_key(file_path)
                    {
                        self.extension_metadata
                            .get_mut(&extension_commit.extension)
                            .unwrap()
                            .insert(file_path.clone(), extension_commit.files[file_path].clone());
                        changed = true;
                    }
                }
            }
            None => {}
        };

        changed
    }
}

impl IcebergMetadata {
    pub(crate) fn select_file_stats(&self, file_name: &String) -> Vec<IcebergFileStats> {
        self.file_stats
            .iter()
            .filter(|stats| stats.file_path.contains(file_name))
            .cloned()
            .collect()
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ProposedCompaction {
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
    #[serde(default)]
    pub serving: Option<ServingTableConfig>,
    #[serde(default)]
    pub dynamodb: Option<DynamoDbTableConfig>,
    #[serde(default)]
    pub mongodb: Option<MongoDbTableConfig>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableDescription {
    pub name: String,
    pub tags: HashMap<String, String>,
    #[serde(default)]
    pub serving: Option<ServingTableConfig>,
    #[serde(default)]
    pub dynamodb: Option<DynamoDbTableConfig>,
    #[serde(default)]
    pub mongodb: Option<MongoDbTableConfig>,
}

impl TableDescription {
    pub fn from_create_table(create_table: &CreateTable) -> Self {
        TableDescription {
            name: create_table.name.clone(),
            tags: create_table.tags.clone(),
            serving: create_table.serving.clone(),
            dynamodb: create_table.dynamodb.clone(),
            mongodb: create_table.mongodb.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct ServingTableConfig {
    #[serde(default)]
    pub patterns: Vec<ServingPattern>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServingAggregateMeasure {
    pub function: String,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServingAggregateSpec {
    #[serde(default)]
    pub group_by: Vec<String>,
    #[serde(default)]
    pub measures: Vec<ServingAggregateMeasure>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServingPattern {
    pub name: String,
    #[serde(default)]
    pub eq_fields: Vec<String>,
    #[serde(default)]
    pub range_field: Option<String>,
    #[serde(default)]
    pub order_field: Option<String>,
    #[serde(default)]
    pub descending: bool,
    #[serde(default)]
    pub max_limit: Option<u64>,
    #[serde(default)]
    pub projection: Option<Vec<String>>,
    #[serde(default)]
    pub aggregate: Option<ServingAggregateSpec>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DynamoDbGlobalSecondaryIndexConfig {
    pub name: String,
    pub partition_key: String,
    #[serde(default)]
    pub sort_key: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DynamoDbLocalSecondaryIndexConfig {
    pub name: String,
    pub sort_key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DynamoDbTableConfig {
    pub partition_key: String,
    #[serde(default)]
    pub sort_key: Option<String>,
    #[serde(default)]
    pub local_secondary_indexes: Vec<DynamoDbLocalSecondaryIndexConfig>,
    #[serde(default)]
    pub global_secondary_indexes: Vec<DynamoDbGlobalSecondaryIndexConfig>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MongoDbIdConfig {
    pub field: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MongoDbTableConfig {
    pub enabled: bool,
    pub database: String,
    pub collection: String,
    pub id: MongoDbIdConfig,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AddAlias {
    pub table_name: String,
    pub alias: String,
}

impl FileSetPayload {
    pub fn new() -> Self {
        FileSetPayload {
            file_paths: vec![],
            sizes: vec![],
            file_schemas: vec![],
            schemas: vec![],
        }
    }

    pub fn validate(&self) -> () {
        let max_index = self.schemas.len() as u64;
        for index in self.file_schemas.iter() {
            assert!(index < &max_index);
        }
    }

    pub fn single(file_path: String, size: u64, schema: PowdrrSchema) -> Self {
        FileSetPayload {
            file_paths: vec![file_path],
            sizes: vec![size],
            file_schemas: vec![0],
            schemas: vec![schema],
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
        self.file_paths
            .iter()
            .zip(self.sizes.iter())
            .zip(self.file_schemas.iter())
            .map(|((path, size), schema_index)| FileDescriptor {
                file_path: path.clone(),
                schema: self.schemas[*schema_index as usize].clone(),
                size: *size,
            })
            .collect()
    }

    fn selected_file(file_path: &String, index: u64, num: u64) -> bool {
        // TODO: validate this is a stable hash (aka it will give the same value on every machine on every run)
        let mut hasher = DefaultHasher::new();
        file_path.hash(&mut hasher);
        let hash_val = hasher.finish();
        hash_val % num == index
    }

    pub fn as_selected_tuples(&self, index: u64, num: u64) -> Vec<FileDescriptor> {
        self.as_file_tuples()
            .iter()
            .filter(|x| Self::selected_file(&x.file_path, index, num))
            .cloned()
            .collect()
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
        if let Some(index) = self
            .schemas
            .iter()
            .position(|item| item == &file_descriptor.schema)
        {
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
                    file_paths: vec![file_path.clone()],
                    sizes: vec![self.sizes[i]],
                    file_schemas: vec![0],
                    schemas: vec![self.schemas[self.file_schemas[i] as usize].clone()],
                };
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
            if speedboat_commit_table_info.commit_type == "commit"
                || speedboat_commit_table_info.commit_type == "compaction"
            {
                self.speedboat_files
                    .merge_inplace(&speedboat_commit_table_info.as_file_set_payload());
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompactionWorkItemTracker {
    pub in_progress: bool,
    pub work_item: CompactionWorkItem,
}

impl CompactionWorkItem {
    pub fn from_checkpoint(
        checkpoint: &TableMetadataCheckpoint,
        checkpoints_to_delete: &Vec<String>,
    ) -> Self {
        assert!(checkpoint.speedboat_metadata.is_some());
        CompactionWorkItem {
            id: IdInstance::next_id().to_string(),
            table_schema: checkpoint.schema.clone(),
            speedboat_files: checkpoint
                .speedboat_metadata
                .as_ref()
                .unwrap()
                .files
                .clone(),
            delete_files: checkpoint
                .deletes_metadata
                .as_ref()
                .map_or(vec![], |x| x.files.clone()),
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
    #[serde(default)]
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
    #[serde(default)]
    pub dynamic: Option<StringOrBool>,
    pub _meta: Option<MetaInfo>,
    pub properties: HashMap<String, PropertyInfo>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexMappingSettings {
    pub total_fields: IndexMappingFieldSettings,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexMappingFieldSettings {
    pub limit: Option<u32>,
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
    pub index: IndexSettings,
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
    pub fn parse(content: &String) -> Result<Self, serde_json::Error> {
        if content.len() == 0 {
            Ok(CreateIndexBody {
                aliases: None,
                mappings: None,
                settings: None,
            })
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
    DynamoDb(Option<String>),
    TestingDynamoDb(Option<String>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceMode {
    pub impl_type: ServiceImplType,
}

impl ServiceMode {
    pub fn test() -> Self {
        ServiceMode {
            impl_type: ServiceImplType::TestingDynamoDb(None),
        }
    }

    pub fn as_testing_mode(&self) -> TestProcessingMode {
        match &self.impl_type {
            ServiceImplType::TestingDynamoDb(address) => {
                TestProcessingMode::dynamo_testing(address.clone())
            }
            _ => TestProcessingMode::default(),
        }
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
    pub fn to_org_info(&self) -> OrgInfo {
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

pub const TEST_ACCESS_KEY: &str = "access_key";
pub const TEST_SECRET_KEY: &str = "secret_key";

#[cfg(test)]
mod tests {
    use super::{SpeedboatCommitTableInfo, SpeedboatSegmentFile};

    #[test]
    fn speedboat_table_info_prefers_explicit_segments() {
        let table_info = SpeedboatCommitTableInfo::from_segments(
            "commit".to_string(),
            "logs".to_string(),
            vec![SpeedboatSegmentFile {
                segment_id: "segment-1".to_string(),
                file_path: "s3://warehouse/default/ingest/logs/commit/segment-1".to_string(),
                size: 42,
            }],
            None,
        );

        let segments = table_info.segment_files();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].segment_id, "segment-1");
        assert_eq!(
            segments[0].file_path,
            "s3://warehouse/default/ingest/logs/commit/segment-1"
        );
        assert_eq!(segments[0].size, 42);
    }

    #[test]
    fn speedboat_table_info_backfills_legacy_segment_ids() {
        let table_info = SpeedboatCommitTableInfo {
            commit_type: "commit".to_string(),
            table_name: "logs".to_string(),
            segments: vec![],
            files: vec!["s3://warehouse/default/ingest/logs/commit/legacy".to_string()],
            sizes: vec![123],
            schema: None,
        };

        let segments = table_info.segment_files();
        assert_eq!(segments.len(), 1);
        assert_eq!(
            segments[0].file_path,
            "s3://warehouse/default/ingest/logs/commit/legacy"
        );
        assert_eq!(segments[0].size, 123);
        assert!(segments[0].segment_id.starts_with("legacy-"));
    }
}
