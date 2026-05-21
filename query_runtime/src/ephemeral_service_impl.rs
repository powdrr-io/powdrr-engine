use crate::data_contract::{
    CleanupCommit, CleanupWorkItem, CompactionWorkItemTracker, CreateIndexTemplateBody,
    IcebergMetadata, OrgInfo, OrgSettings,
};
use crate::data_contract::{
    CompactionCommit, CompactionWorkItem, CreateTable, DeletesMetadata, ExtensionCommit,
    ExtensionFile, ExtensionWorkItem, FileSetPayload, IcebergCommit, SpeedboatCommit,
    SpeedboatCommitTableInfo, SpeedboatMetadata, TableDescription, TableMetadataCheckpoint,
};
use crate::metadata_store::{
    CheckpointUpdateRequest, ClaimedCleanupWorkItem, ClaimedCompactionWorkItem,
    ClaimedExtensionWorkItem, MetadataClaimKind, MetadataStore, PublishedCheckpointRecord,
    PublishedCheckpointRole, PublishedCheckpointSelector,
};
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::schema_massager::PowdrrSchema;
use crate::state_provider::ServiceApiError;
use crate::test_api::TestProcessingMode;
use idgenerator::IdInstance;
use powdrr_control_plane::ilm_policy::ILMPolicyDefinition;
use std::collections::HashMap;

type CommittedCheckpoints = HashMap<String, String>;

#[cfg(test)]
fn create_table_request(
    name: String,
    tags: HashMap<String, String>,
    serving: Option<crate::data_contract::ServingTableConfig>,
    dynamodb: Option<crate::data_contract::DynamoDbTableConfig>,
    mongodb: Option<crate::data_contract::MongoDbTableConfig>,
) -> CreateTable {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "tags": tags,
        "serving": serving,
        "dynamodb": dynamodb,
        "mongodb": mongodb,
    }))
    .expect("table metadata request should deserialize")
}

fn table_description_from_parts(
    name: String,
    tags: HashMap<String, String>,
    serving: Option<crate::data_contract::ServingTableConfig>,
    dynamodb: Option<crate::data_contract::DynamoDbTableConfig>,
    mongodb: Option<crate::data_contract::MongoDbTableConfig>,
) -> TableDescription {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "tags": tags,
        "serving": serving,
        "dynamodb": dynamodb,
        "mongodb": mongodb,
    }))
    .expect("table description should deserialize")
}

pub struct EphemeralServiceImpl {
    mode: TestProcessingMode,
    org_settings: HashMap<String, OrgSettings>,
    tables: HashMap<String, TableDescription>,
    // alias name -> table name
    table_aliases: HashMap<String, String>,
    table_templates: HashMap<String, CreateIndexTemplateBody>,
    pipelines: HashMap<String, PipelineDefinition>,
    lifetime_policies: HashMap<String, ILMPolicyDefinition>,
    latest_committed_checkpoint_id: HashMap<Option<String>, CommittedCheckpoints>,
    published_checkpoint_id: HashMap<Option<String>, CommittedCheckpoints>,
    checkpoint_publication_requests: HashMap<String, CheckpointUpdateRequest>,
    compaction_work_items: HashMap<String, CompactionWorkItemTracker>,
    not_compacted_checkpoint_ids: HashMap<String, Vec<String>>,
    extension_work_items: HashMap<String, HashMap<String, ExtensionWorkItem>>,
    cleanup_work_items: Vec<CleanupWorkItem>,
    compactions: HashMap<String, (String, CompactionCommit)>,
    checkpoints: HashMap<String, TableMetadataCheckpoint>,
    checkpoints_needing_extension_work: HashMap<String, Vec<String>>,
    recent_file_extension_metadata: HashMap<String, Vec<ExtensionFile>>,
}

impl EphemeralServiceImpl {
    pub fn new(mode: TestProcessingMode) -> Self {
        EphemeralServiceImpl {
            mode: mode,
            org_settings: HashMap::new(),
            tables: HashMap::new(),
            table_aliases: HashMap::new(),
            table_templates: HashMap::new(),
            pipelines: HashMap::new(),
            lifetime_policies: HashMap::new(),
            latest_committed_checkpoint_id: HashMap::new(),
            published_checkpoint_id: HashMap::new(),
            checkpoint_publication_requests: HashMap::new(),
            compaction_work_items: HashMap::new(),
            not_compacted_checkpoint_ids: HashMap::new(),
            compactions: HashMap::new(),
            cleanup_work_items: Vec::new(),
            checkpoints: HashMap::new(),
            checkpoints_needing_extension_work: HashMap::new(),
            extension_work_items: HashMap::from([("es".to_string(), HashMap::new())]),
            recent_file_extension_metadata: HashMap::new(),
        }
    }

    fn checkpoints_needing_extension_work(
        &self,
        table_name: &String,
        extension_name: &String,
    ) -> Option<Vec<String>> {
        self.checkpoints_needing_extension_work
            .get(&format!("{}_{}", table_name, extension_name))
            .cloned()
    }

    fn canonical_table_name(&self, table_name: &String) -> String {
        self.table_aliases
            .get(table_name)
            .unwrap_or(table_name)
            .clone()
    }

    fn recent_file_extension_metadata(
        &self,
        table_name: &String,
        extension_name: &String,
        file_name: &String,
    ) -> Option<Vec<ExtensionFile>> {
        self.recent_file_extension_metadata
            .get(&format!("{}_{}_{}", table_name, extension_name, file_name))
            .cloned()
    }

    fn add_recent_extension_files(&mut self, table_name: &String, commit: &ExtensionCommit) -> () {
        for (file_name, extension_files) in commit.files.iter() {
            self.recent_file_extension_metadata.insert(
                format!("{}_{}_{}", table_name, commit.extension, file_name),
                extension_files.clone(),
            );
        }
    }

    fn try_fill_checkpoint_extension_metadata(
        &mut self,
        extension_name: &String,
        metadata: &mut TableMetadataCheckpoint,
    ) -> (FileSetPayload, FileSetPayload) {
        metadata.retain_current_extension_metadata();
        if !metadata.extension_metadata.contains_key(extension_name) {
            metadata
                .extension_metadata
                .insert(extension_name.clone(), HashMap::new());
        }

        let extension_metadata = metadata.extension_metadata.get_mut(extension_name).unwrap();

        let mut iceberg_file_set = FileSetPayload::new();
        match metadata.iceberg_metadata.as_ref() {
            Some(im) => {
                for file_desc in im.files.as_file_tuples() {
                    match self.recent_file_extension_metadata(
                        &metadata.table_name,
                        extension_name,
                        &file_desc.file_path,
                    ) {
                        Some(metadata) => {
                            extension_metadata.insert(file_desc.file_path.clone(), metadata);
                        }
                        None => {
                            iceberg_file_set.add(&file_desc);
                        }
                    }
                }
            }
            None => (),
        };

        let mut speedboat_file_set = FileSetPayload::new();
        match metadata.speedboat_metadata.as_ref() {
            Some(im) => {
                for file_desc in im.files.as_file_tuples() {
                    match self.recent_file_extension_metadata(
                        &metadata.table_name,
                        extension_name,
                        &file_desc.file_path,
                    ) {
                        Some(metadata) => {
                            extension_metadata.insert(file_desc.file_path.clone(), metadata);
                        }
                        None => {
                            speedboat_file_set.add(&file_desc);
                        }
                    }
                }
            }
            None => (),
        };

        (speedboat_file_set, iceberg_file_set)
    }

    #[allow(dead_code)]
    fn set_latest_committed_checkpoint_id(
        &mut self,
        extension: Option<String>,
        table_name: &String,
        checkpoint_id: &String,
    ) -> () {
        if !self.latest_committed_checkpoint_id.contains_key(&extension) {
            self.latest_committed_checkpoint_id
                .insert(extension.clone(), HashMap::new());
        }
        self.latest_committed_checkpoint_id
            .get_mut(&extension)
            .unwrap()
            .insert(table_name.clone(), checkpoint_id.clone());
    }

    fn get_frontier_checkpoint_sync(
        frontier: &HashMap<Option<String>, CommittedCheckpoints>,
        table_name: &String,
        extensions: Option<String>,
    ) -> Option<String> {
        frontier
            .get(&extensions)
            .and_then(|c| c.get(table_name).cloned())
    }

    fn set_frontier_checkpoint(
        frontier: &mut HashMap<Option<String>, CommittedCheckpoints>,
        table_name: &String,
        extensions: Option<String>,
        checkpoint_id: &String,
    ) {
        if !frontier.contains_key(&extensions) {
            frontier.insert(extensions.clone(), HashMap::new());
        }
        frontier
            .get_mut(&extensions)
            .unwrap()
            .insert(table_name.clone(), checkpoint_id.clone());
    }

    fn get_published_checkpoint_sync(
        &self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Option<String> {
        let real_table_name = self.canonical_table_name(table_name);
        Self::get_frontier_checkpoint_sync(
            &self.published_checkpoint_id,
            &real_table_name,
            extensions,
        )
    }

    fn set_published_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
        checkpoint_id: &String,
    ) {
        let real_table_name = self.canonical_table_name(table_name);
        Self::set_frontier_checkpoint(
            &mut self.published_checkpoint_id,
            &real_table_name,
            extensions,
            checkpoint_id,
        );
    }

    fn enqueue_checkpoint_publication(&mut self, table_name: &String) {
        let real_table_name = self.canonical_table_name(table_name);
        self.checkpoint_publication_requests.insert(
            real_table_name.clone(),
            CheckpointUpdateRequest::new("fake_org_id".to_string(), real_table_name),
        );
    }

    pub async fn add_checkpoint(
        &mut self,
        _org_info: &OrgInfo,
        metadata: &TableMetadataCheckpoint,
    ) -> Result<(), ServiceApiError> {
        // To make testing a little easier, we'll just magic up a table as necessary
        if !self.tables.contains_key(&metadata.table_name) {
            self.tables.insert(
                metadata.table_name.clone(),
                table_description_from_parts(
                    metadata.table_name.clone(),
                    Default::default(),
                    None,
                    None,
                    None,
                ),
            );
        }
        let key = format!("{}_{}", &metadata.table_name, &metadata.checkpoint_id);
        if !self.checkpoints.contains_key(&key) {
            self.checkpoints.insert(key, metadata.clone());
        }
        self.set_latest_committed_checkpoint_id(
            None,
            &metadata.table_name,
            &metadata.checkpoint_id,
        );
        self.set_published_checkpoint(&metadata.table_name, None, &metadata.checkpoint_id);
        if metadata.extension_metadata.len() > 0 {
            for extension in metadata.extension_metadata.keys() {
                self.set_latest_committed_checkpoint_id(
                    Some(extension.clone()),
                    &metadata.table_name,
                    &metadata.checkpoint_id,
                );
                self.set_published_checkpoint(
                    &metadata.table_name,
                    Some(extension.clone()),
                    &metadata.checkpoint_id,
                );
            }
        } else {
            self.fill_extension_work_item(
                &metadata.table_name,
                &"es".to_string(),
                &metadata.checkpoint_id,
                &metadata.schema,
                metadata
                    .speedboat_metadata
                    .as_ref()
                    .map_or(&FileSetPayload::new(), |m| &m.files),
                metadata
                    .iceberg_metadata
                    .as_ref()
                    .map_or(&FileSetPayload::new(), |m| &m.files),
            )
        }
        Ok(())
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

        let es_work_items = self
            .extension_work_items
            .get_mut(&"es".to_string())
            .unwrap();
        if !es_work_items.contains_key(table_name) {
            es_work_items.insert(
                table_name.clone(),
                ExtensionWorkItem {
                    id: IdInstance::next_id().to_string(),
                    extension_type: extension.clone(),
                    table_name: table_name.clone(),
                    checkpoint_id: Some(checkpoint_id.clone()),
                    table_schema: schema.clone(),
                    speedboat_files: speedboat_files.clone(),
                    iceberg_files: iceberg_files.clone(),
                },
            );
        } else {
            let table_work_item = es_work_items.get_mut(table_name).unwrap();
            table_work_item.checkpoint_id = Some(checkpoint_id.clone());
            table_work_item.table_schema = schema.clone();
            table_work_item.speedboat_files =
                table_work_item.speedboat_files.merge(speedboat_files);
            table_work_item.iceberg_files = table_work_item.iceberg_files.merge(&iceberg_files);
        }
        let key = format!("{}_{}", table_name, extension);
        if !self.checkpoints_needing_extension_work.contains_key(&key) {
            self.checkpoints_needing_extension_work
                .insert(key, vec![checkpoint_id.clone()]);
        } else {
            self.checkpoints_needing_extension_work
                .get_mut(&key)
                .unwrap()
                .push(checkpoint_id.clone());
        }
    }

    fn handle_compactions(
        &mut self,
        compactions: &Vec<String>,
        checkpoint: &mut TableMetadataCheckpoint,
    ) -> () {
        // TODO: remove this method and use the one on TableMetadataCheckpoint
        for compaction in compactions {
            self.handle_compaction(&Some(compaction.clone()), checkpoint);
        }
    }

    fn handle_compaction(
        &mut self,
        compaction: &Option<String>,
        checkpoint: &mut TableMetadataCheckpoint,
    ) -> () {
        // TODO: remove this method and use the one on TableMetadataCheckpoint
        if compaction.is_none() {
            return;
        }

        let (_table_name, compaction_obj) =
            self.compactions.get(compaction.as_ref().unwrap()).unwrap();

        match checkpoint.speedboat_metadata.as_mut() {
            Some(speedboat) => {
                speedboat
                    .files
                    .remove(&compaction_obj.removed_speedboat_files);
            }
            None => (),
        };

        match checkpoint.deletes_metadata.as_mut() {
            Some(deletes) => {
                deletes
                    .files
                    .retain(|x| !compaction_obj.removed_delete_files.contains(x));
            }
            None => (),
        };

        for metadata in checkpoint.extension_metadata.values_mut() {
            metadata.retain(|key, _| !compaction_obj.removed_speedboat_files.contains(key));
        }
    }

    fn get_latest_committed_checkpoint_sync(
        &self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Option<String> {
        let real_table_name = self.canonical_table_name(table_name);
        Self::get_frontier_checkpoint_sync(
            &self.latest_committed_checkpoint_id,
            &real_table_name,
            extensions,
        )
    }

    fn set_latest_committed_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
        checkpoint_id: &String,
    ) {
        let real_table_name = self.canonical_table_name(table_name);
        Self::set_frontier_checkpoint(
            &mut self.latest_committed_checkpoint_id,
            &real_table_name,
            extensions,
            checkpoint_id,
        );
    }

    async fn speedboat_commit_type_commit(
        &mut self,
        table_info: &SpeedboatCommitTableInfo,
        compaction: &Option<String>,
    ) -> Result<(), ServiceApiError> {
        let latest_checkpoint =
            match self.get_latest_committed_checkpoint_sync(&table_info.table_name, None) {
                Some(checkpoint_id) => {
                    let key = format!("{}_{}", &table_info.table_name, checkpoint_id);
                    match self.checkpoints.get(&key) {
                        Some(c) => c,
                        None => panic!(
                            "Found latest checkpoint id but checkpoint missing = {}",
                            key
                        ),
                    }
                }
                None => &TableMetadataCheckpoint {
                    table_name: table_info.table_name.clone(),
                    original_checkpoint_id: None,
                    checkpoint_id: "".to_string(),
                    iceberg_metadata: None,
                    speedboat_metadata: None,
                    deletes_metadata: None,
                    extension_metadata: HashMap::new(),
                    schema: table_info.schema.as_ref().unwrap().clone(),
                },
            };

        let new_checkpoint_id = IdInstance::next_id().to_string();
        let new_speedboat_metadata = match &latest_checkpoint.speedboat_metadata {
            None => SpeedboatMetadata {
                files: FileSetPayload {
                    file_paths: table_info.files.clone(),
                    sizes: table_info.sizes.clone(),
                    schemas: vec![table_info.schema.as_ref().unwrap().clone()],
                    file_schemas: table_info.files.iter().map(|_| 0).collect(),
                },
            },
            Some(existing) => SpeedboatMetadata {
                files: existing.files.merge(&table_info.as_file_set_payload()),
            },
        };

        let mut merged_schema = latest_checkpoint.schema.clone();
        if table_info.schema.is_some() {
            merged_schema.merge_from(table_info.schema.as_ref().unwrap());
        }

        let mut new_latest_checkpoint = TableMetadataCheckpoint {
            table_name: table_info.table_name.clone(),
            original_checkpoint_id: None,
            checkpoint_id: new_checkpoint_id.clone(),
            iceberg_metadata: latest_checkpoint.iceberg_metadata.clone(),
            speedboat_metadata: Some(new_speedboat_metadata.clone()),
            deletes_metadata: latest_checkpoint.deletes_metadata.clone(),
            extension_metadata: latest_checkpoint.extension_metadata.clone(),
            schema: merged_schema.clone(),
        };

        self.handle_compaction(compaction, &mut new_latest_checkpoint);
        let (speedboat_files, iceberg_files) = self
            .try_fill_checkpoint_extension_metadata(&"es".to_string(), &mut new_latest_checkpoint);

        self.checkpoints.insert(
            format!("{}_{}", &table_info.table_name, &new_checkpoint_id),
            new_latest_checkpoint.clone(),
        );

        self.fill_extension_work_item(
            &table_info.table_name,
            &"es".to_string(),
            &new_checkpoint_id,
            &merged_schema,
            &speedboat_files,
            &iceberg_files,
        );

        self.set_latest_committed_checkpoint(&table_info.table_name, None, &new_checkpoint_id);

        self.maybe_create_compaction_work_item(&new_latest_checkpoint);
        Ok(())
    }

    fn replace_and_delete_checkpoints(
        &mut self,
        compaction: &String,
        iceberg_metadata: &IcebergMetadata,
    ) -> CleanupWorkItem {
        let (table_name, compaction_obj) = self.compactions.get(compaction).unwrap();

        let checkpoint_key = format!("{}_{}", table_name, compaction_obj.checkpoint_id_to_replace);
        assert!(self.checkpoints.contains_key(&checkpoint_key));
        let checkpoint_to_replace = self.checkpoints.get_mut(&checkpoint_key).unwrap();
        assert!(checkpoint_to_replace.speedboat_metadata.is_some());
        checkpoint_to_replace.apply_compaction_for_replacement(compaction_obj, iceberg_metadata);

        for checkpoint_id in &compaction_obj.checkpoints_to_delete {
            let checkpoint_key = format!("{}_{}", table_name, checkpoint_id);
            assert!(self.checkpoints.contains_key(&checkpoint_key));
            self.checkpoints.remove(&checkpoint_key);
        }

        match self.compaction_work_items.get(table_name) {
            Some(tracker) => {
                assert!(tracker.in_progress);
                assert_eq!(
                    tracker.work_item.checkpoint_id_to_replace,
                    compaction_obj.checkpoint_id_to_replace
                );
                self.compaction_work_items.remove(table_name);
            }
            None => {
                panic!("Compaction work item missing = {}", table_name);
            }
        }

        CleanupWorkItem {
            id: IdInstance::next_id().to_string(),
            table_name: table_name.clone(),
            files_to_delete: compaction_obj
                .removed_speedboat_files
                .iter()
                .chain(compaction_obj.removed_delete_files.iter())
                .map(|x| x.clone())
                .collect(),
        }
    }

    fn maybe_create_compaction_work_item(&mut self, checkpoint: &TableMetadataCheckpoint) -> () {
        // If we have a work item waiting, then we just wait for that to happen before creating a new one.
        if self
            .compaction_work_items
            .contains_key(&checkpoint.table_name)
        {
            self.not_compacted_checkpoint_ids
                .get_mut(&checkpoint.table_name)
                .unwrap()
                .push(checkpoint.checkpoint_id.clone());
            return;
        }

        // We only compact speedboat so no speedboat metadata means no work item
        if checkpoint.speedboat_metadata.is_none() {
            return;
        }

        // If the files in the speedboat metadata surpass the compaction threshold then make a work item
        let speedboat_files = &checkpoint.speedboat_metadata.as_ref().unwrap().files;
        let num_files_threshold = self.mode.compaction_mode.threshold();
        let compact = speedboat_files.file_paths.len() as u64 >= num_files_threshold
            || speedboat_files.sizes.iter().sum::<u64>() > 30 * 1024 * 1024;
        if compact {
            self.compaction_work_items.insert(
                checkpoint.table_name.clone(),
                CompactionWorkItemTracker {
                    work_item: CompactionWorkItem::from_checkpoint(
                        checkpoint,
                        self.not_compacted_checkpoint_ids
                            .get(&checkpoint.table_name)
                            .as_ref()
                            .unwrap(),
                    ),
                    in_progress: false,
                },
            );
            self.not_compacted_checkpoint_ids
                .get_mut(&checkpoint.table_name)
                .unwrap()
                .clear();
        }
    }

    async fn speedboat_commit_type_delete(
        &mut self,
        table_info: &SpeedboatCommitTableInfo,
        compaction: &Option<String>,
    ) -> Result<(), ServiceApiError> {
        let latest_checkpoint =
            match self.get_latest_committed_checkpoint_sync(&table_info.table_name, None) {
                Some(checkpoint_id) => {
                    let key = format!("{}_{}", &table_info.table_name, checkpoint_id);
                    match self.checkpoints.get(&key) {
                        Some(c) => c,
                        None => panic!(
                            "Found latest checkpoint id but checkpoint missing = {}",
                            key
                        ),
                    }
                }
                None => &TableMetadataCheckpoint {
                    table_name: table_info.table_name.clone(),
                    original_checkpoint_id: None,
                    checkpoint_id: "".to_string(),
                    iceberg_metadata: None,
                    speedboat_metadata: None,
                    deletes_metadata: None,
                    extension_metadata: HashMap::new(),
                    schema: PowdrrSchema::minimal(),
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
                DeletesMetadata { files }
            }
        };

        tracing::info!(
            "Delete commit has {} files",
            new_deletes_metadata.files.len()
        );
        for file in &new_deletes_metadata.files {
            tracing::info!("Delete file {}", file)
        }

        let mut new_latest_checkpoint = TableMetadataCheckpoint {
            table_name: table_info.table_name.clone(),
            original_checkpoint_id: None,
            checkpoint_id: new_checkpoint_id.clone(),
            iceberg_metadata: latest_checkpoint.iceberg_metadata.clone(),
            speedboat_metadata: latest_checkpoint.speedboat_metadata.clone(),
            deletes_metadata: Some(new_deletes_metadata),
            extension_metadata: latest_checkpoint.extension_metadata.clone(),
            schema: latest_checkpoint.schema.clone(),
        };
        self.handle_compaction(&compaction, &mut new_latest_checkpoint);
        self.try_fill_checkpoint_extension_metadata(&"es".to_string(), &mut new_latest_checkpoint);

        self.checkpoints.insert(
            format!("{}_{}", &table_info.table_name, &new_checkpoint_id),
            new_latest_checkpoint.clone(),
        );
        self.set_latest_committed_checkpoint(&table_info.table_name, None, &new_checkpoint_id);
        Ok(())
    }

    fn get_checkpoint_sync(
        &self,
        table_name: &String,
        checkpoint_id: &String,
    ) -> Option<TableMetadataCheckpoint> {
        let key = format!("{}_{}", table_name, checkpoint_id);
        self.checkpoints.get(&key).cloned()
    }

    fn add_coverage_for(
        &mut self,
        table_name: &String,
        checkpoint_id: &String,
        extension_commit: &ExtensionCommit,
    ) -> Option<String> {
        let key = format!("{}_{}", table_name, checkpoint_id);

        let checkpoint = self.checkpoints.get_mut(&key).unwrap();

        checkpoint.add_coverage(extension_commit);

        if checkpoint.fully_covered_for_extension(&extension_commit.extension) {
            let key = format!("{}_{}", table_name, extension_commit.extension);
            if self.checkpoints_needing_extension_work.contains_key(&key) {
                self.checkpoints_needing_extension_work
                    .get_mut(&key)
                    .unwrap()
                    .retain(|x| x != checkpoint_id);
            }
            Some(checkpoint_id.clone())
        } else {
            None
        }
    }

    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        Ok(self.tables.keys().cloned().collect())
    }

    pub async fn create_table(
        &mut self,
        _org_info: &OrgInfo,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        match self.tables.get(&create_table.name) {
            Some(_) => {
                self.tables.insert(
                    create_table.name.clone(),
                    TableDescription::from_create_table(create_table),
                );
            }
            None => {
                self.tables.insert(
                    create_table.name.clone(),
                    TableDescription::from_create_table(create_table),
                );
                self.not_compacted_checkpoint_ids
                    .insert(create_table.name.clone(), Vec::new());
            }
        }
        Ok(true)
    }

    pub async fn describe_table(
        &mut self,
        _org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError> {
        let final_name = self.table_aliases.get(name).unwrap_or_else(|| name);
        match self.tables.get(final_name) {
            Some(d) => Ok(Some(d.clone())),
            None => Ok(None),
        }
    }

    pub async fn add_alias(
        &mut self,
        _org_info: &OrgInfo,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        // TODO: check if something exists
        self.table_aliases.insert(alias.clone(), table_name.clone());
        Ok(true)
    }

    pub async fn remove_alias(
        &mut self,
        _org_info: &OrgInfo,
        _table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        // TODO: check if something exists
        self.table_aliases.remove(alias);
        Ok(true)
    }

    pub async fn create_table_template(
        &mut self,
        _org_info: &OrgInfo,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceApiError> {
        match self.table_templates.get(name) {
            Some(_) => {
                self.table_templates.remove(name);
                self.table_templates.insert(name.clone(), template.clone());
                Ok(true)
            }
            None => {
                self.table_templates.insert(name.clone(), template.clone());
                Ok(true)
            }
        }
    }

    pub async fn describe_table_template(
        &mut self,
        _org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        match self.table_templates.get(name) {
            Some(d) => Ok(Some(d.clone())),
            None => Ok(None),
        }
    }

    pub async fn create_pipeline(
        &mut self,
        _org_info: &OrgInfo,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceApiError> {
        match self.pipelines.get(name) {
            Some(_) => panic!("Need to do a real error path now"),
            None => {
                self.pipelines.insert(name.clone(), pipeline.clone());
                Ok(true)
            }
        }
    }

    pub async fn describe_pipeline(
        &mut self,
        _org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        match self.pipelines.get(name) {
            Some(p) => Ok(Some(p.clone())),
            None => Ok(None),
        }
    }

    pub async fn create_lifetime_policy(
        &mut self,
        _org_info: &OrgInfo,
        name: &String,
        policy: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceApiError> {
        match self.lifetime_policies.get(name) {
            Some(_) => panic!("Need to do a real error path now"),
            None => {
                self.lifetime_policies.insert(name.clone(), policy.clone());
                Ok(true)
            }
        }
    }

    pub async fn describe_lifetime_policy(
        &mut self,
        _org_info: &OrgInfo,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        match self.lifetime_policies.get(name) {
            Some(p) => Ok(Some(p.clone())),
            None => Ok(None),
        }
    }

    pub async fn speedboat_commit(
        &mut self,
        org_info: &OrgInfo,
        commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceApiError> {
        assert!(
            commit.compaction.is_none(),
            "Speedboat commits do not yet support compactions"
        );
        for table_info in commit.type_files.iter() {
            if table_info.commit_type == "commit" || table_info.commit_type == "compact" {
                self.speedboat_commit_type_commit(table_info, &commit.compaction)
                    .await?;
            } else if table_info.commit_type == "delete" {
                self.speedboat_commit_type_delete(table_info, &commit.compaction)
                    .await?;
            } else {
                panic!("Unknown Speedboat commit type")
            }
        }
        if let Some(table_info) = commit.type_files.first() {
            MetadataStore::queue_checkpoint_publication(
                self,
                &CheckpointUpdateRequest::new(
                    org_info.org_id.clone(),
                    table_info.table_name.clone(),
                ),
            )
            .await?;
        }
        Ok(true)
    }

    pub async fn iceberg_commit(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        iceberg_commit: &IcebergCommit,
    ) -> Result<bool, ServiceApiError> {
        let latest_checkpoint = match self.get_latest_committed_checkpoint_sync(table_name, None) {
            Some(checkpoint_id) => {
                let key = format!("{}_{}", table_name, checkpoint_id);
                match self.checkpoints.get(&key) {
                    Some(c) => c,
                    None => panic!(
                        "Found latest checkpoint id but checkpoint missing = {}",
                        key
                    ),
                }
            }
            None => &TableMetadataCheckpoint {
                table_name: table_name.clone(),
                original_checkpoint_id: None,
                checkpoint_id: "".to_string(),
                iceberg_metadata: None,
                speedboat_metadata: None,
                deletes_metadata: None,
                extension_metadata: HashMap::new(),
                schema: iceberg_commit.metadata.table_schema.clone(),
            },
        };

        let new_checkpoint_id = IdInstance::next_id().to_string();

        let mut merged_schema = latest_checkpoint.schema.clone();
        merged_schema.merge_from(&iceberg_commit.metadata.table_schema);

        let mut new_latest_checkpoint = TableMetadataCheckpoint {
            table_name: table_name.clone(),
            original_checkpoint_id: None,
            checkpoint_id: new_checkpoint_id.clone(),
            iceberg_metadata: Some(iceberg_commit.metadata.clone()),
            speedboat_metadata: latest_checkpoint.speedboat_metadata.clone(),
            deletes_metadata: latest_checkpoint.deletes_metadata.clone(),
            extension_metadata: latest_checkpoint.extension_metadata.clone(),
            schema: merged_schema.clone(),
        };
        self.handle_compactions(&iceberg_commit.compactions, &mut new_latest_checkpoint);
        let (speedboat_files, iceberg_files) = self
            .try_fill_checkpoint_extension_metadata(&"es".to_string(), &mut new_latest_checkpoint);

        self.checkpoints.insert(
            format!("{}_{}", &table_name, &new_checkpoint_id),
            new_latest_checkpoint.clone(),
        );
        self.set_latest_committed_checkpoint(&table_name, None, &new_checkpoint_id);

        self.fill_extension_work_item(
            &table_name,
            &"es".to_string(),
            &new_checkpoint_id,
            &merged_schema,
            &speedboat_files,
            &iceberg_files,
        );

        for compaction in iceberg_commit.compactions.iter() {
            let cleanup_work_item =
                self.replace_and_delete_checkpoints(compaction, &iceberg_commit.metadata);
            self.cleanup_work_items.push(cleanup_work_item);
        }

        MetadataStore::queue_checkpoint_publication(
            self,
            &CheckpointUpdateRequest::new(org_info.org_id.clone(), table_name.clone()),
        )
        .await?;

        Ok(true)
    }

    pub async fn extension_commit(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError> {
        let waiting_checkpoint_ids = self
            .checkpoints_needing_extension_work(table_name, &commit.extension)
            .unwrap_or_else(|| vec![]);
        assert!(waiting_checkpoint_ids.len() > 0);

        let removed_checkpoint_ids: Vec<String> = waiting_checkpoint_ids
            .iter()
            .map(|x| self.add_coverage_for(table_name, x, commit))
            .flatten()
            .collect();
        assert!(removed_checkpoint_ids.len() > 0);

        let max_id = removed_checkpoint_ids.iter().max().unwrap();

        self.add_recent_extension_files(table_name, commit);

        match self.get_latest_committed_checkpoint_sync(table_name, Some("es".to_string())) {
            Some(latest) => {
                if max_id > &latest {
                    self.set_latest_committed_checkpoint(
                        table_name,
                        Some("es".to_string()),
                        max_id,
                    );
                }
            }
            None => {
                self.set_latest_committed_checkpoint(table_name, Some("es".to_string()), max_id);
            }
        };
        MetadataStore::queue_checkpoint_publication(
            self,
            &CheckpointUpdateRequest::new(org_info.org_id.clone(), table_name.clone()),
        )
        .await?;
        Ok(true)
    }

    pub async fn compaction_commit(
        &mut self,
        _org_info: &OrgInfo,
        table_name: &String,
        commit: &CompactionCommit,
    ) -> Result<bool, ServiceApiError> {
        // NOTE: this just notes what the compactor is saying. We don't generate the new checkpoint
        // until we see an iceberg or speedboat commit with the new info.
        self.compactions.insert(
            commit.compaction_id.clone(),
            (table_name.clone(), commit.clone()),
        );
        Ok(true)
    }

    pub async fn cleanup_commit(
        &mut self,
        _org_info: &OrgInfo,
        _commit: &CleanupCommit,
    ) -> Result<bool, ServiceApiError> {
        Ok(true)
    }

    pub async fn get_latest_committed_checkpoint(
        &mut self,
        _org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        Ok(self.get_latest_committed_checkpoint_sync(table_name, extensions))
    }

    pub async fn get_published_active_checkpoint(
        &mut self,
        _org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        Ok(self.get_published_checkpoint_sync(table_name, extensions))
    }

    pub async fn get_checkpoint(
        &mut self,
        _org_info: &OrgInfo,
        snapshot: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        match self.get_checkpoint_sync(&snapshot.table_name, &snapshot.checkpoint_id) {
            Some(v) => Ok(Some(v.clone())),
            None => Ok(None),
        }
    }

    pub async fn get_extension_work_items(
        &mut self,
        _org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        if extension_type == "es" {
            // TODO: priority by index? allow index filtering?
            let mut collected_work_items = vec![];
            match self.extension_work_items.get_mut(extension_type) {
                Some(items) => {
                    for (_, work_items) in items.iter_mut() {
                        if work_items.has_work() {
                            collected_work_items.push(work_items.clone());
                            work_items.clear();
                        }
                    }
                }
                None => (),
            };
            Ok(collected_work_items)
        } else {
            Ok(vec![])
        }
    }

    pub async fn get_compaction_work_items(
        &mut self,
        _org_info: &OrgInfo,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        let mut work_items = vec![];
        for (table_name, compaction_tracker) in self.compaction_work_items.iter_mut() {
            if !compaction_tracker.in_progress {
                tracing::info!("Returning compaction work item for {}", table_name);
                work_items.push((table_name.clone(), compaction_tracker.work_item.clone()));
                compaction_tracker.in_progress = true;
            }
        }
        Ok(work_items)
    }

    pub async fn get_cleanup_work_items(
        &mut self,
        _org_info: &OrgInfo,
    ) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        let work_items = self.cleanup_work_items.clone();
        tracing::info!("Returning {} cleanup work items", work_items.len());
        self.cleanup_work_items.clear();
        Ok(work_items)
    }

    pub async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        let table_names: Vec<String> = self
            .checkpoint_publication_requests
            .keys()
            .cloned()
            .collect();
        let mut changed = false;
        for table_name in &table_names {
            let available_extensions: Vec<Option<String>> = self
                .latest_committed_checkpoint_id
                .iter()
                .filter_map(|(extension, checkpoints)| {
                    if checkpoints.contains_key(table_name) {
                        Some(extension.clone())
                    } else {
                        None
                    }
                })
                .collect();
            for extension in available_extensions {
                let committed =
                    self.get_latest_committed_checkpoint_sync(table_name, extension.clone());
                let published = self.get_published_checkpoint_sync(table_name, extension.clone());
                if let Some(committed_checkpoint) = committed {
                    if published.as_ref() != Some(&committed_checkpoint) {
                        self.set_published_checkpoint(table_name, extension, &committed_checkpoint);
                        changed = true;
                    }
                }
            }
            self.checkpoint_publication_requests.remove(table_name);
        }
        Ok(changed)
    }

    pub async fn create_org(&mut self, _settings: &OrgSettings) -> Result<(), ServiceApiError> {
        self.org_settings
            .insert(_settings.org_id.clone(), _settings.clone());
        Ok(())
    }

    pub async fn lookup_org(
        &mut self,
        access_key: &String,
        secret_key: &String,
    ) -> Result<Option<OrgInfo>, ServiceApiError> {
        for settings in self.org_settings.values() {
            if settings.creds.iter().any(|creds| {
                &creds.access_key_id == access_key && &creds.secret_access_key == secret_key
            }) {
                return Ok(Some(settings.to_org_info()));
            }
        }
        Ok(None)
    }

    pub async fn lookup_secret_access_key(
        &mut self,
        access_key: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        let mut matches = self
            .org_settings
            .values()
            .flat_map(|settings| settings.creds.iter())
            .filter(|creds| &creds.access_key_id == access_key)
            .map(|creds| creds.secret_access_key.clone());
        let first = matches.next();
        if matches.next().is_some() {
            return Err(ServiceApiError::new(format!(
                "Multiple org credentials share access key {}",
                access_key
            )));
        }
        Ok(first)
    }
}

#[async_trait::async_trait]
impl MetadataStore for EphemeralServiceImpl {
    async fn queue_checkpoint_publication(
        &mut self,
        request: &CheckpointUpdateRequest,
    ) -> Result<(), ServiceApiError> {
        self.enqueue_checkpoint_publication(&request.table_name);
        Ok(())
    }

    async fn get_latest_committed_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        EphemeralServiceImpl::get_latest_committed_checkpoint(self, org_info, table_name, extension)
            .await
    }

    async fn get_published_checkpoint_record(
        &mut self,
        org_info: &OrgInfo,
        selector: &PublishedCheckpointSelector,
    ) -> Result<Option<PublishedCheckpointRecord>, ServiceApiError> {
        let checkpoint_id = match selector.role {
            PublishedCheckpointRole::Active => {
                EphemeralServiceImpl::get_published_active_checkpoint(
                    self,
                    org_info,
                    &selector.table_name,
                    selector.extension.clone(),
                )
                .await?
            }
            PublishedCheckpointRole::Target => {
                EphemeralServiceImpl::get_latest_committed_checkpoint(
                    self,
                    org_info,
                    &selector.table_name,
                    selector.extension.clone(),
                )
                .await?
            }
        };

        Ok(
            checkpoint_id.map(|checkpoint_id| PublishedCheckpointRecord {
                selector: selector.clone(),
                checkpoint_id,
            }),
        )
    }

    async fn get_checkpoint_metadata(
        &mut self,
        org_info: &OrgInfo,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        EphemeralServiceImpl::get_checkpoint(self, org_info, checkpoint).await
    }

    async fn claim_extension_work_items(
        &mut self,
        org_info: &OrgInfo,
        extension_type: &String,
    ) -> Result<Vec<ClaimedExtensionWorkItem>, ServiceApiError> {
        Ok(
            EphemeralServiceImpl::get_extension_work_items(self, org_info, extension_type)
                .await?
                .into_iter()
                .map(|work_item| ClaimedExtensionWorkItem {
                    claim: MetadataClaimKind::ProcessLocal,
                    work_item,
                })
                .collect(),
        )
    }

    async fn claim_compaction_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<ClaimedCompactionWorkItem>, ServiceApiError> {
        Ok(
            EphemeralServiceImpl::get_compaction_work_items(self, org_info)
                .await?
                .into_iter()
                .map(|(table_name, work_item)| ClaimedCompactionWorkItem {
                    claim: MetadataClaimKind::ProcessLocal,
                    table_name,
                    work_item,
                })
                .collect(),
        )
    }

    async fn claim_cleanup_work_items(
        &mut self,
        org_info: &OrgInfo,
    ) -> Result<Vec<ClaimedCleanupWorkItem>, ServiceApiError> {
        Ok(EphemeralServiceImpl::get_cleanup_work_items(self, org_info)
            .await?
            .into_iter()
            .map(|work_item| ClaimedCleanupWorkItem {
                claim: MetadataClaimKind::ProcessLocal,
                work_item,
            })
            .collect())
    }

    async fn advance_published_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        EphemeralServiceImpl::update_all_checkpoints(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_contract::{FileSetPayload, IcebergMetadata, LicenseType};
    use crate::metadata_store::{MetadataStore, PublishedCheckpointSelector};
    use crate::schema_massager::PowdrrSchema;
    use std::collections::HashMap;

    fn fake_org_info() -> OrgInfo {
        OrgInfo {
            org_id: "fake_org_id".to_string(),
            license_type: LicenseType::Free,
        }
    }

    fn iceberg_metadata(file_path: &String, snapshot_id: &str) -> IcebergMetadata {
        let schema = PowdrrSchema::minimal();
        IcebergMetadata {
            table_schema: schema.clone(),
            snapshot_id: Some(snapshot_id.to_string()),
            files: FileSetPayload::single(file_path.clone(), 128, schema),
            partition_spec: vec![],
            sort_order: vec![],
            column_names: vec![],
            column_stats: vec![],
            access_artifacts: vec![],
            file_stats: vec![],
        }
    }

    #[tokio::test]
    async fn metadata_store_committed_and_published_frontiers_diverge_until_advanced() {
        let mut service_impl = EphemeralServiceImpl::new(TestProcessingMode::default());
        let org_info = fake_org_info();
        let table_name = "ephemeral_frontier_table".to_string();
        let file_path = "s3://warehouse/table/data-0001.parquet".to_string();

        service_impl
            .create_table(
                &org_info,
                &create_table_request(table_name.clone(), HashMap::new(), None, None, None),
            )
            .await
            .unwrap();

        service_impl
            .iceberg_commit(
                &org_info,
                &table_name,
                &IcebergCommit {
                    metadata: iceberg_metadata(&file_path, "1"),
                    deletes_table_info: None,
                    compactions: vec![],
                },
            )
            .await
            .unwrap();

        let committed_checkpoint = MetadataStore::get_latest_committed_checkpoint(
            &mut service_impl,
            &org_info,
            &table_name,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            MetadataStore::get_published_checkpoint_record(
                &mut service_impl,
                &org_info,
                &PublishedCheckpointSelector::active(table_name.clone(), None),
            )
            .await
            .unwrap(),
            None
        );
        let target_record = MetadataStore::get_published_checkpoint_record(
            &mut service_impl,
            &org_info,
            &PublishedCheckpointSelector::target(table_name.clone(), None),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(target_record.checkpoint_id, committed_checkpoint);

        assert!(
            MetadataStore::advance_published_checkpoints(&mut service_impl)
                .await
                .unwrap()
        );

        let published_record = MetadataStore::get_published_checkpoint_record(
            &mut service_impl,
            &org_info,
            &PublishedCheckpointSelector::active(table_name.clone(), None),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(published_record.checkpoint_id, committed_checkpoint);
    }
}
