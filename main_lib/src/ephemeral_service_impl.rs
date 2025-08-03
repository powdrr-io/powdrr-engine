use std::collections::HashMap;
use idgenerator::IdInstance;
use crate::data_contract::CreateIndexTemplateBody;
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::pipeline::PipelineDefinition;
use crate::schema_massager::PowdrrSchema;
use crate::data_contract::{CompactionCommit, CompactionWorkItem, CreateTable, DeletesMetadata, ExtensionCommit, ExtensionFile, ExtensionWorkItem, FileSetPayload, IcebergCommit, SpeedboatCommit, SpeedboatCommitTableInfo, SpeedboatMetadata, TableDescription, TableMetadataCheckpoint};
use crate::state_provider::ServiceApiError;
use crate::peers::{CheckpointDescriptor};

type CommittedCheckpoints = HashMap<String, String>;


pub struct EphemeralServiceImpl {
    tables: HashMap<String, TableDescription>,
    // alias name -> table name
    table_aliases: HashMap<String, String>,
    table_templates: HashMap<String, CreateIndexTemplateBody>,
    pipelines: HashMap<String, PipelineDefinition>,
    lifetime_policies: HashMap<String, ILMPolicyDefinition>,
    latest_committed_checkpoint_id: HashMap<Option<String>, CommittedCheckpoints>,
    compaction_work_items: HashMap<String, CompactionWorkItem>,
    extension_work_items: HashMap<String, HashMap<String, ExtensionWorkItem>>,
    compactions: HashMap<String, CompactionCommit>,
    checkpoints: HashMap<String, TableMetadataCheckpoint>,
    checkpoints_needing_extension_work: HashMap<String, Vec<String>>,
    recent_file_extension_metadata: HashMap<String, Vec<ExtensionFile>>,
}

impl EphemeralServiceImpl {
    pub fn new() -> Self {
        EphemeralServiceImpl{
            tables: HashMap::new(),
            table_aliases: HashMap::new(),
            table_templates: HashMap::new(),
            pipelines: HashMap::new(),
            lifetime_policies: HashMap::new(),
            latest_committed_checkpoint_id: HashMap::new(),
            compaction_work_items: HashMap::new(),
            compactions: HashMap::new(),
            checkpoints: HashMap::new(),
            checkpoints_needing_extension_work: HashMap::new(),
            extension_work_items: HashMap::from([("es".to_string(), HashMap::new())]),
            recent_file_extension_metadata: HashMap::new(),
        }
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

    #[allow(dead_code)]
    fn set_latest_committed_checkpoint_id(&mut self, extension: Option<String>, table_name: &String, checkpoint_id: &String) -> () {
        if !self.latest_committed_checkpoint_id.contains_key(&extension) {
            self.latest_committed_checkpoint_id.insert(extension.clone(), HashMap::new());
        }
        self.latest_committed_checkpoint_id.get_mut(&extension).unwrap().insert(table_name.clone(), checkpoint_id.clone());
    }

    pub async fn add_checkpoint(&mut self, metadata: &TableMetadataCheckpoint) -> Result<(), ServiceApiError> {
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
        self.set_latest_committed_checkpoint_id(None, &metadata.table_name, &metadata.checkpoint_id);
        if metadata.extension_metadata.len() > 0 {
            for extension in metadata.extension_metadata.keys() {
                self.set_latest_committed_checkpoint_id(Some(extension.clone()), &metadata.table_name, &metadata.checkpoint_id);
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
        self.latest_committed_checkpoint_id.get(&extensions).map_or(None, |c| c.get(real_table_name).cloned())
    }

    fn set_latest_committed_checkpoint(&mut self, table_name: &String, extensions: Option<String>, checkpoint_id: &String) {
        let real_table_name = self.table_aliases.get(table_name).unwrap_or(table_name);
        if !self.latest_committed_checkpoint_id.contains_key(&extensions) {
            self.latest_committed_checkpoint_id.insert(extensions.clone(), HashMap::new());
        }
        self.latest_committed_checkpoint_id.get_mut(&extensions).unwrap().insert(real_table_name.clone(), checkpoint_id.clone());
    }

    async fn speedboat_commit_type_commit(&mut self, table_info: &SpeedboatCommitTableInfo, compactions: &Vec<String>) -> Result<(), ServiceApiError> {
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

        self.fill_extension_work_item(
            &table_info.table_name,
            &"es".to_string(),
            &new_checkpoint_id,
            &merged_schema,
            &speedboat_files,
            &iceberg_files
        );

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

    async fn speedboat_commit_type_delete(&mut self, table_info: &SpeedboatCommitTableInfo, compactions: &Vec<String>) -> Result<(), ServiceApiError> {
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

    pub async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        todo!()
    }

    pub async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), ServiceApiError> {
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

    pub async fn describe_table(&mut self, name: &String) -> Result<Option<TableDescription>, ServiceApiError> {
        let final_name = self.table_aliases.get(name).unwrap_or_else(|| name);
        match self.tables.get(final_name) {
            Some(d) => Ok(Some(d.clone())),
            None => Ok(None)
        }
    }

    pub async fn add_alias(&mut self, table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        // TODO: check if something exists
        self.table_aliases.insert(alias.clone(), table_name.clone());
        Ok(())
    }

    pub async fn remove_alias(&mut self, _table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        // TODO: check if something exists
        self.table_aliases.remove(alias);
        Ok(())
    }

    pub async fn create_table_template(&mut self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceApiError> {
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

    pub async fn describe_table_template(&mut self, name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        match self.table_templates.get(name) {
            Some(d) => Ok(Some(d.clone())),
            None => Ok(None)
        }
    }

    pub async fn create_pipeline(&mut self, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceApiError> {
        match self.pipelines.get(name) {
            Some(_) => panic!("Need to do a real error path now"),
            None => {
                self.pipelines.insert(name.clone(), pipeline.clone());
                Ok(())
            }
        }
    }

    pub async fn describe_pipeline(&mut self, name: &String) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        match self.pipelines.get(name) {
            Some(p) => Ok(Some(p.clone())),
            None => Ok(None)
        }
    }

    pub async fn create_lifetime_policy(&mut self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), ServiceApiError> {
        match self.lifetime_policies.get(name) {
            Some(_) => panic!("Need to do a real error path now"),
            None => {
                self.lifetime_policies.insert(name.clone(), policy.clone());
                Ok(())
            }
        }
    }

    pub async fn describe_lifetime_policy(&mut self, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        match self.lifetime_policies.get(name) {
            Some(p) => Ok(Some(p.clone())),
            None => Ok(None)
        }
    }


    pub async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), ServiceApiError> {
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

    pub async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceApiError> {
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

        self.fill_extension_work_item(
            &table_name,
            &"es".to_string(),
            &new_checkpoint_id,
            &merged_schema,
            &speedboat_files,
            &iceberg_files
        );

        Ok(())
    }

    pub async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), ServiceApiError> {
        let waiting_checkpoint_ids = self.checkpoints_needing_extension_work(table_name, &commit.extension).unwrap_or_else(|| vec!());
        assert!(waiting_checkpoint_ids.len() > 0);

        let removed_checkpoint_ids: Vec<String> = waiting_checkpoint_ids.iter().map(|x|self.add_coverage_for(table_name, x, commit)).flatten().collect();
        assert!(removed_checkpoint_ids.len() > 0);

        let max_id = removed_checkpoint_ids.iter().max().unwrap();

        self.add_recent_extension_files(table_name, commit);

        match self.get_latest_committed_checkpoint_sync(table_name, Some("es".to_string())) {
            Some(latest) => {
                if max_id > &latest {
                    self.set_latest_committed_checkpoint(table_name, Some("es".to_string()), max_id);
                }
            },
            None => {
                self.set_latest_committed_checkpoint(table_name, Some("es".to_string()), max_id);
            },
        };
        Ok(())
    }

    pub async fn compaction_commit(&mut self, _table_name: &String, commit: &CompactionCommit) -> Result<(), ServiceApiError> {
        // NOTE: this just notes what the compactor is saying. We don't generate the new checkpoint
        // until we see an iceberg or speedboat commit with the new info.
        self.compactions.insert(commit.compaction_id.clone(), commit.clone());
        Ok(())
    }

    pub async fn get_latest_committed_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, ServiceApiError> {
        Ok(self.get_latest_committed_checkpoint_sync(table_name, extensions))
    }

    pub async fn get_checkpoint(&mut self, snapshot: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        match self.get_checkpoint_sync(&snapshot.table_name, &snapshot.checkpoint_id) {
            Some(v) => Ok(Some(v.clone())),
            None => Ok(None)
        }
    }

    pub async fn get_extension_work_items(&mut self, extension_type: &String) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
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

    pub async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        let mut work_items = vec!();
        for (table_name, compaction) in self.compaction_work_items.iter_mut() {
            tracing::info!("Compaction work item stats: size = {}/{}, files = {}/200",
                compaction.speedboat_files.sizes.iter().sum::<u64>(),
                30 * 1024 * 1024,
                compaction.speedboat_files.sizes.len()
            );
            let do_compaction = compaction.speedboat_files.sizes.iter().sum::<u64>() > 30 * 1024 * 1024 || compaction.speedboat_files.sizes.len() > 200;
            //let do_compaction = true;
            if do_compaction {
                work_items.push((table_name.clone(), compaction.clone()));
                compaction.speedboat_files.clear();
            }
        }
        Ok(work_items)
    }

    pub async fn update_checkpoint(&mut self, _table_name: &String) -> Result<(), ServiceApiError> {
        unimplemented!()
    }
}
