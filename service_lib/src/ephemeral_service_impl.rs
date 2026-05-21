use crate::data_contract::{
    CleanupCommit, CleanupWorkItem, CreateIndexTemplateBody, IcebergMetadata, OrgInfo, OrgSettings,
};
use crate::data_contract::{
    CompactionCommit, CompactionWorkItem, CreateTable, DeletesMetadata, ExtensionCommit,
    ExtensionFile, ExtensionWorkItem, FileSetPayload, IcebergCommit, SpeedboatCommit,
    SpeedboatCommitTableInfo, SpeedboatMetadata, TableDescription, TableMetadataCheckpoint,
};
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::metadata_store::{
    CheckpointCutoverRequest, CheckpointCutoverState, CheckpointUpdateRequest,
    ClaimedCleanupWorkItem, ClaimedCompactionWorkItem, ClaimedExtensionWorkItem, CutoverEpoch,
    CutoverMembershipView, MetadataClaimKind, MetadataStore, PublishedCheckpointRecord,
    PublishedCheckpointRole, PublishedCheckpointSelector, ServingNodeActivationAck,
    ServingNodeLease,
};
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::schema_massager::PowdrrSchema;
use crate::state_provider::ServiceApiError;
use crate::test_api::TestProcessingMode;
use idgenerator::IdInstance;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

const LEASE_LENGTH_MS: i64 = 60 * 1000;
#[cfg(not(test))]
const WORK_CLAIM_LEASE_LENGTH_MS: i64 = 60 * 1000;
#[cfg(test)]
const WORK_CLAIM_LEASE_LENGTH_MS: i64 = 50;

type CommittedCheckpoints = HashMap<String, String>;
type SerializedCommittedCheckpoints = HashMap<String, CommittedCheckpoints>;
type PublishedCheckpoints = HashMap<String, String>;
type CheckpointPublicationRequests = HashMap<String, CheckpointUpdateRequest>;
type CheckpointCutoverEpochs = HashMap<String, CutoverEpoch>;
type CutoverMembershipViews = HashMap<String, CutoverMembershipView>;
type ServingNodeLeases = HashMap<String, ServingNodeLease>;
type ServingNodeActivations = HashMap<String, HashMap<String, ServingNodeActivationAck>>;
type ExtensionWorkItems = HashMap<String, HashMap<String, ExtensionWorkItemTracker>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtensionWorkItemTracker {
    work_item: ExtensionWorkItem,
    #[serde(default)]
    lease_expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactionWorkItemTracker {
    work_item: CompactionWorkItem,
    #[serde(default)]
    lease_expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CleanupWorkItemTracker {
    work_item: CleanupWorkItem,
    #[serde(default)]
    lease_expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EphemeralServiceSnapshot {
    tables: HashMap<String, TableDescription>,
    table_aliases: HashMap<String, String>,
    table_templates: HashMap<String, CreateIndexTemplateBody>,
    pipelines: HashMap<String, PipelineDefinition>,
    lifetime_policies: HashMap<String, ILMPolicyDefinition>,
    latest_committed_checkpoint_id: SerializedCommittedCheckpoints,
    published_checkpoint_id: PublishedCheckpoints,
    checkpoint_publication_requests: CheckpointPublicationRequests,
    checkpoint_cutover_epochs: CheckpointCutoverEpochs,
    cutover_membership_views: CutoverMembershipViews,
    serving_node_leases: ServingNodeLeases,
    serving_node_activations: ServingNodeActivations,
    compaction_work_items: HashMap<String, CompactionWorkItemTracker>,
    not_compacted_checkpoint_ids: HashMap<String, Vec<String>>,
    extension_work_items: ExtensionWorkItems,
    cleanup_work_items: Vec<CleanupWorkItemTracker>,
    compactions: HashMap<String, (String, CompactionCommit)>,
    checkpoints: HashMap<String, TableMetadataCheckpoint>,
    checkpoints_needing_extension_work: HashMap<String, Vec<String>>,
    recent_file_extension_metadata: HashMap<String, Vec<ExtensionFile>>,
    org_settings_by_id: HashMap<String, OrgSettings>,
    org_lookup: HashMap<String, OrgInfo>,
}

pub struct EphemeralServiceImpl {
    mode: TestProcessingMode,
    tables: HashMap<String, TableDescription>,
    // alias name -> table name
    table_aliases: HashMap<String, String>,
    table_templates: HashMap<String, CreateIndexTemplateBody>,
    pipelines: HashMap<String, PipelineDefinition>,
    lifetime_policies: HashMap<String, ILMPolicyDefinition>,
    latest_committed_checkpoint_id: HashMap<Option<String>, CommittedCheckpoints>,
    published_checkpoint_id: PublishedCheckpoints,
    checkpoint_publication_requests: CheckpointPublicationRequests,
    checkpoint_cutover_epochs: CheckpointCutoverEpochs,
    cutover_membership_views: CutoverMembershipViews,
    serving_node_leases: ServingNodeLeases,
    serving_node_activations: ServingNodeActivations,
    compaction_work_items: HashMap<String, CompactionWorkItemTracker>,
    not_compacted_checkpoint_ids: HashMap<String, Vec<String>>,
    extension_work_items: ExtensionWorkItems,
    cleanup_work_items: Vec<CleanupWorkItemTracker>,
    compactions: HashMap<String, (String, CompactionCommit)>,
    checkpoints: HashMap<String, TableMetadataCheckpoint>,
    checkpoints_needing_extension_work: HashMap<String, Vec<String>>,
    recent_file_extension_metadata: HashMap<String, Vec<ExtensionFile>>,
    org_settings_by_id: HashMap<String, OrgSettings>,
    org_lookup: HashMap<String, OrgInfo>,
}

impl EphemeralServiceImpl {
    pub fn new(mode: TestProcessingMode) -> Self {
        Self::from_snapshot(mode, EphemeralServiceSnapshot::default())
    }

    pub fn from_snapshot(mode: TestProcessingMode, snapshot: EphemeralServiceSnapshot) -> Self {
        EphemeralServiceImpl {
            mode: mode,
            tables: snapshot.tables,
            table_aliases: snapshot.table_aliases,
            table_templates: snapshot.table_templates,
            pipelines: snapshot.pipelines,
            lifetime_policies: snapshot.lifetime_policies,
            latest_committed_checkpoint_id: Self::deserialize_latest_committed_checkpoint_id(
                snapshot.latest_committed_checkpoint_id,
            ),
            published_checkpoint_id: snapshot.published_checkpoint_id,
            checkpoint_publication_requests: snapshot.checkpoint_publication_requests,
            checkpoint_cutover_epochs: snapshot.checkpoint_cutover_epochs,
            cutover_membership_views: snapshot.cutover_membership_views,
            serving_node_leases: snapshot.serving_node_leases,
            serving_node_activations: snapshot.serving_node_activations,
            compaction_work_items: snapshot.compaction_work_items,
            not_compacted_checkpoint_ids: snapshot.not_compacted_checkpoint_ids,
            extension_work_items: if snapshot.extension_work_items.is_empty() {
                HashMap::from([("es".to_string(), HashMap::new())])
            } else {
                snapshot.extension_work_items
            },
            cleanup_work_items: snapshot.cleanup_work_items,
            compactions: snapshot.compactions,
            checkpoints: snapshot.checkpoints,
            checkpoints_needing_extension_work: snapshot.checkpoints_needing_extension_work,
            recent_file_extension_metadata: snapshot.recent_file_extension_metadata,
            org_settings_by_id: snapshot.org_settings_by_id,
            org_lookup: snapshot.org_lookup,
        }
    }

    pub fn snapshot_state(&self) -> EphemeralServiceSnapshot {
        EphemeralServiceSnapshot {
            tables: self.tables.clone(),
            table_aliases: self.table_aliases.clone(),
            table_templates: self.table_templates.clone(),
            pipelines: self.pipelines.clone(),
            lifetime_policies: self.lifetime_policies.clone(),
            latest_committed_checkpoint_id: self.serialize_latest_committed_checkpoint_id(),
            published_checkpoint_id: self.published_checkpoint_id.clone(),
            checkpoint_publication_requests: self.checkpoint_publication_requests.clone(),
            checkpoint_cutover_epochs: self.checkpoint_cutover_epochs.clone(),
            cutover_membership_views: self.cutover_membership_views.clone(),
            serving_node_leases: self.serving_node_leases.clone(),
            serving_node_activations: self.serving_node_activations.clone(),
            compaction_work_items: self.compaction_work_items.clone(),
            not_compacted_checkpoint_ids: self.not_compacted_checkpoint_ids.clone(),
            extension_work_items: self.extension_work_items.clone(),
            cleanup_work_items: self.cleanup_work_items.clone(),
            compactions: self.compactions.clone(),
            checkpoints: self.checkpoints.clone(),
            checkpoints_needing_extension_work: self.checkpoints_needing_extension_work.clone(),
            recent_file_extension_metadata: self.recent_file_extension_metadata.clone(),
            org_settings_by_id: self.org_settings_by_id.clone(),
            org_lookup: self.org_lookup.clone(),
        }
    }

    fn org_info_key(access_key_id: &String, secret_access_key: &String) -> String {
        format!("{}:{}", access_key_id, secret_access_key)
    }

    fn role_key(role: PublishedCheckpointRole) -> &'static str {
        match role {
            PublishedCheckpointRole::Active => "active",
            PublishedCheckpointRole::Target => "target",
        }
    }

    fn extension_storage_key(extension: &Option<String>) -> String {
        extension.clone().unwrap_or_default()
    }

    fn extension_from_storage_key(key: &String) -> Option<String> {
        if key.is_empty() {
            None
        } else {
            Some(key.clone())
        }
    }

    fn selector_storage_key(
        role: PublishedCheckpointRole,
        table_name: &String,
        extension: &Option<String>,
    ) -> String {
        format!(
            "{}|{}|{}",
            Self::role_key(role),
            Self::extension_storage_key(extension),
            table_name
        )
    }

    fn selector_group_key(table_name: &String, extension: &Option<String>) -> String {
        format!("{}|{}", Self::extension_storage_key(extension), table_name)
    }

    fn checkpoint_publication_request_key(org_id: &String, table_name: &String) -> String {
        format!("{org_id}|{table_name}")
    }

    fn current_timestamp_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default()
    }

    fn claim_lease_expires_at_ms() -> i64 {
        Self::current_timestamp_ms() + WORK_CLAIM_LEASE_LENGTH_MS
    }

    fn lease_is_available(lease_expires_at_ms: Option<i64>) -> bool {
        lease_expires_at_ms
            .map(|expires_at_ms| expires_at_ms <= Self::current_timestamp_ms())
            .unwrap_or(true)
    }

    fn bump_cutover_epoch(
        &mut self,
        table_name: &String,
        extension: &Option<String>,
    ) -> CutoverEpoch {
        let key = Self::selector_group_key(table_name, extension);
        let next_epoch = self
            .checkpoint_cutover_epochs
            .get(&key)
            .copied()
            .unwrap_or_default()
            .0
            + 1;
        let epoch = CutoverEpoch(next_epoch);
        self.checkpoint_cutover_epochs.insert(key, epoch);
        epoch
    }

    fn clear_serving_node_activations(&mut self, table_name: &String, extension: &Option<String>) {
        self.serving_node_activations
            .remove(&Self::selector_group_key(table_name, extension));
    }

    fn get_cutover_membership_view(
        &self,
        table_name: &String,
        extension: &Option<String>,
    ) -> Option<&CutoverMembershipView> {
        self.cutover_membership_views
            .get(&Self::selector_group_key(table_name, extension))
    }

    fn store_cutover_membership_view(
        &mut self,
        table_name: &String,
        extension: &Option<String>,
        epoch: CutoverEpoch,
        target_checkpoint_id: &String,
        required_node_ids: Vec<String>,
    ) {
        self.cutover_membership_views.insert(
            Self::selector_group_key(table_name, extension),
            CutoverMembershipView {
                selector: PublishedCheckpointSelector::target(
                    table_name.clone(),
                    extension.clone(),
                ),
                epoch,
                target_checkpoint_id: target_checkpoint_id.clone(),
                required_node_ids,
            },
        );
    }

    fn prune_expired_serving_node_leases(&mut self) {
        let earliest_live_ms = Self::current_timestamp_ms() - LEASE_LENGTH_MS;
        self.serving_node_leases
            .retain(|_, lease| lease.observed_at_ms >= earliest_live_ms);
    }

    fn live_serving_node_ids(&mut self) -> HashSet<String> {
        self.prune_expired_serving_node_leases();
        self.serving_node_leases.keys().cloned().collect()
    }

    fn maybe_reconfigure_cutover_membership_for_target(
        &mut self,
        table_name: &String,
        extension: &Option<String>,
        target_checkpoint_id: &String,
    ) {
        let Some(existing_view) = self
            .get_cutover_membership_view(table_name, extension)
            .cloned()
        else {
            return;
        };
        if existing_view.target_checkpoint_id != *target_checkpoint_id {
            return;
        }

        let live_node_ids = self.live_serving_node_ids();
        if live_node_ids.is_empty() {
            return;
        }
        if existing_view
            .required_node_ids
            .iter()
            .all(|node_id| live_node_ids.contains(node_id))
        {
            return;
        }

        let mut required_node_ids: Vec<String> = live_node_ids.into_iter().collect();
        required_node_ids.sort();
        let epoch = self.bump_cutover_epoch(table_name, extension);
        self.store_cutover_membership_view(
            table_name,
            extension,
            epoch,
            target_checkpoint_id,
            required_node_ids,
        );
        self.clear_serving_node_activations(table_name, extension);
    }

    fn capture_cutover_membership_for_target(
        &mut self,
        table_name: &String,
        extension: &Option<String>,
        target_checkpoint_id: &String,
    ) {
        if self
            .get_cutover_membership_view(table_name, extension)
            .map(|view| view.target_checkpoint_id == *target_checkpoint_id)
            .unwrap_or(false)
        {
            return;
        }

        let mut required_node_ids: Vec<String> = self.live_serving_node_ids().into_iter().collect();
        required_node_ids.sort();
        let epoch = self.bump_cutover_epoch(table_name, extension);
        self.store_cutover_membership_view(
            table_name,
            extension,
            epoch,
            target_checkpoint_id,
            required_node_ids,
        );
        self.clear_serving_node_activations(table_name, extension);
    }

    fn backfill_cutover_membership_for_target(
        &mut self,
        table_name: &String,
        extension: &Option<String>,
        target_checkpoint_id: &String,
    ) {
        let Some(existing_view) = self
            .get_cutover_membership_view(table_name, extension)
            .cloned()
        else {
            return;
        };

        if existing_view.target_checkpoint_id != *target_checkpoint_id
            || !existing_view.required_node_ids.is_empty()
        {
            return;
        }

        let mut required_node_ids: Vec<String> = self.live_serving_node_ids().into_iter().collect();
        required_node_ids.sort();
        if required_node_ids.is_empty() {
            return;
        }

        self.store_cutover_membership_view(
            table_name,
            extension,
            existing_view.epoch,
            target_checkpoint_id,
            required_node_ids,
        );
    }

    fn get_published_checkpoint_sync(
        &self,
        role: PublishedCheckpointRole,
        table_name: &String,
        extension: &Option<String>,
    ) -> Option<String> {
        self.published_checkpoint_id
            .get(&Self::selector_storage_key(role, table_name, extension))
            .cloned()
    }

    fn set_published_checkpoint(
        &mut self,
        role: PublishedCheckpointRole,
        table_name: &String,
        extension: &Option<String>,
        checkpoint_id: &String,
    ) {
        self.published_checkpoint_id.insert(
            Self::selector_storage_key(role, table_name, extension),
            checkpoint_id.clone(),
        );
    }

    fn published_checkpoint_needs_activation(
        &self,
        table_name: &String,
        extension: &Option<String>,
    ) -> bool {
        match self.get_published_checkpoint_sync(
            PublishedCheckpointRole::Target,
            table_name,
            extension,
        ) {
            Some(target_checkpoint_id) => {
                self.get_published_checkpoint_sync(
                    PublishedCheckpointRole::Active,
                    table_name,
                    extension,
                ) != Some(target_checkpoint_id)
            }
            None => false,
        }
    }

    fn checkpoint_publication_still_pending(&self, table_name: &String) -> bool {
        self.extension_work_items
            .get("es")
            .and_then(|items| items.get(table_name))
            .map(|item| item.work_item.has_work())
            .unwrap_or(false)
            || self
                .checkpoints_needing_extension_work
                .contains_key(&format!("{}_{}", table_name, "es"))
            || self.published_checkpoint_needs_activation(table_name, &None)
            || self.published_checkpoint_needs_activation(table_name, &Some("es".to_string()))
    }

    fn checkpoint_references_any_cleanup_file(
        checkpoint: &TableMetadataCheckpoint,
        files_to_delete: &HashSet<String>,
    ) -> bool {
        checkpoint
            .speedboat_metadata
            .as_ref()
            .map(|metadata| {
                metadata
                    .files
                    .file_paths
                    .iter()
                    .any(|file_path| files_to_delete.contains(file_path))
            })
            .unwrap_or(false)
            || checkpoint
                .deletes_metadata
                .as_ref()
                .map(|metadata| {
                    metadata
                        .files
                        .iter()
                        .any(|file_path| files_to_delete.contains(file_path))
                })
                .unwrap_or(false)
    }

    fn cleanup_work_item_is_ready(&self, work_item: &CleanupWorkItem) -> bool {
        let files_to_delete: HashSet<String> = work_item.files_to_delete.iter().cloned().collect();
        for extension in [None, Some("es".to_string())] {
            let Some(checkpoint_id) = self.get_published_checkpoint_sync(
                PublishedCheckpointRole::Active,
                &work_item.table_name,
                &extension,
            ) else {
                continue;
            };
            let Some(checkpoint) = self.get_checkpoint_sync(&work_item.table_name, &checkpoint_id)
            else {
                continue;
            };
            if Self::checkpoint_references_any_cleanup_file(&checkpoint, &files_to_delete) {
                return false;
            }
        }

        true
    }

    fn get_latest_materialized_checkpoint_sync(
        &self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Option<String> {
        let real_table_name = self.table_aliases.get(table_name).unwrap_or(table_name);
        self.latest_committed_checkpoint_id
            .get(&extensions)
            .and_then(|c| c.get(real_table_name).cloned())
    }

    fn deserialize_latest_committed_checkpoint_id(
        serialized: SerializedCommittedCheckpoints,
    ) -> HashMap<Option<String>, CommittedCheckpoints> {
        serialized
            .into_iter()
            .map(|(extension, checkpoints)| {
                (Self::extension_from_storage_key(&extension), checkpoints)
            })
            .collect()
    }

    fn serialize_latest_committed_checkpoint_id(&self) -> SerializedCommittedCheckpoints {
        self.latest_committed_checkpoint_id
            .iter()
            .map(|(extension, checkpoints)| {
                (Self::extension_storage_key(extension), checkpoints.clone())
            })
            .collect()
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

    pub async fn add_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        metadata: &TableMetadataCheckpoint,
    ) -> Result<(), ServiceApiError> {
        // To make testing a little easier, we'll just magic up a table as necessary
        if !self.tables.contains_key(&metadata.table_name) {
            self.tables.insert(
                metadata.table_name.clone(),
                TableDescription {
                    name: metadata.table_name.clone(),
                    tags: Default::default(),
                    serving: None,
                    dynamodb: None,
                    mongodb: None,
                },
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
        if metadata.extension_metadata.len() > 0 {
            for extension in metadata.extension_metadata.keys() {
                self.set_latest_committed_checkpoint_id(
                    Some(extension.clone()),
                    &metadata.table_name,
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
        self.queue_publication_request(&org_info.org_id, &metadata.table_name);
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
                ExtensionWorkItemTracker {
                    work_item: ExtensionWorkItem {
                        id: IdInstance::next_id().to_string(),
                        extension_type: extension.clone(),
                        table_name: table_name.clone(),
                        checkpoint_id: Some(checkpoint_id.clone()),
                        table_schema: schema.clone(),
                        speedboat_files: speedboat_files.clone(),
                        iceberg_files: iceberg_files.clone(),
                    },
                    lease_expires_at_ms: None,
                },
            );
        } else {
            let table_work_item = es_work_items.get_mut(table_name).unwrap();
            table_work_item.work_item.checkpoint_id = Some(checkpoint_id.clone());
            table_work_item.work_item.table_schema = schema.clone();
            table_work_item.work_item.speedboat_files = table_work_item
                .work_item
                .speedboat_files
                .merge(speedboat_files);
            table_work_item.work_item.iceberg_files = table_work_item
                .work_item
                .iceberg_files
                .merge(&iceberg_files);
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

    fn set_latest_committed_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
        checkpoint_id: &String,
    ) {
        let real_table_name = self.table_aliases.get(table_name).unwrap_or(table_name);
        if !self
            .latest_committed_checkpoint_id
            .contains_key(&extensions)
        {
            self.latest_committed_checkpoint_id
                .insert(extensions.clone(), HashMap::new());
        }
        self.latest_committed_checkpoint_id
            .get_mut(&extensions)
            .unwrap()
            .insert(real_table_name.clone(), checkpoint_id.clone());
    }

    async fn speedboat_commit_type_commit(
        &mut self,
        table_info: &SpeedboatCommitTableInfo,
        compaction: &Option<String>,
    ) -> Result<(), ServiceApiError> {
        let latest_checkpoint =
            match self.get_latest_materialized_checkpoint_sync(&table_info.table_name, None) {
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
        _iceberg_metadata: &IcebergMetadata,
    ) -> CleanupWorkItem {
        let (table_name, compaction_obj) = self.compactions.get(compaction).unwrap();

        match self.compaction_work_items.get(table_name) {
            Some(tracker) => {
                assert_eq!(
                    tracker.work_item.checkpoint_id_to_replace,
                    compaction_obj.checkpoint_id_to_replace
                );
                assert_eq!(tracker.work_item.id, *compaction);
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
        self.not_compacted_checkpoint_ids
            .entry(checkpoint.table_name.clone())
            .or_default();

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
                    lease_expires_at_ms: None,
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
            match self.get_latest_materialized_checkpoint_sync(&table_info.table_name, None) {
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

    fn apply_extension_commit_to_work_item(
        &mut self,
        table_name: &String,
        extension_commit: &ExtensionCommit,
    ) {
        let Some(table_work_item) = self
            .extension_work_items
            .get_mut(&extension_commit.extension)
            .and_then(|items| items.get_mut(table_name))
        else {
            return;
        };

        let committed_files: Vec<String> = extension_commit.files.keys().cloned().collect();
        table_work_item
            .work_item
            .speedboat_files
            .remove(&committed_files);
        table_work_item
            .work_item
            .iceberg_files
            .remove(&committed_files);
        table_work_item.lease_expires_at_ms = None;
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
        let mut touched_tables = vec![];
        assert!(
            commit.compaction.is_none(),
            "Speedboat commits do not yet support compactions"
        );
        for table_info in commit.type_files.iter() {
            if !touched_tables.contains(&table_info.table_name) {
                touched_tables.push(table_info.table_name.clone());
            }
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
        for table_name in touched_tables.iter() {
            self.queue_publication_request(&org_info.org_id, table_name);
        }
        Ok(true)
    }

    pub async fn iceberg_commit(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        iceberg_commit: &IcebergCommit,
    ) -> Result<bool, ServiceApiError> {
        let latest_checkpoint = match self.get_latest_materialized_checkpoint_sync(table_name, None)
        {
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
            self.cleanup_work_items.push(CleanupWorkItemTracker {
                work_item: cleanup_work_item,
                lease_expires_at_ms: None,
            });
        }

        self.queue_publication_request(&org_info.org_id, table_name);
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
        self.apply_extension_commit_to_work_item(table_name, commit);

        match self.get_latest_materialized_checkpoint_sync(table_name, Some("es".to_string())) {
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
        self.queue_publication_request(&org_info.org_id, table_name);
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
        commit: &CleanupCommit,
    ) -> Result<bool, ServiceApiError> {
        self.cleanup_work_items
            .retain(|work_item| work_item.work_item.id != commit.id);
        Ok(true)
    }

    pub async fn get_latest_committed_checkpoint(
        &mut self,
        _org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        Ok(self.get_latest_materialized_checkpoint_sync(table_name, extensions))
    }

    pub async fn get_published_active_checkpoint(
        &mut self,
        _org_info: &OrgInfo,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        Ok(self.get_published_checkpoint_sync(
            PublishedCheckpointRole::Active,
            table_name,
            &extensions,
        ))
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
                        if work_items.work_item.has_work()
                            && Self::lease_is_available(work_items.lease_expires_at_ms)
                        {
                            collected_work_items.push(work_items.work_item.clone());
                            work_items.lease_expires_at_ms =
                                Some(Self::claim_lease_expires_at_ms());
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
            if Self::lease_is_available(compaction_tracker.lease_expires_at_ms) {
                tracing::info!("Returning compaction work item for {}", table_name);
                work_items.push((table_name.clone(), compaction_tracker.work_item.clone()));
                compaction_tracker.lease_expires_at_ms = Some(Self::claim_lease_expires_at_ms());
            }
        }
        Ok(work_items)
    }

    pub async fn get_cleanup_work_items(
        &mut self,
        _org_info: &OrgInfo,
    ) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        let mut work_items = vec![];
        let ready_indexes: Vec<usize> = self
            .cleanup_work_items
            .iter()
            .enumerate()
            .filter_map(|(index, cleanup_work_item)| {
                (self.cleanup_work_item_is_ready(&cleanup_work_item.work_item)
                    && Self::lease_is_available(cleanup_work_item.lease_expires_at_ms))
                .then_some(index)
            })
            .collect();

        for index in ready_indexes {
            let cleanup_work_item = &mut self.cleanup_work_items[index];
            work_items.push(cleanup_work_item.work_item.clone());
            cleanup_work_item.lease_expires_at_ms = Some(Self::claim_lease_expires_at_ms());
        }
        tracing::info!("Returning {} cleanup work items", work_items.len());
        Ok(work_items)
    }

    fn queue_publication_request(&mut self, org_id: &String, table_name: &String) {
        let request = CheckpointUpdateRequest::new(org_id.clone(), table_name.clone());
        self.checkpoint_publication_requests.insert(
            Self::checkpoint_publication_request_key(org_id, table_name),
            request,
        );
    }

    fn advance_target_checkpoint(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> bool {
        let Some(checkpoint_id) =
            self.get_latest_materialized_checkpoint_sync(table_name, extension.clone())
        else {
            return false;
        };

        if self.get_published_checkpoint_sync(
            PublishedCheckpointRole::Target,
            table_name,
            &extension,
        ) == Some(checkpoint_id.clone())
        {
            return false;
        }

        self.set_published_checkpoint(
            PublishedCheckpointRole::Target,
            table_name,
            &extension,
            &checkpoint_id,
        );
        self.capture_cutover_membership_for_target(table_name, &extension, &checkpoint_id);
        true
    }

    fn activation_matches_target(
        &mut self,
        table_name: &String,
        extension: &Option<String>,
        target_checkpoint_id: &String,
    ) -> bool {
        self.backfill_cutover_membership_for_target(table_name, extension, target_checkpoint_id);
        self.maybe_reconfigure_cutover_membership_for_target(
            table_name,
            extension,
            target_checkpoint_id,
        );

        let Some(view) = self
            .get_cutover_membership_view(table_name, extension)
            .cloned()
        else {
            return false;
        };
        if view.target_checkpoint_id != *target_checkpoint_id || view.required_node_ids.is_empty() {
            return false;
        }
        let Some(acks) = self
            .serving_node_activations
            .get(&Self::selector_group_key(table_name, extension))
        else {
            return false;
        };

        view.required_node_ids.into_iter().all(|node_id| {
            acks.get(&node_id)
                .map(|ack| ack.checkpoint_id == *target_checkpoint_id && ack.epoch == view.epoch)
                .unwrap_or(false)
        })
    }

    fn promote_active_checkpoint_if_ready(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> bool {
        let Some(target_checkpoint_id) = self.get_published_checkpoint_sync(
            PublishedCheckpointRole::Target,
            table_name,
            &extension,
        ) else {
            return false;
        };

        if self.get_published_checkpoint_sync(
            PublishedCheckpointRole::Active,
            table_name,
            &extension,
        ) == Some(target_checkpoint_id.clone())
        {
            return false;
        }

        if !self.activation_matches_target(table_name, &extension, &target_checkpoint_id) {
            return false;
        }

        self.set_published_checkpoint(
            PublishedCheckpointRole::Active,
            table_name,
            &extension,
            &target_checkpoint_id,
        );
        true
    }

    pub async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        let mut work_done = false;
        let pending_requests: Vec<CheckpointUpdateRequest> = self
            .checkpoint_publication_requests
            .values()
            .cloned()
            .collect();

        for request in pending_requests.iter() {
            work_done |= self.advance_target_checkpoint(&request.table_name, None);
            work_done |= self.promote_active_checkpoint_if_ready(&request.table_name, None);
            work_done |=
                self.advance_target_checkpoint(&request.table_name, Some("es".to_string()));
            work_done |= self
                .promote_active_checkpoint_if_ready(&request.table_name, Some("es".to_string()));

            if !self.checkpoint_publication_still_pending(&request.table_name) {
                self.checkpoint_publication_requests.remove(
                    &Self::checkpoint_publication_request_key(&request.org_id, &request.table_name),
                );
            }
        }

        Ok(work_done)
    }

    pub async fn create_org(&mut self, _settings: &OrgSettings) -> Result<(), ServiceApiError> {
        let settings = _settings.clone();
        let org_info = settings.to_org_info();
        self.org_settings_by_id
            .insert(settings.org_id.clone(), settings.clone());
        for creds in settings.creds.iter() {
            self.org_lookup.insert(
                Self::org_info_key(&creds.access_key_id, &creds.secret_access_key),
                org_info.clone(),
            );
        }
        Ok(())
    }

    pub async fn lookup_org(
        &mut self,
        access_key: &String,
        secret_key: &String,
    ) -> Result<Option<OrgInfo>, ServiceApiError> {
        Ok(self
            .org_lookup
            .get(&Self::org_info_key(access_key, secret_key))
            .cloned())
    }
}

#[async_trait::async_trait]
impl MetadataStore for EphemeralServiceImpl {
    async fn queue_checkpoint_publication(
        &mut self,
        request: &CheckpointUpdateRequest,
    ) -> Result<(), ServiceApiError> {
        self.queue_publication_request(&request.org_id, &request.table_name);
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
        _org_info: &OrgInfo,
        selector: &PublishedCheckpointSelector,
    ) -> Result<Option<PublishedCheckpointRecord>, ServiceApiError> {
        Ok(self
            .get_published_checkpoint_sync(selector.role, &selector.table_name, &selector.extension)
            .map(|checkpoint_id| PublishedCheckpointRecord {
                selector: selector.clone(),
                checkpoint_id,
            }))
    }

    async fn plan_checkpoint_cutover(
        &mut self,
        request: &CheckpointCutoverRequest,
    ) -> Result<(), ServiceApiError> {
        if self.get_published_checkpoint_sync(
            PublishedCheckpointRole::Target,
            &request.selector.table_name,
            &request.selector.extension,
        ) != Some(request.target_checkpoint_id.clone())
        {
            self.set_published_checkpoint(
                PublishedCheckpointRole::Target,
                &request.selector.table_name,
                &request.selector.extension,
                &request.target_checkpoint_id,
            );
            self.capture_cutover_membership_for_target(
                &request.selector.table_name,
                &request.selector.extension,
                &request.target_checkpoint_id,
            );
        }
        Ok(())
    }

    async fn get_checkpoint_cutover_state(
        &mut self,
        _org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceApiError> {
        let key = Self::selector_group_key(table_name, &extension);
        Ok(CheckpointCutoverState {
            selector: PublishedCheckpointSelector::target(table_name.clone(), extension.clone()),
            epoch: self
                .checkpoint_cutover_epochs
                .get(&key)
                .copied()
                .unwrap_or_default(),
            active_checkpoint_id: self.get_published_checkpoint_sync(
                PublishedCheckpointRole::Active,
                table_name,
                &extension,
            ),
            target_checkpoint_id: self
                .get_published_checkpoint_sync(
                    PublishedCheckpointRole::Target,
                    table_name,
                    &extension,
                )
                .or_else(|| {
                    self.get_published_checkpoint_sync(
                        PublishedCheckpointRole::Active,
                        table_name,
                        &extension,
                    )
                }),
        })
    }

    async fn heartbeat_serving_node(
        &mut self,
        _org_info: &OrgInfo,
        lease: &ServingNodeLease,
    ) -> Result<(), ServiceApiError> {
        self.prune_expired_serving_node_leases();
        self.serving_node_leases
            .insert(lease.node_id.clone(), lease.clone());
        Ok(())
    }

    async fn record_serving_node_activation(
        &mut self,
        _org_info: &OrgInfo,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceApiError> {
        let key = Self::selector_group_key(&ack.selector.table_name, &ack.selector.extension);
        self.serving_node_activations
            .entry(key)
            .or_default()
            .insert(ack.node_id.clone(), ack.clone());
        Ok(())
    }

    async fn list_serving_node_activations(
        &mut self,
        _org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ServingNodeActivationAck>, ServiceApiError> {
        Ok(self
            .serving_node_activations
            .get(&Self::selector_group_key(table_name, &extension))
            .map(|acks| acks.values().cloned().collect())
            .unwrap_or_default())
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
                    claim: MetadataClaimKind::Leased,
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
                    claim: MetadataClaimKind::Leased,
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
                claim: MetadataClaimKind::Leased,
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
    use super::EphemeralServiceImpl;
    use crate::data_contract::{
        CreateTable, FileSetPayload, IcebergCommit, IcebergMetadata, LicenseType, OrgCreds,
        OrgInfo, OrgSettings,
    };
    use crate::metadata_store::{MetadataStore, ServingNodeActivationAck, ServingNodeLease};
    use crate::schema_massager::PowdrrSchema;
    use crate::test_api::TestProcessingMode;
    use std::collections::HashMap;

    fn org_info() -> OrgInfo {
        OrgInfo {
            org_id: "org-1".to_string(),
            license_type: LicenseType::Pro,
        }
    }

    fn iceberg_metadata(file_path: &str, snapshot_id: &str) -> IcebergMetadata {
        let schema = PowdrrSchema::minimal();
        IcebergMetadata {
            table_schema: schema.clone(),
            snapshot_id: Some(snapshot_id.to_string()),
            files: FileSetPayload::single(file_path.to_string(), 128, schema),
            partition_spec: vec![],
            sort_order: vec![],
            column_names: vec![],
            column_stats: vec![],
            access_artifacts: vec![],
            file_stats: vec![],
        }
    }

    #[tokio::test]
    async fn org_lookup_survives_snapshot_round_trip() {
        let mut state = EphemeralServiceImpl::new(TestProcessingMode::default());
        let settings = OrgSettings {
            org_id: "org-1".to_string(),
            license_type: LicenseType::Pro,
            creds: vec![OrgCreds {
                access_key_id: "access".to_string(),
                secret_access_key: "secret".to_string(),
                nickname: Some("primary".to_string()),
            }],
        };

        state.create_org(&settings).await.unwrap();
        let snapshot = state.snapshot_state();
        let mut restored =
            EphemeralServiceImpl::from_snapshot(TestProcessingMode::default(), snapshot);

        let org = restored
            .lookup_org(&"access".to_string(), &"secret".to_string())
            .await
            .unwrap();

        assert_eq!(org.unwrap().org_id, "org-1".to_string());
    }

    #[tokio::test]
    async fn committed_target_and_active_frontiers_diverge_until_activation() {
        let mut state = EphemeralServiceImpl::new(TestProcessingMode::default());
        let org_info = org_info();
        let table_name = "ephemeral-frontier-table".to_string();

        state
            .create_table(
                &org_info,
                &CreateTable {
                    name: table_name.clone(),
                    tags: HashMap::new(),
                    serving: None,
                    dynamodb: None,
                    mongodb: None,
                },
            )
            .await
            .unwrap();

        state
            .iceberg_commit(
                &org_info,
                &table_name,
                &IcebergCommit {
                    metadata: iceberg_metadata("s3://warehouse/table/data-0001.parquet", "1"),
                    deletes_table_info: None,
                    compactions: vec![],
                },
            )
            .await
            .unwrap();

        let committed_checkpoint = MetadataStore::get_latest_committed_checkpoint(
            &mut state,
            &org_info,
            &table_name,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            MetadataStore::get_latest_target_checkpoint(&mut state, &org_info, &table_name, None)
                .await
                .unwrap(),
            Some(committed_checkpoint.clone())
        );
        assert_eq!(
            MetadataStore::get_published_active_checkpoint(
                &mut state,
                &org_info,
                &table_name,
                None,
            )
            .await
            .unwrap(),
            None
        );

        assert!(
            MetadataStore::advance_published_checkpoints(&mut state)
                .await
                .unwrap()
        );
        assert_eq!(
            MetadataStore::get_latest_target_checkpoint(&mut state, &org_info, &table_name, None)
                .await
                .unwrap(),
            Some(committed_checkpoint.clone())
        );
        assert_eq!(
            MetadataStore::get_published_active_checkpoint(
                &mut state,
                &org_info,
                &table_name,
                None,
            )
            .await
            .unwrap(),
            None
        );

        let cutover_state =
            MetadataStore::get_checkpoint_cutover_state(&mut state, &org_info, &table_name, None)
                .await
                .unwrap();
        let observed_at_ms = chrono::Utc::now().timestamp_millis();
        MetadataStore::heartbeat_serving_node(
            &mut state,
            &org_info,
            &ServingNodeLease {
                node_id: "warm-cache".to_string(),
                membership_epoch: cutover_state.epoch,
                observed_at_ms,
            },
        )
        .await
        .unwrap();
        MetadataStore::record_serving_node_activation(
            &mut state,
            &org_info,
            &ServingNodeActivationAck {
                selector: cutover_state.selector,
                node_id: "warm-cache".to_string(),
                epoch: cutover_state.epoch,
                checkpoint_id: committed_checkpoint.clone(),
                activated_at_ms: observed_at_ms + 1,
            },
        )
        .await
        .unwrap();

        assert!(
            MetadataStore::advance_published_checkpoints(&mut state)
                .await
                .unwrap()
        );
        assert_eq!(
            MetadataStore::get_published_active_checkpoint(
                &mut state,
                &org_info,
                &table_name,
                None,
            )
            .await
            .unwrap(),
            Some(committed_checkpoint)
        );
    }
}
