use crate::data_contract::OrgInfo;
use crate::metadata_store::{
    CheckpointCutoverState, CutoverEpoch, MetadataStore, PublishedCheckpointSelector,
    ServingNodeActivationAck, ServingNodeLease,
};
use crate::state_provider::ServiceApiError;
use async_trait::async_trait;
use std::collections::HashSet;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnlyTargetPhase {
    AwaitingReadiness,
    AwaitingActivation,
    Active,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactClass {
    SnapshotLookupMmap,
    SnapshotExactLookupMmap,
    SearchProjection,
    StatsManifest,
    Custom(String),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReadinessAck {
    pub selector: PublishedCheckpointSelector,
    pub checkpoint_id: String,
    pub epoch: CutoverEpoch,
    pub artifact_class: ArtifactClass,
    pub producer_id: String,
    pub ready_at_ms: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ReadOnlyCheckpointCoordinationState {
    pub selector: PublishedCheckpointSelector,
    pub epoch: CutoverEpoch,
    pub active_checkpoint_id: Option<String>,
    pub target_checkpoint_id: Option<String>,
    pub phase: ReadOnlyTargetPhase,
    pub readiness_acks: Vec<ArtifactReadinessAck>,
}

pub fn required_artifact_classes(extension: Option<&String>) -> Vec<ArtifactClass> {
    match extension {
        None => vec![
            ArtifactClass::SnapshotLookupMmap,
            ArtifactClass::SnapshotExactLookupMmap,
        ],
        Some(extension) if extension == "es" => vec![ArtifactClass::SearchProjection],
        Some(extension) => vec![ArtifactClass::Custom(extension.clone())],
    }
}

fn readiness_satisfies_target(
    readiness_acks: &[ArtifactReadinessAck],
    target_checkpoint_id: &Option<String>,
    extension: Option<&String>,
) -> bool {
    let Some(target_checkpoint_id) = target_checkpoint_id.as_ref() else {
        return false;
    };

    let required_classes = required_artifact_classes(extension);
    if required_classes.is_empty() {
        return true;
    }

    let ready_classes: HashSet<ArtifactClass> = readiness_acks
        .iter()
        .filter(|ack| ack.checkpoint_id == *target_checkpoint_id)
        .map(|ack| ack.artifact_class.clone())
        .collect();

    required_classes
        .into_iter()
        .all(|artifact_class| ready_classes.contains(&artifact_class))
}

#[async_trait]
pub trait ReadOnlyCoordinationStore {
    async fn get_published_active_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError>;

    async fn get_latest_target_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError>;

    async fn get_checkpoint_cutover_state(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceApiError>;

    async fn heartbeat_serving_node(
        &mut self,
        org_info: &OrgInfo,
        lease: &ServingNodeLease,
    ) -> Result<(), ServiceApiError>;

    async fn record_serving_node_activation(
        &mut self,
        org_info: &OrgInfo,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceApiError>;

    async fn list_serving_node_activations(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ServingNodeActivationAck>, ServiceApiError>;

    async fn record_artifact_readiness(
        &mut self,
        _org_info: &OrgInfo,
        _ack: &ArtifactReadinessAck,
    ) -> Result<(), ServiceApiError> {
        Ok(())
    }

    async fn list_artifact_readiness(
        &mut self,
        _org_info: &OrgInfo,
        _table_name: &String,
        _extension: Option<String>,
    ) -> Result<Vec<ArtifactReadinessAck>, ServiceApiError> {
        Ok(vec![])
    }

    async fn get_read_only_coordination_state(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<ReadOnlyCheckpointCoordinationState, ServiceApiError> {
        let cutover_state = self
            .get_checkpoint_cutover_state(org_info, table_name, extension.clone())
            .await?;
        let readiness_acks = self
            .list_artifact_readiness(org_info, table_name, extension)
            .await?;
        let phase = if cutover_state.active_checkpoint_id.is_some()
            && cutover_state.active_checkpoint_id == cutover_state.target_checkpoint_id
        {
            ReadOnlyTargetPhase::Active
        } else if readiness_satisfies_target(
            &readiness_acks,
            &cutover_state.target_checkpoint_id,
            cutover_state.selector.extension.as_ref(),
        ) {
            ReadOnlyTargetPhase::AwaitingActivation
        } else {
            ReadOnlyTargetPhase::AwaitingReadiness
        };
        Ok(ReadOnlyCheckpointCoordinationState {
            selector: cutover_state.selector,
            epoch: cutover_state.epoch,
            active_checkpoint_id: cutover_state.active_checkpoint_id,
            target_checkpoint_id: cutover_state.target_checkpoint_id,
            phase,
            readiness_acks,
        })
    }
}

#[async_trait]
impl<T> ReadOnlyCoordinationStore for T
where
    T: MetadataStore + Send,
{
    async fn get_published_active_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        MetadataStore::get_published_active_checkpoint(self, org_info, table_name, extension).await
    }

    async fn get_latest_target_checkpoint(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        MetadataStore::get_latest_committed_checkpoint(self, org_info, table_name, extension).await
    }

    async fn get_checkpoint_cutover_state(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceApiError> {
        let mut cutover_state =
            MetadataStore::get_checkpoint_cutover_state(self, org_info, table_name, extension)
                .await?;
        cutover_state.target_checkpoint_id = MetadataStore::get_latest_committed_checkpoint(
            self,
            org_info,
            table_name,
            cutover_state.selector.extension.clone(),
        )
        .await?;
        Ok(cutover_state)
    }

    async fn heartbeat_serving_node(
        &mut self,
        org_info: &OrgInfo,
        lease: &ServingNodeLease,
    ) -> Result<(), ServiceApiError> {
        MetadataStore::heartbeat_serving_node(self, org_info, lease).await
    }

    async fn record_serving_node_activation(
        &mut self,
        org_info: &OrgInfo,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceApiError> {
        MetadataStore::record_serving_node_activation(self, org_info, ack).await
    }

    async fn list_serving_node_activations(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ServingNodeActivationAck>, ServiceApiError> {
        MetadataStore::list_serving_node_activations(self, org_info, table_name, extension).await
    }

    async fn record_artifact_readiness(
        &mut self,
        org_info: &OrgInfo,
        ack: &ArtifactReadinessAck,
    ) -> Result<(), ServiceApiError> {
        MetadataStore::record_artifact_readiness(self, org_info, ack).await
    }

    async fn list_artifact_readiness(
        &mut self,
        org_info: &OrgInfo,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Vec<ArtifactReadinessAck>, ServiceApiError> {
        MetadataStore::list_artifact_readiness(self, org_info, table_name, extension).await
    }
}
