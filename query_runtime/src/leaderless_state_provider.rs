use crate::data_contract::{
    ACCESS_KEY_HEADER_KEY, CleanupCommit, CleanupWorkItem, CreateIndexTemplateBody, OrgSettings,
    SECRET_KEY_HEADER_KEY,
};
use crate::data_contract::{
    AddAlias, CompactionCommit, CompactionWorkItem, CreateTable, ExtensionCommit,
    ExtensionWorkItem, GetLatestCheckpoint, IcebergCommit, SpeedboatCommit, TableDescription,
    TableMetadataCheckpoint,
};
use crate::ephemeral_fetch_tracker::EphemeralFetchTracker;
use crate::metadata_store::{
    CheckpointCutoverState, CutoverEpoch, ServingNodeActivationAck, ServingNodeLease,
};
use crate::peers::CheckpointDescriptor;
use crate::pipeline::PipelineDefinition;
use crate::state_provider::ServiceApiError;
use crate::test_api::TestProcessingMode;
use powdrr_control_plane::ilm_policy::ILMPolicyDefinition;
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactClass {
    SnapshotLookupMmap,
    SnapshotExactLookupMmap,
    SearchProjection,
    Custom(String),
}

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
struct ArtifactReadinessAck {
    selector: crate::metadata_store::PublishedCheckpointSelector,
    checkpoint_id: String,
    epoch: CutoverEpoch,
    artifact_class: ArtifactClass,
    producer_id: String,
    ready_at_ms: i64,
}

pub struct LeaderlessStateProvider {
    base_address: String,
    client: Client,
    access_key: String,
    secret_key: String,
    fetch_tracker: EphemeralFetchTracker,
    serving_node_id: String,
}

impl LeaderlessStateProvider {
    fn required_artifact_classes(extension: Option<&String>) -> Vec<ArtifactClass> {
        match extension {
            None => vec![
                ArtifactClass::SnapshotLookupMmap,
                ArtifactClass::SnapshotExactLookupMmap,
            ],
            Some(extension) if extension == "es" => vec![ArtifactClass::SearchProjection],
            Some(extension) => vec![ArtifactClass::Custom(extension.clone())],
        }
    }

    fn default_serving_node_id() -> String {
        if let Ok(node_id) = std::env::var("POWDRR_SERVING_NODE_ID") {
            return node_id;
        }

        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        format!("leaderless-{}-{}", hostname, std::process::id())
    }

    fn current_timestamp_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default()
    }

    #[allow(dead_code)]
    pub(crate) fn new(
        mode: TestProcessingMode,
        address: String,
        access_key: String,
        secret_key: String,
    ) -> Self {
        LeaderlessStateProvider {
            base_address: address,
            client: Client::new(),
            access_key,
            secret_key,
            fetch_tracker: EphemeralFetchTracker::new(mode),
            serving_node_id: Self::default_serving_node_id(),
        }
    }

    pub(crate) async fn add_checkpoint(&mut self, _checkpoint: &TableMetadataCheckpoint) -> () {
        todo!()
    }

    fn get(&self, url: String) -> RequestBuilder {
        self.client
            .get(url)
            .header(ACCESS_KEY_HEADER_KEY, self.access_key.clone())
            .header(SECRET_KEY_HEADER_KEY, self.secret_key.clone())
    }

    fn post(&self, url: String) -> RequestBuilder {
        self.client
            .post(url)
            .header(ACCESS_KEY_HEADER_KEY, self.access_key.clone())
            .header(SECRET_KEY_HEADER_KEY, self.secret_key.clone())
    }

    async fn handle_response_body<T>(
        request_result: Result<Response, reqwest::Error>,
    ) -> Result<T, ServiceApiError>
    where
        T: Sized + DeserializeOwned,
    {
        match request_result {
            Ok(success) => {
                if success.status().is_success() {
                    let body = success.text().await.unwrap();
                    let json = serde_json::from_str::<T>(body.as_str());
                    match json {
                        Ok(j) => Ok(j),
                        Err(e) => Err(ServiceApiError {
                            message: format!("Request body failed to parse: {}", e),
                        }),
                    }
                } else {
                    Err(ServiceApiError {
                        message: success.text().await.unwrap(),
                    })
                }
            }
            Err(e) => Err(ServiceApiError::from_reqwest(e)),
        }
    }

    async fn handle_response_body_option<T>(
        request_result: Result<Response, reqwest::Error>,
    ) -> Result<Option<T>, ServiceApiError>
    where
        T: Sized + DeserializeOwned,
    {
        match request_result {
            Ok(success) => {
                if success.status().is_success() {
                    let body = success.text().await.unwrap();
                    let json = serde_json::from_str::<T>(body.as_str());
                    match json {
                        Ok(j) => Ok(Some(j)),
                        Err(e) => Err(ServiceApiError {
                            message: format!("Request body failed to parse: {}", e),
                        }),
                    }
                } else if success.status() == StatusCode::NOT_FOUND {
                    Ok(None)
                } else {
                    Err(ServiceApiError::new(success.text().await.unwrap()))
                }
            }
            Err(e) => Err(ServiceApiError::from_reqwest(e)),
        }
    }

    #[allow(dead_code)]
    async fn handle_response(
        request_result: Result<Response, reqwest::Error>,
    ) -> Result<(), ServiceApiError> {
        match request_result {
            Ok(success) => {
                if success.status().is_success() {
                    Ok(())
                } else {
                    Err(ServiceApiError::new(format!(
                        "Request failed: {}",
                        success.status()
                    )))
                }
            }
            Err(e) => Err(ServiceApiError::from_reqwest(e)),
        }
    }

    pub(crate) async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        let base_address = &self.base_address;
        let resp = self
            .client
            .get(format!("{base_address}/api/v1/iceberg_tables"))
            .send()
            .await;
        match resp {
            Ok(r) => {
                let json = r.json::<Vec<String>>().await;
                match json {
                    Ok(j) => Ok(j),
                    Err(e) => Err(ServiceApiError::from_reqwest(e)),
                }
            }
            Err(e) => Err(ServiceApiError::from_reqwest(e)),
        }
    }

    pub(crate) async fn create_table(
        &mut self,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        Self::handle_response_body(
            self.post(format!("{}/api/v1/create_table", self.base_address))
                .body(serde_json::to_string(create_table).unwrap())
                .send()
                .await,
        )
        .await
    }

    pub(crate) async fn upsert_table_metadata(
        &mut self,
        create_table: &CreateTable,
    ) -> Result<bool, ServiceApiError> {
        self.create_table(create_table).await
    }

    pub(crate) async fn create_org(
        &mut self,
        settings: &OrgSettings,
    ) -> Result<(), ServiceApiError> {
        Self::handle_response(
            self.client
                .post(format!("{}/api/v1/create_org", self.base_address))
                .body(serde_json::to_string(settings).unwrap())
                .send()
                .await,
        )
        .await
    }

    pub(crate) async fn lookup_secret_access_key(
        &mut self,
        access_key: &String,
    ) -> Result<Option<String>, ServiceApiError> {
        if access_key == &self.access_key {
            Ok(Some(self.secret_key.clone()))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn describe_table(
        &mut self,
        name: &String,
    ) -> Result<Option<TableDescription>, ServiceApiError> {
        Self::handle_response_body_option(
            self.get(format!(
                "{}/api/v1/describe_table/{}",
                self.base_address, name
            ))
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn add_alias(
        &mut self,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        let payload = AddAlias {
            table_name: table_name.to_owned(),
            alias: alias.to_owned(),
        };
        Self::handle_response_body(
            self.post(format!("{}/api/v1/add_alias", self.base_address))
                .body(serde_json::to_string(&payload).unwrap())
                .send()
                .await,
        )
        .await
    }

    pub(crate) async fn remove_alias(
        &mut self,
        table_name: &String,
        alias: &String,
    ) -> Result<bool, ServiceApiError> {
        let payload = AddAlias {
            table_name: table_name.to_owned(),
            alias: alias.to_owned(),
        };
        Self::handle_response_body(
            self.post(format!("{}/api/v1/remove_alias", self.base_address))
                .body(serde_json::to_string(&payload).unwrap())
                .send()
                .await,
        )
        .await
    }

    pub(crate) async fn create_table_template(
        &mut self,
        name: &String,
        template: &CreateIndexTemplateBody,
    ) -> Result<bool, ServiceApiError> {
        Self::handle_response_body(
            self.post(format!(
                "{}/api/v1/create_table_template/{}",
                self.base_address, name
            ))
            .body(serde_json::to_string(&template).unwrap())
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn describe_table_template(
        &mut self,
        name: &String,
    ) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        Self::handle_response_body_option(
            self.get(format!(
                "{}/api/v1/describe_table_template/{}",
                self.base_address, name
            ))
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn create_pipeline(
        &mut self,
        name: &String,
        pipeline: &PipelineDefinition,
    ) -> Result<bool, ServiceApiError> {
        Self::handle_response_body(
            self.post(format!(
                "{}/api/v1/create_pipeline/{}",
                self.base_address, name
            ))
            .body(serde_json::to_string(&pipeline).unwrap())
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn describe_pipeline(
        &mut self,
        name: &String,
    ) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        Self::handle_response_body_option(
            self.get(format!(
                "{}/api/v1/describe_pipeline/{}",
                self.base_address, name
            ))
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn create_lifetime_policy(
        &mut self,
        name: &String,
        pipeline: &ILMPolicyDefinition,
    ) -> Result<bool, ServiceApiError> {
        Self::handle_response_body(
            self.post(format!(
                "{}/api/v1/create_lifetime_policy/{}",
                self.base_address, name
            ))
            .body(serde_json::to_string(&pipeline).unwrap())
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn describe_lifetime_policy(
        &mut self,
        name: &String,
    ) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        Self::handle_response_body_option(
            self.get(format!(
                "{}/api/v1/describe_lifetime_policy/{}",
                self.base_address, name
            ))
            .send()
            .await,
        )
        .await
    }

    async fn update_prefetch_checkpoints(
        &mut self,
        result: Result<bool, ServiceApiError>,
        table_names: &Vec<String>,
        extensions: Option<String>,
    ) -> Result<bool, ServiceApiError> {
        match result {
            Ok(val) => {
                if val {
                    for table_name in table_names {
                        let checkpoint_id = self
                            .get_remote_latest_target_checkpoint(table_name, extensions.clone())
                            .await?;
                        if let Some(checkpoint_id) = checkpoint_id {
                            self.fetch_tracker
                                .set_next_prefetch_checkpoints(
                                    table_name,
                                    extensions.clone(),
                                    &checkpoint_id,
                                )
                                .await?;
                        }
                    }
                }
                Ok(val)
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) async fn speedboat_commit(
        &mut self,
        commit: &SpeedboatCommit,
    ) -> Result<bool, ServiceApiError> {
        self.update_prefetch_checkpoints(
            Self::handle_response_body(
                self.post(format!("{}/api/v1/speedboat_commit", self.base_address))
                    .body(serde_json::to_string(&commit).unwrap())
                    .send()
                    .await,
            )
            .await,
            &commit
                .type_files
                .iter()
                .map(|t| t.table_name.clone())
                .collect(),
            None,
        )
        .await
    }

    pub(crate) async fn iceberg_commit(
        &mut self,
        name: &String,
        iceberg_commit: &IcebergCommit,
    ) -> Result<bool, ServiceApiError> {
        self.update_prefetch_checkpoints(
            Self::handle_response_body(
                self.post(format!(
                    "{}/api/v1/iceberg_commit/{}",
                    self.base_address, name
                ))
                .body(serde_json::to_string(&iceberg_commit).unwrap())
                .send()
                .await,
            )
            .await,
            &vec![name.clone()],
            None,
        )
        .await
    }

    pub(crate) async fn extension_commit(
        &mut self,
        name: &String,
        commit: &ExtensionCommit,
    ) -> Result<bool, ServiceApiError> {
        self.update_prefetch_checkpoints(
            Self::handle_response_body(
                self.post(format!(
                    "{}/api/v1/extension_commit/{}",
                    self.base_address, name
                ))
                .body(serde_json::to_string(&commit).unwrap())
                .send()
                .await,
            )
            .await,
            &vec![name.clone()],
            None,
        )
        .await
    }

    pub(crate) async fn compaction_commit(
        &mut self,
        name: &String,
        commit: &CompactionCommit,
    ) -> Result<bool, ServiceApiError> {
        self.update_prefetch_checkpoints(
            Self::handle_response_body(
                self.post(format!(
                    "{}/api/v1/compaction_commit/{}",
                    self.base_address, name
                ))
                .body(serde_json::to_string(&commit).unwrap())
                .send()
                .await,
            )
            .await,
            &vec![name.clone()],
            None,
        )
        .await
    }

    pub(crate) async fn cleanup_commit(
        &mut self,
        commit: &CleanupCommit,
    ) -> Result<bool, ServiceApiError> {
        Self::handle_response_body(
            self.post(format!("{}/api/v1/cleanup_commit", self.base_address))
                .body(serde_json::to_string(&commit).unwrap())
                .send()
                .await,
        )
        .await
    }

    pub(crate) async fn get_latest_committed_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        let payload = GetLatestCheckpoint {
            table_name: table_name.to_owned(),
            extension: extensions,
        };
        Self::handle_response_body_option(
            self.get(format!(
                "{}/api/v1/get_latest_checkpoint",
                self.base_address
            ))
            .body(serde_json::to_string(&payload).unwrap())
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn get_published_active_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        let payload = GetLatestCheckpoint {
            table_name: table_name.to_owned(),
            extension: extensions,
        };
        Self::handle_response_body_option(
            self.get(format!(
                "{}/api/v1/get_published_active_checkpoint",
                self.base_address
            ))
            .body(serde_json::to_string(&payload).unwrap())
            .send()
            .await,
        )
        .await
    }

    async fn get_remote_latest_target_checkpoint(
        &mut self,
        table_name: &String,
        extensions: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        let payload = GetLatestCheckpoint {
            table_name: table_name.to_owned(),
            extension: extensions,
        };
        Self::handle_response_body_option(
            self.get(format!(
                "{}/api/v1/get_latest_target_checkpoint",
                self.base_address
            ))
            .body(serde_json::to_string(&payload).unwrap())
            .send()
            .await,
        )
        .await
    }

    async fn get_checkpoint_cutover_state(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<CheckpointCutoverState, ServiceApiError> {
        let payload = GetLatestCheckpoint {
            table_name: table_name.to_owned(),
            extension,
        };
        Self::handle_response_body(
            self.get(format!(
                "{}/api/v1/get_checkpoint_cutover_state",
                self.base_address
            ))
            .body(serde_json::to_string(&payload).unwrap())
            .send()
            .await,
        )
        .await
    }

    async fn record_serving_node_activation(
        &mut self,
        ack: &ServingNodeActivationAck,
    ) -> Result<(), ServiceApiError> {
        Self::handle_response(
            self.post(format!(
                "{}/api/v1/record_serving_node_activation",
                self.base_address
            ))
            .body(serde_json::to_string(ack).unwrap())
            .send()
            .await,
        )
        .await
    }

    async fn record_artifact_readiness(
        &mut self,
        ack: &ArtifactReadinessAck,
    ) -> Result<(), ServiceApiError> {
        Self::handle_response(
            self.post(format!(
                "{}/api/v1/record_artifact_readiness",
                self.base_address
            ))
            .body(serde_json::to_string(ack).unwrap())
            .send()
            .await,
        )
        .await
    }

    async fn heartbeat_serving_node(&mut self) -> Result<(), ServiceApiError> {
        Self::handle_response(
            self.post(format!(
                "{}/api/v1/heartbeat_serving_node",
                self.base_address
            ))
            .body(
                serde_json::to_string(&ServingNodeLease {
                    node_id: self.serving_node_id.clone(),
                    membership_epoch: CutoverEpoch::default(),
                    observed_at_ms: Self::current_timestamp_ms(),
                })
                .unwrap(),
            )
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn get_checkpoint(
        &mut self,
        checkpoint: &CheckpointDescriptor,
    ) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        Self::handle_response_body_option(
            self.get(format!("{}/api/v1/get_checkpoint", self.base_address))
                .body(serde_json::to_string(&checkpoint).unwrap())
                .send()
                .await,
        )
        .await
    }

    pub(crate) async fn update_all_checkpoints(&mut self) -> Result<bool, ServiceApiError> {
        // Do nothing. This happens automatically on remote services.
        Ok(false)
    }

    pub(crate) async fn get_extension_work_items(
        &mut self,
        extension_name: &String,
    ) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        Self::handle_response_body(
            self.get(format!(
                "{}/api/v1/get_extension_work_items/{}",
                self.base_address, extension_name
            ))
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn get_compaction_work_items(
        &mut self,
    ) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        Self::handle_response_body(
            self.get(format!(
                "{}/api/v1/get_compaction_work_items",
                self.base_address
            ))
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn get_cleanup_work_items(
        &mut self,
    ) -> Result<Vec<CleanupWorkItem>, ServiceApiError> {
        Self::handle_response_body(
            self.get(format!(
                "{}/api/v1/get_cleanup_work_items",
                self.base_address
            ))
            .send()
            .await,
        )
        .await
    }

    pub(crate) async fn get_next_prefetch_checkpoints(
        &mut self,
        extensions: Option<String>,
    ) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        self.heartbeat_serving_node().await?;
        self.fetch_tracker
            .get_next_prefetch_checkpoints(extensions)
            .await
    }

    pub(crate) async fn get_latest_target_checkpoint(
        &mut self,
        table_name: &String,
        extension: Option<String>,
    ) -> Result<Option<String>, ServiceApiError> {
        self.fetch_tracker
            .get_latest_target_checkpoint(table_name, extension)
            .await
    }

    pub(crate) async fn set_target_checkpoints(
        &mut self,
        descriptors: &Vec<CheckpointDescriptor>,
        extension: Option<String>,
    ) -> Result<(), ServiceApiError> {
        self.heartbeat_serving_node().await?;
        self.fetch_tracker
            .set_target_checkpoints(descriptors, extension.clone())
            .await?;

        for descriptor in descriptors.iter() {
            let cutover_state = self
                .get_checkpoint_cutover_state(&descriptor.table_name, extension.clone())
                .await?;
            if cutover_state.target_checkpoint_id != Some(descriptor.checkpoint_id.clone()) {
                continue;
            }

            for artifact_class in Self::required_artifact_classes(extension.as_ref()) {
                self.record_artifact_readiness(&ArtifactReadinessAck {
                    selector: cutover_state.selector.clone(),
                    checkpoint_id: descriptor.checkpoint_id.clone(),
                    epoch: cutover_state.epoch,
                    artifact_class,
                    producer_id: self.serving_node_id.clone(),
                    ready_at_ms: Self::current_timestamp_ms(),
                })
                .await?;
            }

            self.record_serving_node_activation(&ServingNodeActivationAck {
                selector: cutover_state.selector,
                node_id: self.serving_node_id.clone(),
                epoch: cutover_state.epoch,
                checkpoint_id: descriptor.checkpoint_id.clone(),
                activated_at_ms: Self::current_timestamp_ms(),
            })
            .await?;
        }

        Ok(())
    }
}
