use reqwest::{Client, Response, StatusCode};
use serde::de::DeserializeOwned;
use crate::elastic_search_ingest::CreateIndexTemplateBody;
use crate::elastic_search_lifetime_policy::ILMPolicyDefinition;
use crate::pipeline::PipelineDefinition;
use crate::data_contract::{AddAlias, CompactionCommit, CompactionWorkItem, CreateTable, ExtensionCommit, ExtensionWorkItem, GetLatestCheckpoint, IcebergCommit, SpeedboatCommit, TableDescription, TableMetadataCheckpoint};
use crate::state_provider::ServiceApiError;
use crate::peers::{CheckpointDescriptor, PeerClient};
use crate::test_api::TestProcessingMode;


pub struct LeaderlessStateProvider {
    base_address: String,
    client: Client,
}

impl LeaderlessStateProvider {
    #[allow(dead_code)]
    fn new(address: String) -> Self {
        LeaderlessStateProvider {
            base_address: address,
            client: reqwest::Client::new(),
        }
    }

    pub async fn clear_and_set(&mut self, _mode: TestProcessingMode) {
    }

    pub(crate) async fn add_checkpoint(&mut self, _checkpoint: &TableMetadataCheckpoint) -> () {
        todo!()
    }

    pub(crate) async fn get_latest_target_checkpoint(&self, _table_name: &String, _extension: Option<String>) -> Result<Option<String>, ServiceApiError> {
        todo!()
    }

    pub(crate) async fn set_prefetch_checkpoints(&self, _descriptors: &Vec<CheckpointDescriptor>, _extension: Option<String>) -> Result<(), ServiceApiError> {
        todo!()
    }


    async fn handle_response_body<T>(request_result: Result<Response, reqwest::Error>) -> Result<T, ServiceApiError>
    where T: Sized + DeserializeOwned
    {
        match request_result {
            Ok(success) => {
                if success.status().is_success() {
                    let body = success.text().await.unwrap();
                    let json = serde_json::from_str::<T>(body.as_str());
                    match json {
                        Ok(j) => Ok(j),
                        Err(e) => Err(ServiceApiError { message: format!("Request body failed to parse: {}", e) })
                    }
                } else {
                    Err(ServiceApiError { message: success.text().await.unwrap() })
                }
            },
            Err(e) => {
                Err(ServiceApiError::from_reqwest(e))
            }
        }
    }

    async fn handle_response_body_option<T>(request_result: Result<Response, reqwest::Error>) -> Result<Option<T>, ServiceApiError>
    where T: Sized + DeserializeOwned
    {
        match request_result {
            Ok(success) => {
                if success.status().is_success() {
                    let body = success.text().await.unwrap();
                    let json = serde_json::from_str::<T>(body.as_str());
                    match json {
                        Ok(j) => Ok(Some(j)),
                        Err(e) => Err(ServiceApiError { message: format!("Request body failed to parse: {}", e) })
                    }
                } else if success.status() == StatusCode::NOT_FOUND {
                    Ok(None)
                } else {
                    Err(ServiceApiError::new(success.text().await.unwrap()))
                }
            },
            Err(e) => {
                Err(ServiceApiError::from_reqwest(e))
            }
        }
    }


    async fn handle_response(request_result: Result<Response, reqwest::Error>) -> Result<(), ServiceApiError> {
        match request_result {
            Ok(success) => {
                if success.status().is_success() {
                    Ok(())
                } else {
                    Err(ServiceApiError::new(format!("Request failed: {}", success.status())))
                }
            },
            Err(e) => {
                Err(ServiceApiError::from_reqwest(e))
            }
        }
    }

    pub(crate) async fn get_all_iceberg_tables(&mut self) -> Result<Vec<String>, ServiceApiError> {
        let base_address = &self.base_address;
        let resp = self.client.get(format!("{base_address}/api/v1/iceberg_tables")).send().await;
        match resp {
            Ok(r) => {
                let json = r.json::<Vec<String>>().await;
                match json {
                    Ok(j) => Ok(j),
                    Err(e) => Err(ServiceApiError::from_reqwest(e)),
                }
            },
            Err(e) => {
                Err(ServiceApiError::from_reqwest(e))
            }
        }
    }

    pub(crate) async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), ServiceApiError> {
        Self::handle_response(self.client.post(format!("{}/api/v1/create_table", self.base_address))
            .body(serde_json::to_string(create_table).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn describe_table(&mut self, name: &String) -> Result<Option<TableDescription>, ServiceApiError> {
        Self::handle_response_body_option(self.client.get(format!("{}/api/v1/describe_table/{}", self.base_address, name))
            .send().await
        ).await
    }

    pub(crate) async fn add_alias(&mut self, table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        let payload = AddAlias {
            table_name: table_name.to_owned(),
            alias: alias.to_owned(),
        };
        Self::handle_response(self.client.post(format!("{}/api/v1/add_alias", self.base_address))
            .body(serde_json::to_string(&payload).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn remove_alias(&mut self, table_name: &String, alias: &String) -> Result<(), ServiceApiError> {
        let payload = AddAlias {
            table_name: table_name.to_owned(),
            alias: alias.to_owned(),
        };
        Self::handle_response(self.client.post(format!("{}/api/v1/remove_alias", self.base_address))
            .body(serde_json::to_string(&payload).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn create_table_template(&mut self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), ServiceApiError> {
        Self::handle_response(self.client.post(format!("{}/api/v1/create_table_template/{}", self.base_address, name))
            .body(serde_json::to_string(&template).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn describe_table_template(&mut self, name: &String) -> Result<Option<CreateIndexTemplateBody>, ServiceApiError> {
        Self::handle_response_body_option(self.client.get(format!("{}/api/v1/describe_table_template/{}", self.base_address, name))
            .send().await
        ).await
    }

    pub(crate) async fn create_pipeline(&mut self, name: &String, pipeline: &PipelineDefinition) -> Result<(), ServiceApiError> {
        Self::handle_response(self.client.post(format!("{}/api/v1/create_pipeline/{}", self.base_address, name))
            .body(serde_json::to_string(&pipeline).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn describe_pipeline(&mut self, name: &String) -> Result<Option<PipelineDefinition>, ServiceApiError> {
        Self::handle_response_body_option(self.client.get(format!("{}/api/v1/describe_pipeline/{}", self.base_address, name))
            .send().await
        ).await
    }

    pub(crate) async fn create_lifetime_policy(&mut self, name: &String, pipeline: &ILMPolicyDefinition) -> Result<(), ServiceApiError> {
        Self::handle_response(self.client.post(format!("{}/api/v1/create_lifetime_policy/{}", self.base_address, name))
            .body(serde_json::to_string(&pipeline).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn describe_lifetime_policy(&mut self, name: &String) -> Result<Option<ILMPolicyDefinition>, ServiceApiError> {
        Self::handle_response_body_option(self.client.get(format!("{}/api/v1/describe_lifetime_policy/{}", self.base_address, name))
            .send().await
        ).await
    }

    pub(crate) async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), ServiceApiError> {
        Self::handle_response(self.client.post(format!("{}/api/v1/speedboat_commit", self.base_address))
            .body(serde_json::to_string(&commit).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn iceberg_commit(&mut self, name: &String, iceberg_commit: &IcebergCommit) -> Result<(), ServiceApiError> {
        Self::handle_response(self.client.post(format!("{}/api/v1/iceberg_commit/{}", self.base_address, name))
            .body(serde_json::to_string(&iceberg_commit).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn extension_commit(&mut self, name: &String, commit: &ExtensionCommit) -> Result<(), ServiceApiError> {
        Self::handle_response(self.client.post(format!("{}/api/v1/extension_commit/{}", self.base_address, name))
            .body(serde_json::to_string(&commit).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn compaction_commit(&mut self, name: &String, commit: &CompactionCommit) -> Result<(), ServiceApiError> {
        Self::handle_response(self.client.post(format!("{}/api/v1/compaction_commit/{}", self.base_address, name))
            .body(serde_json::to_string(&commit).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn get_latest_committed_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, ServiceApiError> {
        let payload = GetLatestCheckpoint {
            table_name: table_name.to_owned(),
            extension: extensions,
        };
        Self::handle_response_body_option(self.client.get(format!("{}/api/v1/get_latest_checkpoint", self.base_address))
            .body(serde_json::to_string(&payload).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn get_checkpoint(&mut self, checkpoint: &CheckpointDescriptor) -> Result<Option<TableMetadataCheckpoint>, ServiceApiError> {
        Self::handle_response_body_option(self.client.get(format!("{}/api/v1/get_checkpoint", self.base_address))
            .body(serde_json::to_string(&checkpoint).unwrap())
            .send().await
        ).await
    }

    pub(crate) async fn get_extension_work_items(&mut self, extension_name: &String) -> Result<Vec<ExtensionWorkItem>, ServiceApiError> {
        Self::handle_response_body(self.client.get(format!("{}/api/v1/get_extension_work_items/{}", self.base_address, extension_name))
            .send().await
        ).await
    }

    pub(crate) async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, ServiceApiError> {
        Self::handle_response_body(self.client.get(format!("{}/api/v1/get_compaction_work_items", self.base_address))
            .send().await
        ).await
    }

    pub(crate) async fn get_peer_clients(&mut self) -> Vec<Box<dyn PeerClient>> {
        todo!("nope")
    }

    pub(crate) async fn get_next_prefetch_checkpoints(&mut self, _extensions: Option<String>) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        Ok(vec!())
    }

}
