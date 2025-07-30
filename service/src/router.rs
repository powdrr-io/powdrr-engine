use gotham::pipeline::{new_pipeline, single_pipeline};
use gotham::prelude::{DefineSingleRoute, DrawRoutes};
use gotham::router::{build_router, Router};
use powdrr_lib::test_api;
use crate::v1_handlers;

/// Create a `Router`
///
/// Results in a tree of routes that that looks like:
///
/// | _private/v1/_sql              --> POST
/// | {tables}/_search --> POST

/// matching on.
///
///     async fn create_table(&mut self, create_table: &CreateTable) -> Result<(), Box<dyn Error>>;
//
//     async fn describe_table(&mut self, name: &String) -> Result<Option<TableDescription>, Box<dyn Error>>;
//
//     async fn add_alias(&mut self, table_name: &String, alias: &String) -> Result<(), Box<dyn Error>>;
//
//     async fn remove_alias(&mut self, table_name: &String, alias: &String) -> Result<(), Box<dyn Error>>;
//
//     async fn create_table_template(&mut self, name: &String, template: &CreateIndexTemplateBody) -> Result<(), Box<dyn Error>>;
//
//     async fn describe_table_template(&mut self, name: &String) -> Result<Option<CreateIndexTemplateBody>, Box<dyn Error>>;
//
//     async fn create_pipeline(&mut self, name: &String, pipeline: &PipelineDefinition) -> Result<(), Box<dyn Error>>;
//
//     async fn describe_pipeline(&mut self, name: &String) -> Result<Option<PipelineDefinition>, Box<dyn Error>>;
//
//     async fn create_lifetime_policy(&mut self, name: &String, policy: &ILMPolicyDefinition) -> Result<(), Box<dyn Error>>;
//
//     async fn describe_lifetime_policy(&mut self, name: &String) -> Result<Option<ILMPolicyDefinition>, Box<dyn Error>>;
//
//     async fn speedboat_commit(&mut self, commit: &SpeedboatCommit) -> Result<(), Box<dyn Error>>;
//
//     async fn iceberg_commit(&mut self, table_name: &String, iceberg_commit: &IcebergCommit) -> Result<(), Box<dyn Error>>;
//
//     async fn extension_commit(&mut self, table_name: &String, commit: &ExtensionCommit) -> Result<(), Box<dyn Error>>;
//
//     async fn compaction_commit(&mut self, table_name: &String, commit: &CompactionCommit) -> Result<(), Box<dyn Error>>;
//
//     async fn get_latest_checkpoint(&mut self, table_name: &String, extensions: Option<String>) -> Result<Option<String>, Box<dyn Error>>;
//
//     async fn get_checkpoint(&mut self, snapshot: &CheckpointDescriptor) -> Result<TableMetadataCheckpoint, Box<dyn Error>>;
//
//     async fn get_extension_work_items(&mut self, extension_name: &String) -> Result<Vec<ExtensionWorkItem>, Box<dyn Error>>;
//
//     async fn get_compaction_work_items(&mut self) -> Result<Vec<(String, CompactionWorkItem)>, Box<dyn Error>>;
//
//     async fn get_peer_clients(&mut self) -> Result<Vec<Box<dyn PeerClient>>, Box<dyn Error>>;
//
//     async fn get_next_prefetch_checkpoints(&mut self, extension: Option<String>) -> Result<Vec<CheckpointDescriptor>, Box<dyn Error>>;
//
//     async fn set_prefetch_checkpoints(&mut self, checkpoints: &Vec<CheckpointDescriptor>, extension: Option<String>) -> Result<(), Box<dyn Error>>;
///
pub fn router(include_test_apis: bool) -> Router {
    let (chain, pipelines) = single_pipeline(new_pipeline().build());

    build_router(chain, pipelines, |route| {
        route.scope("/api", |route| {
            route.scope("/v1", |route| {
                route.post("/create_table").to(v1_handlers::create_table);
                /*
                route.post("/describe_table").to(v1_handlers::describe_table);
                route.post("/add_alias").to(v1_handlers::add_alias);
                route.post("/remove_alias").to(v1_handlers::remove_alias);
                route.post("/create_table_template").to(v1_handlers::create_table_template);
                route.post("/describe_table_template").to(v1_handlers::describe_table_template);
                route.post("/create_pipeline").to(v1_handlers::create_pipeline);
                route.post("/describe_pipeline").to(v1_handlers::describe_pipeline);
                route.post("/create_lifetime_policy").to(v1_handlers::create_lifetime_policy);
                route.post("/describe_lifetime_policy").to(v1_handlers::describe_lifetime_policy);
                route.post("/speedboat_commit").to(v1_handlers::speedboat_commit);
                route.post("/iceberg_commit").to(v1_handlers::iceberg_commit);
                route.post("/extension_commit").to(v1_handlers::extension_commit);
                route.post("/compaction_commit").to(v1_handlers::compaction_commit);
                route.post("/get_latest_checkpoint").to(v1_handlers::get_latest_checkpoint);
                route.post("/get_checkpoint").to(v1_handlers::get_checkpoint);
                route.post("/get_extension_work_items").to(v1_handlers::get_extension_work_items);
                route.post("/get_compaction_work_items").to(v1_handlers::get_compaction_work_items);

                 */
            })
        });

        if include_test_apis {
            route.scope("/_test", |route| {
                route.scope("/v1", |route| {
                    route.post("/_create_index").to(test_api::test_v1_create_index);
                    route.post("/_add_checkpoint").to(test_api::test_v1_add_checkpoint);
                    route.put("/_testing_mode").to(test_api::test_v1_set_testing_mode);
                    route.put("/_testing_and_processing_mode").to(test_api::test_v1_set_testing_processing_mode);
                    route.put("/_process_work").to(test_api::test_v1_process_work);
                })
            });
        }

    })
}
