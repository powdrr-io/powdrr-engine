use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, fs::File};
use std::io::Write;
use futures::FutureExt;
use gotham::mime;
use http::header::LOCATION;
use http::StatusCode;
use idgenerator::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::{mpsc, oneshot};
use uuid_b64::UuidB64;

use crate::elastic_search_commands::{to_serde_value, LookupById};
use crate::elastic_search_common::{load_command_raw_result, CommandContext, ElasticSearchResponse, MIME_ES_JSON};
use crate::elastic_search_responses::{BulkResult, ErrorDetails, OperationResult, Shards, SingleDocCreateFailedResult};
use crate::data_access;
use crate::elastic_search_parser::UpdateBody;
use crate::elastic_search_storage_schema::{FullRecord, RecordDelete, RecordInput, SpeedboatCommitBuilder};
use crate::schema_massager::PowdrrSchema;
use crate::state_hosted_service::{CreateTable, SpeedboatCommit, SpeedboatCommitTableInfo, TableDescription, API_SERVICE_CLIENT};
use crate::util::log_err;



#[derive(Debug)]
pub(crate) struct IngestError {
    pub message: String,
}

impl Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = f.write_str(&self.message);
        Ok(())
    }
}

impl Error for IngestError {}


fn default_as_false() -> bool {
    false
}




#[derive(Clone)]
pub(crate) struct WriteBuffer {
    lines: Vec<String>,
    schema: Option<PowdrrSchema>
}


impl WriteBuffer {
    pub fn insert_and_update(schema: PowdrrSchema, lines: Vec<String>) -> Self {
        WriteBuffer {
            lines,
            schema: Some(schema)
        }
    }

    pub fn delete(lines: Vec<String>) -> Self {
        WriteBuffer {
            lines,
            schema: None
        }
    }

    fn write_to_file(&self, file_name: &String) -> Result<(), IngestError> {
        assert!(self.lines.len() > 0, "Cannot write empty buffer to file");
        let mut file_write = File::create(file_name).expect("Cannot create file");
        for line in self.lines.iter() {
            match writeln!(&mut file_write, "{}", line) {
                Err(e) => return Err(IngestError { message: format!("{}", e).to_string() }),
                _ => ()
            }
        }
        Ok(())
    }

    pub(crate) fn num_records(&self) -> usize {
        self.lines.len()
    }

    fn total_size(&self) -> u64 {
        self.lines.iter().fold(0, |acc, line| acc + line.len() as u64 + 1)
    }
}

#[derive(Deserialize)]
struct IndexOrCreateBody {
    #[serde(rename(deserialize = "_index"))]
    index: Option<String>,
    #[serde(rename(deserialize = "_id"))]
    id: Option<String>,
    #[serde(default = "default_as_false")]
    #[allow(dead_code)]
    list_executed_pipelines: bool,
    #[serde(default = "default_as_false")]
    #[allow(dead_code)]
    require_alias: bool,
    #[allow(dead_code)]
    dynamic_templates: Option<HashMap<String, String>>,
}


#[derive(Deserialize)]
struct UpdateOrDeleteBody {
    #[serde(rename(deserialize = "_index"))]
    #[allow(dead_code)]
    index: Option<String>,
    #[serde(rename(deserialize = "_id"))]
    #[allow(dead_code)]
    id: Option<String>,
    #[serde(default = "default_as_false")]
    #[allow(dead_code)]
    require_alias: bool,
}


#[derive(Deserialize)]
pub(crate) struct Create {
    create: IndexOrCreateBody
}


#[derive(Deserialize)]
#[allow(dead_code)]
pub(crate) struct Index {
    index: IndexOrCreateBody
}


#[derive(Deserialize)]
#[allow(dead_code)]
struct Delete {
    delete: UpdateOrDeleteBody
}


#[derive(Deserialize)]
#[allow(dead_code)]
struct Update {
    update: UpdateOrDeleteBody
}


#[derive(Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum IngestCommand {
    Create(Create),
    Index(Index),
    Update(Update),
}


#[derive(Serialize, Deserialize, Clone)]
struct CreateIndexSettings {
    index: IndexSettings
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
struct AliasInfo {
    is_hidden: bool
}

#[derive(Serialize, Deserialize, Clone)]
struct MetaInfo {
    #[serde(rename = "migrationMappingPropertyHashes")]
    migration_mapping_property_hashes: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum StringOrBool {
    Bool(bool),
    String(String),
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
struct CreateIndexBody {
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
#[serde(untagged)]
enum CreateIndexSettingsOption {
    Indirect(CreateIndexSettings),
    Direct(IndexSettings),
}


#[derive(Serialize)]
pub(crate) struct CreateIndexResult {
    acknowledged: bool,
    shards_acknowledged: bool,
    index: String,
}


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct CreateIndexTemplateBody {
    #[serde(default)]
    index_patterns: Vec<String>,
    priority: Option<u32>,
    version: Option<u32>,
    template: CreateIndexBody,
}

pub(crate) async fn create_index(table: &String, body: &String) -> Result<CreateIndexResult, IngestError> {
    let parsed_body = if body.len() == 0 {
        CreateIndexBody{ aliases: None, mappings: None, settings: None }
    } else {
        match CreateIndexBody::parse(body) {
            Ok(pb) => pb,
            Err(_e) => return log_err(IngestError { message: "body parsing error".to_string() })
        }
        // TODO: fill in defaults
    };

    let serialized_body = match serde_json::to_string(&parsed_body) {
        Ok(s) => s,
        Err(_) => panic!("What happen?")
    };

    API_SERVICE_CLIENT.create_table(&CreateTable{ 
        name: table.clone(),
        tags: HashMap::from([("_es_original".to_string(), serialized_body)])
    }).await;

    if parsed_body.aliases.is_some() {
        for (name, _) in parsed_body.aliases.unwrap() {
            API_SERVICE_CLIENT.add_alias(table, &name).await;
        }
    }

    Ok(CreateIndexResult { index: table.clone(), shards_acknowledged: true, acknowledged: true })
}


pub(crate) async fn create_index_template(table: &String, body: &String) -> Result<CreateIndexResult, IngestError> {
    let parsed_body: CreateIndexTemplateBody = match serde_json::from_str(body) {
        Ok(pb) => pb,
        Err(_e) => return log_err(IngestError{ message: "body parsing error".to_string() })
    };

    API_SERVICE_CLIENT.create_table_template(&table, &parsed_body).await;
    
    Ok(CreateIndexResult { index: table.clone(), shards_acknowledged: true, acknowledged: true })
}


#[derive(Serialize, Deserialize, Clone)]
struct Aliases {
    actions: Vec<AliasAction>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum AliasAction {
    Add(AliasAdd),
    Remove(AliasRemove),
    RemoveIndex(AliasRemoveIndex),
}

#[derive(Serialize, Deserialize, Clone)]
struct AliasAdd {
    add: AliasAddBody
}

#[derive(Serialize, Deserialize, Clone)]
struct AliasAddBody {
    // TODO: there are many more fields
    index: String,
    alias: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct AliasRemove {
    remove: AliasRemoveBody
}

#[derive(Serialize, Deserialize, Clone)]
struct AliasRemoveBody {
    // TODO: there are many more fields
    index: String,
    alias: String,    
}

#[derive(Serialize, Deserialize, Clone)]
struct AliasRemoveIndex {
    remove_index: AliasRemoveIndexBody,
}

#[derive(Serialize, Deserialize, Clone)]
struct AliasRemoveIndexBody {
    index: String,
}


pub(crate) async fn update_aliases(body: &String) -> Result<(), IngestError> {
    let parsed_body: Aliases = match serde_json::from_str(body) {
        Ok(pb) => pb,
        Err(_e) => return log_err(IngestError{ message: "body parsing error".to_string() })
    };

    // TODO: the actions should probably be pushed up in bulk
    for action in parsed_body.actions {
        match action {
            AliasAction::Add(a) => {
                API_SERVICE_CLIENT.add_alias(&a.add.index, &a.add.alias).await;
            },
            AliasAction::Remove(r) => {
                API_SERVICE_CLIENT.remove_alias(&r.remove.index, &r.remove.alias).await;
            },
            AliasAction::RemoveIndex(_) => {
                panic!("TODO: What does this mean?")
            },
        }
    }
    Ok(())
}


pub(crate) fn ingest_create(
    create: &Create, 
    doc: &Value,
    version: u64,
    status: Option<u32>,
    buffer: &mut SpeedboatCommitBuilder
) -> () {
    let id = match &create.create.id {
        Some(id) => id.clone(),
        None => UuidB64::new().to_string()
    };
    let doc_with_id = RecordInput::new(
        id.clone(),
        version,
        &doc,
        status,
    );
    buffer.insert(&doc_with_id);
}

pub(crate) struct IngestResult {
    tables: HashMap<String, SpeedboatCommitBuilder>,
    operations: Vec<OperationResult>,
}

impl IngestResult {
    fn new() -> Self {
        IngestResult { 
            tables: HashMap::new(),
            operations: vec!(),
        }
    }

    fn get(&mut self, table: &String) -> &mut SpeedboatCommitBuilder {
        match self.tables.get_mut(table) {
            Some(_) => (),
            None => {
                self.tables.insert(table.clone(), SpeedboatCommitBuilder::new(table));
            }
        }
        self.tables.get_mut(table).unwrap()
    }
}


#[derive(Serialize)]
#[allow(dead_code)]
struct CreateSingleSuccessResult {

}

#[derive(Serialize)]
#[allow(dead_code)]
struct CreateSingleErrorResult {

}


pub(crate) async fn create_single(index: &String, doc_id: &String, payload: &String) -> Result<ElasticSearchResponse, IngestError> {
    create_single_worker(index, doc_id, payload).await
}

pub(crate) async fn upsert_single(index: &String, doc_id: &String, payload: &String) -> Result<ElasticSearchResponse, IngestError> {
    update_single_worker(index, doc_id, payload).await
}

pub(crate) fn write_to_file(buffer: &WriteBuffer, index: &String, label: &String) -> Result<String, IngestError> {
    // TODO: need real paths into S3
    let file_path = format!("tests/data/ingest/{}-{}-{}.json", label, index, IdInstance::next_id().to_string());
    let write_to_file_result = buffer.write_to_file(&file_path);
    tracing::info!("Ingest: op {} on table {} wrote {} records", label, index, buffer.num_records());

    match write_to_file_result {
        Ok(_) => (),
        Err(_) => return Err(IngestError{ message: "File error".to_string() })
    }

    Ok(file_path)
}


pub(crate) async fn commit_speedboat(table: &String, inserts_and_updates: &WriteBuffer, deletes: &WriteBuffer, compactions: &Vec<String>, commit_type: &String) -> Result<(), IngestError> {
    let mut table_infos = vec!();
    if inserts_and_updates.lines.len() != 0 {
        let insert_update_path = write_to_file(inserts_and_updates, table, commit_type)?;
        table_infos.push(SpeedboatCommitTableInfo {
            commit_type: commit_type.clone(),
            table_name: table.clone(),
            files: vec!(insert_update_path),
            sizes: vec!(inserts_and_updates.total_size()),
            schema: inserts_and_updates.schema.clone(),
        });
    }
    if deletes.lines.len() != 0 {
        let deletes_path = write_to_file(deletes, table, &"delete".to_string())?;
        table_infos.push(SpeedboatCommitTableInfo {
            commit_type: "delete".to_string(),
            table_name: table.clone(),
            files: vec!(deletes_path),
            sizes: vec!(deletes.total_size()),
            schema: deletes.schema.clone(),
        });
    }
    match API_SERVICE_CLIENT.speedboat_commit(&SpeedboatCommit {
        type_files: table_infos,
        compactions: compactions.clone(),
    }).await {
        Ok(_) => (),
        Err(_) => panic!("nope")
    }

    Ok(())
}


struct ExistingDocs {
    docs: Vec<Value>,
    #[allow(dead_code)]
    schema: Option<PowdrrSchema>
}


async fn get_existing_docs(index: &String, doc_ids: &Vec<String>) -> Result<ExistingDocs, IngestError> {
    let docs = match load_command_raw_result(CommandContext{}, Arc::new(LookupById::new(&index, &doc_ids))).await {
        Ok(lcrr) => match lcrr {
            Some(raw_table) => {
                let df = match data_access::execute_sql(&format!("SELECT * from {raw_table}")).await {
                    Ok(df) => df,
                    Err(_) => panic!("weird")
                };

                let (docs, schema) = to_serde_value(&df).await;
                data_access::drop(&raw_table).await;
                ExistingDocs{ docs, schema }
            },
            None => ExistingDocs{ docs: vec!(), schema: None }
        },
        Err(_) => panic!("weird")
    };
    Ok(docs)
}

async fn create_single_worker(index: &String, doc_id: &String, payload: &String) -> Result<ElasticSearchResponse, IngestError> {
    let table_description: TableDescription = match API_SERVICE_CLIENT.describe_table(&index).await {
        Some(t) => t,
        None => return Err(IngestError{ message: "Index does not exist".to_string() })
    };
    let doc: Result<Value, serde_json::Error> = serde_json::from_str(payload);
    match doc {
        Ok(valid_doc) => {
            let docs = get_existing_docs(&table_description.name, &vec!(doc_id.to_string())).await?;
            
            if docs.docs.len() != 0 {
                // TODO: get version from existing doc
                let response = SingleDocCreateFailedResult {
                    error: ErrorDetails::single_cause(
                        &"version_conflict_engine_exception".to_string(),
                        &format!("[{}]: version conflict, document already exists (current version [{}])", doc_id, 1),
                        Some("what is an index uuid?".to_string()),
                        Some("1".to_string()),
                        Some(index.to_string()),
                    ),
                    status: 409,
                };
                return Ok(ElasticSearchResponse { status: StatusCode::CONFLICT, mime: mime::APPLICATION_JSON, body: serde_json::to_string(&response).unwrap(), headers: vec!() })
            };

            let mut buffer = SpeedboatCommitBuilder::new(index);
            ingest_create(
                &Create{ create: IndexOrCreateBody { index: None, id: Some(doc_id.clone()), list_executed_pipelines: false, require_alias: false, dynamic_templates: None }},
                &valid_doc,
                1,
                None,
                &mut buffer
            );

            let commit_result = buffer.commit().await?;
            assert_eq!(commit_result.operations.len(), 1);
            let headers = vec!((LOCATION, format!("/{}/_doc/{}", table_description.name, url_escape::encode_userinfo(doc_id))));
            Ok(ElasticSearchResponse {
                status: StatusCode::CREATED,
                mime: MIME_ES_JSON.clone(),
                body: serde_json::to_string(&commit_result.operations[0]).unwrap(),
                headers: headers
            })
        },
        Err(e) => {
            let error = format!("{}", e);
            println!("{}", error);
            Ok(ElasticSearchResponse{ status: StatusCode::BAD_REQUEST, mime: mime::APPLICATION_JSON, body: "Bad request".to_string(), headers: vec!() })
        }
    }
}


fn merge_source(existing_doc: &Value, update_doc: &Value) -> Value {
    assert!(existing_doc.is_object());
    assert!(update_doc.is_object());

    let mut new_doc = existing_doc.clone();
    let new_doc_map = new_doc.as_object_mut().unwrap();
    for (key, value) in update_doc.as_object().unwrap().iter() {
        match new_doc_map.get(key) {
            Some(new_doc_value) => {
                if new_doc_value.is_object() && value.is_object() {
                    new_doc_map.insert(key.clone(), merge_source(new_doc_value, value));
                } else {
                    new_doc_map.insert(key.clone(), value.clone());
                }
            },
            None => {
                new_doc_map.insert(key.clone(), value.clone());
            }
        }
    }
    new_doc
}


async fn update_single_worker(index: &String, doc_id: &String, payload: &String) -> Result<ElasticSearchResponse, IngestError> {
    let table_description: TableDescription = match API_SERVICE_CLIENT.describe_table(&index).await {
        Some(t) => t,
        None => return Err(IngestError{ message: "Index does not exist".to_string() })
    };

    let update_request: UpdateBody = match serde_json::from_str(payload) {
        Ok(body) => body,
        Err(_) => {
            return Ok(ElasticSearchResponse{ status: StatusCode::BAD_REQUEST, mime: mime::APPLICATION_JSON, body: "Bad request".to_string(), headers: vec!() })
        }
    };

    let docs = get_existing_docs(&table_description.name, &vec!(doc_id.to_string())).await?;

    let mut buffer = SpeedboatCommitBuilder::new(&table_description.name);
    if docs.docs.len() != 0 {
        assert_eq!(docs.docs.len(), 1);
        if update_request.doc.is_none() {
            todo!("What do we do here?")
        }
        let mut existing_doc = FullRecord::from_record(&docs.docs[0]);
        existing_doc.record_input.ensure_source();
        let mut updated_doc = RecordInput::new(
            existing_doc.record_input.id().clone(),
            existing_doc.record_input.version() + 1,
            &merge_source(existing_doc.record_input.source().unwrap(), update_request.doc.as_ref().unwrap()),
            None
        );
        updated_doc.ensure_source();
        buffer.update(&updated_doc);
        buffer.delete(&RecordDelete::new(&existing_doc.record_input.id(), existing_doc.seq_no));
    } else {
        if update_request.upsert.is_none() {
            // TODO: this is the doc_as_upsert path to figure out
            todo!("Need to implement upsert")
        }

        let upsert_doc = update_request.upsert.unwrap();
        ingest_create(
            &Create { create: IndexOrCreateBody { index: None, id: Some(doc_id.clone()), list_executed_pipelines: false, require_alias: false, dynamic_templates: None } },
            &upsert_doc,
            1,
            None,
            &mut buffer
        );
    };

    let result = buffer.commit().await?;
    assert_eq!(result.operations.len(), 1);
    let headers = vec!((LOCATION, format!("/{}/_doc/{}", table_description.name, url_escape::encode_userinfo(doc_id))));
    Ok(ElasticSearchResponse {
        status: StatusCode::CREATED,
        mime: MIME_ES_JSON.clone(),
        body: serde_json::to_string(&result.operations[0]).unwrap(),
        headers: headers
    })

}


pub(crate) async fn delete(index: &String, doc_id: &String) -> Result<ElasticSearchResponse, IngestError> {
    let table_description: TableDescription = match API_SERVICE_CLIENT.describe_table(&index).await {
        Some(t) => t,
        None => return Err(IngestError{ message: "Index does not exist".to_string() })
    };

    let docs = get_existing_docs(&table_description.name, &vec!(doc_id.clone())).await?;
    if docs.docs.len() == 0 {
        let result = OperationResult {
            _index: index.clone(),
            _id: doc_id.clone(),
            _version: 1,
            result: "not_found".to_string(),
            _shards: Shards {
                total: 1,
                successful: 1,
                failed: 0,
            },
            _seq_no: 0,
            _primary_term: 1,
            status: None,
            get: None,
        };
        return Ok(ElasticSearchResponse { status: StatusCode::NOT_FOUND, mime: mime::APPLICATION_JSON, body: serde_json::to_string(&result).unwrap(), headers: vec!() })
    }
    let mut buffer = SpeedboatCommitBuilder::new(&table_description.name);
    let target_seq_no = docs.docs[0].get("_seq_no").unwrap().as_u64().unwrap();
    buffer.delete(&RecordDelete::new(doc_id, target_seq_no));
    let result = buffer.commit().await?;
    assert_eq!(result.operations.len(), 1);
    Ok(ElasticSearchResponse { status: StatusCode::OK, mime: mime::APPLICATION_JSON, body: serde_json::to_string(&result.operations[0]).unwrap(), headers: vec!() })
}


pub(crate) async fn ingest(provided_index: Option<&String>, payload: &String) -> Result<IngestResult, IngestError> {
    let payload_split = payload.lines();
    let mut ingest_result = IngestResult::new();
    let mut iterator = payload_split.into_iter().peekable();
    while iterator.peek() != None {
        let command_str = iterator.next().unwrap();
        if command_str.len() == 0 {
            continue;
        }

        let deser_command: Result<IngestCommand, serde_json::Error> = serde_json::from_str(command_str);
        match deser_command {
            // TODO: we could bulkify the fetching of the docs to be updated
            Ok(command) => {
                match command {
                    IngestCommand::Create(c) => {
                        let index = match &c.create.index {
                            Some(i) => {
                                match provided_index {
                                    Some(pi) => if i != pi {
                                        return Err(IngestError{ message: "Can not provide a index in create here".to_string() });
                                    },
                                    None => (),
                                }
                                i
                            }
                            None => {
                                match provided_index {
                                    Some(pi) => pi,
                                    None => return Err(IngestError{ message: "Must provide index name".to_string() }),
                                }
                            }                               
                        };
                        let table_description = match API_SERVICE_CLIENT.describe_table(&index).await {
                            Some(t) => t,
                            None => return Err(IngestError{ message: "Index does not exist".to_string() })
                        };
                        let doc_str = match iterator.next() {
                            Some(ds) => ds.trim(),
                            None => panic!("How do I make my own error? This should return an error instead of panic")                            
                        };
                        let doc: Result<Value, serde_json::Error>  = serde_json::from_str(doc_str);
                        match doc {
                            Ok(valid_doc) => {
                                ingest_create(
                                    &c,
                                    &valid_doc,
                                    1,
                                    Some(201),
                                    ingest_result.get(&table_description.name)
                                );
                            },
                            Err(_) => return Err(IngestError{ message: "Serde error".to_string() })
                        }
                    },
                    IngestCommand::Update(u) => {
                        let index = match &u.update.index {
                            Some(i) => {
                                match provided_index {
                                    Some(pi) => if i != pi {
                                        return Err(IngestError{ message: "Can not provide a index in create here".to_string() });
                                    },
                                    None => (),
                                }
                                i
                            }
                            None => {
                                match provided_index {
                                    Some(pi) => pi,
                                    None => return Err(IngestError{ message: "Must provide index name".to_string() }),
                                }
                            }
                        };
                        let table_description = match API_SERVICE_CLIENT.describe_table(&index).await {
                            Some(t) => t,
                            None => return Err(IngestError{ message: "Index does not exist".to_string() })
                        };
                        let existing_docs = get_existing_docs(&table_description.name, &vec!(u.update.id.unwrap())).await?;
                        if existing_docs.docs.len() == 0 {
                            todo!("Need to handle this case")
                        }
                        let doc_str = match iterator.next() {
                            Some(ds) => ds.trim(),
                            None => panic!("How do I make my own error? This should return an error instead of panic")
                        };
                        let doc: Result<UpdateBody, serde_json::Error>  = serde_json::from_str(doc_str);
                        match doc {
                            Ok(update_request) => {
                                if update_request.doc.is_none() {
                                    todo!("What do we do here?")
                                }
                                let mut existing_doc = FullRecord::from_record(&existing_docs.docs[0]);
                                existing_doc.record_input.ensure_source();

                                let mut updated_doc = RecordInput::new(
                                    existing_doc.record_input.id().clone(),
                                    existing_doc.record_input.version() + 1,
                                    &merge_source(existing_doc.record_input.source().unwrap(), update_request.doc.as_ref().unwrap()),
                                    Some(201)
                                );
                                updated_doc.ensure_source();
                                ingest_result.get(index).update(&updated_doc);
                                ingest_result.get(index).delete(&RecordDelete::new(existing_doc.record_input.id(), existing_doc.seq_no));
                            },
                            Err(_) => return Err(IngestError{ message: "Serde error".to_string() })
                        }
                    }
                    _ => {
                        panic!("Not implemented")
                    },
                }
            },
            Err(_) => return Err(IngestError{ message: "Serde error".to_string() })
        }
    }

    Ok(ingest_result)
}


struct IngestRequest {
    response: Vec<OperationResult>,
    respond_to: Option<oneshot::Sender<Result<BulkResult, IngestError>>>,
}

struct IngestActor {
    tables: HashMap<String, SpeedboatCommitBuilder>,
    requests: Vec<IngestRequest>,
    receiver: mpsc::Receiver<IngestActorMessage>,
}

enum IngestActorMessage {
    IngestSingleTable {
        table: String,
        payload: String,
        respond_to: oneshot::Sender<Result<BulkResult, IngestError>>,
    },
    Ingest {
        payload: String,
        respond_to: oneshot::Sender<Result<BulkResult, IngestError>>,
    },
    Commit {
        respond_to: oneshot::Sender<()>,
    }
}

impl IngestActor {
    fn new(receiver: mpsc::Receiver<IngestActorMessage>) -> Self {
        IngestActor {
            tables: HashMap::new(),
            requests: vec!(),
            receiver: receiver,
        }
    }

    fn merge_table_buffers(&mut self, tables: &HashMap<String, SpeedboatCommitBuilder>) -> () {
        for (table, buffer_builder) in tables {
            match self.tables.get_mut(table) {
                Some(buffer) => buffer.extend(buffer_builder),
                None => {
                    self.tables.insert(table.clone(), buffer_builder.clone());
                }
            }
        }
    }

    async fn do_ingest(&mut self, table: Option<&String>, payload: &String, respond_to: oneshot::Sender<Result<BulkResult, IngestError>>) -> () {
        let buffer_items = ingest(table, &payload).await;
        match buffer_items {
            Ok(bi) => {
                let request = IngestRequest { 
                    response: bi.operations,
                    respond_to: Some(respond_to)
                };
                self.requests.push(request);
                self.merge_table_buffers(&bi.tables);
            },
            Err(e) => {
                let _ = respond_to.send(Err(e));
            }
        };        
    }

    async fn handle_message(&mut self, msg: IngestActorMessage) -> () {
        match msg {
            IngestActorMessage::IngestSingleTable { table, payload, respond_to } => {
                self.do_ingest(Some(&table), &payload, respond_to).await
            },
            IngestActorMessage::Ingest { payload, respond_to } => {
                self.do_ingest(None, &payload, respond_to).await
            },            
            IngestActorMessage::Commit { respond_to } => {
                let _ = self.commit().await;
                let _ = respond_to.send(());
            }
        }
    }

    async fn commit(&mut self) -> Result<(), IngestError> {
        if self.requests.len() == 0 {
            return Ok(());
        }

        for (_, buffer_builder) in self.tables.iter_mut() {
            buffer_builder.commit().await?;
        }

        for request in self.requests.iter_mut() {
            // TODO: track and report time correctly
            let _ = request.respond_to.take().unwrap().send(Ok(BulkResult::success(0, request.response.clone())));
        }

        self.requests.clear();
        self.tables.clear();

        Ok(())
    }


}


#[derive(Clone)]
pub struct IngestHandle {
    sender: mpsc::Sender<IngestActorMessage>,
}


async fn run_ingest_message_pump(mut actor: IngestActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg).await;
    }
}


impl IngestHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor: IngestActor = IngestActor::new(receiver);
        tokio::spawn(run_ingest_message_pump(actor));
        Self { sender }
    }

    pub async fn send(&self, payload: &String) -> Result<BulkResult, IngestError> {
        let (send, recv) = oneshot::channel();
        let msg = IngestActorMessage::Ingest { 
            payload: payload.clone(),
            respond_to: send
        };

        let _ = self.sender.send(msg).await;
        match recv.await {
            Ok(r) => r,
            Err(_) => panic!("RecvError")
        }
    }    

    #[allow(dead_code)]
    pub async fn send_single_table(&self, table: &String, payload: &String) -> Result<BulkResult, IngestError> {
        let (send, recv) = oneshot::channel();
        let msg = IngestActorMessage::IngestSingleTable { 
            table: table.clone(), 
            payload: payload.clone(),
            respond_to: send
        };

        let _ = self.sender.send(msg).await;
        match recv.await {
            Ok(r) => r,
            Err(_) => panic!("RecvError")
        }
    }

    pub async fn commit(&self) -> Result<(), RecvError> {
        let (send, recv) = oneshot::channel();
        let msg = IngestActorMessage::Commit { 
            respond_to: send
        };

        let _ = self.sender.send(msg).await;
        recv.await
    }    
}


fn commit_messages() -> Pin<Box<dyn Future<Output = ()> + Send>> {
    async move {
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = INGEST_HANDLE.commit().await;
        }
    }.boxed()
}


fn create_ingest() -> IngestHandle {
    let handle = IngestHandle::new();
    tokio::spawn(commit_messages());
    handle
}

pub(crate) static INGEST_HANDLE: std::sync::LazyLock<IngestHandle> = std::sync::LazyLock::new(|| create_ingest());



#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs};

    use crate::elastic_search_ingest::{IngestCommand, PropertyInfo};

    use super::{CreateIndexBody, CreateIndexTemplateBody};

    #[test]
    fn test_create_deser() {
        let empty_deser: IngestCommand = serde_json::from_str("{\"create\": {} }").unwrap();
        match empty_deser {
            IngestCommand::Create(c) => {
                assert_eq!(c.create.index, None);
                assert_eq!(c.create.id, None);
            },
            _ => panic!("This should be a create"),
        }


        let index_deser: IngestCommand = serde_json::from_str("{\"create\": { \"_index\": \"test\" } }").unwrap();
        match index_deser {
            IngestCommand::Create(c) => {
                assert_eq!(c.create.id, None);
                match c.create.index {
                    Some(cci) => assert_eq!(cci, "test".to_string()),
                    _ => panic!("Should be index == test"),
                }
            },
            _ => panic!("This should be a create"),
        }        
        
    }

    #[test]
    fn test_index_deser() {
        let empty_deser: IngestCommand = serde_json::from_str("{\"index\": {} }").unwrap();
        match empty_deser {
            IngestCommand::Index(i) => {
                assert_eq!(i.index.index, None);
                assert_eq!(i.index.id, None);
            },
            _ => panic!("This should be a create"),
        }
    }
   
    #[test]
    fn test_create_index_deser() {
        let mini_test_val = r#"{        "migrationVersion": {
          "dynamic": "true",
          "properties": {
            "task": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            }
          }
    }}"#;

        let _mini_deser: HashMap<String, PropertyInfo> = match serde_json::from_str(mini_test_val) {
            Ok(d) => d,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("nope");
            }
        };


        let test_val = r#"{
  ".kibana_task_manager_8.7.1_001": {
    "aliases": {
      ".kibana_task_manager": {
        "is_hidden": true
      },
      ".kibana_task_manager_8.7.1": {
        "is_hidden": true
      }
    },
    "mappings": {
      "dynamic": "strict",
      "_meta": {
        "migrationMappingPropertyHashes": {
          "migrationVersion": "4a1746014a75ade3a714e1db5763276f",
          "originId": "2f4316de49999235636386fe51dc06c1",
          "task": "b3d0a471610ff17077e60653f422491d",
          "updated_at": "00da57df13e94e9d98437d13ace4bfe0",
          "references": "7997cf5a56cc02bdc9c93361bde732b0",
          "namespace": "2f4316de49999235636386fe51dc06c1",
          "created_at": "00da57df13e94e9d98437d13ace4bfe0",
          "coreMigrationVersion": "2f4316de49999235636386fe51dc06c1",
          "type": "2f4316de49999235636386fe51dc06c1",
          "namespaces": "2f4316de49999235636386fe51dc06c1"
        }
      },
      "properties": {
        "coreMigrationVersion": {
          "type": "keyword"
        },
        "created_at": {
          "type": "date"
        },
        "migrationVersion": {
          "dynamic": "true",
          "properties": {
            "task": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            }
          }
        },
        "namespace": {
          "type": "keyword"
        },
        "namespaces": {
          "type": "keyword"
        },
        "originId": {
          "type": "keyword"
        },
        "references": {
          "type": "nested",
          "properties": {
            "id": {
              "type": "keyword"
            },
            "name": {
              "type": "keyword"
            },
            "type": {
              "type": "keyword"
            }
          }
        },
        "task": {
          "properties": {
            "attempts": {
              "type": "integer"
            },
            "enabled": {
              "type": "boolean"
            },
            "ownerId": {
              "type": "keyword"
            },
            "params": {
              "type": "text"
            },
            "retryAt": {
              "type": "date"
            },
            "runAt": {
              "type": "date"
            },
            "schedule": {
              "properties": {
                "interval": {
                  "type": "keyword"
                }
              }
            },
            "scheduledAt": {
              "type": "date"
            },
            "scope": {
              "type": "keyword"
            },
            "startedAt": {
              "type": "date"
            },
            "state": {
              "type": "text"
            },
            "status": {
              "type": "keyword"
            },
            "taskType": {
              "type": "keyword"
            },
            "traceparent": {
              "type": "text"
            },
            "user": {
              "type": "keyword"
            }
          }
        },
        "type": {
          "type": "keyword"
        },
        "updated_at": {
          "type": "date"
        }
      }
    }
  }
}"#;
        let deser: HashMap<String, CreateIndexBody> = match serde_json::from_str(test_val) {
            Ok(d) => d,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("nope");
            }
        };
        let index = deser.get(".kibana_task_manager_8.7.1_001").unwrap().clone();
        assert_eq!(index.aliases.map_or_else(|| 0, |x| x.len()), 2);

/* 
        let file_content = match read_to_string("main_lib/tests/data/example_create_index.json") {
            Ok(f) => f,
            Err(_) => panic!("Missing test file")
        };
        let deser_file: CreateIndexBody =  match serde_json::from_str(file_content.as_str()) {
            Ok(d) => d,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                let _ = fs::write("main_lib/output.txt", error);
                panic!("nope");
            }
        };
*/

        let test_val = include_str!("../tests/data/component_template_2.json");

        let _deser: CreateIndexTemplateBody = match serde_json::from_str(test_val) {
            Ok(d) => d,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                let _ = fs::write("../output.txt", error);
                panic!("nope");
            }
        };

        let test_val = r#"{
  "template": {
    "settings": {},
    "mappings": {
      "dynamic": "strict",
      "properties": {
        "monitor": {
          "properties": {
            "id": {
              "type": "keyword"
            },
            "name": {
              "type": "keyword"
            },
            "type": {
              "type": "keyword"
            }
          }
        },
        "url": {
          "properties": {
            "full": {
              "type": "keyword"
            }
          }
        },
        "observer": {
          "properties": {
            "geo": {
              "properties": {
                "name": {
                  "type": "keyword"
                }
              }
            }
          }
        },
        "error": {
          "properties": {
            "message": {
              "type": "text"
            }
          }
        },
        "agent": {
          "properties": {
            "name": {
              "type": "keyword"
            }
          }
        },
        "tls": {
          "properties": {
            "server": {
              "properties": {
                "x509": {
                  "properties": {
                    "issuer": {
                      "properties": {
                        "common_name": {
                          "type": "keyword"
                        }
                      }
                    },
                    "subject": {
                      "properties": {
                        "common_name": {
                          "type": "keyword"
                        }
                      }
                    },
                    "not_after": {
                      "type": "date"
                    },
                    "not_before": {
                      "type": "date"
                    }
                  }
                },
                "hash": {
                  "properties": {
                    "sha256": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }
          }
        },
        "anomaly": {
          "properties": {
            "start": {
              "type": "date"
            },
            "bucket_span": {
              "properties": {
                "minutes": {
                  "type": "keyword"
                }
              }
            }
          }
        },
        "kibana": {
          "properties": {
            "alert": {
              "properties": {
                "evaluation": {
                  "properties": {
                    "threshold": {
                      "type": "scaled_float",
                      "scaling_factor": 100
                    },
                    "value": {
                      "type": "scaled_float",
                      "scaling_factor": 100
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}"#;

        let _deser: CreateIndexTemplateBody = match serde_json::from_str(test_val) {
            Ok(d) => d,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                let _ = fs::write("../output.txt", error);
                panic!("nope");
            }
        };        

        let test_val = r#"{
  "template": {
    "settings": {},
    "mappings": {
      "dynamic": false,
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "event": {
          "properties": {
            "action": {
              "type": "keyword"
            },
            "kind": {
              "type": "keyword"
            }
          }
        },
        "tags": {
          "type": "keyword"
        },
        "kibana": {
          "properties": {
            "alert": {
              "properties": {
                "rule": {
                  "properties": {
                    "parameters": {
                      "type": "flattened",
                      "ignore_above": 4096
                    },
                    "rule_type_id": {
                      "type": "keyword"
                    },
                    "consumer": {
                      "type": "keyword"
                    },
                    "producer": {
                      "type": "keyword"
                    },
                    "author": {
                      "type": "keyword"
                    },
                    "category": {
                      "type": "keyword"
                    },
                    "uuid": {
                      "type": "keyword"
                    },
                    "created_at": {
                      "type": "date"
                    },
                    "created_by": {
                      "type": "keyword"
                    },
                    "description": {
                      "type": "keyword"
                    },
                    "enabled": {
                      "type": "keyword"
                    },
                    "execution": {
                      "properties": {
                        "uuid": {
                          "type": "keyword"
                        }
                      }
                    },
                    "from": {
                      "type": "keyword"
                    },
                    "interval": {
                      "type": "keyword"
                    },
                    "license": {
                      "type": "keyword"
                    },
                    "name": {
                      "type": "keyword"
                    },
                    "note": {
                      "type": "keyword"
                    },
                    "references": {
                      "type": "keyword"
                    },
                    "rule_id": {
                      "type": "keyword"
                    },
                    "rule_name_override": {
                      "type": "keyword"
                    },
                    "tags": {
                      "type": "keyword"
                    },
                    "to": {
                      "type": "keyword"
                    },
                    "type": {
                      "type": "keyword"
                    },
                    "updated_at": {
                      "type": "date"
                    },
                    "updated_by": {
                      "type": "keyword"
                    },
                    "version": {
                      "type": "keyword"
                    },
                    "building_block_type": {
                      "type": "keyword"
                    },
                    "exceptions_list": {
                      "type": "object"
                    },
                    "false_positives": {
                      "type": "keyword"
                    },
                    "immutable": {
                      "type": "keyword"
                    },
                    "max_signals": {
                      "type": "long"
                    },
                    "threat": {
                      "properties": {
                        "framework": {
                          "type": "keyword"
                        },
                        "tactic": {
                          "properties": {
                            "id": {
                              "type": "keyword"
                            },
                            "name": {
                              "type": "keyword"
                            },
                            "reference": {
                              "type": "keyword"
                            }
                          }
                        },
                        "technique": {
                          "properties": {
                            "id": {
                              "type": "keyword"
                            },
                            "name": {
                              "type": "keyword"
                            },
                            "reference": {
                              "type": "keyword"
                            },
                            "subtechnique": {
                              "properties": {
                                "id": {
                                  "type": "keyword"
                                },
                                "name": {
                                  "type": "keyword"
                                },
                                "reference": {
                                  "type": "keyword"
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "timeline_id": {
                      "type": "keyword"
                    },
                    "timeline_title": {
                      "type": "keyword"
                    },
                    "timestamp_override": {
                      "type": "keyword"
                    }
                  }
                },
                "uuid": {
                  "type": "keyword"
                },
                "instance": {
                  "properties": {
                    "id": {
                      "type": "keyword"
                    }
                  }
                },
                "start": {
                  "type": "date"
                },
                "time_range": {
                  "type": "date_range",
                  "format": "epoch_millis||strict_date_optional_time"
                },
                "end": {
                  "type": "date"
                },
                "duration": {
                  "properties": {
                    "us": {
                      "type": "long"
                    }
                  }
                },
                "severity": {
                  "type": "keyword"
                },
                "status": {
                  "type": "keyword"
                },
                "flapping": {
                  "type": "boolean"
                },
                "risk_score": {
                  "type": "float"
                },
                "workflow_status": {
                  "type": "keyword"
                },
                "workflow_user": {
                  "type": "keyword"
                },
                "workflow_reason": {
                  "type": "keyword"
                },
                "system_status": {
                  "type": "keyword"
                },
                "action_group": {
                  "type": "keyword"
                },
                "reason": {
                  "type": "keyword"
                },
                "case_ids": {
                  "type": "keyword"
                },
                "suppression": {
                  "properties": {
                    "terms": {
                      "properties": {
                        "field": {
                          "type": "keyword"
                        },
                        "value": {
                          "type": "keyword"
                        }
                      }
                    },
                    "start": {
                      "type": "date"
                    },
                    "end": {
                      "type": "date"
                    },
                    "docs_count": {
                      "type": "long"
                    }
                  }
                },
                "last_detected": {
                  "type": "date"
                },
                "ancestors": {
                  "type": "object",
                  "properties": {
                    "depth": {
                      "type": "long"
                    },
                    "id": {
                      "type": "keyword"
                    },
                    "index": {
                      "type": "keyword"
                    },
                    "rule": {
                      "type": "keyword"
                    },
                    "type": {
                      "type": "keyword"
                    }
                  }
                },
                "building_block_type": {
                  "type": "keyword"
                },
                "depth": {
                  "type": "long"
                },
                "group": {
                  "properties": {
                    "id": {
                      "type": "keyword"
                    },
                    "index": {
                      "type": "integer"
                    }
                  }
                },
                "original_event": {
                  "properties": {
                    "action": {
                      "type": "keyword"
                    },
                    "agent_id_status": {
                      "type": "keyword"
                    },
                    "category": {
                      "type": "keyword"
                    },
                    "code": {
                      "type": "keyword"
                    },
                    "created": {
                      "type": "date"
                    },
                    "dataset": {
                      "type": "keyword"
                    },
                    "duration": {
                      "type": "keyword"
                    },
                    "end": {
                      "type": "date"
                    },
                    "hash": {
                      "type": "keyword"
                    },
                    "id": {
                      "type": "keyword"
                    },
                    "ingested": {
                      "type": "date"
                    },
                    "kind": {
                      "type": "keyword"
                    },
                    "module": {
                      "type": "keyword"
                    },
                    "original": {
                      "type": "keyword"
                    },
                    "outcome": {
                      "type": "keyword"
                    },
                    "provider": {
                      "type": "keyword"
                    },
                    "reason": {
                      "type": "keyword"
                    },
                    "reference": {
                      "type": "keyword"
                    },
                    "risk_score": {
                      "type": "float"
                    },
                    "risk_score_norm": {
                      "type": "float"
                    },
                    "sequence": {
                      "type": "long"
                    },
                    "severity": {
                      "type": "long"
                    },
                    "start": {
                      "type": "date"
                    },
                    "timezone": {
                      "type": "keyword"
                    },
                    "type": {
                      "type": "keyword"
                    },
                    "url": {
                      "type": "keyword"
                    }
                  }
                },
                "original_time": {
                  "type": "date"
                },
                "threshold_result": {
                  "properties": {
                    "cardinality": {
                      "type": "object",
                      "properties": {
                        "field": {
                          "type": "keyword"
                        },
                        "value": {
                          "type": "long"
                        }
                      }
                    },
                    "count": {
                      "type": "long"
                    },
                    "from": {
                      "type": "date"
                    },
                    "terms": {
                      "type": "object",
                      "properties": {
                        "field": {
                          "type": "keyword"
                        },
                        "value": {
                          "type": "keyword"
                        }
                      }
                    }
                  }
                },
                "new_terms": {
                  "type": "keyword"
                }
              }
            },
            "space_ids": {
              "type": "keyword"
            },
            "version": {
              "type": "version"
            }
          }
        },
        "ecs": {
          "properties": {
            "version": {
              "type": "keyword"
            }
          }
        },
        "signal": {
          "properties": {
            "ancestors": {
              "properties": {
                "depth": {
                  "type": "alias",
                  "path": "kibana.alert.ancestors.depth"
                },
                "id": {
                  "type": "alias",
                  "path": "kibana.alert.ancestors.id"
                },
                "index": {
                  "type": "alias",
                  "path": "kibana.alert.ancestors.index"
                },
                "type": {
                  "type": "alias",
                  "path": "kibana.alert.ancestors.type"
                }
              }
            },
            "depth": {
              "type": "alias",
              "path": "kibana.alert.depth"
            },
            "group": {
              "properties": {
                "id": {
                  "type": "alias",
                  "path": "kibana.alert.group.id"
                },
                "index": {
                  "type": "alias",
                  "path": "kibana.alert.group.index"
                }
              }
            },
            "original_event": {
              "properties": {
                "action": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.action"
                },
                "category": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.category"
                },
                "code": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.code"
                },
                "created": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.created"
                },
                "dataset": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.dataset"
                },
                "duration": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.duration"
                },
                "end": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.end"
                },
                "hash": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.hash"
                },
                "id": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.id"
                },
                "kind": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.kind"
                },
                "module": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.module"
                },
                "outcome": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.outcome"
                },
                "provider": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.provider"
                },
                "reason": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.reason"
                },
                "risk_score": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.risk_score"
                },
                "risk_score_norm": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.risk_score_norm"
                },
                "sequence": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.sequence"
                },
                "severity": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.severity"
                },
                "start": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.start"
                },
                "timezone": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.timezone"
                },
                "type": {
                  "type": "alias",
                  "path": "kibana.alert.original_event.type"
                }
              }
            },
            "original_time": {
              "type": "alias",
              "path": "kibana.alert.original_time"
            },
            "reason": {
              "type": "alias",
              "path": "kibana.alert.reason"
            },
            "rule": {
              "properties": {
                "author": {
                  "type": "alias",
                  "path": "kibana.alert.rule.author"
                },
                "building_block_type": {
                  "type": "alias",
                  "path": "kibana.alert.building_block_type"
                },
                "created_at": {
                  "type": "alias",
                  "path": "kibana.alert.rule.created_at"
                },
                "created_by": {
                  "type": "alias",
                  "path": "kibana.alert.rule.created_by"
                },
                "description": {
                  "type": "alias",
                  "path": "kibana.alert.rule.description"
                },
                "enabled": {
                  "type": "alias",
                  "path": "kibana.alert.rule.enabled"
                },
                "false_positives": {
                  "type": "alias",
                  "path": "kibana.alert.rule.false_positives"
                },
                "from": {
                  "type": "alias",
                  "path": "kibana.alert.rule.from"
                },
                "id": {
                  "type": "alias",
                  "path": "kibana.alert.rule.uuid"
                },
                "immutable": {
                  "type": "alias",
                  "path": "kibana.alert.rule.immutable"
                },
                "interval": {
                  "type": "alias",
                  "path": "kibana.alert.rule.interval"
                },
                "license": {
                  "type": "alias",
                  "path": "kibana.alert.rule.license"
                },
                "max_signals": {
                  "type": "alias",
                  "path": "kibana.alert.rule.max_signals"
                },
                "name": {
                  "type": "alias",
                  "path": "kibana.alert.rule.name"
                },
                "note": {
                  "type": "alias",
                  "path": "kibana.alert.rule.note"
                },
                "references": {
                  "type": "alias",
                  "path": "kibana.alert.rule.references"
                },
                "risk_score": {
                  "type": "alias",
                  "path": "kibana.alert.risk_score"
                },
                "rule_id": {
                  "type": "alias",
                  "path": "kibana.alert.rule.rule_id"
                },
                "rule_name_override": {
                  "type": "alias",
                  "path": "kibana.alert.rule.rule_name_override"
                },
                "severity": {
                  "type": "alias",
                  "path": "kibana.alert.severity"
                },
                "tags": {
                  "type": "alias",
                  "path": "kibana.alert.rule.tags"
                },
                "threat": {
                  "properties": {
                    "framework": {
                      "type": "alias",
                      "path": "kibana.alert.rule.threat.framework"
                    },
                    "tactic": {
                      "properties": {
                        "id": {
                          "type": "alias",
                          "path": "kibana.alert.rule.threat.tactic.id"
                        },
                        "name": {
                          "type": "alias",
                          "path": "kibana.alert.rule.threat.tactic.name"
                        },
                        "reference": {
                          "type": "alias",
                          "path": "kibana.alert.rule.threat.tactic.reference"
                        }
                      }
                    },
                    "technique": {
                      "properties": {
                        "id": {
                          "type": "alias",
                          "path": "kibana.alert.rule.threat.technique.id"
                        },
                        "name": {
                          "type": "alias",
                          "path": "kibana.alert.rule.threat.technique.name"
                        },
                        "reference": {
                          "type": "alias",
                          "path": "kibana.alert.rule.threat.technique.reference"
                        },
                        "subtechnique": {
                          "properties": {
                            "id": {
                              "type": "alias",
                              "path": "kibana.alert.rule.threat.technique.subtechnique.id"
                            },
                            "name": {
                              "type": "alias",
                              "path": "kibana.alert.rule.threat.technique.subtechnique.name"
                            },
                            "reference": {
                              "type": "alias",
                              "path": "kibana.alert.rule.threat.technique.subtechnique.reference"
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "timeline_id": {
                  "type": "alias",
                  "path": "kibana.alert.rule.timeline_id"
                },
                "timeline_title": {
                  "type": "alias",
                  "path": "kibana.alert.rule.timeline_title"
                },
                "timestamp_override": {
                  "type": "alias",
                  "path": "kibana.alert.rule.timestamp_override"
                },
                "to": {
                  "type": "alias",
                  "path": "kibana.alert.rule.to"
                },
                "type": {
                  "type": "alias",
                  "path": "kibana.alert.rule.type"
                },
                "updated_at": {
                  "type": "alias",
                  "path": "kibana.alert.rule.updated_at"
                },
                "updated_by": {
                  "type": "alias",
                  "path": "kibana.alert.rule.updated_by"
                },
                "version": {
                  "type": "alias",
                  "path": "kibana.alert.rule.version"
                }
              }
            },
            "status": {
              "type": "alias",
              "path": "kibana.alert.workflow_status"
            },
            "threshold_result": {
              "properties": {
                "from": {
                  "type": "alias",
                  "path": "kibana.alert.threshold_result.from"
                },
                "terms": {
                  "properties": {
                    "field": {
                      "type": "alias",
                      "path": "kibana.alert.threshold_result.terms.field"
                    },
                    "value": {
                      "type": "alias",
                      "path": "kibana.alert.threshold_result.terms.value"
                    }
                  }
                },
                "cardinality": {
                  "properties": {
                    "field": {
                      "type": "alias",
                      "path": "kibana.alert.threshold_result.cardinality.field"
                    },
                    "value": {
                      "type": "alias",
                      "path": "kibana.alert.threshold_result.cardinality.value"
                    }
                  }
                },
                "count": {
                  "type": "alias",
                  "path": "kibana.alert.threshold_result.count"
                }
              }
            }
          }
        }
      }
    }
  }
}
"#;
        let _deser: CreateIndexTemplateBody = match serde_json::from_str(test_val) {
            Ok(d) => d,
            Err(e) => {
                let error = format!("{}", e);
                let error_str = error.as_str();
                println!("{}", error_str);
                let _ = fs::write("/Users/gregory/code/powdrr-engine/main_lib/output.txt", error);
                panic!("nope");
            }
        };

        let test_val = include_str!("../tests/data/component_template_1.json");

        let _deser: CreateIndexTemplateBody = match serde_json::from_str(test_val) {
            Ok(d) => d,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                let _ = fs::write("../output.txt", error);
                panic!("nope");
            }
        };
    }

    #[test]
    fn test_create_index_template() {
        let test_val = r#"{"version":1,"index_patterns":[".apm-source-map"],"template":{"settings":{"index":{"number_of_shards":1,"auto_expand_replicas":"0-2","hidden":true}},"mappings":{"dynamic":"strict","properties":{"fleet_id":{"type":"keyword"},"created":{"type":"date"},"content":{"type":"binary"},"content_sha256":{"type":"keyword"},"file.path":{"type":"keyword"},"main_lib.name":{"type":"keyword"},"main_lib.version":{"type":"keyword"}}}}}"#;

        let _deser: CreateIndexTemplateBody = match serde_json::from_str(test_val) {
            Ok(d) => d,
            Err(e) => {
                let error = format!("{}", e);
                println!("{}", error);
                panic!("nope");
            }
        };
    }

}
