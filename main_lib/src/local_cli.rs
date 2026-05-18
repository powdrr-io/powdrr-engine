use crate::data_contract::{
    ExtensionFile, FileDescriptor, FileSetPayload, IcebergMetadata, TableMetadataCheckpoint,
};
use crate::elastic_search_common::CommandContext;
use crate::elastic_search_index::{IndexError, create_index_inner_with_doc_id};
use crate::elastic_search_parser;
use crate::schema_massager::{PowdrrSchema, to_powdrr_schema};
use crate::search_executor;
use crate::state_provider::STATE_PROVIDER;
use crate::test_api::PeerModeType;
use crate::util::add_file_suffix;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use futures_util::TryStreamExt;
use idgenerator::IdInstance;
use object_store::{
    ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path as ObjectStorePath,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MANIFEST_FILE_NAME: &str = "manifest.json";
const FILES_DIR_NAME: &str = "files";
const ANALYZE_TABLE_NAME: &str = "__powdrr_cli_analysis__";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LocalQueryLanguage {
    ElasticsearchJson,
}

#[derive(Clone, Debug)]
pub struct LocalParquetBuildRequest {
    pub source: String,
    pub cache_dir: PathBuf,
    pub table_name: String,
    pub doc_id_field: String,
    pub replace: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct LocalParquetBuildResult {
    pub source: String,
    pub cache_dir: String,
    pub table_name: String,
    pub doc_id_field: String,
    pub file_count: usize,
}

#[derive(Clone, Debug)]
pub struct LocalParquetQueryRequest {
    pub cache_dir: PathBuf,
    pub language: LocalQueryLanguage,
    pub body: String,
    pub rest_total_hits_as_int: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct LocalQueryResponse {
    pub status_code: u16,
    pub body: String,
}

#[derive(Clone, Debug)]
pub struct LocalQueryAnalysisRequest {
    pub language: LocalQueryLanguage,
    pub body: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LocalQueryPerformanceClassification {
    HighlyOptimized,
    SupportedButProbablySlow,
    Unsupported,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LocalQueryExecutionPath {
    TypedNodeMerge,
    LegacySqlFanout,
    Unsupported,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalQueryPerformanceAnalysis {
    pub classification: LocalQueryPerformanceClassification,
    pub execution_path: LocalQueryExecutionPath,
    pub reason: String,
}

#[derive(Debug)]
pub struct LocalCliError {
    message: String,
}

impl LocalCliError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn from_io(context: &str, error: std::io::Error) -> Self {
        Self::new(format!("{context}: {error}"))
    }
}

impl Display for LocalCliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl Error for LocalCliError {}

impl From<crate::elastic_search_common::ParseError> for LocalCliError {
    fn from(value: crate::elastic_search_common::ParseError) -> Self {
        Self::new(value.to_string())
    }
}

impl From<IndexError> for LocalCliError {
    fn from(value: IndexError) -> Self {
        Self::new(value.to_string())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CacheManifest {
    version: u32,
    source: String,
    table_name: String,
    doc_id_field: String,
    files: Vec<String>,
}

#[derive(Clone, Debug)]
struct LocalSourceFile {
    source_path: PathBuf,
    relative_path: PathBuf,
}

#[derive(Clone, Debug)]
struct S3SourceFile {
    object_path: String,
    relative_path: PathBuf,
}

pub async fn build_local_parquet_cache(
    request: &LocalParquetBuildRequest,
) -> Result<LocalParquetBuildResult, LocalCliError> {
    prepare_cache_dir(&request.cache_dir, request.replace)?;
    let canonical_cache_dir = request.cache_dir.canonicalize().map_err(|error| {
        LocalCliError::from_io(
            &format!(
                "Failed to resolve cache directory {}",
                request.cache_dir.display()
            ),
            error,
        )
    })?;
    let files_dir = canonical_cache_dir.join(FILES_DIR_NAME);
    fs::create_dir_all(&files_dir)
        .map_err(|error| LocalCliError::from_io("Failed to create cache files directory", error))?;

    let cached_file_paths = if request.source.starts_with("s3://") {
        cache_s3_source(&request.source, &files_dir).await?
    } else {
        cache_local_source(&request.source, &files_dir)?
    };

    if cached_file_paths.is_empty() {
        return Err(LocalCliError::new(format!(
            "No parquet files found in source {}",
            request.source
        )));
    }

    let file_descriptors =
        validate_and_describe_cached_files(&cached_file_paths, &request.doc_id_field)?;
    let dummy_speedboat_files = vec![];
    create_index_inner_with_doc_id(
        &file_descriptors,
        &dummy_speedboat_files,
        &request.doc_id_field,
    )
    .await?;

    let manifest = CacheManifest {
        version: 1,
        source: request.source.clone(),
        table_name: request.table_name.clone(),
        doc_id_field: request.doc_id_field.clone(),
        files: cached_file_paths
            .iter()
            .map(|path| {
                path.strip_prefix(&canonical_cache_dir)
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            })
            .collect(),
    };
    write_manifest(&canonical_cache_dir, &manifest)?;

    Ok(LocalParquetBuildResult {
        source: request.source.clone(),
        cache_dir: canonical_cache_dir.display().to_string(),
        table_name: request.table_name.clone(),
        doc_id_field: request.doc_id_field.clone(),
        file_count: manifest.files.len(),
    })
}

pub async fn query_local_parquet_cache(
    request: &LocalParquetQueryRequest,
) -> Result<LocalQueryResponse, LocalCliError> {
    let canonical_cache_dir = request.cache_dir.canonicalize().map_err(|error| {
        LocalCliError::from_io(
            &format!(
                "Failed to resolve cache directory {}",
                request.cache_dir.display()
            ),
            error,
        )
    })?;
    let manifest = read_manifest(&canonical_cache_dir)?;
    let cached_file_paths = manifest
        .files
        .iter()
        .map(|relative| canonical_cache_dir.join(relative))
        .collect::<Vec<PathBuf>>();

    let file_descriptors =
        validate_and_describe_cached_files(&cached_file_paths, &manifest.doc_id_field)?;
    let dummy_speedboat_files = vec![];
    create_index_inner_with_doc_id(
        &file_descriptors,
        &dummy_speedboat_files,
        &manifest.doc_id_field,
    )
    .await?;

    let checkpoint = build_checkpoint(&manifest, &file_descriptors);
    STATE_PROVIDER.set_peer_mode(&PeerModeType::SelfOnly).await;
    STATE_PROVIDER.add_checkpoint(&checkpoint).await;

    let plan = match request.language {
        LocalQueryLanguage::ElasticsearchJson => elastic_search_parser::parse_search_plan(
            Some(manifest.table_name.clone()),
            &request.body,
        )?,
    };

    let query_string = crate::elastic_search_endpoints::QueryStringSearch {
        allow_partial_search_results: None,
        sort: None,
        ignore_unavailable: None,
        allow_no_indices: None,
        expand_wildcards: None,
        rest_total_hits_as_int: request.rest_total_hits_as_int,
    };
    let command = search_executor::search_plan_to_command_with_options(
        plan,
        &query_string,
        Some(manifest.doc_id_field.as_str()),
        false,
    )?;
    let response =
        search_executor::execute_search_command(CommandContext {}, Arc::new(command)).await;

    Ok(LocalQueryResponse {
        status_code: response.status.as_u16(),
        body: response.body,
    })
}

pub fn analyze_local_query(request: &LocalQueryAnalysisRequest) -> LocalQueryPerformanceAnalysis {
    match request.language {
        LocalQueryLanguage::ElasticsearchJson => {
            match elastic_search_parser::parse_search_plan(
                Some(ANALYZE_TABLE_NAME.to_string()),
                &request.body,
            )
            .and_then(|plan| {
                search_executor::search_plan_to_command(
                    plan,
                    &crate::elastic_search_endpoints::QueryStringSearch::new(),
                )
            }) {
                Ok(command) => {
                    let assessment = command.performance_assessment();
                    match assessment.path {
                        search_executor::SearchPerformancePath::TypedNodeMerge => {
                            LocalQueryPerformanceAnalysis {
                                classification:
                                    LocalQueryPerformanceClassification::HighlyOptimized,
                                execution_path: LocalQueryExecutionPath::TypedNodeMerge,
                                reason: assessment.reason,
                            }
                        }
                        search_executor::SearchPerformancePath::LegacySqlFanout => {
                            LocalQueryPerformanceAnalysis {
                                classification:
                                    LocalQueryPerformanceClassification::SupportedButProbablySlow,
                                execution_path: LocalQueryExecutionPath::LegacySqlFanout,
                                reason: assessment.reason,
                            }
                        }
                    }
                }
                Err(error) => LocalQueryPerformanceAnalysis {
                    classification: LocalQueryPerformanceClassification::Unsupported,
                    execution_path: LocalQueryExecutionPath::Unsupported,
                    reason: error.to_string(),
                },
            }
        }
    }
}

fn prepare_cache_dir(cache_dir: &Path, replace: bool) -> Result<(), LocalCliError> {
    if cache_dir.exists() {
        if !replace {
            return Err(LocalCliError::new(format!(
                "Cache directory {} already exists. Re-run with --replace to rebuild it.",
                cache_dir.display()
            )));
        }
        fs::remove_dir_all(cache_dir).map_err(|error| {
            LocalCliError::from_io("Failed to remove existing cache directory", error)
        })?;
    }

    fs::create_dir_all(cache_dir)
        .map_err(|error| LocalCliError::from_io("Failed to create cache directory", error))
}

fn cache_local_source(source: &str, files_dir: &Path) -> Result<Vec<PathBuf>, LocalCliError> {
    let source_path = PathBuf::from(source);
    let canonical_source = source_path.canonicalize().map_err(|error| {
        LocalCliError::from_io(
            &format!("Failed to resolve local source {}", source_path.display()),
            error,
        )
    })?;

    let source_files = collect_local_source_files(&canonical_source)?;
    let mut cached_paths = Vec::with_capacity(source_files.len());
    for source_file in source_files.iter() {
        let destination = files_dir.join(&source_file.relative_path);
        ensure_parent_dir(&destination)?;
        fs::copy(&source_file.source_path, &destination).map_err(|error| {
            LocalCliError::from_io(
                &format!(
                    "Failed to copy {} to {}",
                    source_file.source_path.display(),
                    destination.display()
                ),
                error,
            )
        })?;
        cached_paths.push(destination.canonicalize().map_err(|error| {
            LocalCliError::from_io(
                &format!("Failed to resolve cached file {}", destination.display()),
                error,
            )
        })?);
    }
    Ok(cached_paths)
}

async fn cache_s3_source(source: &str, files_dir: &Path) -> Result<Vec<PathBuf>, LocalCliError> {
    let (bucket, key_prefix) = parse_s3_source(source)?;
    let store = build_s3_store(bucket.as_str())?;
    let source_files = list_s3_source_files(store.as_ref(), &bucket, key_prefix.as_deref()).await?;
    let mut cached_paths = Vec::with_capacity(source_files.len());
    for source_file in source_files.iter() {
        let destination = files_dir.join(&source_file.relative_path);
        ensure_parent_dir(&destination)?;
        let bytes = store
            .get(&ObjectStorePath::from(source_file.object_path.clone()))
            .await
            .map_err(|error| {
                LocalCliError::new(format!(
                    "Failed to download s3://{}/{}: {}",
                    bucket, source_file.object_path, error
                ))
            })?
            .bytes()
            .await
            .map_err(|error| {
                LocalCliError::new(format!(
                    "Failed to read s3://{}/{}: {}",
                    bucket, source_file.object_path, error
                ))
            })?;
        fs::write(&destination, bytes.as_ref()).map_err(|error| {
            LocalCliError::from_io(
                &format!("Failed to write cached file {}", destination.display()),
                error,
            )
        })?;
        cached_paths.push(destination.canonicalize().map_err(|error| {
            LocalCliError::from_io(
                &format!("Failed to resolve cached file {}", destination.display()),
                error,
            )
        })?);
    }
    Ok(cached_paths)
}

fn collect_local_source_files(source_path: &Path) -> Result<Vec<LocalSourceFile>, LocalCliError> {
    let metadata = fs::metadata(source_path).map_err(|error| {
        LocalCliError::from_io(
            &format!("Failed to stat local source {}", source_path.display()),
            error,
        )
    })?;

    if metadata.is_file() {
        if !is_parquet_file(source_path) {
            return Err(LocalCliError::new(format!(
                "Source file {} is not a parquet file",
                source_path.display()
            )));
        }
        return Ok(vec![LocalSourceFile {
            source_path: source_path.to_path_buf(),
            relative_path: PathBuf::from(
                source_path
                    .file_name()
                    .ok_or_else(|| LocalCliError::new("Source file has no name"))?,
            ),
        }]);
    }

    let mut results = vec![];
    collect_local_source_files_recursive(source_path, source_path, &mut results)?;
    results.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(results)
}

fn collect_local_source_files_recursive(
    root: &Path,
    current: &Path,
    results: &mut Vec<LocalSourceFile>,
) -> Result<(), LocalCliError> {
    let entries = fs::read_dir(current).map_err(|error| {
        LocalCliError::from_io(
            &format!(
                "Failed to list local source directory {}",
                current.display()
            ),
            error,
        )
    })?;

    for entry in entries {
        let entry = entry.map_err(|error| {
            LocalCliError::from_io(
                &format!("Failed to read entry in {}", current.display()),
                error,
            )
        })?;
        let path = entry.path();
        let entry_type = entry.file_type().map_err(|error| {
            LocalCliError::from_io(
                &format!("Failed to read file type for {}", path.display()),
                error,
            )
        })?;
        if entry_type.is_dir() {
            collect_local_source_files_recursive(root, &path, results)?;
        } else if entry_type.is_file() && is_parquet_file(&path) {
            results.push(LocalSourceFile {
                source_path: path.clone(),
                relative_path: path
                    .strip_prefix(root)
                    .map_err(|error| LocalCliError::new(format!("{}", error)))?
                    .to_path_buf(),
            });
        }
    }
    Ok(())
}

fn is_parquet_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.eq_ignore_ascii_case("parquet"))
        .unwrap_or(false)
}

fn parse_s3_source(source: &str) -> Result<(String, Option<String>), LocalCliError> {
    let without_scheme = source
        .strip_prefix("s3://")
        .ok_or_else(|| LocalCliError::new(format!("Invalid S3 source {}", source)))?;
    let mut parts = without_scheme.splitn(2, '/');
    let bucket = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LocalCliError::new(format!("Invalid S3 source {}", source)))?;
    let key_prefix = parts
        .next()
        .map(|value| value.trim_matches('/').to_string());
    Ok((
        bucket.to_string(),
        key_prefix.filter(|value| !value.is_empty()),
    ))
}

fn build_s3_store(bucket: &str) -> Result<Arc<dyn ObjectStore>, LocalCliError> {
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    if std::env::var("AWS_REGION").is_err() && std::env::var("AWS_DEFAULT_REGION").is_err() {
        builder = builder.with_region("us-east-1");
    }

    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL_S3")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        .or_else(|_| std::env::var("AWS_ENDPOINT"))
    {
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
        builder = builder.with_endpoint(endpoint);
    }

    builder
        .build()
        .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
        .map_err(|error| LocalCliError::new(format!("Failed to configure S3 store: {}", error)))
}

async fn list_s3_source_files(
    store: &dyn ObjectStore,
    bucket: &str,
    key_prefix: Option<&str>,
) -> Result<Vec<S3SourceFile>, LocalCliError> {
    let mut results = vec![];

    if let Some(key_prefix) = key_prefix {
        if key_prefix.ends_with(".parquet") {
            store
                .head(&ObjectStorePath::from(key_prefix.to_string()))
                .await
                .map_err(|error| {
                    LocalCliError::new(format!(
                        "Failed to inspect s3://{}/{}: {}",
                        bucket, key_prefix, error
                    ))
                })?;
            results.push(S3SourceFile {
                object_path: key_prefix.to_string(),
                relative_path: PathBuf::from(
                    Path::new(key_prefix)
                        .file_name()
                        .ok_or_else(|| LocalCliError::new("S3 object has no file name"))?,
                ),
            });
            return Ok(results);
        }
    }

    let prefix = key_prefix.map(|value| ObjectStorePath::from(value.to_string()));
    let metas = store
        .list(prefix.as_ref())
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| {
            LocalCliError::new(format!(
                "Failed to list S3 source s3://{}/{}: {}",
                bucket,
                key_prefix.unwrap_or_default(),
                error
            ))
        })?;

    for meta in metas
        .into_iter()
        .filter(|meta| meta.location.as_ref().ends_with(".parquet"))
    {
        let object_path = meta.location.to_string();
        let relative_path = match key_prefix {
            Some(prefix) => {
                let normalized_prefix = prefix.trim_matches('/');
                let suffix = object_path
                    .strip_prefix(normalized_prefix)
                    .unwrap_or(object_path.as_str())
                    .trim_start_matches('/');
                if suffix.is_empty() {
                    PathBuf::from(
                        Path::new(&object_path)
                            .file_name()
                            .ok_or_else(|| LocalCliError::new("S3 object has no file name"))?,
                    )
                } else {
                    PathBuf::from(suffix)
                }
            }
            None => PathBuf::from(&object_path),
        };
        results.push(S3SourceFile {
            object_path,
            relative_path,
        });
    }

    results.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(results)
}

fn ensure_parent_dir(path: &Path) -> Result<(), LocalCliError> {
    let parent = path
        .parent()
        .ok_or_else(|| LocalCliError::new(format!("Path {} has no parent", path.display())))?;
    fs::create_dir_all(parent)
        .map_err(|error| LocalCliError::from_io("Failed to create parent directory", error))
}

fn validate_and_describe_cached_files(
    cached_file_paths: &[PathBuf],
    doc_id_field: &str,
) -> Result<Vec<FileDescriptor>, LocalCliError> {
    let mut descriptors = Vec::with_capacity(cached_file_paths.len());
    for path in cached_file_paths.iter() {
        let file = fs::File::open(path).map_err(|error| {
            LocalCliError::from_io(
                &format!("Failed to open cached parquet file {}", path.display()),
                error,
            )
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            LocalCliError::new(format!("Failed to read {}: {}", path.display(), error))
        })?;
        let schema = to_powdrr_schema(builder.schema().as_ref());
        if !schema
            .fields()
            .iter()
            .any(|field| field.name == doc_id_field)
        {
            return Err(LocalCliError::new(format!(
                "File {} is missing doc id field {}",
                path.display(),
                doc_id_field
            )));
        }
        let size = fs::metadata(path)
            .map_err(|error| {
                LocalCliError::from_io(
                    &format!("Failed to stat cached parquet file {}", path.display()),
                    error,
                )
            })?
            .len();
        descriptors.push(FileDescriptor {
            file_path: path.display().to_string(),
            schema,
            size,
        });
    }
    Ok(descriptors)
}

fn build_checkpoint(
    manifest: &CacheManifest,
    file_descriptors: &[FileDescriptor],
) -> TableMetadataCheckpoint {
    let mut file_set = FileSetPayload::new();
    let mut merged_schema = PowdrrSchema::minimal();
    let mut extension_files = HashMap::new();

    for file_descriptor in file_descriptors.iter() {
        merged_schema.merge_from(&file_descriptor.schema);
        file_set.add(file_descriptor);
        extension_files.insert(
            file_descriptor.file_path.clone(),
            vec![ExtensionFile {
                suffix: "search_index".to_string(),
                location: add_file_suffix(
                    &file_descriptor.file_path,
                    &"search_index".to_string(),
                    Some(&".parquet".to_string()),
                ),
            }],
        );
    }

    TableMetadataCheckpoint {
        table_name: manifest.table_name.clone(),
        original_checkpoint_id: None,
        checkpoint_id: IdInstance::next_id().to_string(),
        iceberg_metadata: Some(IcebergMetadata {
            table_schema: merged_schema.clone(),
            snapshot_id: None,
            files: file_set,
            column_names: vec![],
            column_stats: vec![],
            file_stats: vec![],
        }),
        speedboat_metadata: None,
        deletes_metadata: None,
        extension_metadata: HashMap::from([("es".to_string(), extension_files)]),
        schema: merged_schema,
    }
}

fn write_manifest(cache_dir: &Path, manifest: &CacheManifest) -> Result<(), LocalCliError> {
    let manifest_path = cache_dir.join(MANIFEST_FILE_NAME);
    let body = serde_json::to_vec_pretty(manifest).map_err(|error| {
        LocalCliError::new(format!("Failed to serialize cache manifest: {}", error))
    })?;
    fs::write(&manifest_path, body).map_err(|error| {
        LocalCliError::from_io(
            &format!("Failed to write cache manifest {}", manifest_path.display()),
            error,
        )
    })
}

fn read_manifest(cache_dir: &Path) -> Result<CacheManifest, LocalCliError> {
    let manifest_path = cache_dir.join(MANIFEST_FILE_NAME);
    let body = fs::read_to_string(&manifest_path).map_err(|error| {
        LocalCliError::from_io(
            &format!("Failed to read cache manifest {}", manifest_path.display()),
            error,
        )
    })?;
    serde_json::from_str(&body).map_err(|error| {
        LocalCliError::new(format!(
            "Failed to parse cache manifest {}: {}",
            manifest_path.display(),
            error
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        LocalParquetBuildRequest, LocalParquetQueryRequest, LocalQueryLanguage,
        build_local_parquet_cache, query_local_parquet_cache,
    };
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use serde_json::Value;
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn write_test_parquet(path: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("doc_id", DataType::Int64, false),
            Field::new("message", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec!["login failed", "payment accepted"])) as ArrayRef,
            ],
        )
        .unwrap();

        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn build_and_query_local_cache() {
        let source_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();
        let parquet_path = source_dir.path().join("events.parquet");
        write_test_parquet(&parquet_path);

        build_local_parquet_cache(&LocalParquetBuildRequest {
            source: source_dir.path().display().to_string(),
            cache_dir: cache_dir.path().join("cache"),
            table_name: "events".to_string(),
            doc_id_field: "doc_id".to_string(),
            replace: true,
        })
        .await
        .unwrap();

        let response = query_local_parquet_cache(&LocalParquetQueryRequest {
            cache_dir: cache_dir.path().join("cache"),
            language: LocalQueryLanguage::ElasticsearchJson,
            body: r#"{
  "query": {
    "match": {
      "message": {
        "query": "failed"
      }
    }
  }
}"#
            .to_string(),
            rest_total_hits_as_int: None,
        })
        .await
        .unwrap();

        assert_eq!(response.status_code, 200);
        let parsed = serde_json::from_str::<Value>(&response.body).unwrap();
        assert_eq!(parsed["hits"]["total"]["value"], 1);
        assert_eq!(
            parsed["hits"]["hits"][0]["_source"]["message"],
            "login failed"
        );
    }
}
