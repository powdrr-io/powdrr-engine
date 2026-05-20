use crate::data_contract::{
    IcebergAccessArtifact, IcebergColumnStats, IcebergFileStats, IcebergPartitionField,
    IcebergPartitionValue, IcebergRowGroupStats, IcebergSortField,
};
use crate::elastic_search_ingest::JSON_MODE;
use crate::util::log_err;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::FileReader as ArrowIpcFileReader;
use datafusion::common::HashMap;
use datafusion::config::ConfigOptions;
use datafusion::datasource::{
    file_format::parquet::ParquetFormat,
    listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
};
use datafusion::execution::options::JsonReadOptions;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::prelude::SessionConfig;
use datafusion::{
    arrow,
    arrow::array::RecordBatch,
    error::DataFusionError,
    prelude::{DataFrame, SessionContext},
};
use futures::stream::{self, StreamExt};
use futures_util::TryStreamExt;
use iceberg::Catalog;
use iceberg::arrow::ArrowFileReader;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{DataContentType, DataFile, Literal, ManifestContentType, PrimitiveType, Type};
use iceberg::table::Table;
use iceberg::transaction::ApplyTransactionAction;
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use idgenerator::IdInstance;
#[cfg(target_os = "linux")]
use liquid_cache_datafusion::cache::LiquidCacheParquetRef;
#[cfg(target_os = "linux")]
use liquid_cache_parquet::LiquidCacheLocalBuilder;
#[cfg(target_os = "linux")]
use liquid_cache_parquet::storage::cache::squeeze_policies::Evict;
#[cfg(target_os = "linux")]
use liquid_cache_parquet::storage::cache_policies::LiquidPolicy;
use lru_mem::{HeapSize, LruCache, TryInsertError};
use object_store::client::SpawnedReqwestConnector;
use object_store::{
    ObjectStoreExt, PutPayload,
    aws::{AmazonS3, AmazonS3Builder},
};
use parquet_55::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet_55::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use parquet_55::file::statistics::Statistics;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Cursor;
use std::string::ToString;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::runtime::Handle;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinSet;
use url::Url;

const DEFAULT_S3_ENDPOINT_VALUE: &str = "http://localhost:9000";
const DEFAULT_ICEBERG_ENDPOINT_VALUE: &str = "http://localhost:8181";
const S3_ACCESS_KEY_ID_VALUE: &str = "admin";
const S3_SECRET_ACCESS_KEY_VALUE: &str = "password";
const S3_REGION_VALUE: &str = "us-east-1";
const PARQUET_ROW_GROUP_STATS_CACHE_MAX_ENTRIES: usize = 2048;
const ICEBERG_TABLE_METADATA_CACHE_MAX_ENTRIES: usize = 256;
const ICEBERG_ROW_GROUP_STATS_LOAD_PARALLELISM_MAX: usize = 16;
const ACCESS_ARTIFACT_KIND_BLOOM_FILTER: &str = "bloom-filter";
const ACCESS_ARTIFACT_KIND_FILE_STATS: &str = "file-stats";
const ACCESS_ARTIFACT_KIND_PAGE_INDEX: &str = "page-index";
const ACCESS_ARTIFACT_KIND_PARTITION_SPEC: &str = "partition-spec";
const ACCESS_ARTIFACT_KIND_ROW_GROUP_STATS: &str = "row-group-stats";
const ACCESS_ARTIFACT_KIND_SORT_ORDER: &str = "sort-order";
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const SERVING_LIQUID_CACHE_DIR_ENV_VAR: &str = "POWDRR_SERVING_CACHE_DIR";
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const SERVING_LIQUID_CACHE_ROOT_DIR_NAME: &str = "powdrr-engine";
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const SERVING_LIQUID_CACHE_NAMESPACE_DIR_NAME: &str = "serving-liquid-cache";

#[derive(Default)]
struct ParquetRowGroupStatsCache {
    entries: HashMap<String, Vec<IcebergRowGroupStats>>,
    access_order: VecDeque<String>,
    max_entries: usize,
}

impl ParquetRowGroupStatsCache {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            access_order: VecDeque::new(),
            max_entries,
        }
    }

    fn get(&mut self, file_path: &str) -> Option<Vec<IcebergRowGroupStats>> {
        let entry = self.entries.get(file_path).cloned()?;
        self.touch(file_path);
        Some(entry)
    }

    fn cached_row_group_count(&self, file_path: &str) -> Option<usize> {
        self.entries
            .get(file_path)
            .map(|row_groups| row_groups.len())
    }

    fn insert(&mut self, file_path: &str, row_groups: Vec<IcebergRowGroupStats>) {
        self.entries.insert(file_path.to_string(), row_groups);
        self.touch(file_path);
        self.evict_if_needed();
    }

    fn remove(&mut self, file_path: &str) {
        self.entries.remove(file_path);
        self.access_order.retain(|existing| existing != file_path);
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.access_order.clear();
    }

    fn touch(&mut self, file_path: &str) {
        self.access_order.retain(|existing| existing != file_path);
        self.access_order.push_back(file_path.to_string());
    }

    fn evict_if_needed(&mut self) {
        while self.entries.len() > self.max_entries {
            let Some(oldest) = self.access_order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }
}

static PARQUET_ROW_GROUP_STATS_CACHE: LazyLock<Mutex<ParquetRowGroupStatsCache>> =
    LazyLock::new(|| {
        Mutex::new(ParquetRowGroupStatsCache::new(
            PARQUET_ROW_GROUP_STATS_CACHE_MAX_ENTRIES,
        ))
    });

#[derive(Clone)]
struct IcebergTableMetadataCacheEntry {
    metadata: IcebergLibMetadata,
}

#[derive(Default)]
struct IcebergTableMetadataCache {
    entries: HashMap<String, IcebergTableMetadataCacheEntry>,
    access_order: VecDeque<String>,
    max_entries: usize,
}

impl IcebergTableMetadataCache {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            access_order: VecDeque::new(),
            max_entries,
        }
    }

    fn get(&mut self, table_key: &str, snapshot_id: i64) -> Option<IcebergLibMetadata> {
        let entry = self.entries.get(table_key)?;
        if entry.metadata.snapshot_id != snapshot_id {
            return None;
        }

        let metadata = entry.metadata.clone();
        self.touch(table_key);
        Some(metadata)
    }

    fn contains(&self, table_key: &str, snapshot_id: i64) -> bool {
        self.entries
            .get(table_key)
            .map(|entry| entry.metadata.snapshot_id == snapshot_id)
            .unwrap_or(false)
    }

    fn insert(&mut self, table_key: &str, metadata: IcebergLibMetadata) {
        self.entries.insert(
            table_key.to_string(),
            IcebergTableMetadataCacheEntry { metadata },
        );
        self.touch(table_key);
        self.evict_if_needed();
    }

    fn remove(&mut self, table_key: &str) {
        self.entries.remove(table_key);
        self.access_order.retain(|existing| existing != table_key);
    }

    fn remove_namespace(&mut self, namespace: &str) {
        let namespace_prefix = format!("{}/", namespace);
        let table_keys = self
            .entries
            .keys()
            .filter(|table_key| table_key.starts_with(&namespace_prefix))
            .cloned()
            .collect::<Vec<_>>();
        for table_key in table_keys {
            self.remove(&table_key);
        }
    }

    fn invalidate_file(&mut self, file_path: &str) {
        let table_keys = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry
                    .metadata
                    .files
                    .iter()
                    .any(|existing| existing == file_path)
            })
            .map(|(table_key, _)| table_key.clone())
            .collect::<Vec<_>>();
        for table_key in table_keys {
            self.remove(&table_key);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.access_order.clear();
    }

    fn touch(&mut self, table_key: &str) {
        self.access_order.retain(|existing| existing != table_key);
        self.access_order.push_back(table_key.to_string());
    }

    fn evict_if_needed(&mut self) {
        while self.entries.len() > self.max_entries {
            let Some(oldest) = self.access_order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MetadataCacheCoverage {
    pub files_cached: usize,
    pub row_groups_cached: usize,
}

static ICEBERG_TABLE_METADATA_CACHE: LazyLock<Mutex<IcebergTableMetadataCache>> =
    LazyLock::new(|| {
        Mutex::new(IcebergTableMetadataCache::new(
            ICEBERG_TABLE_METADATA_CACHE_MAX_ENTRIES,
        ))
    });

fn get_cached_parquet_row_group_stats(file_path: &str) -> Option<Vec<IcebergRowGroupStats>> {
    PARQUET_ROW_GROUP_STATS_CACHE.lock().unwrap().get(file_path)
}

fn cache_parquet_row_group_stats(file_path: &str, row_groups: &[IcebergRowGroupStats]) {
    PARQUET_ROW_GROUP_STATS_CACHE
        .lock()
        .unwrap()
        .insert(file_path, row_groups.to_vec());
}

pub(crate) fn cached_parquet_row_group_stats_coverage(
    file_paths: &[String],
) -> MetadataCacheCoverage {
    let cache = PARQUET_ROW_GROUP_STATS_CACHE.lock().unwrap();
    let mut coverage = MetadataCacheCoverage::default();
    for file_path in file_paths {
        if let Some(row_group_count) = cache.cached_row_group_count(file_path) {
            coverage.files_cached += 1;
            coverage.row_groups_cached += row_group_count;
        }
    }
    coverage
}

fn invalidate_parquet_row_group_stats(file_path: &str) {
    PARQUET_ROW_GROUP_STATS_CACHE
        .lock()
        .unwrap()
        .remove(file_path);
}

fn clear_parquet_row_group_stats_cache() {
    PARQUET_ROW_GROUP_STATS_CACHE.lock().unwrap().clear();
}

fn get_cached_iceberg_table_metadata(
    namespace: &str,
    name: &str,
    snapshot_id: i64,
) -> Option<IcebergLibMetadata> {
    ICEBERG_TABLE_METADATA_CACHE
        .lock()
        .unwrap()
        .get(&iceberg_table_key(namespace, name), snapshot_id)
}

fn cache_iceberg_table_metadata(namespace: &str, name: &str, metadata: &IcebergLibMetadata) {
    let mut cached = metadata.clone();
    cached.compactions.clear();
    ICEBERG_TABLE_METADATA_CACHE
        .lock()
        .unwrap()
        .insert(&iceberg_table_key(namespace, name), cached);
}

pub(crate) fn iceberg_table_metadata_cache_contains(
    namespace: &str,
    name: &str,
    snapshot_id: i64,
) -> bool {
    ICEBERG_TABLE_METADATA_CACHE
        .lock()
        .unwrap()
        .contains(&iceberg_table_key(namespace, name), snapshot_id)
}

fn invalidate_iceberg_table_metadata(namespace: &str, name: &str) {
    ICEBERG_TABLE_METADATA_CACHE
        .lock()
        .unwrap()
        .remove(&iceberg_table_key(namespace, name));
}

fn invalidate_iceberg_namespace_table_metadata(namespace: &str) {
    ICEBERG_TABLE_METADATA_CACHE
        .lock()
        .unwrap()
        .remove_namespace(namespace);
}

fn invalidate_iceberg_table_metadata_for_file(file_path: &str) {
    ICEBERG_TABLE_METADATA_CACHE
        .lock()
        .unwrap()
        .invalidate_file(file_path);
}

fn clear_iceberg_table_metadata_cache() {
    ICEBERG_TABLE_METADATA_CACHE.lock().unwrap().clear();
}

#[cfg(test)]
pub(crate) fn prime_parquet_row_group_stats_cache_for_test(
    file_path: &str,
    row_groups: &[IcebergRowGroupStats],
) {
    cache_parquet_row_group_stats(file_path, row_groups);
}

#[cfg(test)]
pub(crate) fn reset_serving_metadata_caches_for_test() {
    clear_parquet_row_group_stats_cache();
    clear_iceberg_table_metadata_cache();
    clear_iceberg_table_row_group_stats_tracker();
    clear_serving_bulk_cache_warmup();
    clear_serving_cache_manager_operation();
}

pub(crate) fn evict_serving_metadata_for_files(file_paths: &[String]) {
    for file_path in file_paths {
        remove_file_from_iceberg_table_row_group_stats(file_path);
        invalidate_iceberg_table_metadata_for_file(file_path);
    }
}

#[derive(Default)]
struct IcebergTableRowGroupStatsTracker {
    files_by_table: HashMap<String, HashSet<String>>,
}

impl IcebergTableRowGroupStatsTracker {
    fn replace_files(&mut self, table_key: &str, current_files: HashSet<String>) -> Vec<String> {
        let previous_files = self
            .files_by_table
            .insert(table_key.to_string(), current_files.clone())
            .unwrap_or_default();
        previous_files
            .into_iter()
            .filter(|file_path| !current_files.contains(file_path))
            .collect()
    }

    fn remove_table(&mut self, table_key: &str) -> Vec<String> {
        self.files_by_table
            .remove(table_key)
            .unwrap_or_default()
            .into_iter()
            .collect()
    }

    fn remove_namespace(&mut self, namespace: &str) -> Vec<String> {
        let namespace_prefix = format!("{}/", namespace);
        let table_keys = self
            .files_by_table
            .keys()
            .filter(|table_key| table_key.starts_with(&namespace_prefix))
            .cloned()
            .collect::<Vec<_>>();
        let mut removed_files = vec![];
        for table_key in table_keys {
            removed_files.extend(self.remove_table(&table_key));
        }
        removed_files
    }

    fn remove_file(&mut self, file_path: &str) {
        self.files_by_table.retain(|_, files| {
            files.remove(file_path);
            !files.is_empty()
        });
    }

    fn clear(&mut self) {
        self.files_by_table.clear();
    }
}

static ICEBERG_TABLE_ROW_GROUP_STATS_TRACKER: LazyLock<Mutex<IcebergTableRowGroupStatsTracker>> =
    LazyLock::new(|| Mutex::new(IcebergTableRowGroupStatsTracker::default()));

fn iceberg_table_key(namespace: &str, name: &str) -> String {
    format!("{}/{}", namespace, name)
}

fn reconcile_iceberg_table_row_group_stats(
    namespace: &str,
    name: &str,
    current_files: &HashSet<String>,
) {
    let removed_files = ICEBERG_TABLE_ROW_GROUP_STATS_TRACKER
        .lock()
        .unwrap()
        .replace_files(&iceberg_table_key(namespace, name), current_files.clone());
    for removed_file in removed_files {
        invalidate_parquet_row_group_stats(&removed_file);
    }
}

fn clear_iceberg_table_row_group_stats(namespace: &str, name: &str) {
    let removed_files = ICEBERG_TABLE_ROW_GROUP_STATS_TRACKER
        .lock()
        .unwrap()
        .remove_table(&iceberg_table_key(namespace, name));
    for removed_file in removed_files {
        invalidate_parquet_row_group_stats(&removed_file);
    }
}

fn clear_iceberg_namespace_row_group_stats(namespace: &str) {
    let removed_files = ICEBERG_TABLE_ROW_GROUP_STATS_TRACKER
        .lock()
        .unwrap()
        .remove_namespace(namespace);
    for removed_file in removed_files {
        invalidate_parquet_row_group_stats(&removed_file);
    }
}

fn remove_file_from_iceberg_table_row_group_stats(file_path: &str) {
    ICEBERG_TABLE_ROW_GROUP_STATS_TRACKER
        .lock()
        .unwrap()
        .remove_file(file_path);
    invalidate_parquet_row_group_stats(file_path);
}

fn clear_iceberg_table_row_group_stats_tracker() {
    ICEBERG_TABLE_ROW_GROUP_STATS_TRACKER
        .lock()
        .unwrap()
        .clear();
}

fn collect_iceberg_partition_spec(table: &Table) -> Vec<IcebergPartitionField> {
    let schema = table.metadata().current_schema();
    let mut partition_fields = table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .filter_map(|field| {
            let source_field_name = schema.name_by_field_id(field.source_id)?.to_string();
            Some(IcebergPartitionField {
                source_field_id: field.source_id,
                source_field_name,
                field_id: field.field_id,
                field_name: field.name.clone(),
                transform: field.transform.to_string(),
            })
        })
        .collect::<Vec<_>>();
    partition_fields.sort_by(|left, right| left.field_id.cmp(&right.field_id));
    partition_fields
}

fn collect_iceberg_sort_order(table: &Table) -> Vec<IcebergSortField> {
    let schema = table.metadata().current_schema();
    let mut sort_fields = table
        .metadata()
        .default_sort_order()
        .fields
        .iter()
        .filter_map(|field| {
            let source_field_name = schema.name_by_field_id(field.source_id)?.to_string();
            Some(IcebergSortField {
                source_field_id: field.source_id,
                source_field_name,
                transform: field.transform.to_string(),
                descending: matches!(field.direction, iceberg::spec::SortDirection::Descending),
                nulls_first: matches!(field.null_order, iceberg::spec::NullOrder::First),
            })
        })
        .collect::<Vec<_>>();
    sort_fields.sort_by(|left, right| left.source_field_id.cmp(&right.source_field_id));
    sort_fields
}

fn collect_iceberg_partition_values(
    data_file: &DataFile,
    partition_spec: &iceberg::spec::PartitionSpec,
    schema: &iceberg::spec::Schema,
) -> Vec<IcebergPartitionValue> {
    let partition_fields = data_file.partition().fields();
    let mut values = partition_spec
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            let source_field = schema.field_by_id(field.source_id)?;
            let source_field_name = schema.name_by_field_id(field.source_id)?.to_string();
            let partition_type = field
                .transform
                .result_type(source_field.field_type.as_ref())
                .ok()?;
            let value = partition_fields
                .get(index)
                .and_then(|value| value.as_ref())
                .and_then(|value| value.clone().try_into_json(&partition_type).ok());
            Some(IcebergPartitionValue {
                source_field_name,
                field_name: field.name.clone(),
                transform: field.transform.to_string(),
                value,
            })
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.field_name.cmp(&right.field_name));
    values
}

fn collect_iceberg_access_artifacts(
    partition_spec: &[IcebergPartitionField],
    sort_order: &[IcebergSortField],
    file_stats: &[IcebergFileStats],
) -> Vec<IcebergAccessArtifact> {
    let mut artifacts = Vec::new();
    let mut tracked_names = HashSet::new();

    let mut add_artifact = |artifact: IcebergAccessArtifact| {
        if tracked_names.insert(artifact.name.clone()) {
            artifacts.push(artifact);
        }
    };

    let stat_fields = file_stats
        .iter()
        .flat_map(|stats| stats.columns.iter().map(|column| column.field_name.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if !stat_fields.is_empty() {
        add_artifact(IcebergAccessArtifact {
            name: ACCESS_ARTIFACT_KIND_FILE_STATS.to_string(),
            kind: ACCESS_ARTIFACT_KIND_FILE_STATS.to_string(),
            fields: stat_fields.clone(),
            exact: false,
            supports_eq: true,
            supports_range: true,
            supports_order: false,
        });
    }

    let row_group_fields = file_stats
        .iter()
        .flat_map(|stats| {
            stats
                .row_groups
                .iter()
                .flat_map(|row_group| {
                    row_group
                        .columns
                        .iter()
                        .map(|column| column.field_name.clone())
                })
                .collect::<Vec<_>>()
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if !row_group_fields.is_empty() {
        add_artifact(IcebergAccessArtifact {
            name: ACCESS_ARTIFACT_KIND_ROW_GROUP_STATS.to_string(),
            kind: ACCESS_ARTIFACT_KIND_ROW_GROUP_STATS.to_string(),
            fields: row_group_fields.clone(),
            exact: false,
            supports_eq: true,
            supports_range: true,
            supports_order: false,
        });
    }

    if file_stats.iter().any(|stats| {
        stats
            .row_groups
            .iter()
            .any(|row_group| row_group.page_index_present)
    }) {
        add_artifact(IcebergAccessArtifact {
            name: ACCESS_ARTIFACT_KIND_PAGE_INDEX.to_string(),
            kind: ACCESS_ARTIFACT_KIND_PAGE_INDEX.to_string(),
            fields: row_group_fields.clone(),
            exact: false,
            supports_eq: true,
            supports_range: true,
            supports_order: false,
        });
    }

    if file_stats.iter().any(|stats| {
        stats
            .row_groups
            .iter()
            .any(|row_group| row_group.bloom_filter_present)
    }) {
        add_artifact(IcebergAccessArtifact {
            name: ACCESS_ARTIFACT_KIND_BLOOM_FILTER.to_string(),
            kind: ACCESS_ARTIFACT_KIND_BLOOM_FILTER.to_string(),
            fields: row_group_fields.clone(),
            exact: false,
            supports_eq: true,
            supports_range: false,
            supports_order: false,
        });
    }

    for field in partition_spec {
        add_artifact(IcebergAccessArtifact {
            name: format!(
                "{}:{}",
                ACCESS_ARTIFACT_KIND_PARTITION_SPEC, field.field_name
            ),
            kind: ACCESS_ARTIFACT_KIND_PARTITION_SPEC.to_string(),
            fields: vec![field.source_field_name.clone()],
            exact: field.transform == "identity",
            supports_eq: true,
            supports_range: field.transform == "identity",
            supports_order: false,
        });
    }

    for field in sort_order {
        add_artifact(IcebergAccessArtifact {
            name: format!(
                "{}:{}",
                ACCESS_ARTIFACT_KIND_SORT_ORDER, field.source_field_name
            ),
            kind: ACCESS_ARTIFACT_KIND_SORT_ORDER.to_string(),
            fields: vec![field.source_field_name.clone()],
            exact: false,
            supports_eq: false,
            supports_range: false,
            supports_order: true,
        });
    }

    artifacts.sort_by(|left, right| left.name.cmp(&right.name));
    artifacts
}

#[derive(Clone)]
struct PendingIcebergFileStats {
    file_path: String,
    record_count: Option<u64>,
    columns: Vec<IcebergColumnStats>,
    partition_values: Vec<IcebergPartitionValue>,
}

fn iceberg_row_group_stats_load_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| {
            parallelism
                .get()
                .clamp(4, ICEBERG_ROW_GROUP_STATS_LOAD_PARALLELISM_MAX)
        })
        .unwrap_or(8)
}

/// This code is lifted from the 'threadpool' example in the Datafusion repo.
/// It is slightly modified to use the main Tokio runtime for CPU bound tasks
/// and shift the IO bound tasks to a separate thread.

/// Creates a Tokio [`Runtime`] for use with IO bound tasks
///
/// Tokio forbids dropping `Runtime`s in async contexts, so creating a separate
/// `Runtime` correctly is somewhat tricky. This structure manages the creation
/// and shutdown of a separate thread.
///
/// # Notes
/// On drop, the thread will wait for all remaining tasks to complete.
///
/// Depending on your application, more sophisticated shutdown logic may be
/// required, such as ensuring that no new tasks are added to the runtime.
///
/// # Credits
/// This code is derived from code originally written for [InfluxDB 3.0]
///
/// [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
///
struct CPURuntime {
    /// Handle is the tokio structure for interacting with a Runtime.
    handle: Handle,
    /// Signal to start shutting down
    notify_shutdown: Arc<Notify>,
    /// When thread is active, is Some
    thread_join_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for CPURuntime {
    fn drop(&mut self) {
        // Notify the thread to shutdown.
        self.notify_shutdown.notify_one();
        // In a production system you also need to ensure your code stops adding
        // new tasks to the underlying runtime after this point to allow the
        // thread to complete its work and exit cleanly.
        if let Some(thread_join_handle) = self.thread_join_handle.take() {
            // If the thread is still running, we wait for it to finish
            tracing::info!("Shutting down IO runtime thread...");
            if let Err(e) = thread_join_handle.join() {
                tracing::info!("Error joining IO runtime thread: {e:?}",);
            } else {
                tracing::info!("IO runtime thread shutdown successfully.");
            }
        }
    }
}

impl CPURuntime {
    /// Create a new Tokio Runtime for CPU bound tasks
    pub fn try_new() -> Result<Self, std::io::Error> {
        let cpu_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .enable_time()
            .build()?;
        let handle = cpu_runtime.handle().clone();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        // The cpu_runtime runs and is dropped on a separate thread
        let thread_join_handle = std::thread::spawn(move || {
            cpu_runtime.block_on(async move {
                notify_shutdown_captured.notified().await;
            });
            // Note: io_runtime is dropped here, which will wait for all tasks
            // to complete
        });

        Ok(Self {
            handle,
            notify_shutdown,
            thread_join_handle: Some(thread_join_handle),
        })
    }

    /// Return a handle suitable for spawning CPU bound tasks
    ///
    /// # Notes
    ///
    /// If a task spawned on this handle attempts to do IO, it will error with a
    /// message such as:
    ///
    /// ```text
    ///A Tokio 1.x context was found, but IO is disabled.
    /// ```
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

static CPU_RUNTIME: std::sync::LazyLock<CPURuntime> =
    std::sync::LazyLock::new(|| CPURuntime::try_new().unwrap());

fn serving_session_config() -> SessionConfig {
    let options = ConfigOptions::default();
    let mut config = SessionConfig::from(options)
        .with_parquet_pruning(true)
        .with_parquet_bloom_filter_pruning(true)
        .with_parquet_page_index_pruning(true);
    config.options_mut().execution.parquet.pushdown_filters = true;
    config
}

fn create_store(address: &String) -> Arc<AmazonS3> {
    let io_runtime = Handle::current();
    let s3_file_system: object_store::aws::AmazonS3 = AmazonS3Builder::new()
        .with_access_key_id(S3_ACCESS_KEY_ID_VALUE)
        .with_secret_access_key(S3_SECRET_ACCESS_KEY_VALUE)
        .with_region(S3_REGION_VALUE)
        .with_endpoint(address)
        .with_bucket_name("warehouse")
        .with_allow_http(true)
        .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
        .build()
        .unwrap();

    Arc::new(s3_file_system)
}

const S3_BASE_PATH: &str = "s3://warehouse";

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Debug, PartialEq, Eq)]
struct ServingLiquidCacheLocation {
    root_dir: PathBuf,
    namespace: String,
    cache_dir: PathBuf,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServingBulkCacheWarmupStats {
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub targeted: bool,
    #[serde(default)]
    pub matched_patterns: Vec<String>,
    #[serde(default)]
    pub shaped_queries: usize,
    #[serde(default)]
    pub files_considered: usize,
    #[serde(default)]
    pub files_selected: usize,
    #[serde(default)]
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServingCacheManagerOperationStats {
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub warmed_files: usize,
    #[serde(default)]
    pub evicted_files: usize,
    #[serde(default)]
    pub targeted_ranges: usize,
    #[serde(default)]
    pub matched_patterns: Vec<String>,
    #[serde(default)]
    pub matched_artifacts: Vec<String>,
    #[serde(default)]
    pub metadata_refreshed: bool,
    #[serde(default)]
    pub bulk_cache_flushed: bool,
    #[serde(default)]
    pub bulk_cache_reset: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServingBulkCacheStats {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub persistent: bool,
    #[serde(default)]
    pub memory_usage_bytes: u64,
    #[serde(default)]
    pub disk_usage_bytes: u64,
    #[serde(default)]
    pub last_manager_operation: Option<ServingCacheManagerOperationStats>,
    #[serde(default)]
    pub last_warmup: Option<ServingBulkCacheWarmupStats>,
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct ServingLiquidCacheRuntime {
    location: ServingLiquidCacheLocation,
    cache: LiquidCacheParquetRef,
}

#[cfg(target_os = "linux")]
static SERVING_LIQUID_CACHE_RUNTIME: LazyLock<Mutex<Option<ServingLiquidCacheRuntime>>> =
    LazyLock::new(|| Mutex::new(None));

static LAST_SERVING_BULK_CACHE_WARMUP: LazyLock<Mutex<Option<ServingBulkCacheWarmupStats>>> =
    LazyLock::new(|| Mutex::new(None));
static LAST_SERVING_CACHE_MANAGER_OPERATION: LazyLock<
    Mutex<Option<ServingCacheManagerOperationStats>>,
> = LazyLock::new(|| Mutex::new(None));

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn resolve_serving_liquid_cache_location(
    explicit_root: Option<PathBuf>,
    xdg_cache_home: Option<PathBuf>,
    home_dir: Option<PathBuf>,
    temp_dir: PathBuf,
    bucket_name: &str,
    s3_endpoint: &str,
) -> ServingLiquidCacheLocation {
    let root_dir = explicit_root.unwrap_or_else(|| {
        xdg_cache_home
            .map(|path| path.join(SERVING_LIQUID_CACHE_ROOT_DIR_NAME))
            .or_else(|| {
                home_dir.map(|path| path.join(".cache").join(SERVING_LIQUID_CACHE_ROOT_DIR_NAME))
            })
            .unwrap_or_else(|| temp_dir.join(SERVING_LIQUID_CACHE_ROOT_DIR_NAME))
            .join(SERVING_LIQUID_CACHE_NAMESPACE_DIR_NAME)
    });
    let scope = format!("{bucket_name}@{s3_endpoint}");
    let namespace = format!(
        "{}-{:016x}",
        sanitize_cache_namespace_component(bucket_name),
        stable_cache_namespace_hash(&scope)
    );
    let cache_dir = root_dir.join(&namespace);

    ServingLiquidCacheLocation {
        root_dir,
        namespace,
        cache_dir,
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn current_serving_liquid_cache_location(
    bucket_name: &str,
    s3_endpoint: &str,
) -> ServingLiquidCacheLocation {
    resolve_serving_liquid_cache_location(
        std::env::var_os(SERVING_LIQUID_CACHE_DIR_ENV_VAR).map(PathBuf::from),
        std::env::var_os("XDG_CACHE_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
        std::env::temp_dir(),
        bucket_name,
        s3_endpoint,
    )
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn stable_cache_namespace_hash(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn sanitize_cache_namespace_component(value: &str) -> String {
    let mut sanitized = String::new();
    let mut last_was_separator = false;

    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            sanitized.push(character.to_ascii_lowercase());
            last_was_separator = false;
            continue;
        }

        if !last_was_separator && !sanitized.is_empty() {
            sanitized.push('-');
            last_was_separator = true;
        }
    }

    let trimmed = sanitized.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "cache".to_string()
    } else {
        trimmed
    }
}

#[cfg(target_os = "linux")]
fn set_serving_liquid_cache_runtime(runtime: ServingLiquidCacheRuntime) {
    SERVING_LIQUID_CACHE_RUNTIME
        .lock()
        .unwrap()
        .replace(runtime);
}

#[cfg(target_os = "linux")]
fn current_serving_liquid_cache_runtime() -> Option<ServingLiquidCacheRuntime> {
    SERVING_LIQUID_CACHE_RUNTIME.lock().unwrap().clone()
}

fn current_serving_bulk_cache_warmup() -> Option<ServingBulkCacheWarmupStats> {
    LAST_SERVING_BULK_CACHE_WARMUP.lock().unwrap().clone()
}

fn current_serving_cache_manager_operation() -> Option<ServingCacheManagerOperationStats> {
    LAST_SERVING_CACHE_MANAGER_OPERATION.lock().unwrap().clone()
}

fn clear_serving_bulk_cache_warmup() {
    LAST_SERVING_BULK_CACHE_WARMUP.lock().unwrap().take();
}

fn clear_serving_cache_manager_operation() {
    LAST_SERVING_CACHE_MANAGER_OPERATION.lock().unwrap().take();
}

pub(crate) fn record_serving_bulk_cache_warmup(stats: ServingBulkCacheWarmupStats) {
    LAST_SERVING_BULK_CACHE_WARMUP
        .lock()
        .unwrap()
        .replace(stats);
}

pub(crate) fn record_serving_cache_manager_operation(stats: ServingCacheManagerOperationStats) {
    LAST_SERVING_CACHE_MANAGER_OPERATION
        .lock()
        .unwrap()
        .replace(stats);
}

pub(crate) fn serving_bulk_cache_stats() -> ServingBulkCacheStats {
    let last_warmup = current_serving_bulk_cache_warmup();
    let last_manager_operation = current_serving_cache_manager_operation();
    #[cfg(target_os = "linux")]
    {
        if let Some(runtime) = current_serving_liquid_cache_runtime() {
            return ServingBulkCacheStats {
                enabled: true,
                persistent: true,
                memory_usage_bytes: runtime.cache.memory_usage_bytes() as u64,
                disk_usage_bytes: runtime.cache.disk_usage_bytes() as u64,
                last_manager_operation,
                last_warmup,
            };
        }
    }

    ServingBulkCacheStats {
        last_manager_operation,
        last_warmup,
        ..ServingBulkCacheStats::default()
    }
}

pub(crate) async fn flush_serving_bulk_cache() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        let Some(runtime) = current_serving_liquid_cache_runtime() else {
            return Ok(());
        };

        let before_memory = runtime.cache.memory_usage_bytes();
        let before_disk = runtime.cache.disk_usage_bytes();
        runtime.cache.flush_data().await.map_err(|_| {
            format!(
                "Failed to flush serving LiquidCache data to {}",
                runtime.location.cache_dir.display()
            )
        })?;
        tracing::info!(
            cache_dir = %runtime.location.cache_dir.display(),
            cache_namespace = runtime.location.namespace,
            memory_before_bytes = before_memory,
            disk_before_bytes = before_disk,
            memory_after_bytes = runtime.cache.memory_usage_bytes(),
            disk_after_bytes = runtime.cache.disk_usage_bytes(),
            "Flushed serving LiquidCache data to disk"
        );
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn create_session(file_store: Arc<AmazonS3>, s3_endpoint: &str) -> SessionContext {
    let config = serving_session_config();
    let cache_location = current_serving_liquid_cache_location("warehouse", s3_endpoint);
    std::fs::create_dir_all(&cache_location.cache_dir).unwrap_or_else(|error| {
        panic!(
            "Failed to create serving LiquidCache directory {}: {}",
            cache_location.cache_dir.display(),
            error
        )
    });
    tracing::info!(
        cache_dir = %cache_location.cache_dir.display(),
        cache_namespace = cache_location.namespace,
        "Using persistent serving LiquidCache directory"
    );

    let build_cache = async {
        LiquidCacheLocalBuilder::new()
            .with_max_memory_bytes(10 * 1024 * 1024 * 1024) // 10GB
            .with_cache_dir(cache_location.cache_dir.clone())
            .with_cache_policy(Box::new(LiquidPolicy::new()))
            .with_squeeze_policy(Box::new(Evict))
            .build(config)
            .await
    };

    let (ctx, cache) = match tokio::task::block_in_place(|| Handle::current().block_on(build_cache))
    {
        Ok(ctx) => ctx,
        Err(e) => panic!("Failed to create session: {}", e),
    };
    set_serving_liquid_cache_runtime(ServingLiquidCacheRuntime {
        location: cache_location.clone(),
        cache,
    });

    //let ctx = SessionContext::new_with_config(config);

    let s3_url = Url::parse(S3_BASE_PATH).unwrap();

    ctx.register_object_store(&s3_url, file_store.clone());

    ctx
}

#[cfg(not(target_os = "linux"))]
fn create_session(file_store: Arc<AmazonS3>, _s3_endpoint: &str) -> SessionContext {
    let config = serving_session_config();
    let ctx = SessionContext::new_with_config(config);
    let s3_url = Url::parse(S3_BASE_PATH).unwrap();

    ctx.register_object_store(&s3_url, file_store);

    ctx
}

fn get_iceberg_catalog_config(
    rest_catalog_address: &String,
    s3_endpoint: &String,
) -> RestCatalogConfig {
    RestCatalogConfig::builder()
        .uri(rest_catalog_address.clone())
        .props(std::collections::HashMap::from([
            (S3_ENDPOINT.to_string(), s3_endpoint.clone()),
            (
                S3_ACCESS_KEY_ID.to_string(),
                S3_ACCESS_KEY_ID_VALUE.to_string(),
            ),
            (
                S3_SECRET_ACCESS_KEY.to_string(),
                S3_SECRET_ACCESS_KEY_VALUE.to_string(),
            ),
            (S3_REGION.to_string(), S3_REGION_VALUE.to_string()),
        ]))
        .build()
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct IcebergLibMetadata {
    pub snapshot_id: i64,
    pub table_schema: Arc<iceberg::spec::Schema>,
    pub files: Vec<String>,
    pub sizes: Vec<u64>,
    pub schemas: Vec<Arc<iceberg::spec::Schema>>,
    pub compactions: Vec<String>,
    pub partition_spec: Vec<IcebergPartitionField>,
    pub sort_order: Vec<IcebergSortField>,
    pub column_names: Vec<String>,
    // per file, per column lower and upper bounds
    // TODO: this needs to be generalized to support bloom filters
    pub column_stats: Vec<(String, String)>,
    pub access_artifacts: Vec<IcebergAccessArtifact>,
    pub file_stats: Vec<IcebergFileStats>,
}

#[allow(dead_code)]
enum CacheTrackerActorMessage {
    SetS3Config {
        respond_to: oneshot::Sender<()>,
        iceberg_rest_endpont: String,
        access_key_id: String,
        secret_access_key: String,
        region: String,
        endpoint: String,
        bucket_name: String,
    },
    Reserve {
        respond_to: oneshot::Sender<()>,
        top_level_name: String,
        related_names: Vec<String>,
        total_size: u64,
    },
    Release {
        respond_to: oneshot::Sender<()>,
        top_level_name: String,
    },
    LoadTable {
        respond_to: oneshot::Sender<Result<(), DataFusionError>>,
        table_name: String,
        records: Vec<RecordBatch>,
    },
    CreateTable {
        respond_to: oneshot::Sender<Result<(), DataFusionError>>,
        table_name: String,
        file_path: String,
        parquet: bool,
        schema: Option<Schema>,
    },
    CreateMultiTable {
        respond_to: oneshot::Sender<Result<(), DataFusionError>>,
        table_name: String,
        file_paths: Vec<String>,
        schema: Schema,
    },
    CreateTableAs {
        respond_to: oneshot::Sender<Result<(), DataFusionError>>,
        table_name: String,
        sql: String,
    },
    TableDropped {
        respond_to: oneshot::Sender<()>,
        table_name: String,
    },
    GetTables {
        respond_to: oneshot::Sender<Vec<String>>,
    },
    ExecuteSql {
        respond_to: oneshot::Sender<Result<DataFrame, DataFusionError>>,
        sql: String,
    },
    FileExists {
        respond_to: oneshot::Sender<bool>,
        file_path: String,
    },
    FileDelete {
        respond_to: oneshot::Sender<()>,
        file_paths: Vec<String>,
    },
    FilePut {
        respond_to: oneshot::Sender<Result<(), DataFusionError>>,
        file_path: String,
        payload: Vec<u8>,
    },
    FileGet {
        respond_to: oneshot::Sender<Result<Vec<u8>, DataFusionError>>,
        file_path: String,
    },
    DropIcebergTable {
        respond_to: oneshot::Sender<Result<(), iceberg::Error>>,
        namespace: String,
        table_name: String,
    },
    DropAllIcebergTables {
        respond_to: oneshot::Sender<Result<(), iceberg::Error>>,
        namespace: String,
    },
    EnsureIcebergTable {
        respond_to: oneshot::Sender<Result<Table, iceberg::Error>>,
        namespace: String,
        table_name: String,
        schema: iceberg::spec::Schema,
    },
    LoadIcebergTableMetadata {
        respond_to: oneshot::Sender<Result<IcebergLibMetadata, iceberg::Error>>,
        namespace: String,
        table_name: String,
        last_snapshot_id: i64,
    },
    CommitIcebergTransaction {
        respond_to: oneshot::Sender<Result<(), iceberg::Error>>,
        namespace: String,
        table_name: String,
        compaction_id: String,
        data_files: Vec<DataFile>,
    },
}

struct HeapSizeTracker {
    size: u64,
}

impl HeapSize for HeapSizeTracker {
    fn heap_size(&self) -> usize {
        self.size as usize
    }
}

struct CacheTrackerActor {
    receiver: mpsc::Receiver<CacheTrackerActorMessage>,
    lru_cache: LruCache<String, HeapSizeTracker>,
    related: HashMap<String, Vec<String>>,
    reservations: HashMap<String, u64>,
    top_level_to_delete: Vec<String>,
    existing_tables: Vec<String>,
    s3_file_store: Arc<AmazonS3>,
    data_fusion_context: SessionContext,
    rest_catalog: Arc<RestCatalog>,
}

impl CacheTrackerActor {
    pub fn new(receiver: mpsc::Receiver<CacheTrackerActorMessage>) -> Self {
        let file_store = create_store(&DEFAULT_S3_ENDPOINT_VALUE.to_string());
        Self {
            receiver,
            lru_cache: LruCache::new(2 * 1024 * 1024 * 1024),
            related: HashMap::new(),
            reservations: HashMap::new(),
            top_level_to_delete: vec![],
            existing_tables: vec![],
            s3_file_store: file_store.clone(),
            data_fusion_context: create_session(file_store, DEFAULT_S3_ENDPOINT_VALUE),
            rest_catalog: Arc::new(RestCatalog::new(get_iceberg_catalog_config(
                &DEFAULT_ICEBERG_ENDPOINT_VALUE.to_string(),
                &DEFAULT_S3_ENDPOINT_VALUE.to_string(),
            ))),
        }
    }

    fn increment_reservation(&mut self, name: &String) -> () {
        match self.reservations.get_mut(name) {
            Some(r) => {
                *r += 1;
            }
            None => {
                self.reservations.insert(name.clone(), 1);
            }
        }
    }

    fn decrement_reservation(&mut self, name: &String) -> bool {
        match self.reservations.get_mut(name) {
            Some(r) => {
                *r -= 1;
                *r == 0
            }
            None => panic!(
                "Tried to decrement reservation for {} but it doesn't exist",
                name
            ),
        }
    }

    async fn drop(&mut self, name: &String) -> () {
        let _ = self
            .data_fusion_context
            .sql(format!("DROP TABLE IF EXISTS {};", name).as_str())
            .await;
        // assert!(self.existing_tables.contains(&name));
        self.existing_tables.retain(|n| n != name);
        self.reservations.remove(name);
        assert!(self.existing_tables.len() >= self.reservations.len());
    }

    #[allow(unused_assignments)]
    async fn handle_message(&mut self, msg: CacheTrackerActorMessage) {
        match msg {
            CacheTrackerActorMessage::SetS3Config {
                respond_to,
                iceberg_rest_endpont,
                access_key_id,
                secret_access_key,
                region,
                endpoint,
                bucket_name,
            } => {
                // Bogus assert to make sure the compiler doesn't give me warning about unused assignments.
                assert!(
                    format!(
                        "{}{}{}{} ",
                        access_key_id, secret_access_key, region, bucket_name
                    )
                    .len()
                        > 0
                );
                if let Err(error) = flush_serving_bulk_cache().await {
                    tracing::warn!(
                        error = %error,
                        "Failed to flush serving LiquidCache before rebuilding session"
                    );
                }
                self.s3_file_store = create_store(&endpoint);
                self.data_fusion_context = create_session(self.s3_file_store.clone(), &endpoint);
                self.rest_catalog = Arc::new(RestCatalog::new(get_iceberg_catalog_config(
                    &iceberg_rest_endpont,
                    &endpoint,
                )));
                // Setting a new context effectively drops all tables.
                self.existing_tables.clear();
                self.reservations.clear();
                self.related.clear();
                self.lru_cache.clear();
                self.top_level_to_delete.clear();
                clear_parquet_row_group_stats_cache();
                clear_iceberg_table_metadata_cache();
                clear_iceberg_table_row_group_stats_tracker();
                clear_serving_bulk_cache_warmup();
                clear_serving_cache_manager_operation();
                let _ = respond_to.send(());
            }
            CacheTrackerActorMessage::Reserve {
                respond_to,
                top_level_name,
                related_names,
                total_size,
            } => {
                // Increment the reservation count on the top level.
                self.increment_reservation(&top_level_name);

                // Touch the top level file in the LRU to load it or keep it fresh.
                // This will also update the total size for this top level file in the LRU.
                // That can happen if extension files have been generated since this file was
                // first loaded.

                // TODO: This is an optimistic add impl which is probably totally misguided since
                // under normal operation the LRU is always full. This should be replaced
                // with something that assumes that removes are necessary.
                assert!(total_size > 0);
                loop {
                    let mut local_total_size = total_size;
                    match self.lru_cache.try_insert(
                        top_level_name.clone(),
                        HeapSizeTracker {
                            size: local_total_size,
                        },
                    ) {
                        Err(err) => match err {
                            TryInsertError::EntryTooLarge {
                                key: _,
                                value: _,
                                entry_size: _,
                                max_size: _,
                            } => panic!(
                                "Files with top level {} is too large to fit in the LRU",
                                top_level_name
                            ),
                            TryInsertError::OccupiedEntry { key, value } => {
                                local_total_size = if local_total_size > value.size {
                                    local_total_size
                                } else {
                                    value.size
                                };
                                self.lru_cache.remove(&key);
                            }
                            TryInsertError::WouldEjectLru {
                                key: _,
                                value: _,
                                entry_size: _,
                                free_memory: _,
                            } => match self.lru_cache.remove_lru() {
                                Some((key, value)) => {
                                    assert!(value.size > 0);
                                    self.top_level_to_delete.push(key.clone());
                                }
                                None => panic!("LRU cache is empty"),
                            },
                        },
                        Ok(_) => break,
                    }
                }

                // Ensure the related files are tracked appropriately.
                match self.related.get_mut(&top_level_name) {
                    Some(existing_related_names) => {
                        // Add any new related files to the list.
                        // TODO: This is O(n^2) but we expect the number of related files to be small.
                        // If it becomes a problem, we can optimize this.
                        for related_name in related_names.iter() {
                            if !existing_related_names.contains(related_name) {
                                existing_related_names.push(related_name.clone());
                            }
                        }
                    }
                    None => {
                        self.related
                            .insert(top_level_name.clone(), related_names.clone());
                    }
                };
                let _ = respond_to.send(());
            }
            CacheTrackerActorMessage::Release {
                respond_to,
                top_level_name,
            } => {
                self.decrement_reservation(&top_level_name);

                let mut to_delete = vec![];
                for possible_delete in self.top_level_to_delete.iter_mut() {
                    let should_drop =
                        self.reservations.get_mut(possible_delete).unwrap_or(&mut 0) == &0;
                    if should_drop {
                        to_delete.push(possible_delete.clone());
                    }
                }
                self.top_level_to_delete
                    .retain(|name| !to_delete.contains(name));

                for top_level_name in to_delete {
                    self.drop(&top_level_name).await;
                    let related_names = self
                        .related
                        .get(&top_level_name)
                        .map(|names| names.clone())
                        .unwrap_or_default();
                    for related_name in related_names {
                        self.drop(&related_name).await;
                    }
                    self.related.remove(&top_level_name);
                }
                let _ = respond_to.send(());
            }
            CacheTrackerActorMessage::LoadTable {
                respond_to,
                table_name,
                records,
            } => {
                let _ = respond_to.send(self.load_table(&table_name, &records).await);
            }
            CacheTrackerActorMessage::CreateTable {
                respond_to,
                table_name,
                file_path,
                parquet,
                schema,
            } => {
                let _ = respond_to.send(
                    self.create_table(&table_name, &file_path, parquet, schema)
                        .await,
                );
            }
            CacheTrackerActorMessage::CreateMultiTable {
                respond_to,
                table_name,
                file_paths,
                schema,
            } => {
                let _ = respond_to.send(
                    self.create_multi_table(&table_name, &file_paths, &schema)
                        .await,
                );
            }
            CacheTrackerActorMessage::CreateTableAs {
                respond_to,
                table_name,
                sql,
            } => {
                let _ = respond_to.send(self.create_table_as(&table_name, &sql).await);
            }
            CacheTrackerActorMessage::TableDropped {
                respond_to,
                table_name,
            } => {
                assert!(self.existing_tables.contains(&table_name));
                self.existing_tables.retain(|name| name != &table_name);
                match self
                    .data_fusion_context
                    .sql(format!("DROP TABLE IF EXISTS {};", table_name).as_str())
                    .await
                {
                    Ok(_) => (),
                    Err(e) => panic!("Failed to drop table {}: {}", table_name, e),
                }
                let _ = respond_to.send(());
            }
            CacheTrackerActorMessage::GetTables { respond_to } => {
                let _ = respond_to.send(self.existing_tables.clone());
            }
            CacheTrackerActorMessage::ExecuteSql { respond_to, sql } => {
                let mut result: Result<DataFrame, DataFusionError> = Err(
                    DataFusionError::Execution("Unable to execute SQL".to_string()),
                );
                for try_num in 1..=NUM_TRIES {
                    match private_execute_sql(&self.data_fusion_context, &sql).await {
                        Ok(df) => {
                            result = Ok(df);
                            break;
                        }
                        Err(e) => {
                            if try_num == NUM_TRIES {
                                result = Err(e);
                                break;
                            } else {
                                match e {
                                    // The metadata tracking means that in normal operation we'll never ask for an S3 object
                                    // that we don't have a record of. Therefore most likely if there is an issue
                                    // fetching an object it is some eventually consistency or rate limiting issue.
                                    // We'll do some exponential backoff and hope that the issue resolves itself.
                                    DataFusionError::ParquetError(_) => {
                                        tokio::time::sleep(Duration::from_millis(
                                            3_u64.pow(try_num),
                                        ))
                                        .await;
                                    }
                                    DataFusionError::ObjectStore(_) => {
                                        tokio::time::sleep(Duration::from_millis(
                                            3_u64.pow(try_num),
                                        ))
                                        .await;
                                    }
                                    _ => {
                                        result = Err(e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                respond_to.send(result).expect("Failed to send response");
            }
            CacheTrackerActorMessage::FileExists {
                respond_to,
                file_path,
            } => {
                let retval = if file_path.starts_with("s3://") {
                    let path_only = file_path.replace(S3_BASE_PATH, "");
                    match self
                        .s3_file_store
                        .as_ref()
                        .get(&object_store::path::Path::parse(path_only).unwrap())
                        .await
                    {
                        Ok(_) => true,
                        Err(_) => false,
                    }
                } else {
                    Path::new(&file_path).exists()
                };
                respond_to.send(retval).expect("Failed to send response");
            }
            CacheTrackerActorMessage::FileDelete {
                respond_to,
                file_paths,
            } => {
                for file_path in file_paths {
                    assert!(file_path.starts_with(S3_BASE_PATH));
                    let final_file_path = file_path.replace(S3_BASE_PATH, "");
                    let path = object_store::path::Path::from_url_path(&final_file_path).unwrap();
                    match self.s3_file_store.delete(&path).await {
                        Ok(_) => (),
                        Err(e) => panic!("Failed to delete file {}: {}", file_path, e),
                    }
                    remove_file_from_iceberg_table_row_group_stats(&file_path);
                    invalidate_iceberg_table_metadata_for_file(&file_path);
                }
                respond_to.send(()).expect("Failed to send response");
            }
            CacheTrackerActorMessage::FilePut {
                respond_to,
                file_path,
                payload,
            } => {
                assert!(file_path.starts_with(S3_BASE_PATH));
                let path_str = file_path.replace(S3_BASE_PATH, "");
                let path = match object_store::path::Path::from_url_path(path_str) {
                    Ok(p) => p,
                    Err(e) => {
                        respond_to
                            .send(log_err(DataFusionError::ObjectStore(Box::new(e.into()))))
                            .expect("Failed to send response");
                        return;
                    }
                };
                let payload = PutPayload::from_bytes(payload.to_vec().into());
                let retval = match self.s3_file_store.put(&path, payload).await {
                    Ok(_) => Ok(()),
                    Err(e) => log_err(DataFusionError::ObjectStore(Box::new(e.into()))),
                };
                respond_to.send(retval).expect("Failed to send response");
            }
            CacheTrackerActorMessage::FileGet {
                respond_to,
                file_path,
            } => {
                let retval = if file_path.starts_with("s3://") {
                    let path_str = file_path.replace(S3_BASE_PATH, "");
                    let path = match object_store::path::Path::from_url_path(path_str) {
                        Ok(p) => p,
                        Err(e) => {
                            respond_to
                                .send(log_err(DataFusionError::ObjectStore(Box::new(e.into()))))
                                .expect("Failed to send response");
                            return;
                        }
                    };

                    match self.s3_file_store.get(&path).await {
                        Ok(result) => match result.bytes().await {
                            Ok(bytes) => Ok(bytes.to_vec()),
                            Err(e) => log_err(DataFusionError::ObjectStore(Box::new(e.into()))),
                        },
                        Err(e) => log_err(DataFusionError::ObjectStore(Box::new(e.into()))),
                    }
                } else {
                    let final_file_path = if file_path.starts_with("file://") {
                        file_path.replace("file://", "")
                    } else {
                        file_path
                    };
                    match std::fs::read(&final_file_path) {
                        Ok(bytes) => Ok(bytes),
                        Err(e) => log_err(DataFusionError::IoError(e)),
                    }
                };
                respond_to.send(retval).expect("Failed to send response");
            }
            CacheTrackerActorMessage::DropIcebergTable {
                respond_to,
                namespace,
                table_name,
            } => {
                respond_to
                    .send(
                        drop_iceberg_table_worker(
                            self.rest_catalog.clone(),
                            &namespace,
                            &table_name,
                        )
                        .await,
                    )
                    .expect("Failed to send response");
            }
            CacheTrackerActorMessage::DropAllIcebergTables {
                respond_to,
                namespace,
            } => {
                respond_to
                    .send(
                        drop_all_iceberg_tables_worker(self.rest_catalog.clone(), &namespace).await,
                    )
                    .expect("Failed to send response");
            }
            CacheTrackerActorMessage::EnsureIcebergTable {
                respond_to,
                namespace,
                table_name,
                schema,
            } => {
                respond_to
                    .send(
                        ensure_iceberg_table_worker(
                            self.rest_catalog.clone(),
                            &namespace,
                            &table_name,
                            &schema,
                        )
                        .await,
                    )
                    .expect("Failed to send response");
            }
            CacheTrackerActorMessage::LoadIcebergTableMetadata {
                respond_to,
                namespace,
                table_name,
                last_snapshot_id,
            } => {
                respond_to
                    .send(
                        load_iceberg_table_metadata_worker(
                            self.rest_catalog.clone(),
                            &namespace,
                            &table_name,
                            last_snapshot_id,
                        )
                        .await,
                    )
                    .expect("Failed to send response");
            }
            CacheTrackerActorMessage::CommitIcebergTransaction {
                respond_to,
                namespace,
                table_name,
                compaction_id,
                data_files,
            } => {
                respond_to
                    .send(
                        commit_iceberg_transaction_worker(
                            self.rest_catalog.clone(),
                            &namespace,
                            &table_name,
                            &compaction_id,
                            &data_files,
                        )
                        .await,
                    )
                    .expect("Failed to send response");
            }
        }
    }

    async fn track_table(&mut self, table_name: &String) -> () {
        if !self.existing_tables.contains(&table_name) {
            self.existing_tables.push(table_name.clone());
        }
    }

    async fn load_table(
        &mut self,
        table_name: &String,
        records: &Vec<RecordBatch>,
    ) -> Result<(), DataFusionError> {
        let schema = records.get(0).unwrap().schema();
        let concated = match arrow::compute::concat_batches(&records[0].schema(), records) {
            Ok(batch) => batch,
            Err(e) => {
                return {
                    tracing::error!("Failed to concat_batches: {}", e);
                    log_err(DataFusionError::ArrowError(Box::new(e), None))
                };
            }
        };
        let table = match datafusion::datasource::MemTable::try_new(schema, vec![vec![concated]]) {
            Ok(t) => Arc::new(t),
            Err(e) => {
                return {
                    tracing::error!("Failed to create MemTable: {}", e);
                    log_err(e)
                };
            }
        };
        match self.data_fusion_context.register_table(table_name, table) {
            Ok(_) => {
                self.track_table(&table_name).await;
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to register MemTable: {}", e);
                log_err(e)
            }
        }
    }

    async fn create_table(
        &mut self,
        table_name: &String,
        file_path: &String,
        parquet: bool,
        schema: Option<Schema>,
    ) -> Result<(), DataFusionError> {
        if parquet {
            match load_parquet_file_as_table(&self.data_fusion_context, &file_path, &table_name)
                .await
            {
                Err(e) => return log_err(e),
                Ok(_) => (),
            }
        } else {
            assert!(
                schema.is_some(),
                "You must provide a schema for a JSON file"
            );
            match load_json_file_as_table(
                &self.data_fusion_context,
                file_path,
                &table_name,
                &schema.unwrap(),
            )
            .await
            {
                Err(e) => return log_err(e),
                Ok(_) => (),
            }
        }
        self.track_table(&table_name).await;

        Ok(())
    }

    async fn create_multi_table(
        &mut self,
        table_name: &String,
        file_paths: &Vec<String>,
        schema: &Schema,
    ) -> Result<(), DataFusionError> {
        load_parquet_files_as_table(&self.data_fusion_context, file_paths, table_name, schema)
            .await?;
        self.track_table(table_name).await;
        Ok(())
    }

    async fn create_table_as(
        &mut self,
        table_name: &String,
        sql: &String,
    ) -> Result<(), DataFusionError> {
        match private_execute_sql(
            &self.data_fusion_context,
            &format!("CREATE TABLE {} AS {}", table_name, sql),
        )
        .await
        {
            Ok(_) => {
                self.track_table(&table_name).await;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Clone)]
pub struct LRUCacheHandle {
    sender: mpsc::Sender<CacheTrackerActorMessage>,
}

async fn run_lru_cache_actor_message_pump(mut actor: CacheTrackerActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg).await;
    }
}

impl LRUCacheHandle {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let actor = CacheTrackerActor::new(receiver);
        tokio::spawn(run_lru_cache_actor_message_pump(actor));
        Self { sender }
    }

    async fn set_s3_config(&self, iceberg_rest_endpoint: &String, s3_endpoint: &String) -> () {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::SetS3Config {
            respond_to: send,
            iceberg_rest_endpont: iceberg_rest_endpoint.clone(),
            access_key_id: "dummy".to_string(),
            secret_access_key: "dummy".to_string(),
            region: "dummy".to_string(),
            endpoint: s3_endpoint.clone(),
            bucket_name: "dummy".to_string(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn reserve(&self, top_level_name: &String, size: u64, related_names: Vec<String>) -> () {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::Reserve {
            respond_to: send,
            top_level_name: top_level_name.clone(),
            total_size: size,
            related_names,
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn release(&self, top_level_name: &String) -> () {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::Release {
            respond_to: send,
            top_level_name: top_level_name.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn load_table(
        &self,
        table_name: &String,
        records: &Vec<RecordBatch>,
    ) -> Result<(), DataFusionError> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::LoadTable {
            respond_to: send,
            table_name: table_name.clone(),
            records: records.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn create_table(
        &self,
        table_name: &String,
        file_path: &String,
        parquet: bool,
        schema: Option<Schema>,
    ) -> Result<(), DataFusionError> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::CreateTable {
            respond_to: send,
            table_name: table_name.clone(),
            file_path: file_path.clone(),
            parquet,
            schema,
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn create_multi_table(
        &self,
        table_name: &String,
        file_paths: &Vec<String>,
        schema: &Schema,
    ) -> Result<(), DataFusionError> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::CreateMultiTable {
            respond_to: send,
            table_name: table_name.clone(),
            file_paths: file_paths.clone(),
            schema: schema.clone(),
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    async fn create_table_as(
        &self,
        table_name: &String,
        sql: &String,
    ) -> Result<(), DataFusionError> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::CreateTableAs {
            respond_to: send,
            table_name: table_name.clone(),
            sql: sql.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn table_dropped(&self, table_name: &String) -> () {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::TableDropped {
            respond_to: send,
            table_name: table_name.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn file_exists(&self, file_path: &String) -> bool {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::FileExists {
            respond_to: send,
            file_path: file_path.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn file_delete(&self, file_paths: &Vec<String>) -> () {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::FileDelete {
            respond_to: send,
            file_paths: file_paths.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn file_put(&self, file_path: &String, payload: &Vec<u8>) -> Result<(), DataFusionError> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::FilePut {
            respond_to: send,
            file_path: file_path.clone(),
            payload: payload.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn file_get(&self, file_path: &String) -> Result<Vec<u8>, DataFusionError> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::FileGet {
            respond_to: send,
            file_path: file_path.clone(),
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[allow(dead_code)]
    async fn get_tables(&self) -> Vec<String> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::GetTables { respond_to: send };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn execute_sql(&self, sql: &String) -> Result<DataFrame, DataFusionError> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::ExecuteSql {
            respond_to: send,
            sql: sql.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    #[allow(dead_code)]
    async fn drop_iceberg_table(
        &self,
        namespace: &String,
        table_name: &String,
    ) -> Result<(), iceberg::Error> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::DropIcebergTable {
            respond_to: send,
            namespace: namespace.clone(),
            table_name: table_name.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn drop_all_iceberg_tables(&self, namespace: &String) -> Result<(), iceberg::Error> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::DropAllIcebergTables {
            respond_to: send,
            namespace: namespace.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn ensure_iceberg_table(
        &self,
        namespace: &String,
        table_name: &String,
        iceberg_schema: &iceberg::spec::Schema,
    ) -> Result<Table, iceberg::Error> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::EnsureIcebergTable {
            respond_to: send,
            namespace: namespace.clone(),
            table_name: table_name.clone(),
            schema: iceberg_schema.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn load_iceberg_table_metadata(
        &self,
        namespace: &String,
        table_name: &String,
        last_snapshot_id: i64,
    ) -> Result<IcebergLibMetadata, iceberg::Error> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::LoadIcebergTableMetadata {
            respond_to: send,
            namespace: namespace.clone(),
            table_name: table_name.clone(),
            last_snapshot_id: last_snapshot_id,
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }

    async fn commit_iceberg_transaction(
        &self,
        namespace: &String,
        table_name: &String,
        compaction_id: &String,
        data_files: &Vec<DataFile>,
    ) -> Result<(), iceberg::Error> {
        let (send, recv) = oneshot::channel();
        let msg = CacheTrackerActorMessage::CommitIcebergTransaction {
            respond_to: send,
            namespace: namespace.clone(),
            table_name: table_name.clone(),
            compaction_id: compaction_id.clone(),
            data_files: data_files.clone(),
        };

        let _ = self.sender.send(msg).await;
        // TODO: deal with errors
        recv.await.expect("Actor task has been killed")
    }
}

static LRU_CACHE_HANDLE: LazyLock<LRUCacheHandle> = LazyLock::new(|| LRUCacheHandle::new());

pub(crate) async fn set_s3_endpoint(
    rest_endpoint: &Option<String>,
    s3_endpoint: &Option<String>,
) -> () {
    LRU_CACHE_HANDLE
        .set_s3_config(
            &rest_endpoint
                .clone()
                .unwrap_or(DEFAULT_ICEBERG_ENDPOINT_VALUE.to_string()),
            &s3_endpoint
                .clone()
                .unwrap_or(DEFAULT_S3_ENDPOINT_VALUE.to_string()),
        )
        .await
}

pub(crate) async fn reserve(
    top_level_name: &String,
    total_size: u64,
    related_names: Vec<String>,
) -> () {
    assert!(total_size > 0);
    LRU_CACHE_HANDLE
        .reserve(top_level_name, total_size, related_names)
        .await
}

pub(crate) async fn release(top_level_name: &String) -> () {
    LRU_CACHE_HANDLE.release(top_level_name).await
}

async fn load_parquet_file_as_table(
    data_fusion_context: &SessionContext,
    file_path: &String,
    local_name: &String,
) -> Result<(), DataFusionError> {
    match data_fusion_context.table_exist(local_name) {
        Ok(exists) => match exists {
            true => return Ok(()),
            false => (),
        },
        Err(e) => return log_err(e),
    };
    tracing::info!("Loading PARQUET file {}", file_path);
    if file_path.starts_with("s3:") {
        let file_path_var = file_path;
        let local_name_var = local_name;

        let query_str = format!(
            r#"CREATE EXTERNAL TABLE {local_name_var}
        STORED AS PARQUET
        LOCATION '{file_path_var}';"#
        );
        loop {
            match data_fusion_context.sql(&query_str).await {
                Err(_e) => {
                    let _ = data_fusion_context
                        .sql(format!("DROP TABLE IF EXISTS {local_name_var};").as_str())
                        .await;
                }
                _ => return Ok(()),
            }
        }
    } else {
        load_local_parquet_files_as_table(data_fusion_context, &vec![file_path.clone()], local_name)
    }
}

fn normalize_local_file_path(file_path: &str) -> &str {
    file_path.strip_prefix("file://").unwrap_or(file_path)
}

fn read_local_parquet_batches(
    file_path: &str,
) -> Result<(Arc<arrow::datatypes::Schema>, Vec<RecordBatch>), DataFusionError> {
    let local_file_path = normalize_local_file_path(file_path);
    let file = std::fs::File::open(local_file_path)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error.into()), None))?;
    let schema = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|error| DataFusionError::ArrowError(Box::new(error.into()), None))?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    Ok((schema, batches))
}

fn load_local_parquet_files_as_table(
    data_fusion_context: &SessionContext,
    file_paths: &Vec<String>,
    local_name: &String,
) -> Result<(), DataFusionError> {
    let mut table_schema: Option<Arc<arrow::datatypes::Schema>> = None;
    let mut partitions = Vec::with_capacity(file_paths.len());

    for file_path in file_paths {
        let (schema, batches) = read_local_parquet_batches(file_path)?;
        if let Some(existing_schema) = table_schema.as_ref() {
            if existing_schema.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(format!(
                    "Local parquet files for {} did not share a schema",
                    local_name
                )));
            }
        } else {
            table_schema = Some(schema);
        }
        partitions.push(batches);
    }

    let table = Arc::new(datafusion::datasource::MemTable::try_new(
        table_schema.expect("local parquet load requires at least one file"),
        partitions,
    )?);
    match data_fusion_context.register_table(local_name, table) {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.message().contains("already exists") {
                Ok(())
            } else {
                log_err(e)
            }
        }
    }
}

async fn load_parquet_files_as_table(
    data_fusion_context: &SessionContext,
    file_paths: &Vec<String>,
    local_name: &String,
    schema: &Schema,
) -> Result<(), DataFusionError> {
    match data_fusion_context.table_exist(local_name) {
        Ok(exists) => match exists {
            true => return Ok(()),
            false => (),
        },
        Err(e) => return log_err(e),
    };

    if file_paths.is_empty() {
        return log_err(DataFusionError::Execution(
            "No parquet files were provided".to_string(),
        ));
    }

    if file_paths.len() == 1 {
        return load_parquet_file_as_table(data_fusion_context, &file_paths[0], local_name).await;
    }

    if file_paths
        .iter()
        .all(|file_path| !file_path.starts_with("s3:"))
    {
        return load_local_parquet_files_as_table(data_fusion_context, file_paths, local_name);
    }

    tracing::info!(
        "Loading {} PARQUET files into {}",
        file_paths.len(),
        local_name
    );

    let table_paths = match file_paths
        .iter()
        .map(ListingTableUrl::parse)
        .collect::<datafusion::error::Result<Vec<_>>>()
    {
        Ok(paths) => paths,
        Err(e) => return log_err(e),
    };
    let listing_options =
        ListingOptions::new(Arc::new(ParquetFormat::default().with_enable_pruning(true)))
            .with_file_extension(".parquet");
    let config = ListingTableConfig::new_with_multi_paths(table_paths)
        .with_listing_options(listing_options)
        .with_schema(Arc::new(schema.clone()));
    let table = match ListingTable::try_new(config) {
        Ok(table) => Arc::new(table),
        Err(e) => return log_err(e),
    };

    match data_fusion_context.register_table(local_name, table) {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.message().contains("already exists") {
                Ok(())
            } else {
                log_err(e)
            }
        }
    }
}

async fn load_json_file_as_table(
    data_fusion_context: &SessionContext,
    file_path_without_suffix: &String,
    local_name: &String,
    schema: &Schema,
) -> Result<(), DataFusionError> {
    match data_fusion_context.table_exist(local_name) {
        Ok(exists) => match exists {
            true => return Ok(()),
            false => (),
        },
        Err(e) => return log_err(e),
    };

    let ends_with_json = file_path_without_suffix.ends_with(".json");
    if JSON_MODE || ends_with_json {
        let file_path = if ends_with_json {
            file_path_without_suffix.clone()
        } else {
            format!("{}.json", file_path_without_suffix)
        };
        tracing::info!("Loading JSON file {}", file_path);
        let reader_options = JsonReadOptions::default().schema(&schema);
        match data_fusion_context
            .register_json(local_name, file_path, reader_options)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.message().contains("already exists") {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    } else {
        let file_path = if file_path_without_suffix.ends_with(".arrow") {
            file_path_without_suffix.clone()
        } else {
            format!("{}.arrow", file_path_without_suffix)
        };
        tracing::info!("Loading Arrow file {}", file_path);
        load_arrow_as_memtable(&file_path, local_name).await
    }
}

async fn load_arrow_as_memtable(
    file_path: &String,
    local_name: &String,
) -> Result<(), DataFusionError> {
    let file_contents = LRU_CACHE_HANDLE.file_get(file_path).await?;
    let arrow_reader = ArrowIpcFileReader::try_new(Cursor::new(file_contents), None)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let record_batches = arrow_reader
        .collect::<arrow::error::Result<Vec<RecordBatch>>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    if record_batches.is_empty() {
        return log_err(DataFusionError::Execution(format!(
            "Arrow file {} contained no record batches",
            file_path
        )));
    }

    load_memtable_with_name(local_name, &record_batches).await
}

pub(crate) fn path_to_table_name(file_path: &String) -> String {
    let safe_name = file_path
        .replace("/", "_")
        .replace(".", "_")
        .replace(":", "_")
        .replace("-", "_");
    format!("table_{}", safe_name)
}

pub(crate) async fn load_file_as_table(
    new_local_name: &String,
    file_path: &String,
    parquet: bool,
    schema: Option<Schema>,
) -> Result<(), DataFusionError> {
    LRU_CACHE_HANDLE
        .create_table(new_local_name, file_path, parquet, schema)
        .await
}

pub(crate) async fn load_files_as_table(
    new_local_name: &String,
    file_paths: &Vec<String>,
    schema: &Schema,
) -> Result<(), DataFusionError> {
    LRU_CACHE_HANDLE
        .create_multi_table(new_local_name, file_paths, schema)
        .await
}

#[allow(dead_code)]
pub(crate) async fn load_json_as_memtable(
    file_path: &String,
    local_name: &String,
    schema: &Schema,
) -> Result<(), DataFusionError> {
    let final_file_path = if file_path.starts_with("file://") {
        file_path.replace("file://", "")
    } else {
        file_path.clone()
    };

    let file_contents = match std::fs::read(final_file_path) {
        Ok(c) => c,
        Err(_) => panic!("Could not read file {}", file_path),
    };

    let json_reader = match arrow_json::ReaderBuilder::new(Arc::new(schema.clone()))
        .build(file_contents.as_slice())
    {
        Ok(d) => d,
        Err(_) => panic!("Private API returned result that does not match schema"),
    };

    let record_batches: Vec<RecordBatch> = match json_reader.collect() {
        Ok(batches) => batches,
        Err(e) => return log_err(DataFusionError::ArrowError(Box::new(e), None)),
    };

    load_memtable_with_name(local_name, &record_batches).await
}

pub(crate) async fn load_memtable(records: &Vec<RecordBatch>) -> Result<String, DataFusionError> {
    let result_table_name = format!("table_{}", IdInstance::next_id().to_string());
    load_memtable_with_name(&result_table_name, records).await?;
    Ok(result_table_name)
}

pub(crate) async fn load_memtable_with_name(
    result_table_name: &String,
    records: &Vec<RecordBatch>,
) -> Result<(), DataFusionError> {
    if records.len() == 0 {
        panic!("Do not call this if you have no records");
    }
    LRU_CACHE_HANDLE
        .load_table(result_table_name, records)
        .await
}

const NUM_TRIES: u32 = 4;

pub(crate) async fn execute_sql_async(sql: &String) -> Result<Vec<RecordBatch>, DataFusionError> {
    let (tx, mut rx) = mpsc::channel(2);
    let sql_owned = sql.clone();
    let driver_task = async move {
        // Plan / execute the query
        let results = match execute_sql(&sql_owned).await {
            Ok(r) => r,
            Err(e) => {
                tx.send(log_err(e)).await.unwrap();
                return;
            }
        };

        let batches = match results.collect().await {
            Ok(r) => Ok(r),
            Err(e) => log_err(e),
        };

        tx.send(batches).await.unwrap();
    };

    let mut join_set = JoinSet::new();
    join_set.spawn_on(driver_task, CPU_RUNTIME.handle());
    rx.recv().await.unwrap()
}

pub(crate) async fn execute_sql(sql: &String) -> Result<DataFrame, DataFusionError> {
    assert!(
        !sql.to_lowercase().contains("create table"),
        "Use the create_table function instead"
    );
    assert!(
        !sql.to_lowercase().contains("create external table"),
        "Use the create_table function instead"
    );
    assert!(
        !sql.to_lowercase().contains("drop table"),
        "Use the drop function instead"
    );
    LRU_CACHE_HANDLE.execute_sql(sql).await
}

pub(crate) async fn create_table(table_name: &String, sql: &String) -> Result<(), DataFusionError> {
    LRU_CACHE_HANDLE.create_table_as(table_name, sql).await
}

async fn private_execute_sql(
    data_fusion_context: &SessionContext,
    sql: &String,
) -> Result<DataFrame, DataFusionError> {
    match data_fusion_context.sql(sql).await {
        Ok(d) => Ok(d),
        Err(e) => log_err(e),
    }
}

#[allow(dead_code)]
pub async fn drop_iceberg_table(
    namespace: &String,
    table_name: &String,
) -> Result<(), iceberg::Error> {
    LRU_CACHE_HANDLE
        .drop_iceberg_table(namespace, table_name)
        .await
}

async fn drop_iceberg_table_worker(
    catalog: Arc<RestCatalog>,
    namespace: &String,
    name: &String,
) -> Result<(), iceberg::Error> {
    let namespace_ident = NamespaceIdent::new(namespace.clone());

    let table_ident = TableIdent {
        namespace: namespace_ident.clone(),
        name: name.clone(),
    };

    let result = catalog.drop_table(&table_ident).await;
    if result.is_ok() {
        invalidate_iceberg_table_metadata(namespace, name);
        clear_iceberg_table_row_group_stats(namespace, name);
    }
    result
}

pub async fn drop_all_iceberg_tables(namespace: &String) -> Result<(), iceberg::Error> {
    LRU_CACHE_HANDLE.drop_all_iceberg_tables(namespace).await
}

async fn drop_all_iceberg_tables_worker(
    catalog: Arc<RestCatalog>,
    namespace: &String,
) -> Result<(), iceberg::Error> {
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    let all_tables: Vec<TableIdent> = match catalog.get_namespace(&namespace_ident).await {
        Ok(_) => catalog.list_tables(&namespace_ident).await?,
        Err(_) => vec![],
    };
    for table_ident in all_tables.iter() {
        catalog.drop_table(table_ident).await?
    }
    invalidate_iceberg_namespace_table_metadata(namespace);
    clear_iceberg_namespace_row_group_stats(namespace);
    Ok(())
}

pub async fn ensure_iceberg_table(
    namespace: &String,
    name: &String,
    schema: &iceberg::spec::Schema,
) -> Result<Table, iceberg::Error> {
    LRU_CACHE_HANDLE
        .ensure_iceberg_table(namespace, name, schema)
        .await
}

async fn ensure_iceberg_table_worker(
    catalog: Arc<RestCatalog>,
    namespace: &String,
    name: &String,
    iceberg_schema: &iceberg::spec::Schema,
) -> Result<Table, iceberg::Error> {
    let namespace_ident = NamespaceIdent::new(namespace.clone());

    let table_ident = TableIdent {
        namespace: namespace_ident.clone(),
        name: name.clone(),
    };

    match catalog.get_namespace(&namespace_ident).await {
        Err(_) => {
            catalog
                .create_namespace(&namespace_ident, std::collections::HashMap::new())
                .await?;
        }
        Ok(_) => (),
    };

    match catalog.load_table(&table_ident).await {
        Ok(t) => Ok(t),
        Err(_) => {
            let creation = TableCreation::builder()
                .name(name.clone())
                .schema(iceberg_schema.clone())
                .build();

            match catalog.create_table(&namespace_ident, creation).await {
                Ok(t) => Ok(t),
                Err(e) => {
                    tracing::info!("Failed to create table {}: {}", name, e);
                    Err(e)
                }
            }
        }
    }
}

pub async fn load_iceberg_table_metadata(
    namespace: &String,
    table_name: &String,
    last_snapshot_id: i64,
) -> Result<IcebergLibMetadata, iceberg::Error> {
    LRU_CACHE_HANDLE
        .load_iceberg_table_metadata(namespace, table_name, last_snapshot_id)
        .await
}

fn collect_iceberg_compactions(
    table: &Table,
    last_snapshot_id: i64,
) -> Result<Vec<String>, iceberg::Error> {
    let snapshot_log = Vec::from_iter(table.metadata().history());
    let mut compactions = vec![];
    for snapshot_info in snapshot_log.iter().rev() {
        let snapshot = match table.metadata().snapshot_by_id(snapshot_info.snapshot_id) {
            Some(snapshot) => snapshot,
            None => {
                tracing::info!(
                    "Unable to find iceberg snapshot {}",
                    snapshot_info.snapshot_id
                );
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!(
                        "Unable to find iceberg snapshot {}",
                        snapshot_info.snapshot_id
                    ),
                ));
            }
        };

        if snapshot_info.snapshot_id == last_snapshot_id {
            break;
        }

        if let Some(compaction_id) = snapshot.summary().additional_properties.get("compaction") {
            compactions.push(compaction_id.clone());
        }
    }

    Ok(compactions)
}

async fn load_iceberg_table_metadata_worker(
    catalog: Arc<RestCatalog>,
    namespace: &String,
    name: &String,
    last_snapshot_id: i64,
) -> Result<IcebergLibMetadata, iceberg::Error> {
    let namespace_ident = NamespaceIdent::new(namespace.clone());

    let table_ident = TableIdent {
        namespace: namespace_ident.clone(),
        name: name.clone(),
    };

    let table: Table = match catalog.load_table(&table_ident).await {
        Ok(t) => t,
        Err(_) => {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("No such table {}", name),
            ));
        }
    };

    let compactions = collect_iceberg_compactions(&table, last_snapshot_id)?;

    let current_snapshot = match table.metadata().current_snapshot() {
        Some(c) => c,
        None => {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "No snapshot for this table",
            ));
        }
    };

    if let Some(mut metadata) =
        get_cached_iceberg_table_metadata(namespace, name, current_snapshot.snapshot_id())
    {
        metadata.compactions = compactions;
        return Ok(metadata);
    }

    let partition_spec = collect_iceberg_partition_spec(&table);
    let sort_order = collect_iceberg_sort_order(&table);
    let file_stats = load_iceberg_file_stats(&table, current_snapshot).await?;
    let access_artifacts =
        collect_iceberg_access_artifacts(&partition_spec, &sort_order, &file_stats);
    let current_files = file_stats
        .iter()
        .map(|stats| stats.file_path.clone())
        .collect::<HashSet<_>>();
    reconcile_iceberg_table_row_group_stats(namespace, name, &current_files);

    let table_scan = match table.scan().select_all().build() {
        Ok(s) => s,
        Err(e) => {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("No table scan task generated, {}", e),
            ));
        }
    };

    let plan_files = match table_scan.plan_files().await {
        Ok(p) => p,
        Err(_) => {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "No plan files task generated",
            ));
        }
    };

    let files_result = plan_files
        .map_ok(|f| (f.data_file_path, f.length, f.schema))
        .map_err(|err| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                format!("file scan task generate failed, {}", err),
            )
            .with_source(err)
        })
        .try_collect::<Vec<_>>()
        .await;

    let (files, sizes, schemas) = match files_result {
        Ok(r) => (
            r.iter().map(|(f, _, _)| f.clone()).collect(),
            r.iter().map(|(_, s, _)| *s).collect(),
            r.iter().map(|(_, _, s)| s.clone()).collect(),
        ),
        Err(e) => return Err(e),
    };

    let metadata = IcebergLibMetadata {
        snapshot_id: current_snapshot.snapshot_id(),
        table_schema: table.metadata().current_schema().clone(),
        files: files,
        sizes: sizes,
        schemas: schemas,
        compactions: vec![],
        partition_spec,
        sort_order,
        column_names: vec![],
        column_stats: vec![],
        access_artifacts,
        file_stats,
    };
    cache_iceberg_table_metadata(namespace, name, &metadata);

    let mut response = metadata.clone();
    response.compactions = compactions;
    Ok(response)
}

async fn load_iceberg_file_stats(
    table: &Table,
    current_snapshot: &iceberg::spec::Snapshot,
) -> Result<Vec<IcebergFileStats>, iceberg::Error> {
    let manifest_list = current_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;
    let current_schema = table.metadata().current_schema().clone();
    let current_partition_spec = table.metadata().default_partition_spec().clone();
    let mut pending_file_stats = vec![];

    for manifest_file in manifest_list.entries().iter() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }

        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for manifest_entry in manifest.entries() {
            if !manifest_entry.is_alive() || manifest_entry.content_type() != DataContentType::Data
            {
                continue;
            }

            let data_file = manifest_entry.data_file();
            pending_file_stats.push(PendingIcebergFileStats {
                file_path: data_file.file_path().to_string(),
                record_count: Some(data_file.record_count()),
                columns: collect_iceberg_column_stats(data_file, &current_schema),
                partition_values: collect_iceberg_partition_values(
                    data_file,
                    current_partition_spec.as_ref(),
                    &current_schema,
                ),
            });
        }
    }

    let concurrency = iceberg_row_group_stats_load_parallelism();
    let mut file_stats =
        stream::iter(pending_file_stats.into_iter().map(|pending| {
            let current_schema = current_schema.clone();
            async move {
                let row_groups =
                    match load_parquet_row_group_stats(table, &pending.file_path, &current_schema)
                        .await
                    {
                        Ok(row_groups) => row_groups,
                        Err(error) => {
                            tracing::warn!(
                                "Unable to load parquet row-group stats for {}: {}",
                                pending.file_path,
                                error
                            );
                            vec![]
                        }
                    };

                IcebergFileStats {
                    file_path: pending.file_path,
                    record_count: pending.record_count,
                    columns: pending.columns,
                    partition_values: pending.partition_values,
                    row_groups,
                }
            }
        }))
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    file_stats.sort_by(|left, right| left.file_path.cmp(&right.file_path));
    Ok(file_stats)
}

fn collect_iceberg_column_stats(
    data_file: &DataFile,
    schema: &iceberg::spec::Schema,
) -> Vec<IcebergColumnStats> {
    let mut field_ids = HashSet::new();
    field_ids.extend(data_file.null_value_counts().keys().copied());
    field_ids.extend(data_file.lower_bounds().keys().copied());
    field_ids.extend(data_file.upper_bounds().keys().copied());

    let mut column_stats = field_ids
        .into_iter()
        .filter_map(|field_id| {
            let field = schema.field_by_id(field_id)?;
            let field_name = schema.name_by_field_id(field_id)?.to_string();
            let field_type = field.field_type.as_ref();
            let null_count = data_file.null_value_counts().get(&field_id).copied();
            let lower_bound = data_file
                .lower_bounds()
                .get(&field_id)
                .and_then(|datum| datum_to_json_value(datum, field_type));
            let upper_bound = data_file
                .upper_bounds()
                .get(&field_id)
                .and_then(|datum| datum_to_json_value(datum, field_type));

            if null_count.is_none() && lower_bound.is_none() && upper_bound.is_none() {
                return None;
            }

            Some(IcebergColumnStats {
                field_id,
                field_name,
                null_count,
                lower_bound,
                upper_bound,
            })
        })
        .collect::<Vec<_>>();

    column_stats.sort_by(|left, right| left.field_name.cmp(&right.field_name));
    column_stats
}

async fn load_parquet_row_group_stats(
    table: &Table,
    file_path: &str,
    schema: &iceberg::spec::Schema,
) -> Result<Vec<IcebergRowGroupStats>, iceberg::Error> {
    if let Some(row_groups) = get_cached_parquet_row_group_stats(file_path) {
        return Ok(row_groups);
    }

    let input_file = table.file_io().new_input(file_path).map_err(|error| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("Unable to open parquet file {}", file_path),
        )
        .with_source(error)
    })?;
    let file_metadata = input_file.metadata().await.map_err(|error| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("Unable to stat parquet file {}", file_path),
        )
        .with_source(error)
    })?;
    let reader = input_file.reader().await.map_err(|error| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("Unable to create parquet reader for {}", file_path),
        )
        .with_source(error)
    })?;
    let mut reader = ArrowFileReader::new(file_metadata, reader);
    let metadata = ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new())
        .await
        .map_err(|error| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                format!("Unable to read parquet metadata for {}", file_path),
            )
            .with_source(error)
        })?;

    let row_groups = metadata
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .map(|(row_group_index, row_group)| {
            collect_parquet_row_group_stats(row_group_index, row_group, schema)
        })
        .collect::<Vec<_>>();
    cache_parquet_row_group_stats(file_path, &row_groups);

    Ok(row_groups)
}

fn collect_parquet_row_group_stats(
    row_group_index: usize,
    row_group: &RowGroupMetaData,
    schema: &iceberg::spec::Schema,
) -> IcebergRowGroupStats {
    let mut columns = row_group
        .columns()
        .iter()
        .filter_map(|column| collect_parquet_column_stats(column, schema))
        .collect::<Vec<_>>();
    columns.sort_by(|left, right| left.field_name.cmp(&right.field_name));

    IcebergRowGroupStats {
        row_group_index,
        record_count: u64::try_from(row_group.num_rows()).ok(),
        compressed_bytes: u64::try_from(row_group.compressed_size()).unwrap_or_default(),
        page_index_present: row_group.columns().iter().any(|column| {
            column.column_index_offset().is_some() && column.offset_index_offset().is_some()
        }),
        bloom_filter_present: row_group
            .columns()
            .iter()
            .any(|column| column.bloom_filter_offset().is_some()),
        columns,
    }
}

fn collect_parquet_column_stats(
    column: &ColumnChunkMetaData,
    schema: &iceberg::spec::Schema,
) -> Option<IcebergColumnStats> {
    let field_name = column.column_path().string();
    let field = schema.field_by_name(&field_name)?;
    let statistics = column.statistics()?;
    let field_type = field.field_type.as_ref();
    let null_count = statistics.null_count_opt();
    let lower_bound = parquet_stat_to_json_value(statistics, field_type, true);
    let upper_bound = parquet_stat_to_json_value(statistics, field_type, false);

    if null_count.is_none() && lower_bound.is_none() && upper_bound.is_none() {
        return None;
    }

    Some(IcebergColumnStats {
        field_id: field.id,
        field_name,
        null_count,
        lower_bound,
        upper_bound,
    })
}

fn parquet_stat_to_json_value(
    statistics: &Statistics,
    field_type: &Type,
    lower_bound: bool,
) -> Option<serde_json::Value> {
    match (field_type.as_primitive_type()?, statistics) {
        (PrimitiveType::Boolean, Statistics::Boolean(typed)) => scalar_bool_to_json(
            *select_stat_bound(typed.min_opt(), typed.max_opt(), lower_bound)?,
        ),
        (PrimitiveType::Int, Statistics::Int32(typed)) => Some(serde_json::Value::from(i64::from(
            *select_stat_bound(typed.min_opt(), typed.max_opt(), lower_bound)?,
        ))),
        (PrimitiveType::Long, Statistics::Int64(typed)) => Some(serde_json::Value::from(
            *select_stat_bound(typed.min_opt(), typed.max_opt(), lower_bound)?,
        )),
        (PrimitiveType::Float, Statistics::Float(typed)) => scalar_f64_to_json(f64::from(
            *select_stat_bound(typed.min_opt(), typed.max_opt(), lower_bound)?,
        )),
        (PrimitiveType::Double, Statistics::Double(typed)) => scalar_f64_to_json(
            *select_stat_bound(typed.min_opt(), typed.max_opt(), lower_bound)?,
        ),
        (PrimitiveType::String, Statistics::ByteArray(typed)) => parquet_bytes_to_json_string(
            select_stat_bound(typed.min_opt(), typed.max_opt(), lower_bound)?.data(),
        ),
        (PrimitiveType::String, Statistics::FixedLenByteArray(typed)) => {
            parquet_bytes_to_json_string(
                select_stat_bound(typed.min_opt(), typed.max_opt(), lower_bound)?.data(),
            )
        }
        _ => None,
    }
}

fn select_stat_bound<'a, T>(
    min: Option<&'a T>,
    max: Option<&'a T>,
    lower_bound: bool,
) -> Option<&'a T> {
    if lower_bound { min } else { max }
}

fn scalar_bool_to_json(value: bool) -> Option<serde_json::Value> {
    Some(serde_json::Value::Bool(value))
}

fn scalar_f64_to_json(value: f64) -> Option<serde_json::Value> {
    serde_json::Number::from_f64(value).map(serde_json::Value::Number)
}

fn parquet_bytes_to_json_string(bytes: &[u8]) -> Option<serde_json::Value> {
    String::from_utf8(bytes.to_vec())
        .ok()
        .map(serde_json::Value::String)
}

fn datum_to_json_value(
    datum: &iceberg::spec::Datum,
    field_type: &Type,
) -> Option<serde_json::Value> {
    match Literal::from(datum.clone()).try_into_json(field_type) {
        Ok(serde_json::Value::Null) => None,
        Ok(value) => Some(value),
        Err(_) => None,
    }
}
async fn commit_iceberg_transaction_worker(
    catalog: Arc<RestCatalog>,
    namespace: &String,
    name: &String,
    compaction_id: &String,
    data_files: &Vec<DataFile>,
) -> Result<(), iceberg::Error> {
    let table_ident = TableIdent {
        namespace: NamespaceIdent::new(namespace.clone()),
        name: name.clone(),
    };

    let table = match catalog.load_table(&table_ident).await {
        Ok(t) => t,
        Err(_) => panic!("You must ensure the table exists before calling this function."),
    };

    let tx = iceberg::transaction::Transaction::new(&table);
    let mut action = tx.fast_append();
    action = action.set_snapshot_properties(std::collections::HashMap::from([(
        "compaction".to_string(),
        compaction_id.clone(),
    )]));
    action = action.add_data_files(data_files.clone());
    match action.apply(tx)?.commit(catalog.as_ref()).await {
        Ok(_) => Ok(()),
        Err(e) => return Err(e),
    }
}

pub(crate) async fn file_exists(path: &String) -> bool {
    LRU_CACHE_HANDLE.file_exists(path).await
}

pub(crate) async fn drop(table_name: &String) -> () {
    LRU_CACHE_HANDLE.table_dropped(table_name).await;
}

pub(crate) async fn delete_s3_files(file_paths: &Vec<String>) -> () {
    LRU_CACHE_HANDLE.file_delete(file_paths).await
}

pub(crate) async fn put_s3_file(
    file_path: &String,
    file_contents: &Vec<u8>,
) -> Result<(), DataFusionError> {
    LRU_CACHE_HANDLE.file_put(file_path, file_contents).await
}

pub(crate) async fn commit_iceberg_transaction(
    namespace: &String,
    table_name: &String,
    compaction_id: &String,
    data_files: &Vec<DataFile>,
) -> Result<(), iceberg::Error> {
    LRU_CACHE_HANDLE
        .commit_iceberg_transaction(namespace, table_name, compaction_id, data_files)
        .await
}

pub(crate) fn s3_ingest_base_path() -> String {
    format!("{}/default/ingest", S3_BASE_PATH)
}

#[cfg(test)]
mod tests {
    use super::{
        IcebergLibMetadata, IcebergTableMetadataCache, IcebergTableRowGroupStatsTracker,
        ParquetRowGroupStatsCache, RecordBatch, ServingBulkCacheWarmupStats, drop,
        execute_sql_async, load_file_as_table, load_files_as_table,
        record_serving_bulk_cache_warmup, resolve_serving_liquid_cache_location,
        serving_bulk_cache_stats, serving_session_config,
    };
    use crate::data_contract::{IcebergColumnStats, IcebergRowGroupStats};
    use crate::elastic_search_ingest::WriteBuffer;
    use crate::schema_massager::PowdrrSchema;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::parquet::arrow::ArrowWriter;
    use iceberg::spec::Schema;
    use serde_json::Value;
    use serde_json::json;
    use std::collections::HashSet;
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn sample_row_group_stats(index: usize) -> Vec<IcebergRowGroupStats> {
        vec![IcebergRowGroupStats {
            row_group_index: index,
            record_count: Some(10),
            compressed_bytes: 128,
            page_index_present: true,
            bloom_filter_present: false,
            columns: vec![IcebergColumnStats {
                field_id: 1,
                field_name: "ts".to_string(),
                null_count: Some(0),
                lower_bound: Some(Value::from(index as i64)),
                upper_bound: Some(Value::from(index as i64 + 9)),
            }],
        }]
    }

    fn sample_iceberg_metadata(snapshot_id: i64, file_paths: &[&str]) -> IcebergLibMetadata {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![])
                .build()
                .unwrap(),
        );
        IcebergLibMetadata {
            snapshot_id,
            table_schema: schema.clone(),
            files: file_paths.iter().map(|path| path.to_string()).collect(),
            sizes: file_paths.iter().map(|_| 128).collect(),
            schemas: file_paths.iter().map(|_| schema.clone()).collect(),
            compactions: vec![],
            partition_spec: vec![],
            sort_order: vec![],
            column_names: vec![],
            column_stats: vec![],
            access_artifacts: vec![],
            file_stats: vec![],
        }
    }

    #[test]
    fn parquet_row_group_stats_cache_keeps_recent_entries() {
        let mut cache = ParquetRowGroupStatsCache::new(2);
        cache.insert("s3://warehouse/a.parquet", sample_row_group_stats(1));
        cache.insert("s3://warehouse/b.parquet", sample_row_group_stats(2));

        assert!(cache.get("s3://warehouse/a.parquet").is_some());

        cache.insert("s3://warehouse/c.parquet", sample_row_group_stats(3));

        assert!(cache.get("s3://warehouse/a.parquet").is_some());
        assert!(cache.get("s3://warehouse/b.parquet").is_none());
        assert!(cache.get("s3://warehouse/c.parquet").is_some());
    }

    #[test]
    fn serving_bulk_cache_stats_exposes_last_warmup_summary() {
        super::clear_serving_bulk_cache_warmup();
        record_serving_bulk_cache_warmup(ServingBulkCacheWarmupStats {
            table: "events".to_string(),
            snapshot_id: Some("snapshot_1".to_string()),
            targeted: true,
            matched_patterns: vec!["top_scores".to_string()],
            shaped_queries: 1,
            files_considered: 8,
            files_selected: 2,
            estimated_bytes: 300,
        });

        let stats = serving_bulk_cache_stats();

        assert_eq!(
            stats.last_warmup,
            Some(ServingBulkCacheWarmupStats {
                table: "events".to_string(),
                snapshot_id: Some("snapshot_1".to_string()),
                targeted: true,
                matched_patterns: vec!["top_scores".to_string()],
                shaped_queries: 1,
                files_considered: 8,
                files_selected: 2,
                estimated_bytes: 300,
            })
        );

        super::clear_serving_bulk_cache_warmup();
    }

    #[test]
    fn parquet_row_group_stats_cache_remove_and_clear() {
        let mut cache = ParquetRowGroupStatsCache::new(4);
        cache.insert("s3://warehouse/a.parquet", sample_row_group_stats(1));
        cache.insert("s3://warehouse/b.parquet", sample_row_group_stats(2));

        cache.remove("s3://warehouse/a.parquet");
        assert!(cache.get("s3://warehouse/a.parquet").is_none());
        assert!(cache.get("s3://warehouse/b.parquet").is_some());

        cache.clear();
        assert!(cache.get("s3://warehouse/b.parquet").is_none());
    }

    #[test]
    fn serving_session_config_enables_parquet_filter_pushdown() {
        let config = serving_session_config();

        assert!(config.parquet_pruning());
        assert!(config.parquet_bloom_filter_pruning());
        assert!(config.parquet_page_index_pruning());
        assert!(config.options().execution.parquet.pushdown_filters);
    }

    #[test]
    fn iceberg_table_row_group_stats_tracker_reports_removed_files() {
        let mut tracker = IcebergTableRowGroupStatsTracker::default();
        let initial_files = HashSet::from([
            "s3://warehouse/a.parquet".to_string(),
            "s3://warehouse/b.parquet".to_string(),
        ]);
        let next_files = HashSet::from([
            "s3://warehouse/b.parquet".to_string(),
            "s3://warehouse/c.parquet".to_string(),
        ]);

        let first_removed = tracker.replace_files("default/logs", initial_files);
        assert!(first_removed.is_empty());

        let removed = tracker.replace_files("default/logs", next_files);
        assert_eq!(removed, vec!["s3://warehouse/a.parquet".to_string()]);
    }

    #[test]
    fn iceberg_table_row_group_stats_tracker_remove_namespace_clears_all_tables() {
        let mut tracker = IcebergTableRowGroupStatsTracker::default();
        tracker.replace_files(
            "default/logs",
            HashSet::from(["s3://warehouse/a.parquet".to_string()]),
        );
        tracker.replace_files(
            "default/metrics",
            HashSet::from(["s3://warehouse/b.parquet".to_string()]),
        );
        tracker.replace_files(
            "other/logs",
            HashSet::from(["s3://warehouse/c.parquet".to_string()]),
        );

        let mut removed = tracker.remove_namespace("default");
        removed.sort();

        assert_eq!(
            removed,
            vec![
                "s3://warehouse/a.parquet".to_string(),
                "s3://warehouse/b.parquet".to_string()
            ]
        );
        assert_eq!(tracker.files_by_table.len(), 1);
        assert!(tracker.files_by_table.contains_key("other/logs"));
    }

    #[test]
    fn iceberg_table_metadata_cache_replaces_stale_snapshot_per_table() {
        let mut cache = IcebergTableMetadataCache::new(2);
        cache.insert(
            "default/logs",
            sample_iceberg_metadata(10, &["s3://warehouse/a.parquet"]),
        );

        assert!(cache.contains("default/logs", 10));
        assert!(cache.get("default/logs", 11).is_none());

        cache.insert(
            "default/logs",
            sample_iceberg_metadata(11, &["s3://warehouse/b.parquet"]),
        );

        assert!(!cache.contains("default/logs", 10));
        assert_eq!(
            cache.get("default/logs", 11).unwrap().files,
            vec!["s3://warehouse/b.parquet".to_string()]
        );
    }

    #[test]
    fn iceberg_table_metadata_cache_invalidates_entries_by_file() {
        let mut cache = IcebergTableMetadataCache::new(4);
        cache.insert(
            "default/logs",
            sample_iceberg_metadata(10, &["s3://warehouse/a.parquet"]),
        );
        cache.insert(
            "default/metrics",
            sample_iceberg_metadata(11, &["s3://warehouse/b.parquet"]),
        );

        cache.invalidate_file("s3://warehouse/a.parquet");

        assert!(cache.get("default/logs", 10).is_none());
        assert!(cache.get("default/metrics", 11).is_some());
    }

    #[tokio::test]
    async fn load_files_as_table_reads_single_local_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("single-file.parquet");

        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["acme", "globex"])),
                Arc::new(Int64Array::from(vec![10_i64, 20_i64])),
            ],
        )
        .unwrap();

        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let table_name = "single_local_parquet_test".to_string();
        let file_url = format!("file://{}", parquet_path.display());
        load_files_as_table(&table_name, &vec![file_url], schema.as_ref())
            .await
            .unwrap();

        let sql = format!("SELECT COUNT(*) AS count FROM {}", table_name);
        let batches = execute_sql_async(&sql).await.unwrap();
        let count = batches
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .sum::<i64>();

        assert_eq!(count, 2);
        drop(&table_name).await;
    }

    #[tokio::test]
    async fn load_file_as_table_reads_local_arrow_speedboat_file() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("segment");
        let segment_path = segment_path.to_string_lossy().to_string();
        let buffer = WriteBuffer::delete(vec![json!({
            "_id": "doc-1",
            "_id_seq_no": "doc-1:1",
            "_version": 1
        })]);
        buffer.write_to_file(&segment_path).unwrap();

        let table_name = "local_arrow_speedboat_test".to_string();
        load_file_as_table(
            &table_name,
            &segment_path,
            false,
            Some(PowdrrSchema::deletes().to_arrow_schema()),
        )
        .await
        .unwrap();

        let sql = format!("SELECT COUNT(*) AS count FROM {}", table_name);
        let batches = execute_sql_async(&sql).await.unwrap();
        let count = batches
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .sum::<i64>();

        assert_eq!(count, 1);
        drop(&table_name).await;
    }

    #[test]
    fn serving_liquid_cache_location_prefers_explicit_root() {
        let location = resolve_serving_liquid_cache_location(
            Some(PathBuf::from("/var/cache/powdrr-serving")),
            Some(PathBuf::from("/xdg-cache")),
            Some(PathBuf::from("/home/tester")),
            PathBuf::from("/tmp"),
            "warehouse",
            "http://localhost:9000",
        );

        assert_eq!(
            location.root_dir,
            PathBuf::from("/var/cache/powdrr-serving")
        );
        assert!(
            location
                .cache_dir
                .starts_with(PathBuf::from("/var/cache/powdrr-serving"))
        );
    }

    #[test]
    fn serving_liquid_cache_location_uses_xdg_then_home_then_temp() {
        let xdg_location = resolve_serving_liquid_cache_location(
            None,
            Some(PathBuf::from("/xdg-cache")),
            Some(PathBuf::from("/home/tester")),
            PathBuf::from("/tmp"),
            "warehouse",
            "http://localhost:9000",
        );
        assert_eq!(
            xdg_location.root_dir,
            PathBuf::from("/xdg-cache/powdrr-engine/serving-liquid-cache")
        );

        let home_location = resolve_serving_liquid_cache_location(
            None,
            None,
            Some(PathBuf::from("/home/tester")),
            PathBuf::from("/tmp"),
            "warehouse",
            "http://localhost:9000",
        );
        assert_eq!(
            home_location.root_dir,
            PathBuf::from("/home/tester/.cache/powdrr-engine/serving-liquid-cache")
        );

        let temp_location = resolve_serving_liquid_cache_location(
            None,
            None,
            None,
            PathBuf::from("/tmp"),
            "warehouse",
            "http://localhost:9000",
        );
        assert_eq!(
            temp_location.root_dir,
            PathBuf::from("/tmp/powdrr-engine/serving-liquid-cache")
        );
    }

    #[test]
    fn serving_liquid_cache_location_namespaces_by_backing_store() {
        let first = resolve_serving_liquid_cache_location(
            Some(PathBuf::from("/var/cache/powdrr-serving")),
            None,
            None,
            PathBuf::from("/tmp"),
            "warehouse",
            "http://localhost:9000",
        );
        let second = resolve_serving_liquid_cache_location(
            Some(PathBuf::from("/var/cache/powdrr-serving")),
            None,
            None,
            PathBuf::from("/tmp"),
            "warehouse",
            "http://localhost:9001",
        );

        assert_ne!(first.namespace, second.namespace);
        assert_ne!(first.cache_dir, second.cache_dir);
    }
}
