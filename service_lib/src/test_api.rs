use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub enum StateMode {
    Testing,
    Ephemeral,
    TestingDynamoDb(Option<String>),
    Leaderless {
        server_address: String,
        access_key: String,
        secret_key: String,
    },
}

impl StateMode {
    pub fn is_testing(&self) -> bool {
        matches!(self, StateMode::Testing | StateMode::TestingDynamoDb(_))
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum StorageMode {
    S3 {
        rest_endpoint: Option<String>,
        s3_endpoint: Option<String>,
    },
}

impl StorageMode {
    pub fn default() -> Self {
        Self::S3 {
            rest_endpoint: None,
            s3_endpoint: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum CacheMode {
    Redis(Option<String>),
    Native,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum PeerMode {
    SelfOnly,
    Remote(Vec<String>),
}

#[derive(Serialize, Deserialize, Clone)]
pub enum IndexingMode {
    Sync,
    Async,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum CompactionMode {
    Async(Option<u64>),
    External(String),
    Disabled,
}

impl CompactionMode {
    const DEFAULT_NUM_FILES_THRESHOLD: u64 = 100;

    pub(crate) fn threshold(&self) -> u64 {
        match self {
            CompactionMode::Async(threshold) => {
                threshold.unwrap_or(Self::DEFAULT_NUM_FILES_THRESHOLD)
            }
            _ => Self::DEFAULT_NUM_FILES_THRESHOLD,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum PrefetchMode {
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TestProcessingMode {
    pub state_mode: StateMode,
    pub storage_mode: StorageMode,
    pub cache_mode: CacheMode,
    pub peer_mode: PeerMode,
    pub indexing_mode: IndexingMode,
    pub compaction_mode: CompactionMode,
    pub prefetch_mode: PrefetchMode,
}

impl TestProcessingMode {
    pub fn default() -> Self {
        Self {
            state_mode: StateMode::TestingDynamoDb(None),
            storage_mode: StorageMode::default(),
            cache_mode: CacheMode::Redis(None),
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Sync,
            compaction_mode: CompactionMode::Async(None),
            prefetch_mode: PrefetchMode::Disabled,
        }
    }

    pub fn dynamo_testing(address: Option<String>) -> Self {
        Self {
            state_mode: StateMode::TestingDynamoDb(address),
            storage_mode: StorageMode::default(),
            cache_mode: CacheMode::Redis(None),
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Sync,
            compaction_mode: CompactionMode::Async(None),
            prefetch_mode: PrefetchMode::Disabled,
        }
    }
}
