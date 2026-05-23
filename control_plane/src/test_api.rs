#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum StateMode {
    Testing,
    Ephemeral,
    TestingDynamoDb(Option<String>),
    Leaderless { server_address: String },
}

impl StateMode {
    pub fn is_testing(&self) -> bool {
        matches!(self, StateMode::Testing | StateMode::TestingDynamoDb(_))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
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

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum CacheMode {
    Redis(Option<String>),
    Native,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ApiMode {
    #[default]
    #[serde(rename = "readwrite")]
    ReadWrite,
    #[serde(rename = "readonly")]
    ReadOnly,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum PeerMode {
    SelfOnly,
    Remote(Vec<String>),
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum IndexingMode {
    Sync,
    Async,
    Disabled,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum CompactionMode {
    Async(Option<u64>),
    External(String),
    Disabled,
}

impl CompactionMode {
    const DEFAULT_NUM_FILES_THRESHOLD: u64 = 100;

    pub fn threshold(&self) -> u64 {
        match self {
            CompactionMode::Async(threshold) => {
                threshold.unwrap_or(Self::DEFAULT_NUM_FILES_THRESHOLD)
            }
            _ => Self::DEFAULT_NUM_FILES_THRESHOLD,
        }
    }

    pub fn is_disabled(&self) -> bool {
        matches!(self, CompactionMode::Disabled)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum PrefetchMode {
    Enabled,
    Disabled,
}

impl IndexingMode {
    pub fn is_disabled(&self) -> bool {
        matches!(self, IndexingMode::Disabled)
    }
}

impl PrefetchMode {
    pub fn is_disabled(&self) -> bool {
        matches!(self, PrefetchMode::Disabled)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct TestProcessingMode {
    pub state_mode: StateMode,
    pub storage_mode: StorageMode,
    pub cache_mode: CacheMode,
    #[serde(default)]
    pub api_mode: ApiMode,
    pub peer_mode: PeerMode,
    pub indexing_mode: IndexingMode,
    pub compaction_mode: CompactionMode,
    pub prefetch_mode: PrefetchMode,
}

impl TestProcessingMode {
    pub fn default() -> Self {
        Self::testing_dynamodb_default()
    }

    pub fn ephemeral_default() -> Self {
        Self {
            state_mode: StateMode::Ephemeral,
            storage_mode: StorageMode::default(),
            cache_mode: CacheMode::Native,
            api_mode: ApiMode::ReadWrite,
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Sync,
            compaction_mode: CompactionMode::Async(None),
            prefetch_mode: PrefetchMode::Disabled,
        }
    }

    pub fn testing_dynamodb_default() -> Self {
        Self {
            state_mode: StateMode::TestingDynamoDb(None),
            storage_mode: StorageMode::default(),
            cache_mode: CacheMode::Native,
            api_mode: ApiMode::ReadWrite,
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
            cache_mode: CacheMode::Native,
            api_mode: ApiMode::ReadWrite,
            peer_mode: PeerMode::SelfOnly,
            indexing_mode: IndexingMode::Sync,
            compaction_mode: CompactionMode::Async(None),
            prefetch_mode: PrefetchMode::Disabled,
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.api_mode.is_read_only()
    }
}

impl ApiMode {
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}
