use powdrr_query_runtime::peers::get_docker_peer_ips;
use powdrr_query_runtime::state_provider::STATE_PROVIDER;
use powdrr_query_runtime::test_api::{
    CacheMode, CompactionMode, IndexingMode, PeerMode, PeerModeType, PrefetchMode, StateMode,
    StorageMode, TestProcessingMode,
};

#[derive(Debug, Clone)]
pub(crate) enum PeerDetectionMode {
    SelfOnly,
    Docker,
}

#[derive(Clone)]
pub(crate) struct OperatingMode {
    pub(crate) peer_detection: PeerDetectionMode,
    pub(crate) testing_enabled: bool,
    pub(crate) port: u32,
    pub(crate) mongo_port: Option<u32>,
    pub(crate) redis_port: Option<u32>,
    pub(crate) state_mode: Option<TestProcessingMode>,
}

pub(crate) fn get_operating_mode(_command_line_args: &Vec<String>) -> OperatingMode {
    let mode = match std::env::var("MODE") {
        Ok(mode) => mode.to_lowercase(),
        Err(_) => "default".to_string(),
    };
    let port = match std::env::var("PORT") {
        Ok(port) => port.parse::<u32>().unwrap(),
        Err(_) => 9200,
    };
    let mongo_port = std::env::var("MONGO_PORT")
        .ok()
        .map(|port| port.parse::<u32>().unwrap());
    let redis_port = std::env::var("REDIS_FRONTEND_PORT")
        .ok()
        .map(|port| port.parse::<u32>().unwrap());

    match mode.as_str() {
        "default" => OperatingMode {
            peer_detection: PeerDetectionMode::SelfOnly,
            testing_enabled: true,
            port,
            mongo_port,
            redis_port,
            state_mode: None,
        },
        "docker" => OperatingMode {
            peer_detection: PeerDetectionMode::Docker,
            testing_enabled: true,
            port,
            mongo_port,
            redis_port,
            state_mode: None,
        },
        "leaderless" => OperatingMode {
            peer_detection: PeerDetectionMode::SelfOnly,
            testing_enabled: false,
            port,
            mongo_port,
            redis_port,
            state_mode: Some(TestProcessingMode {
                state_mode: StateMode::Leaderless {
                    server_address: std::env::var("SERVICE_BASE_URL")
                        .expect("SERVICE_BASE_URL must be set for MODE=leaderless"),
                    access_key: std::env::var("SERVICE_ACCESS_KEY")
                        .expect("SERVICE_ACCESS_KEY must be set for MODE=leaderless"),
                    secret_key: std::env::var("SERVICE_SECRET_KEY")
                        .expect("SERVICE_SECRET_KEY must be set for MODE=leaderless"),
                },
                storage_mode: StorageMode::default(),
                cache_mode: CacheMode::Redis(None),
                peer_mode: PeerMode::SelfOnly,
                indexing_mode: IndexingMode::Sync,
                compaction_mode: CompactionMode::Async(None),
                prefetch_mode: PrefetchMode::Disabled,
            }),
        },
        _ => {
            panic!("Invalid mode specified: {}", mode);
        }
    }
}

pub(crate) async fn initialize_state_provider(mode: &OperatingMode) -> () {
    if let Some(state_mode) = &mode.state_mode {
        STATE_PROVIDER.set_testing_mode(state_mode).await;
    }
}

pub(crate) async fn perform_updates(mode: &OperatingMode) -> () {
    match &mode.peer_detection {
        PeerDetectionMode::SelfOnly => (),
        PeerDetectionMode::Docker => {
            let peers = get_docker_peer_ips().await;
            println!("Peers: {:?}", peers);
            STATE_PROVIDER
                .set_peer_mode(&PeerModeType::Remote(peers))
                .await;
        }
    }
}
