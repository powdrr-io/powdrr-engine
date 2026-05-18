use powdrr_engine_lib::peers::get_docker_peer_ips;
use powdrr_engine_lib::state_provider::STATE_PROVIDER;
use powdrr_engine_lib::test_api::PeerModeType;

#[derive(Debug, Clone)]
pub(crate) enum PeerDetectionMode {
    SelfOnly,
    Docker,
}

#[derive(Debug, Clone)]
pub(crate) struct OperatingMode {
    pub(crate) peer_detection: PeerDetectionMode,
    pub(crate) testing_enabled: bool,
    pub(crate) port: u32,
    pub(crate) mongo_port: Option<u32>,
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

    match mode.as_str() {
        "default" => OperatingMode {
            peer_detection: PeerDetectionMode::SelfOnly,
            testing_enabled: true,
            port,
            mongo_port,
        },
        "docker" => OperatingMode {
            peer_detection: PeerDetectionMode::Docker,
            testing_enabled: true,
            port,
            mongo_port,
        },
        _ => {
            panic!("Invalid mode specified: {}", mode);
        }
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
