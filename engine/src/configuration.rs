use powdrr_lib::peers::{get_docker_peer_ips, get_kubernetes_peer_ips};
use powdrr_lib::state_provider::STATE_PROVIDER;
use powdrr_lib::test_api::{PeerModeType};

#[derive(Debug, Clone)]
pub(crate) enum PeerDetectionMode {
    SelfOnly,
    #[allow(dead_code)]
    Kubernetes,
    Docker
}

#[derive(Debug, Clone)]
pub(crate) struct OperatingMode {
    pub(crate) peer_detection: PeerDetectionMode,
    pub(crate) testing_enabled: bool,
    pub(crate) port: u32
}


pub(crate) fn get_operating_mode(_command_line_args: &Vec<String>) -> OperatingMode {
    let mode = match std::env::var("MODE") {
        Ok(mode) => mode.to_lowercase(),
        Err(_) => "default".to_string()
    };
    let port = match std::env::var("PORT") {
        Ok(port) => port.parse::<u32>().unwrap(),
        Err(_) => 9200
    };

    match mode.as_str() {
        "default" => {
            OperatingMode {
                peer_detection: PeerDetectionMode::SelfOnly,
                testing_enabled: true,
                port
            }
        },
        "docker" => {
            OperatingMode {
                peer_detection: PeerDetectionMode::Docker,
                testing_enabled: false,
                port
            }
        },
        _ => {
            panic!("Invalid mode specified: {}", mode);
        }
    }
}


pub(crate) async fn perform_updates(mode: &OperatingMode) -> () {
    match &mode.peer_detection {
        PeerDetectionMode::SelfOnly => (),
        PeerDetectionMode::Kubernetes => {
            let peers = get_kubernetes_peer_ips().await;
            STATE_PROVIDER.set_peer_mode(&PeerModeType::Remote(peers)).await;
        },
        PeerDetectionMode::Docker => {
            let peers = get_docker_peer_ips().await;
            STATE_PROVIDER.set_peer_mode(&PeerModeType::Remote(peers)).await;
        }
    }
}

