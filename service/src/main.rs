extern crate core;

use idgenerator::{IdGeneratorOptions, IdInstance};
use powdrr_service_lib::data_contract::{ServiceImplType, ServiceMode};
use powdrr_service_lib::raft_service_impl::RaftServiceConfig;
use std::collections::BTreeMap;
use std::env;
use std::time::Duration;

mod checkpoint_updater;
mod raft_handlers;
mod response;
mod router;
mod service_impl_provider;
mod v1_handlers;

use crate::service_impl_provider::SERVICE_IMPL;

async fn run_server(port: &String) -> () {
    tracing_subscriber::fmt().init();
    checkpoint_updater::ensure_checkpoint_updater_started();
    let addr = format!("0.0.0.0:{}", port);
    println!("Listening for requests at http://{}", addr);
    gotham::init_server(addr, router::router(true))
        .await
        .unwrap()
}

fn parse_bool_env(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| matches!(value.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn parse_raft_peers(port: &String) -> BTreeMap<u64, String> {
    let default_self_id = std::env::var("RAFT_NODE_ID")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1);
    let default_self_addr =
        std::env::var("RAFT_ADVERTISE_ADDRESS").unwrap_or_else(|_| format!("127.0.0.1:{port}"));

    let peers = std::env::var("RAFT_PEERS")
        .unwrap_or_else(|_| format!("{default_self_id}@{default_self_addr}"));

    peers
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (node_id, address) = entry
                .split_once('@')
                .unwrap_or_else(|| panic!("Invalid RAFT_PEERS entry: {entry}"));
            (
                node_id
                    .parse::<u64>()
                    .unwrap_or_else(|_| panic!("Invalid raft node id in {entry}")),
                address.to_string(),
            )
        })
        .collect()
}

async fn configure_service_impl_from_env(port: &String) {
    let mode = std::env::var("SERVICE_METADATA_MODE")
        .unwrap_or_else(|_| "ephemeral".to_string())
        .to_lowercase();

    match mode.as_str() {
        "ephemeral" => {}
        "dynamodb" => {
            SERVICE_IMPL
                .set_mode(ServiceMode {
                    impl_type: ServiceImplType::DynamoDb(None),
                })
                .await
                .unwrap();
        }
        "raft" => {
            let node_id = std::env::var("RAFT_NODE_ID")
                .unwrap_or_else(|_| "1".to_string())
                .parse::<u64>()
                .unwrap();
            let advertise_address = std::env::var("RAFT_ADVERTISE_ADDRESS")
                .unwrap_or_else(|_| format!("127.0.0.1:{port}"));
            let config = RaftServiceConfig {
                cluster_name: std::env::var("RAFT_CLUSTER_NAME")
                    .unwrap_or_else(|_| "powdrr-service".to_string()),
                node_id,
                advertise_address,
                bootstrap: parse_bool_env("RAFT_BOOTSTRAP"),
                peers: parse_raft_peers(port),
            };
            SERVICE_IMPL.configure_raft(config).await.unwrap();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if let Err(error) = SERVICE_IMPL.bootstrap_raft_if_needed().await {
                    tracing::error!("Raft bootstrap failed: {}", error);
                }
            });
        }
        other => panic!("Unsupported SERVICE_METADATA_MODE: {}", other),
    }
}

#[tokio::main]
async fn main() -> () {
    let args: Vec<String> = env::args().collect();

    let options = IdGeneratorOptions::new().worker_id(1).worker_id_bit_len(6);
    match IdInstance::init(options) {
        Ok(_) => (),
        Err(_) => panic!("What happened?"),
    }

    match args.get(1) {
        None => {
            let port = "7784".to_string();
            configure_service_impl_from_env(&port).await;
            run_server(&port).await;
        }
        Some(val) => {
            configure_service_impl_from_env(val).await;
            run_server(val).await;
        }
    }
}
