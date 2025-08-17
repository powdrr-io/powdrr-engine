use std::{env, io::{self, BufReader}};

use gotham::{anyhow, rustls::{Certificate, PrivateKey, ServerConfig}};
use idgenerator::*;
use rustls_pemfile::{certs, rsa_private_keys};
use rustls_pki_types::PrivatePkcs1KeyDer;
use powdrr_lib::peers::get_peer_ips;

/// Start a server and call the `Handler` we've defined above for each `Request` we receive.
//

fn build_config() -> anyhow::Result<ServerConfig> {
    let mut cert_file = BufReader::new(&include_bytes!("../ca.crt")[..]);
    let mut key_file = BufReader::new(&include_bytes!("../ca.key")[..]);
    let certs = certs(&mut cert_file)
        .map(|result| result.map(|der| Certificate(der.to_vec())))
        .collect::<Result<_, _>>()?;
    let keys: Vec<Result<PrivatePkcs1KeyDer, io::Error>> = rsa_private_keys(&mut key_file).collect();
    let intermediate = keys.get(0).unwrap().as_ref().unwrap();

    let key = PrivateKey(intermediate.secret_pkcs1_der().to_vec());
    ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(Into::into)
}


async fn run_server(port: &String) -> () {
    tracing_subscriber::fmt().init();
    let addr = format!("0.0.0.0:{}", port);
    println!("Listening for requests at http://{}", addr);
    gotham::init_server(addr, powdrr_lib::router::router(true)).await.unwrap();
}


#[allow(dead_code)]
async fn run_ssl_server() -> () {
    tracing_subscriber::fmt().init();
    let addr = "0.0.0.0:9200";
    println!("Listening for requests at https://{}", addr);
    gotham::start_with_tls(addr, powdrr_lib::router::router(true), build_config().unwrap()).unwrap();
}


#[tokio::main]
async fn main() -> () {
    let args: Vec<String> = env::args().collect();
    rustls::crypto::ring::default_provider()
        .install_default().unwrap();

    let options = IdGeneratorOptions::new().worker_id(1).worker_id_bit_len(6);
    match IdInstance::init(options) {
        Ok(_) => (),
        Err(_) => panic!("What happened?")
    }

    tokio::runtime::Handle::current().spawn(async move {
        match args.get(1) {
            None => run_server(&"9200".to_string()).await,
            Some(val) => run_server(val).await,
        }
    });

    tokio::runtime::Handle::current().spawn(async move {
        loop {
            let peer_ips = get_peer_ips().await;
            println!("Peer IPs: {}", peer_ips.len());
            for peer_ip in peer_ips.iter() {
                println!("Peer IP: {}", peer_ip);
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    tokio::signal::ctrl_c().await.unwrap();
}
