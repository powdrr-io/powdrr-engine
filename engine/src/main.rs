use std::{env, io::{self, BufReader}};

use gotham::{anyhow, rustls::{Certificate, PrivateKey, ServerConfig}};
use idgenerator::*;
use rustls_pemfile::{certs, rsa_private_keys};
use rustls_pki_types::PrivatePkcs1KeyDer;


/// Start a server and call the `Handler` we've defined above for each `Request` we receive.
// #[tokio::main]

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


fn run_server() -> () {
    tracing_subscriber::fmt().init();
    let addr = "0.0.0.0:9200";
    println!("Listening for requests at http://{}", addr);
    gotham::start_with_num_threads(addr, powdrr_lib::router::router(true), 16).unwrap()
}


fn run_ssl_server() -> () {
    tracing_subscriber::fmt().init();
    let addr = "0.0.0.0:9200";
    println!("Listening for requests at https://{}", addr);
    gotham::start_with_tls(addr, powdrr_lib::router::router(true), build_config().unwrap()).unwrap();
}


fn main() -> () {
    let args: Vec<String> = env::args().collect();

    let options = IdGeneratorOptions::new().worker_id(1).worker_id_bit_len(6);
    match IdInstance::init(options) {
        Ok(_) => (),
        Err(_) => panic!("What happened?")
    }

    match args.get(1) {
        None => run_server(),
        Some(val) if val == &"tls".to_string() => run_ssl_server(),
        Some(_) => {
            println!("Unrecognized option");
            panic!("I don't know what to do")
        }
    }
}

