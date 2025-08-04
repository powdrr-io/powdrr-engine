extern crate core;

use std::env;
use idgenerator::{IdGeneratorOptions, IdInstance};

mod response;
mod router;
mod v1_handlers;
mod service_impl_provider;

fn run_server(port: &String) -> () {
    tracing_subscriber::fmt().init();
    let addr = format!("0.0.0.0:{}", port);
    println!("Listening for requests at http://{}", addr);
    gotham::start_with_num_threads(addr, router::router(true), 32).unwrap()
}


fn main() -> () {
    let args: Vec<String> = env::args().collect();

    let options = IdGeneratorOptions::new().worker_id(1).worker_id_bit_len(6);
    match IdInstance::init(options) {
        Ok(_) => (),
        Err(_) => panic!("What happened?")
    }

    match args.get(1) {
        None => run_server(&"7784".to_string()),
        Some(val) => run_server(val),
    }
}
