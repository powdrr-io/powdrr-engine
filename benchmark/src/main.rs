use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use futures::future::join_all;

const LINE_LIMIT: u64 = 10000;

fn make_create_header() -> String {
    "{\"create\": {\"_index\": \"logs\"}}".to_string()
}


async fn push_to_service(buffer: Vec<String>) -> () {
    if buffer.len() == 0 {
        return ()
    }
    let payload = buffer.join("\n");

    let client = reqwest::Client::new();
    let _res = match client.post("http://localhost:9200/_bulk")
        .body(payload)
        .send().await {
        Ok(res) => res,
        Err(e) => panic!("Error: {}", e),
    };

}

async fn load_data() -> Result<(), std::io::Error> {
    let file = File::open("corpus.json")?;
    let reader = BufReader::new(file);

    let mut lines_read: u64 = 0;
    let mut accumulator = vec!();
    let mut waiting_for_response = vec!();
    for line in reader.lines() {
        accumulator.push(make_create_header());
        accumulator.push(line?);
        lines_read += 1;

        if accumulator.len() < 100 {
            continue;
        }

        waiting_for_response.push(push_to_service(accumulator.clone()));
        accumulator.clear();

        if lines_read % 1000 == 0 {
            join_all(waiting_for_response).await;
            waiting_for_response = vec!();
            println!("Lines read: {}", lines_read);
        }

        if lines_read >= LINE_LIMIT {
            break;
        }
    }

    push_to_service(accumulator).await;

    Ok(())
}

fn search() -> Result<(), std::io::Error> {
    let body_obj  = r#"
        {
           "query": {
             "match": {
               "text": {
                 "query": "large"
               }
             }
           }
        }"#;


    let client = reqwest::blocking::Client::new();
    let _res = match client.post("http://localhost:9200/logs/_search")
        .body(body_obj)
        .send() {
        Ok(res) => res,
        Err(e) => panic!("Error: {}", e),
    };
    Ok(())
}


fn current_time() -> Duration {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("time should go forward")
}


#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let client = reqwest::Client::new();
    let _res = match client.put("http://localhost:9200/_test/v1/_testing_and_processing_mode")
        .body("")
        .send().await {
        Ok(res) => res,
        Err(e) => panic!("Error: {}", e),
    };

    println!("Starting Benchmark!!!!!!!!!!!!!!!!!!!!!!!!!!");

    println!("Create Index");

    let body_create_index = r#"{
            "settings" : {
                "index": {
                "number_of_shards" : 2,
                "number_of_replicas" : 1
            } } }"#;


    let _res = match client.put("http://localhost:9200/logs")
        .body(body_create_index)
        .send().await {
        Ok(res) => res,
        Err(e) => panic!("Error: {}", e),
    };

    println!("Loading Corpus");

    let time_before_corpus = current_time();
    load_data().await.expect("TODO: panic message");
    let time_after_corpus = current_time();
    println!("Corpus Loaded in {}ms", time_after_corpus.as_millis() - time_before_corpus.as_millis());

    println!("Searching");

    let time_before_search = current_time();
    search().expect("TODO: panic message");
    let time_after_search = current_time();
    println!("Searched in {}ms", time_after_search.as_millis() - time_before_search.as_millis());

    println!("Done");

    Ok(())
}
