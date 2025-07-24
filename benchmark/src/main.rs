use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use idgenerator::{IdGeneratorOptions, IdInstance};
use rand::prelude::ThreadRng;
use rand::{Rng, TryRngCore};
use rand::rngs::OsRng;

const LINE_LIMIT: u64 = 1000000;

const EVENT_TEMPLATES: [&str; 4] = [
    include_str!("okta_system_log_1.json"),
    include_str!("okta_system_log_2.json"),
    include_str!("okta_system_log_3.json"),
    include_str!("okta_system_log_4.json"),
];

const NUM_USERS: u32 = 10000;
const NUM_ORGS: u32 = 1000;

static USER_AND_ORG_IDS: LazyLock<Vec<(String, String)>> = LazyLock::new(|| {
    let mut rng = OsRng{};
    (0..NUM_USERS).map(|_| (format!("user_{}", IdInstance::next_id()), random_org(&mut rng))).collect()
});

static ORG_IDS: LazyLock<Vec<String>> = LazyLock::new(|| {
    (0..NUM_ORGS).map(|_| format!("org_{}", IdInstance::next_id())).collect()
});

fn make_create_header() -> String {
    "{\"create\": {\"_index\": \"logs\"}}".to_string()
}

fn random_org(rng: &mut OsRng) -> String {
    let random_index = rng.try_next_u32().unwrap() % NUM_ORGS;
    ORG_IDS[random_index as usize].clone()
}

fn random_user_and_org(rng: &mut OsRng) -> (String, String) {
    let random_index = rng.try_next_u32().unwrap() % NUM_USERS;
    USER_AND_ORG_IDS[random_index as usize].clone()
}

fn random_event_for_now(rng: &mut OsRng) -> String {
    let random_index = rng.try_next_u32().unwrap() % EVENT_TEMPLATES.len() as u32;
    let (user_id, org_id) = random_user_and_org(rng);
    EVENT_TEMPLATES[random_index as usize].to_string()
        .replace("\n", "")
        .replace("{user_id}",user_id.as_str())
        .replace("{unique_id}", IdInstance::next_id().to_string().as_str())
        .replace("{published}", current_time_rfc3339().as_str())
        .replace("{org_id}", org_id.as_str())
}

async fn push_to_service(buffer: Vec<String>) -> u128 {
    if buffer.len() == 0 {
        return 0
    }
    let payload = buffer.join("\n");

    let client = reqwest::Client::new();
    let time_before = current_time();
    let _res = match client.post("http://localhost:9200/_bulk")
        .body(payload)
        .send().await {
        Ok(res) => res,
        Err(e) => panic!("Error: {}", e),
    };
    let time_after = current_time();
    let time_taken = time_after - time_before;
    time_taken.as_millis()
}


async fn load_data() -> Result<bool, std::io::Error> {
    let mut lines_read: u64 = 0;
    let mut accumulator = vec!();
    let mut waiting_for_response = vec!();
    let mut all_response_times = vec!();
    let mut rng = rand::rngs::OsRng{};
    loop {
        let line_value = random_event_for_now(&mut rng);
        lines_read += 1;

        accumulator.push(make_create_header());
        accumulator.push(line_value);

        if accumulator.len() < 100 {
            continue;
        }

        waiting_for_response.push(push_to_service(accumulator.clone()));
        accumulator.clear();

        if lines_read % 1000 == 0 {
            all_response_times.extend(join_all(waiting_for_response).await);
            waiting_for_response = vec!();
            println!("Events Added: {}", lines_read);
            println!("Ingest - average response time: {} ms", all_response_times.iter().sum::<u128>() / all_response_times.len() as u128);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if lines_read >= LINE_LIMIT {
            break;
        }
    }

    push_to_service(accumulator).await;

    Ok(false)
}

const QUERY_TEMPLATE: &str = r#"
        {
           "query": {
             "bool": {
               "must": [
                 {
                   "term": {
                     "orgId": "{org_id}"
                   }
                 },
                 {
                   "term": {
                     "actor.id": "{user_id}"
                   }
                 },
                 {
                   "range": {
                     "published": {
                       "gte": "now-5m",
                       "lte": "now"
                     }
                   }
                 }
               ]
             }
           }
        }"#;


async fn search() -> Result<(), std::io::Error> {
    let client = reqwest::Client::new();

    let mut rng = OsRng{};
    let mut all_response_times = vec!();

    loop {
        let (user_id, org_id) = random_user_and_org(&mut rng);

        let body_obj = QUERY_TEMPLATE
            .replace("{user_id}", user_id.as_str())
            .replace("{org_id}", org_id.as_str());

        let time_before = current_time();
        let res = match client.post("http://localhost:9200/logs/_search")
            .body(body_obj)
            .send().await {
            Ok(res) => res,
            Err(e) => panic!("Error: {}", e),
        };
        let time_after = current_time();
        all_response_times.push((time_after - time_before).as_millis() as u128);

        assert!(res.status().is_success());
        let response_val = serde_json::from_str::<serde_json::Value>(res.text().await.unwrap().as_str()).unwrap();
        let hits = response_val.as_object().unwrap().get("hits").unwrap().as_object().unwrap().get("total").unwrap().as_object().unwrap().get("value").unwrap().as_u64().unwrap();
        println!("Org Id = {}, User Id = {}, Hits = {}", org_id, user_id, hits);
        println!("Search - average response time: {} ms", all_response_times.iter().sum::<u128>() / all_response_times.len() as u128);

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    //println!("Response: {}", res.text().await.unwrap());
    Ok(())
}


fn current_time() -> Duration {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("time should go forward")
}

fn current_time_rfc3339() -> String {
    let now = SystemTime::now();
    let now: DateTime<Utc> = now.into();
    now.to_rfc3339()
}


#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let options = IdGeneratorOptions::new().worker_id(1).worker_id_bit_len(6);
    match IdInstance::init(options) {
        Ok(_) => (),
        Err(_) => panic!("What happened?")
    }

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


    tokio::spawn(load_data());
    tokio::spawn(search());
    tokio::time::sleep(Duration::from_secs(1000)).await;

    Ok(())
}
