use redis::AsyncCommands;
use std::{error::Error, fmt::Display};
use std::sync::Mutex;
use redis::{Commands, FromRedisValue, ToRedisArgs};
use redis::{aio::MultiplexedConnection, RedisResult};

#[derive(Debug)]
pub(crate) struct CacheError {}

impl Display for CacheError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for CacheError {}


const REDIS_URL: &str = "redis://127.0.0.1:6379/";


static REDIS_CONNECTION: Mutex<Option<MultiplexedConnection>> = Mutex::new(None);

async fn create_connection() -> RedisResult<MultiplexedConnection> {
    let client = redis::Client::open(REDIS_URL)?;
    let con = client.get_multiplexed_tokio_connection().await?;
    Ok(con)
}

pub async fn get_connection() -> Result<MultiplexedConnection, CacheError> {
    let mut opt_guard = match REDIS_CONNECTION.lock() {
        Ok(guard) => guard,
        Err(_) => panic!("Time for some debug")
    };
    if opt_guard.is_none() {
        *opt_guard = Some(create_connection().await.map_err(|_|CacheError{})?);
    }
    Ok(opt_guard.clone().unwrap())
}


async fn increment(key: &String, delta: i64) -> Result<i64, CacheError>  {
    let mut con= get_connection().await?;

    match redis::pipe()
        .atomic()
        .cmd("INCRBY")
        .arg(key)
        .arg(delta)
        .ignore()
        .cmd("GET")
        .arg(key)
        .query_async::<Vec<String>>(&mut con).await {
        Ok(v) => {
            Ok(v.get(0).unwrap().parse::<i64>().unwrap())
        }
        Err(e) => {
            let error = format!("{}", e);
            println!("{}", error);
            panic!("Time for some debug");
        }
    }
}


async fn get<T: FromRedisValue>(key: &String) -> Result<T, CacheError> {
    let mut con = get_connection().await?;

    match con.get(key).await {
        Ok(v) => Ok(v),
        Err(_) => panic!("Time for some debug")
    }
}



async fn set<T: ToRedisArgs + Send + Sync>(key: &String, value: T) -> Result<(), CacheError> {
    let mut con = get_connection().await?;

    match con.set::<&String, T, String>(key, value).await {
        Ok(_) => Ok(()),
        Err(e) => {
            let error = format!("{}", e);
            println!("{}", error);
            panic!("Time for some debug and maybe adding more error path")
        }
    }
}


fn approx_num_records_key(table: &String) -> String {
    format!("{}_approx_num_records", table)
}

fn table_seq_no_key(table: &String) -> String {
    format!("{}_seq_no", table)    
}

pub(crate) async fn get_approx_num_records(table: &String) -> Result<u64, CacheError> {
    get::<u64>(&approx_num_records_key(table)).await
}

pub(crate) async fn create_table(table: &String) -> Result<(), CacheError> {
    set(&approx_num_records_key(table), 0).await?;
    set(&table_seq_no_key(table), 0).await
}

pub(crate) async fn insert_operator(table: &String, num_records: i64) -> Result<i64, CacheError> {
    // Increase the number of records
    increment(&approx_num_records_key(table), num_records).await?;
    // ...and bump the seq no
    increment(&table_seq_no_key(table), 1).await
}

pub(crate) async fn delete_operator(table: &String, num_records: i64) -> Result<i64, CacheError> {
    // Decrease the number of records
    increment(&approx_num_records_key(table), -num_records).await?;
    // ...and up the seq no
    increment(&table_seq_no_key(table), 1).await
}

pub(crate) async fn update_operator(table: &String) -> Result<i64, CacheError> {
    increment(&table_seq_no_key(table), 1).await
}

pub(crate) async fn clear(tables: &Vec<String>) -> Result<(), CacheError> {
    for table in tables.into_iter() {
        create_table(&table).await?;
    }
    Ok(())
}
