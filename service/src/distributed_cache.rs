use std::{error::Error, fmt::Display};

use redis::{Commands, FromRedisValue, ToRedisArgs};

#[derive(Debug)]
pub(crate) struct CacheError {}

impl Display for CacheError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for CacheError {}


const REDIS_URL: &str = "redis://127.0.0.1:6379/";


fn get_connection() -> Result<redis::Connection, CacheError> {
    let client = match redis::Client::open(REDIS_URL) {
        Ok(c) => c,
        Err(_) => return Err(CacheError { }),
    };
    match client.get_connection() {
        Ok(c) => Ok(c),
        Err(_) => Err(CacheError {  })
    }
}


fn increment(key: &String, delta: i64) -> Result<i64, CacheError>  {
    let con = &mut get_connection()?;

    match redis::pipe()
        .atomic()
        .cmd("INCRBY")
        .arg(key)
        .arg(delta)
        .ignore()
        .cmd("GET")
        .arg(key)
        .query::<Vec<String>>(con) {
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


fn get<T: FromRedisValue>(key: &String) -> Result<T, CacheError> {
    let mut con = get_connection()?;

    match con.get(key) {
        Ok(v) => Ok(v),
        Err(_) => panic!("Time for some debug")
    }
}



fn set<T: ToRedisArgs>(key: &String, value: T) -> Result<(), CacheError> {
    let mut con = get_connection()?;

    match con.set::<&String, T, String>(key, value) {
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

pub(crate) fn get_approx_num_records(table: &String) -> Result<u64, CacheError> {
    get::<u64>(&approx_num_records_key(table))
}

pub(crate) fn create_table(table: &String) -> Result<(), CacheError> {
    set(&approx_num_records_key(table), 0)?;
    set(&table_seq_no_key(table), 0)
}

pub(crate) fn insert_operator(table: &String, num_records: i64) -> Result<i64, CacheError> {
    // Increase the number of records
    increment(&approx_num_records_key(table), num_records)?;
    // ...and bump the seq no
    increment(&table_seq_no_key(table), 1)
}

pub(crate) fn delete_operator(table: &String, num_records: i64) -> Result<i64, CacheError> {
    // Decrease the number of records
    increment(&approx_num_records_key(table), -num_records)?;
    // ...and dump the seq no
    increment(&table_seq_no_key(table), 1)
}  

pub(crate) fn clear(tables: &Vec<String>) -> Result<(), CacheError> {
    for table in tables.into_iter() {
        create_table(&table)?;
    }
    Ok(())
}
