use crate::test_api::CacheMode;
use lazy_static::lazy_static;
use redis::{Commands, FromRedisValue, ToRedisArgs};
use std::collections::HashMap;
use std::sync::Mutex;
use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub(crate) struct CacheError {}

impl Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Cache error")
    }
}

impl Error for CacheError {}

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379/";

#[derive(Debug, Clone)]
struct NativeTableState {
    approx_num_records: u64,
    next_seq_no: u64,
}

impl NativeTableState {
    fn new() -> Self {
        Self {
            approx_num_records: 0,
            next_seq_no: 0,
        }
    }
}

#[derive(Debug, Clone)]
enum CacheBackend {
    Redis {
        address: String,
    },
    Native {
        tables: HashMap<String, NativeTableState>,
    },
}

lazy_static! {
    static ref CACHE_BACKEND: Mutex<CacheBackend> = Mutex::new(CacheBackend::Native {
        tables: HashMap::new()
    });
}

pub fn set_cache_mode(mode: &CacheMode) {
    let mut backend = CACHE_BACKEND.lock().unwrap();
    *backend = match mode {
        CacheMode::Redis(address) => {
            let address = address
                .clone()
                .unwrap_or_else(|| DEFAULT_REDIS_URL.to_string());
            tracing::info!("Using redis distributed cache at {}", address);
            CacheBackend::Redis { address }
        }
        CacheMode::Native => {
            tracing::info!("Using native in-memory distributed cache backend");
            CacheBackend::Native {
                tables: HashMap::new(),
            }
        }
    };
}

fn with_redis_address<T>(
    f: impl FnOnce(&String) -> Result<T, CacheError>,
) -> Result<T, CacheError> {
    let address = {
        let backend = CACHE_BACKEND.lock().unwrap();
        match &*backend {
            CacheBackend::Redis { address } => address.clone(),
            CacheBackend::Native { .. } => {
                return Err(CacheError {});
            }
        }
    };
    f(&address)
}

fn get_connection_from_address(address: &String) -> Result<redis::Connection, CacheError> {
    let client = match redis::Client::open(address.clone()) {
        Ok(c) => c,
        Err(_) => {
            tracing::error!("Failed to connect to redis at {}", address);
            return Err(CacheError {});
        }
    };
    match client.get_connection() {
        Ok(c) => Ok(c),
        Err(_) => {
            tracing::error!("Failed to connect to redis at {}", address);
            Err(CacheError {})
        }
    }
}

fn increment_redis(key: &String, delta: i64) -> Result<i64, CacheError> {
    with_redis_address(|address| {
        let con = &mut get_connection_from_address(address)?;
        match redis::pipe()
            .atomic()
            .cmd("INCRBY")
            .arg(key)
            .arg(delta)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query::<Vec<String>>(con)
        {
            Ok(v) => Ok(v.get(0).unwrap().parse::<i64>().unwrap()),
            Err(error) => {
                tracing::error!("Redis increment failed for key {}: {}", key, error);
                Err(CacheError {})
            }
        }
    })
}

fn get_redis<T: FromRedisValue>(key: &String) -> Result<T, CacheError> {
    with_redis_address(|address| {
        let mut con = get_connection_from_address(address)?;
        match con.get(key) {
            Ok(v) => Ok(v),
            Err(error) => {
                tracing::error!("Redis get failed for key {}: {}", key, error);
                Err(CacheError {})
            }
        }
    })
}

fn set_redis<T: ToRedisArgs>(key: &String, value: T) -> Result<(), CacheError> {
    with_redis_address(|address| {
        let mut con = get_connection_from_address(address)?;
        match con.set::<&String, T, String>(key, value) {
            Ok(_) => Ok(()),
            Err(error) => {
                tracing::error!("Redis set failed for key {}: {}", key, error);
                Err(CacheError {})
            }
        }
    })
}

fn approx_num_records_key(table: &String) -> String {
    format!("{}_approx_num_records", table)
}

fn table_seq_no_key(table: &String) -> String {
    format!("{}_seq_no", table)
}

pub(crate) fn get_approx_num_records(table: &String) -> Result<u64, CacheError> {
    let backend = CACHE_BACKEND.lock().unwrap().clone();
    match backend {
        CacheBackend::Redis { .. } => get_redis::<u64>(&approx_num_records_key(table)),
        CacheBackend::Native { tables } => Ok(tables
            .get(table)
            .map(|state| state.approx_num_records)
            .unwrap_or_default()),
    }
}

pub(crate) fn create_table(table: &String) -> Result<(), CacheError> {
    let mut backend = CACHE_BACKEND.lock().unwrap();
    match &mut *backend {
        CacheBackend::Redis { .. } => {
            drop(backend);
            set_redis(&approx_num_records_key(table), 0)?;
            set_redis(&table_seq_no_key(table), 0)
        }
        CacheBackend::Native { tables } => {
            tables.insert(table.clone(), NativeTableState::new());
            Ok(())
        }
    }
}

pub(crate) fn report_table_changes(
    table: &String,
    num_inserts: usize,
    num_updates: usize,
    num_deletes: usize,
) -> Result<Vec<u64>, CacheError> {
    if num_inserts + num_updates + num_deletes == 0 {
        return Ok(vec![]);
    }

    let delta = (num_inserts + num_updates + num_deletes) as u64;
    let mut backend = CACHE_BACKEND.lock().unwrap();
    match &mut *backend {
        CacheBackend::Redis { .. } => {
            drop(backend);
            increment_redis(
                &approx_num_records_key(table),
                num_inserts as i64 - num_deletes as i64,
            )?;
            let new_last_seq_no = increment_redis(&table_seq_no_key(table), delta as i64)?;
            let values: Vec<u64> = (0..delta)
                .map(|offset| (new_last_seq_no as u64).saturating_sub(offset))
                .collect();
            Ok(values)
        }
        CacheBackend::Native { tables } => {
            let state = tables
                .entry(table.clone())
                .or_insert_with(NativeTableState::new);
            let new_approx =
                state.approx_num_records as i64 + num_inserts as i64 - num_deletes as i64;
            state.approx_num_records = new_approx.max(0) as u64;
            state.next_seq_no += delta;
            let new_last_seq_no = state.next_seq_no;
            let values: Vec<u64> = (0..delta).map(|offset| new_last_seq_no - offset).collect();
            Ok(values)
        }
    }
}
