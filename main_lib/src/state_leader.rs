use std::time::Duration;

use crate::state_peers::{PeerClient, SelfPeer};
use ttl_cache::TtlCache;






pub(crate) trait LeaderClient {
    fn get_peers(&self) -> Vec<Box<dyn PeerClient>>;

    #[allow(dead_code)]
    fn get_current_snapshot(&mut self, table_name: String, extensions: Option<String>) -> String;
}

struct RealLeader {
    #[allow(dead_code)]
    cache: TtlCache<String, String>,
    #[allow(dead_code)]
    ttl: Duration,
}

impl RealLeader {
    fn new() -> Self {
        RealLeader {
            cache: TtlCache::new(100),
            ttl: Duration::from_secs(10 * 60),
        }
    }
}


impl LeaderClient for RealLeader {
    fn get_peers(&self) -> Vec<Box<dyn PeerClient>> {
        vec!(Box::new(SelfPeer::new()))
    }

    fn get_current_snapshot(&mut self, table_name: String, extensions: Option<String>) -> String {
        //let table_name_clone = table_name.clone();
        //let extensions_clone = extensions.clone();
        let key = match extensions {
            Some(e) => format!("{table_name},{e}"),
            None => table_name,
        };
        let checkpoint = self.cache.get(&key);

        match checkpoint {
            Some(c) => c.clone(),
            None => {
                /* TODO
                let retrieved_checkpoint = API_SERVICE_CLIENT.get_latest_checkpoint(
                    table_name_clone, extensions_clone
                );
                match retrieved_checkpoint {
                    Ok(c) => {
                        let c_clone = c.clone();
                        self.cache.insert(key, c_clone, self.ttl);
                        c
                    }
                    Err(_e) => {
                        panic!("This needs to be pulled from the Raft impl and therefore never fail.")
                    }
                }
                */
                "super fake".to_string()
            }
        }       
    }
}


static LEADER_CLIENT: std::sync::LazyLock<RealLeader> = std::sync::LazyLock::new(|| RealLeader::new());


pub(crate) fn get_leader() -> &'static dyn LeaderClient {
    &*LEADER_CLIENT
}

