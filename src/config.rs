use std::collections::HashMap;

use serde::Deserialize;

use crate::error::Result;
use crate::raft::node::NodeId;
use crate::storage::StorageType;

#[derive(Debug, PartialEq, Deserialize)]
pub struct Config {
    pub id: NodeId,

    #[serde(default)]
    pub peers: HashMap<NodeId, String>,
    pub log_level: String,

    pub raft_storage_type: StorageType,
    pub raft_listen_addr: String,

    pub sql_storage_type: StorageType,
    pub sql_listen_addr: String,
}

impl Config {
    pub fn new(file: &str) -> Result<Config> {
        let mut cfg = config::Config::builder()
            .set_default("id", 1)?
            .set_default("log_level", "debug")?
            .set_default("raft_storage_type", "memory")?
            .set_default("raft_listen_addr", "0.0.0.0:8811")?
            .set_default("sql_storage_type", "memory")?
            .set_default("sql_listen_addr", "0.0.0.0:8911")?;
        if !file.is_empty() {
            cfg = cfg.add_source(config::File::with_name(file))
        }
        cfg = cfg.add_source(config::Environment::with_prefix("SBOXDB"));
        Ok(cfg.build()?.try_deserialize()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() -> Result<()> {
        let cfg = Config::new("")?;
        assert_eq!(StorageType::Memory, cfg.raft_storage_type);
        Ok(())
    }
}
