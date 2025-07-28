use serde::Deserialize;

use crate::error::Result;
use crate::storage::StorageType;

#[derive(Debug, PartialEq, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct Config {
    pub log_level: LogLevel,
    pub storage_type: StorageType,
}

impl Config {
    #[allow(dead_code)]
    fn new(file: &str) -> Result<Config> {
        let mut cfg = config::Config::builder()
            .set_default("storage_type", "memory")?
            .set_default("log_level", "debug")?;
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
        assert_eq!(LogLevel::Debug, cfg.log_level);
        assert_eq!(StorageType::Memory, cfg.storage_type);
        Ok(())
    }
}
