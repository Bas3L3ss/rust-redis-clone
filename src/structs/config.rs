use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct Config {
    pub expire_at: Option<u64>, // epoch in ms
    pub updated_at: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            expire_at: None,
            updated_at: None,
        }
    }
}

impl Config {
    pub fn is_expired(&self) -> bool {
        if let Some(expire_ts) = self.expire_at {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            return now_ms >= expire_ts;
        }
        false
    }
}
