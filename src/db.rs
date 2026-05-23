use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

pub struct RedisDb {
    values: HashMap<Vec<u8>, Vec<u8>>,
    expires: HashMap<Vec<u8>, Instant>,
    current_time: Instant,
}

impl RedisDb {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            expires: HashMap::new(),
            current_time: Instant::now(),
        }
    }

    pub fn update_time(&mut self, now: Instant) {
        self.current_time = now;
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.values.insert(key.clone(), value);

        // Redis SET clears existing TTL unless KEEPTTL is used.
        self.expires.remove(&key);
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_expired(key) {
            self.delete(key);
            return None;
        }

        self.values.get(key).cloned()
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        let existed = self.values.remove(key).is_some();
        self.expires.remove(key);
        existed
    }

    pub fn expire(&mut self, key: &[u8], ttl: Duration) -> bool {
        if !self.values.contains_key(key) {
            return false;
        }

        self.expires.insert(key.to_vec(), self.current_time + ttl);
        true
    }

    fn is_expired(&self, key: &[u8]) -> bool {
        self.expires
            .get(key)
            .is_some_and(|expires_at| self.current_time >= *expires_at)
    }
}
