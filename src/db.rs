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

    pub fn exists(&mut self, key: &[u8]) -> bool {
        if self.is_expired(key) {
            self.delete(key);
            return false;
        }

        self.values.contains_key(key)
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        let existed = self.values.remove(key).is_some();
        self.expires.remove(key);
        existed
    }

    pub fn expire(&mut self, key: &[u8], ttl: Duration) -> bool {
        if !self.exists(key) {
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

    pub fn ttl(&mut self, key: &[u8]) -> i64 {
        if !self.exists(key) {
            return -2;
        }

        let Some(expires_at) = self.expires.get(key) else {
            return -1;
        };

        expires_at
            .saturating_duration_since(self.current_time)
            .as_secs()
            .try_into()
            .unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_none_for_missing_key() {
        let mut db = RedisDb::new();

        assert_eq!(db.get(b"missing"), None);
    }

    #[test]
    fn set_then_get_returns_value() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"bar".to_vec());

        assert_eq!(db.get(b"foo"), Some(b"bar".to_vec()));
    }

    #[test]
    fn set_overwrites_existing_value() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"old".to_vec());
        db.set(b"foo".to_vec(), b"new".to_vec());

        assert_eq!(db.get(b"foo"), Some(b"new".to_vec()));
    }

    #[test]
    fn delete_removes_existing_key() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"bar".to_vec());

        assert!(db.delete(b"foo"));
        assert_eq!(db.get(b"foo"), None);
    }

    #[test]
    fn delete_returns_false_for_missing_key() {
        let mut db = RedisDb::new();

        assert!(!db.delete(b"missing"));
    }

    #[test]
    fn expire_returns_false_for_missing_key() {
        let mut db = RedisDb::new();

        assert!(!db.expire(b"missing", Duration::from_secs(10)));
    }

    #[test]
    fn expire_returns_true_for_existing_key() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"bar".to_vec());

        assert!(db.expire(b"foo", Duration::from_secs(10)));
    }

    #[test]
    fn get_returns_value_before_expiry() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec());
        db.expire(b"foo", Duration::from_secs(10));
        db.update_time(start + Duration::from_secs(9));

        assert_eq!(db.get(b"foo"), Some(b"bar".to_vec()));
    }

    #[test]
    fn get_lazily_deletes_expired_key() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec());
        db.expire(b"foo", Duration::from_secs(10));
        db.update_time(start + Duration::from_secs(10));

        assert_eq!(db.get(b"foo"), None);
        assert!(!db.values.contains_key(&b"foo"[..]));
        assert!(!db.expires.contains_key(&b"foo"[..]));
    }

    #[test]
    fn zero_duration_expiry_expires_immediately() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec());
        db.expire(b"foo", Duration::ZERO);

        assert_eq!(db.get(b"foo"), None);
    }

    #[test]
    fn set_clears_existing_expiry() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"old".to_vec());
        db.expire(b"foo", Duration::from_secs(10));
        db.set(b"foo".to_vec(), b"new".to_vec());
        db.update_time(start + Duration::from_secs(10));

        assert_eq!(db.get(b"foo"), Some(b"new".to_vec()));
        assert!(!db.expires.contains_key(&b"foo"[..]));
    }

    #[test]
    fn delete_removes_expiry_metadata() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"bar".to_vec());
        db.expire(b"foo", Duration::from_secs(10));
        db.delete(b"foo");

        assert!(!db.expires.contains_key(&b"foo"[..]));
    }

    #[test]
    fn exists_returns_true_for_existing_key() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"bar".to_vec());

        assert!(db.exists(b"foo"));
    }

    #[test]
    fn exists_returns_false_for_missing_key() {
        let mut db = RedisDb::new();

        assert!(!db.exists(b"missing"));
    }

    #[test]
    fn exists_lazily_deletes_expired_key() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec());
        db.expire(b"foo", Duration::from_secs(10));
        db.update_time(start + Duration::from_secs(10));

        assert!(!db.exists(b"foo"));
        assert!(!db.values.contains_key(&b"foo"[..]));
        assert!(!db.expires.contains_key(&b"foo"[..]));
    }
}
