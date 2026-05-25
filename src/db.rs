use indexmap::IndexMap;
use std::time::{Duration, Instant};

const ACTIVE_EXPIRE_SAMPLE_SIZE: usize = 20;

pub struct RedisDb {
    values: IndexMap<Vec<u8>, Vec<u8>>,
    expires: IndexMap<Vec<u8>, Instant>,
    current_time: Instant,
}

impl RedisDb {
    pub fn new() -> Self {
        Self {
            values: IndexMap::new(),
            expires: IndexMap::new(),
            current_time: Instant::now(),
        }
    }

    pub fn update_time(&mut self, now: Instant) {
        self.current_time = now;
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.values.insert(key.clone(), value);

        // Redis SET clears existing TTL unless KEEPTTL is used.
        self.expires.swap_remove(&key);
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
        let existed = self.values.swap_remove(key).is_some();
        self.expires.swap_remove(key);
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

    pub fn active_expire_sample(&mut self) {
        if self.expires.is_empty() {
            return;
        }
        let now = self.current_time;
        'outer: for _ in 0..10 {
            let mut expired_count = 0;
            for _ in 0..ACTIVE_EXPIRE_SAMPLE_SIZE {
                if self.expires.is_empty() {
                    break 'outer;
                }
                let index = fastrand::usize(..self.expires.len());

                let expired_key = self
                    .expires
                    .get_index(index)
                    .and_then(|(key, &expires_at)| {
                        if now >= expires_at {
                            Some(key.clone())
                        } else {
                            None
                        }
                    });

                if let Some(key) = expired_key {
                    self.delete(&key);
                    expired_count += 1;
                }
            }

            // If sample size is greater than 25% of the ACTIVE_EXPIRE_SAMPLE_SIZE
            // rerun the loop again because the sample size is the representative
            // of the total key population - which means there might be many stale
            // keys that needs to be cleaned up.
            if expired_count <= 5 {
                break;
            }
        }
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

    #[test]
    fn ttl_returns_minus_2_for_missing_key() {
        let mut db = RedisDb::new();

        assert_eq!(db.ttl(b"missing"), -2);
    }

    #[test]
    fn ttl_returns_minus_1_for_key_without_expiry() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"bar".to_vec());

        assert_eq!(db.ttl(b"foo"), -1);
    }

    #[test]
    fn ttl_returns_remaining_seconds_for_key_with_expiry() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec());
        db.expire(b"foo", Duration::from_secs(10));
        db.update_time(start + Duration::from_secs(4));

        assert_eq!(db.ttl(b"foo"), 6);
    }

    #[test]
    fn ttl_lazily_deletes_expired_key() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec());
        db.expire(b"foo", Duration::from_secs(10));
        db.update_time(start + Duration::from_secs(10));

        assert_eq!(db.ttl(b"foo"), -2);
        assert!(!db.values.contains_key(&b"foo"[..]));
        assert!(!db.expires.contains_key(&b"foo"[..]));
    }

    #[test]
    fn active_expire_sample_removes_expired_keys() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        for i in 0..50 {
            let key = format!("key-{i}").into_bytes();
            db.set(key.clone(), b"value".to_vec());
            db.expire(&key, Duration::from_secs(10));
        }

        db.update_time(start + Duration::from_secs(10));
        db.active_expire_sample();

        assert!(db.values.is_empty());
        assert!(db.expires.is_empty());
    }

    #[test]
    fn active_expire_sample_keeps_unexpired_keys() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        for i in 0..50 {
            let key = format!("key-{i}").into_bytes();
            db.set(key.clone(), b"value".to_vec());
            db.expire(&key, Duration::from_secs(10));
        }

        db.update_time(start + Duration::from_secs(5));
        db.active_expire_sample();

        assert_eq!(db.values.len(), 50);
        assert_eq!(db.expires.len(), 50);
    }

    #[test]
    fn active_expire_sample_keeps_keys_without_expiry() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        for i in 0..50 {
            let key = format!("expired-{i}").into_bytes();
            db.set(key.clone(), b"value".to_vec());
            db.expire(&key, Duration::from_secs(10));
        }

        for i in 0..10 {
            db.set(format!("persistent-{i}").into_bytes(), b"value".to_vec());
        }

        db.update_time(start + Duration::from_secs(10));
        db.active_expire_sample();

        assert_eq!(db.values.len(), 10);
        assert!(db.expires.is_empty());

        for i in 0..10 {
            assert_eq!(
                db.get(format!("persistent-{i}").as_bytes()),
                Some(b"value".to_vec())
            );
        }
    }
}
