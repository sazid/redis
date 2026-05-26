use indexmap::IndexMap;
use std::time::{Duration, Instant};

/// How many samples to take from total population when actively
/// cleaning up expired keys.
const ACTIVE_EXPIRE_SAMPLE_SIZE: usize = 20;
/// Rough estimate for IndexMap internal + HashMap bucket
const VALUES_ENTRY_OVERHEAD: usize = 64;
const EXPIRES_ENTRY_OVERHEAD: usize = 32;

pub struct RedisDb {
    values: IndexMap<Vec<u8>, Vec<u8>>,
    expires: IndexMap<Vec<u8>, Instant>,
    current_time: Instant,
    memory_used: usize,
}

#[inline(always)]
fn entry_memory_cost(key: &[u8], value: &[u8]) -> usize {
    key.len() + value.len() + VALUES_ENTRY_OVERHEAD + EXPIRES_ENTRY_OVERHEAD
}

impl RedisDb {
    pub fn new() -> Self {
        Self {
            values: IndexMap::new(),
            expires: IndexMap::new(),
            current_time: Instant::now(),
            memory_used: 0,
        }
    }

    pub fn update_time(&mut self, now: Instant) {
        self.current_time = now;
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let entry_cost = entry_memory_cost(&key, &value);

        // If overwriting, free old memory first
        if let Some(old_value) = self.values.get(&key) {
            self.memory_used -= entry_memory_cost(&key, old_value);
        }

        self.memory_used += entry_cost;
        self.values.insert(key.clone(), value);
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
        if let Some(value) = self.values.swap_remove(key) {
            self.memory_used -= entry_memory_cost(key, &value);
            self.expires.swap_remove(key);
            true
        } else {
            false
        }
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

    pub fn memory_used(&self) -> usize {
        self.memory_used
    }

    pub fn key_count(&self) -> usize {
        self.values.len()
    }

    pub fn expires_count(&self) -> usize {
        self.expires.len()
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

    #[test]
    fn memory_used_starts_at_zero() {
        let db = RedisDb::new();
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn set_increases_memory_used() {
        let mut db = RedisDb::new();
        db.set(b"key".to_vec(), b"value".to_vec());

        let cost = entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);
    }

    #[test]
    fn set_overwrite_correctly_updates_memory() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"old".to_vec());
        let old_cost = entry_memory_cost(b"key", b"old");
        assert_eq!(db.memory_used(), old_cost);

        db.set(b"key".to_vec(), b"new".to_vec());
        let new_cost = entry_memory_cost(b"key", b"new");
        assert_eq!(db.memory_used(), new_cost);
    }

    #[test]
    fn set_overwrite_with_smaller_value_frees_memory() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"bigvalue".to_vec());
        let big_cost = entry_memory_cost(b"key", b"bigvalue");
        assert_eq!(db.memory_used(), big_cost);

        db.set(b"key".to_vec(), b"x".to_vec());
        let small_cost = entry_memory_cost(b"key", b"x");
        assert!(db.memory_used() < big_cost);
        assert_eq!(db.memory_used(), small_cost);
    }

    #[test]
    fn delete_decreases_memory_used() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"value".to_vec());
        let cost = entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);

        db.delete(b"key");
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn delete_nonexistent_key_does_not_affect_memory() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"value".to_vec());
        let cost = entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);

        db.delete(b"missing");
        assert_eq!(db.memory_used(), cost);
    }

    #[test]
    fn multiple_keys_track_independent_memory() {
        let mut db = RedisDb::new();

        db.set(b"k1".to_vec(), b"v1".to_vec());
        let cost1 = entry_memory_cost(b"k1", b"v1");

        db.set(b"k2".to_vec(), b"v2".to_vec());
        let cost2 = entry_memory_cost(b"k2", b"v2");

        assert_eq!(db.memory_used(), cost1 + cost2);

        db.delete(b"k1");
        assert_eq!(db.memory_used(), cost2);

        db.delete(b"k2");
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn expire_does_not_affect_memory() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"value".to_vec());
        let cost = entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);

        db.expire(b"key", Duration::from_secs(10));
        assert_eq!(db.memory_used(), cost);
    }

    #[test]
    fn lazy_delete_on_expired_key_frees_memory() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"key".to_vec(), b"value".to_vec());
        db.expire(b"key", Duration::from_secs(10));
        let cost = entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);

        db.update_time(start + Duration::from_secs(10));

        // get on expired key triggers lazy delete
        db.get(b"key");
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn lazy_delete_on_expired_exists_frees_memory() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"key".to_vec(), b"value".to_vec());
        db.expire(b"key", Duration::from_secs(10));
        let cost = entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);

        db.update_time(start + Duration::from_secs(10));

        // exists on expired key triggers lazy delete
        db.exists(b"key");
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn lazy_delete_on_expired_ttl_frees_memory() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"key".to_vec(), b"value".to_vec());
        db.expire(b"key", Duration::from_secs(10));
        let cost = entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);

        db.update_time(start + Duration::from_secs(10));

        // ttl on expired key triggers lazy delete
        db.ttl(b"key");
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn active_expire_sample_frees_memory_on_expired_keys() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        for i in 0..10 {
            let key = format!("key-{i}");
            db.set(key.clone().into_bytes(), b"value".to_vec());
            db.expire(key.as_bytes(), Duration::from_secs(10));
        }

        let cost_per_entry = entry_memory_cost(b"key-0", b"value");
        assert_eq!(db.memory_used(), cost_per_entry * 10);

        db.update_time(start + Duration::from_secs(10));
        db.active_expire_sample();

        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn active_expire_sample_keeps_memory_for_unexpired_keys() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        for i in 0..10 {
            let key = format!("key-{i}");
            db.set(key.clone().into_bytes(), b"value".to_vec());
            db.expire(key.as_bytes(), Duration::from_secs(10));
        }

        let cost_per_entry = entry_memory_cost(b"key-0", b"value");
        assert_eq!(db.memory_used(), cost_per_entry * 10);

        // Advance time but not past expiry
        db.update_time(start + Duration::from_secs(5));
        db.active_expire_sample();

        assert_eq!(db.memory_used(), cost_per_entry * 10);
    }

    #[test]
    fn set_clears_expiry_but_keeps_memory_same() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"value".to_vec());
        db.expire(b"key", Duration::from_secs(10));
        let cost = entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);

        // SET without EX clears TTL but doesn't change memory
        db.set(b"key".to_vec(), b"value".to_vec());
        assert_eq!(db.memory_used(), cost);
    }

    #[test]
    fn entry_memory_cost_includes_overhead() {
        // Verify the cost includes key + value + both overheads
        let key = b"k";
        let value = b"v";
        let cost = entry_memory_cost(key, value);

        assert_eq!(
            cost,
            key.len() + value.len() + VALUES_ENTRY_OVERHEAD + EXPIRES_ENTRY_OVERHEAD
        );
    }
}
