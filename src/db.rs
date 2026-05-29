use indexmap::IndexMap;
use std::time::{Duration, Instant};

use crate::eviction::{EvictionError, EvictionPolicy, enforce_memory_limit};

/// How many samples to take from total population when actively
/// cleaning up expired keys.
const ACTIVE_EXPIRE_SAMPLE_SIZE: usize = 20;
/// Rough estimate for `values` map entry overhead.
const VALUES_ENTRY_OVERHEAD: usize = 64;
/// Rough estimate for `expires` map entry overhead.
const EXPIRES_ENTRY_OVERHEAD: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisDbConfig {
    pub max_memory: Option<usize>,
    pub eviction_policy: EvictionPolicy,
}

impl Default for RedisDbConfig {
    fn default() -> Self {
        Self {
            max_memory: None,
            eviction_policy: EvictionPolicy::AllKeysRandom,
        }
    }
}

struct ValueEntry {
    value: Vec<u8>,
    eviction: EvictionEntryMeta,
}

enum EvictionEntryMeta {
    None,
    Sieve { weight: u8 },
    // Lru { last_accessed: u64 },
}

enum EvictionState {
    None,
    Sieve { hand: usize },
    // Lru { clock: u64 },
}

pub struct RedisDb {
    values: IndexMap<Vec<u8>, ValueEntry>,
    expires: IndexMap<Vec<u8>, Instant>,
    current_time: Instant,

    value_memory_used: usize,
    expires_memory_used: usize,

    config: RedisDbConfig,
    eviction_state: EvictionState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisDbError {
    OutOfMemory,
    NoEvictableKeys,
}

fn eviction_state_for(policy: EvictionPolicy) -> EvictionState {
    match policy {
        EvictionPolicy::AllKeysSieve => EvictionState::Sieve { hand: 0 },
        _ => EvictionState::None,
    }
}

fn entry_meta_for(policy: EvictionPolicy) -> EvictionEntryMeta {
    match policy {
        EvictionPolicy::AllKeysSieve => EvictionEntryMeta::Sieve { weight: 0 },
        _ => EvictionEntryMeta::None,
    }
}

#[inline(always)]
fn value_entry_memory_cost(key: &[u8], value: &[u8]) -> usize {
    key.len() + value.len() + VALUES_ENTRY_OVERHEAD
}

#[inline(always)]
fn expire_entry_memory_cost(key: &[u8]) -> usize {
    key.len() + std::mem::size_of::<Instant>() + EXPIRES_ENTRY_OVERHEAD
}

impl RedisDb {
    pub fn new() -> Self {
        Self::with_config(RedisDbConfig::default())
    }

    pub fn with_config(config: RedisDbConfig) -> Self {
        let eviction_state = eviction_state_for(config.eviction_policy);

        Self {
            values: IndexMap::new(),
            expires: IndexMap::new(),
            current_time: Instant::now(),

            value_memory_used: 0,
            expires_memory_used: 0,

            eviction_state,
            config,
        }
    }

    pub fn update_time(&mut self, now: Instant) {
        self.current_time = now;
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), RedisDbError> {
        if self.should_reject_write(&key, &value) {
            return Err(RedisDbError::OutOfMemory);
        }

        self.set_unchecked(key, value);

        self.enforce_memory_limit()?;

        Ok(())
    }

    fn set_unchecked(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let entry_cost = value_entry_memory_cost(&key, &value);

        // If overwriting, free old value memory first.
        if let Some(old_value) = self.values.get(&key) {
            self.value_memory_used -= value_entry_memory_cost(&key, &old_value.value);
        }

        self.value_memory_used += entry_cost;

        let entry = ValueEntry {
            value,
            eviction: entry_meta_for(self.config.eviction_policy),
        };
        self.values.insert(key.clone(), entry);

        // Redis SET clears existing TTL unless KEEPTTL is used.
        if self.expires.swap_remove(&key).is_some() {
            self.expires_memory_used -= expire_entry_memory_cost(&key);
        }
    }

    fn should_reject_write(&self, key: &[u8], value: &[u8]) -> bool {
        let Some(max_memory) = self.max_memory() else {
            return false;
        };

        if self.config.eviction_policy != EvictionPolicy::NoEviction {
            return false;
        }

        self.projected_memory_after_set(key, value) > max_memory
    }

    fn projected_memory_after_set(&self, key: &[u8], value: &[u8]) -> usize {
        let mut projected = self.memory_used();

        if let Some(old_value) = self.values.get(key) {
            projected -= value_entry_memory_cost(key, &old_value.value);
        }

        projected += value_entry_memory_cost(key, value);

        if self.expires.contains_key(key) {
            projected -= expire_entry_memory_cost(key);
        }

        projected
    }

    fn enforce_memory_limit(&mut self) -> Result<(), RedisDbError> {
        enforce_memory_limit(self).map_err(|err| match err {
            EvictionError::MemoryLimitExceeded => RedisDbError::OutOfMemory,
            EvictionError::NoEvictableKeys => RedisDbError::NoEvictableKeys,
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_expired(key) {
            self.delete(key);
            return None;
        }

        self.values.get(key).map(|entry| entry.value.clone())
    }

    pub fn exists(&mut self, key: &[u8]) -> bool {
        if self.is_expired(key) {
            self.delete(key);
            return false;
        }

        self.values.contains_key(key)
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.values.swap_remove(key) {
            self.value_memory_used -= value_entry_memory_cost(key, &entry.value);

            if self.expires.swap_remove(key).is_some() {
                self.expires_memory_used -= expire_entry_memory_cost(key);
            }

            true
        } else {
            false
        }
    }

    pub fn expire(&mut self, key: &[u8], ttl: Duration) -> bool {
        if !self.exists(key) {
            return false;
        }

        let had_expiry = self.expires.contains_key(key);
        self.expires.insert(key.to_vec(), self.current_time + ttl);

        if !had_expiry {
            self.expires_memory_used += expire_entry_memory_cost(key);
        }

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

    pub(crate) fn eviction_policy(&self) -> EvictionPolicy {
        self.config.eviction_policy
    }

    pub(crate) fn random_key(&self) -> Option<Vec<u8>> {
        if self.values.is_empty() {
            return None;
        }

        let index = fastrand::usize(..self.values.len());
        let (key, _) = self.values.get_index(index)?;
        Some(key.clone())
    }

    pub(crate) fn random_key_with_ttl(&self) -> Option<Vec<u8>> {
        if self.expires.is_empty() {
            return None;
        }

        let index = fastrand::usize(..self.expires.len());
        let (key, _) = self.expires.get_index(index)?;
        Some(key.clone())
    }

    pub(crate) fn key_with_shortest_ttl(&self) -> Option<Vec<u8>> {
        if self.expires.is_empty() {
            return None;
        }

        let sample_size = self.expires.len().min(20);

        let mut index = fastrand::usize(..self.expires.len());
        let (mut best_key, mut best_expiry) = match self.expires.get_index(index) {
            Some((key, &expiry)) => (key.clone(), expiry),
            None => return None,
        };

        for _ in 0..sample_size - 1 {
            index = fastrand::usize(..self.expires.len());
            let Some((key, &expires_at)) = self.expires.get_index(index) else {
                continue;
            };
            if expires_at < best_expiry {
                best_expiry = expires_at;
                best_key = key.clone();
            }
        }

        Some(best_key)
    }

    pub fn memory_used(&self) -> usize {
        self.value_memory_used + self.expires_memory_used
    }

    pub fn key_count(&self) -> usize {
        self.values.len()
    }

    pub fn expires_count(&self) -> usize {
        self.expires.len()
    }

    pub fn max_memory(&self) -> Option<usize> {
        self.config.max_memory
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

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();

        assert_eq!(db.get(b"foo"), Some(b"bar".to_vec()));
    }

    #[test]
    fn set_overwrites_existing_value() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"old".to_vec()).unwrap();
        db.set(b"foo".to_vec(), b"new".to_vec()).unwrap();

        assert_eq!(db.get(b"foo"), Some(b"new".to_vec()));
    }

    #[test]
    fn delete_removes_existing_key() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();

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

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();

        assert!(db.expire(b"foo", Duration::from_secs(10)));
    }

    #[test]
    fn get_returns_value_before_expiry() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.expire(b"foo", Duration::from_secs(10));
        db.update_time(start + Duration::from_secs(9));

        assert_eq!(db.get(b"foo"), Some(b"bar".to_vec()));
    }

    #[test]
    fn get_lazily_deletes_expired_key() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
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

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.expire(b"foo", Duration::ZERO);

        assert_eq!(db.get(b"foo"), None);
    }

    #[test]
    fn set_clears_existing_expiry() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"old".to_vec()).unwrap();
        db.expire(b"foo", Duration::from_secs(10));
        db.set(b"foo".to_vec(), b"new".to_vec()).unwrap();
        db.update_time(start + Duration::from_secs(10));

        assert_eq!(db.get(b"foo"), Some(b"new".to_vec()));
        assert!(!db.expires.contains_key(&b"foo"[..]));
    }

    #[test]
    fn delete_removes_expiry_metadata() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.expire(b"foo", Duration::from_secs(10));
        db.delete(b"foo");

        assert!(!db.expires.contains_key(&b"foo"[..]));
    }

    #[test]
    fn exists_returns_true_for_existing_key() {
        let mut db = RedisDb::new();

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();

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

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
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

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();

        assert_eq!(db.ttl(b"foo"), -1);
    }

    #[test]
    fn ttl_returns_remaining_seconds_for_key_with_expiry() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.expire(b"foo", Duration::from_secs(10));
        db.update_time(start + Duration::from_secs(4));

        assert_eq!(db.ttl(b"foo"), 6);
    }

    #[test]
    fn ttl_lazily_deletes_expired_key() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
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
            db.set(key.clone(), b"value".to_vec()).unwrap();
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
            db.set(key.clone(), b"value".to_vec()).unwrap();
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
            db.set(key.clone(), b"value".to_vec()).unwrap();
            db.expire(&key, Duration::from_secs(10));
        }

        for i in 0..10 {
            db.set(format!("persistent-{i}").into_bytes(), b"value".to_vec())
                .unwrap();
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
        assert_eq!(db.value_memory_used, 0);
        assert_eq!(db.expires_memory_used, 0);
    }

    #[test]
    fn set_increases_value_memory_used() {
        let mut db = RedisDb::new();
        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();

        let cost = value_entry_memory_cost(b"key", b"value");
        assert_eq!(db.value_memory_used, cost);
        assert_eq!(db.expires_memory_used, 0);
        assert_eq!(db.memory_used(), cost);
    }

    #[test]
    fn set_overwrite_correctly_updates_value_memory() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"old".to_vec()).unwrap();
        let old_cost = value_entry_memory_cost(b"key", b"old");
        assert_eq!(db.memory_used(), old_cost);

        db.set(b"key".to_vec(), b"new".to_vec()).unwrap();
        let new_cost = value_entry_memory_cost(b"key", b"new");
        assert_eq!(db.value_memory_used, new_cost);
        assert_eq!(db.expires_memory_used, 0);
        assert_eq!(db.memory_used(), new_cost);
    }

    #[test]
    fn set_overwrite_with_smaller_value_frees_value_memory() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"bigvalue".to_vec()).unwrap();
        let big_cost = value_entry_memory_cost(b"key", b"bigvalue");
        assert_eq!(db.memory_used(), big_cost);

        db.set(b"key".to_vec(), b"x".to_vec()).unwrap();
        let small_cost = value_entry_memory_cost(b"key", b"x");
        assert!(db.memory_used() < big_cost);
        assert_eq!(db.value_memory_used, small_cost);
        assert_eq!(db.memory_used(), small_cost);
    }

    #[test]
    fn delete_decreases_memory_used() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        let cost = value_entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);

        db.delete(b"key");
        assert_eq!(db.value_memory_used, 0);
        assert_eq!(db.expires_memory_used, 0);
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn delete_nonexistent_key_does_not_affect_memory() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        let cost = value_entry_memory_cost(b"key", b"value");
        assert_eq!(db.memory_used(), cost);

        db.delete(b"missing");
        assert_eq!(db.memory_used(), cost);
    }

    #[test]
    fn multiple_keys_track_independent_memory() {
        let mut db = RedisDb::new();

        db.set(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        let cost1 = value_entry_memory_cost(b"k1", b"v1");

        db.set(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        let cost2 = value_entry_memory_cost(b"k2", b"v2");

        assert_eq!(db.memory_used(), cost1 + cost2);

        db.delete(b"k1");
        assert_eq!(db.memory_used(), cost2);

        db.delete(b"k2");
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn expire_increases_expiry_memory_used() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        let value_cost = value_entry_memory_cost(b"key", b"value");
        let expire_cost = expire_entry_memory_cost(b"key");
        assert_eq!(db.memory_used(), value_cost);

        db.expire(b"key", Duration::from_secs(10));
        assert_eq!(db.value_memory_used, value_cost);
        assert_eq!(db.expires_memory_used, expire_cost);
        assert_eq!(db.memory_used(), value_cost + expire_cost);
    }

    #[test]
    fn updating_existing_expiry_does_not_increase_memory() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        db.expire(b"key", Duration::from_secs(10));
        let cost = value_entry_memory_cost(b"key", b"value") + expire_entry_memory_cost(b"key");
        assert_eq!(db.memory_used(), cost);

        db.expire(b"key", Duration::from_secs(20));
        assert_eq!(db.memory_used(), cost);
    }

    #[test]
    fn lazy_delete_on_expired_key_frees_value_and_expiry_memory() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        db.expire(b"key", Duration::from_secs(10));
        let cost = value_entry_memory_cost(b"key", b"value") + expire_entry_memory_cost(b"key");
        assert_eq!(db.memory_used(), cost);

        db.update_time(start + Duration::from_secs(10));

        // get on expired key triggers lazy delete
        db.get(b"key");
        assert_eq!(db.value_memory_used, 0);
        assert_eq!(db.expires_memory_used, 0);
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn lazy_delete_on_expired_exists_frees_value_and_expiry_memory() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        db.expire(b"key", Duration::from_secs(10));
        let cost = value_entry_memory_cost(b"key", b"value") + expire_entry_memory_cost(b"key");
        assert_eq!(db.memory_used(), cost);

        db.update_time(start + Duration::from_secs(10));

        // exists on expired key triggers lazy delete
        db.exists(b"key");
        assert_eq!(db.value_memory_used, 0);
        assert_eq!(db.expires_memory_used, 0);
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn lazy_delete_on_expired_ttl_frees_value_and_expiry_memory() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        db.expire(b"key", Duration::from_secs(10));
        let cost = value_entry_memory_cost(b"key", b"value") + expire_entry_memory_cost(b"key");
        assert_eq!(db.memory_used(), cost);

        db.update_time(start + Duration::from_secs(10));

        // ttl on expired key triggers lazy delete
        db.ttl(b"key");
        assert_eq!(db.value_memory_used, 0);
        assert_eq!(db.expires_memory_used, 0);
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn active_expire_sample_frees_memory_on_expired_keys() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        for i in 0..10 {
            let key = format!("key-{i}");
            db.set(key.clone().into_bytes(), b"value".to_vec()).unwrap();
            db.expire(key.as_bytes(), Duration::from_secs(10));
        }

        let cost_per_entry =
            value_entry_memory_cost(b"key-0", b"value") + expire_entry_memory_cost(b"key-0");
        assert_eq!(db.memory_used(), cost_per_entry * 10);

        db.update_time(start + Duration::from_secs(10));
        db.active_expire_sample();

        assert_eq!(db.value_memory_used, 0);
        assert_eq!(db.expires_memory_used, 0);
        assert_eq!(db.memory_used(), 0);
    }

    #[test]
    fn active_expire_sample_keeps_memory_for_unexpired_keys() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        for i in 0..10 {
            let key = format!("key-{i}");
            db.set(key.clone().into_bytes(), b"value".to_vec()).unwrap();
            db.expire(key.as_bytes(), Duration::from_secs(10));
        }

        let cost_per_entry =
            value_entry_memory_cost(b"key-0", b"value") + expire_entry_memory_cost(b"key-0");
        assert_eq!(db.memory_used(), cost_per_entry * 10);

        // Advance time but not past expiry
        db.update_time(start + Duration::from_secs(5));
        db.active_expire_sample();

        assert_eq!(db.memory_used(), cost_per_entry * 10);
    }

    #[test]
    fn set_clears_expiry_and_frees_expiry_memory() {
        let mut db = RedisDb::new();

        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        db.expire(b"key", Duration::from_secs(10));
        let value_cost = value_entry_memory_cost(b"key", b"value");
        let expire_cost = expire_entry_memory_cost(b"key");
        assert_eq!(db.memory_used(), value_cost + expire_cost);

        // SET without EX clears TTL and frees expiry metadata memory.
        db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        assert_eq!(db.value_memory_used, value_cost);
        assert_eq!(db.expires_memory_used, 0);
        assert_eq!(db.memory_used(), value_cost);
    }

    #[test]
    fn value_entry_memory_cost_includes_value_overhead() {
        let key = b"k";
        let value = b"v";
        let cost = value_entry_memory_cost(key, value);

        assert_eq!(cost, key.len() + value.len() + VALUES_ENTRY_OVERHEAD);
    }

    #[test]
    fn expire_entry_memory_cost_includes_expiry_overhead() {
        let key = b"k";
        let cost = expire_entry_memory_cost(key);

        assert_eq!(
            cost,
            key.len() + std::mem::size_of::<Instant>() + EXPIRES_ENTRY_OVERHEAD
        );
    }

    #[test]
    fn default_config_has_no_max_memory() {
        let db = RedisDb::new();

        assert_eq!(db.max_memory(), None);
    }

    #[test]
    fn with_config_stores_max_memory() {
        let db = RedisDb::with_config(RedisDbConfig {
            max_memory: Some(1024),
            ..RedisDbConfig::default()
        });

        assert_eq!(db.max_memory(), Some(1024));
    }

    #[test]
    fn no_max_memory_does_not_evict() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec()).unwrap();
        assert!(db.enforce_memory_limit().is_ok());
    }

    #[test]
    fn allkeys_random_evicts_until_under_limit() {
        let config = RedisDbConfig {
            max_memory: Some(value_entry_memory_cost(b"k", b"v") * 2),
            ..RedisDbConfig::default()
        };
        let mut db = RedisDb::with_config(config);
        for i in 0..10 {
            let _ = db.set(format!("k{i}").into_bytes(), b"v".to_vec());
        }
        db.enforce_memory_limit().unwrap();
        assert!(db.memory_used() <= db.max_memory().unwrap());
    }

    #[test]
    fn noeviction_rejects_oversized_write() {
        let config = RedisDbConfig {
            max_memory: Some(100),
            eviction_policy: EvictionPolicy::NoEviction,
        };
        let mut db = RedisDb::with_config(config);
        let result = db.set(b"big".to_vec(), vec![0u8; 1000]);
        assert_eq!(result, Err(RedisDbError::OutOfMemory));
    }

    #[test]
    fn volatile_random_evicts_only_keys_with_ttl() {
        let config = RedisDbConfig {
            max_memory: Some(value_entry_memory_cost(b"k", b"v") * 5),
            eviction_policy: EvictionPolicy::VolatileRandom,
        };
        let mut db = RedisDb::with_config(config);

        db.set(b"persistent".to_vec(), b"v".to_vec()).unwrap();
        for i in 0..10 {
            let key = format!("k{i}").into_bytes();
            db.set(key.clone(), b"v".to_vec()).unwrap();
            db.expire(&key, Duration::from_secs(100));
        }

        assert!(db.exists(b"persistent"));
        assert!(db.memory_used() <= db.max_memory().unwrap());
    }

    #[test]
    fn volatile_random_returns_error_when_no_keys_have_ttl() {
        let config = RedisDbConfig {
            max_memory: Some(100),
            eviction_policy: EvictionPolicy::VolatileRandom,
        };
        let mut db = RedisDb::with_config(config);

        let result = db.set(b"no_ttl".to_vec(), vec![0u8; 1000]);
        assert_eq!(result, Err(RedisDbError::NoEvictableKeys));
    }

    #[test]
    fn volatile_ttl_evicts_key_with_shortest_ttl() {
        let start = Instant::now();
        let config = RedisDbConfig {
            max_memory: Some(value_entry_memory_cost(b"k", b"v") * 3),
            eviction_policy: EvictionPolicy::VolatileTTL,
        };
        let mut db = RedisDb::with_config(config);
        db.update_time(start);

        db.set(b"long".to_vec(), b"v".to_vec()).unwrap();
        db.expire(b"long", Duration::from_secs(1000));

        db.set(b"short".to_vec(), b"v".to_vec()).unwrap();
        db.expire(b"short", Duration::from_secs(10));

        db.set(b"overflow".to_vec(), b"v".to_vec()).unwrap();

        assert!(db.exists(b"long"));
        assert!(!db.exists(b"short"));
    }
}
