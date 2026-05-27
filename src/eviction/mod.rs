use crate::db::RedisDb;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysRandom,
}

impl std::fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionPolicy::NoEviction => write!(f, "noeviction"),
            EvictionPolicy::AllKeysRandom => write!(f, "allkeys-random"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionError {
    MemoryLimitExceeded,
    NoEvictableKeys,
}

pub(crate) fn enforce_memory_limit(db: &mut RedisDb) -> Result<(), EvictionError> {
    let Some(max_memory) = db.max_memory() else {
        return Ok(());
    };

    while db.memory_used() > max_memory {
        match db.eviction_policy() {
            EvictionPolicy::NoEviction => return Err(EvictionError::MemoryLimitExceeded),
            EvictionPolicy::AllKeysRandom => {
                if !evict_all_keys_random(db) {
                    return Err(EvictionError::NoEvictableKeys);
                }
            }
        }
    }

    Ok(())
}

fn evict_all_keys_random(db: &mut RedisDb) -> bool {
    let Some(key) = db.random_key() else {
        return false;
    };

    db.delete(&key)
}
