use crate::db::RedisDb;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    VolatileRandom,
    VolatileTTL,
    AllKeysRandom,
    AllKeysLRU,
    AllKeysSieve,
}

impl std::fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionPolicy::NoEviction => write!(f, "noeviction"),
            EvictionPolicy::AllKeysRandom => write!(f, "allkeys-random"),
            EvictionPolicy::VolatileRandom => write!(f, "volatile-random"),
            EvictionPolicy::VolatileTTL => write!(f, "volatile-ttl"),
            EvictionPolicy::AllKeysLRU => write!(f, "allkeys-lru"),
            EvictionPolicy::AllKeysSieve => write!(f, "allkeys-sieve"),
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
                if !db.evict_all_keys_random() {
                    return Err(EvictionError::NoEvictableKeys);
                }
            }
            EvictionPolicy::VolatileRandom => {
                if !db.evict_volatile_random() {
                    return Err(EvictionError::NoEvictableKeys);
                }
            }
            EvictionPolicy::VolatileTTL => {
                if !db.evict_volatile_ttl() {
                    return Err(EvictionError::NoEvictableKeys);
                }
            }
            EvictionPolicy::AllKeysLRU => todo!(),
            EvictionPolicy::AllKeysSieve => {
                if !db.evict_sieve() {
                    return Err(EvictionError::NoEvictableKeys);
                }
            }
        }
    }

    Ok(())
}
