use crate::db::RedisDb;
use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EvictionPolicy {
    #[value(name = "noeviction")]
    NoEviction,
    #[value(name = "volatile-random")]
    VolatileRandom,
    #[value(name = "volatile-ttl")]
    VolatileTTL,
    #[value(name = "allkeys-random")]
    AllKeysRandom,
    #[allow(dead_code)]
    #[value(skip)]
    AllKeysLRU,
    #[value(name = "allkeys-sieve")]
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

impl std::str::FromStr for EvictionPolicy {
    type Err = String;

    fn from_str(policy: &str) -> Result<Self, Self::Err> {
        match policy.to_ascii_lowercase().as_str() {
            "noeviction" => Ok(EvictionPolicy::NoEviction),
            "allkeys-random" => Ok(EvictionPolicy::AllKeysRandom),
            "volatile-random" => Ok(EvictionPolicy::VolatileRandom),
            "volatile-ttl" => Ok(EvictionPolicy::VolatileTTL),
            "allkeys-sieve" => Ok(EvictionPolicy::AllKeysSieve),
            _ => Err(format!("unsupported eviction policy: {policy}")),
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
