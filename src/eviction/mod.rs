use crate::db::RedisDb;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysRandom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionError {
    MemoryLimitExceeded,
    NoEvictableKeys,
}

pub(crate) fn enforce_memory_limit(db: &mut RedisDb) -> Result<(), EvictionError> {
    Ok(())
}
