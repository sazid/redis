use crate::{build, db::RedisDb, resp::RespValue};

pub(super) fn handle_info(_items: &[RespValue], db: &mut RedisDb) -> RespValue {
    RespValue::BulkString(Some(
        format!(
            "# Server\r\nversion:{}\r\ngit:{}\r\nbuild:{}\r\n\r\n# Memory\r\nused_memory:{}\r\nmax_memory:{}\r\nkeys:{}\r\nexpires:{}\r\n",
            build::VERSION,
            build::GIT_HASH,
            build::BUILD_TYPE,
            db.memory_used(),
            db.max_memory().unwrap_or(0),
            db.key_count(),
            db.expires_count(),
        )
        .into_bytes(),
    ))
}
