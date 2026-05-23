use super::value_as_bytes;
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_ttl(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid TTL argument: key".to_owned());
            };

            RespValue::Integer(db.ttl(key))
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'ttl' command".to_owned()),
    }
}
