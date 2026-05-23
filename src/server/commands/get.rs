use super::value_as_bytes;
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_get(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid GET argument: key".to_owned());
            };

            RespValue::BulkString(db.get(key))
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'get' command".to_owned()),
    }
}
