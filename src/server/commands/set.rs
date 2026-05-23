use super::value_as_bytes;
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_set(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key, value] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid SET argument: key".to_owned());
            };
            let Some(value) = value_as_bytes(value) else {
                return RespValue::Error("ERR invalid SET argument: value".to_owned());
            };

            db.set(key.to_owned(), value.to_owned());

            RespValue::SimpleString("OK".to_owned())
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'set' command".to_owned()),
    }
}
