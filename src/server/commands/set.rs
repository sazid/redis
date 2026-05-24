use std::time::Duration;

use super::{value_as_bytes, value_as_i64};
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_set(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key, value] => set_key_value(key, value, db, None),

        [_command, key, value, ex, ttl] => {
            let Some(ex) = value_as_bytes(ex) else {
                return RespValue::Error("ERR invalid SET argument: ex".to_owned());
            };
            if !ex.eq_ignore_ascii_case(b"EX") {
                return RespValue::Error("ERR invalid SET argument: ex".to_owned());
            }
            let Some(ttl) = value_as_i64(ttl) else {
                return RespValue::Error("ERR invalid EXPIRE argument: ttl".to_owned());
            };
            if ttl <= 0 {
                return RespValue::Error("ERR ttl must be greater than 0".to_owned());
            }

            set_key_value(key, value, db, Some(Duration::from_secs(ttl as u64)))
        }
        _ => RespValue::Error("ERR wrong number of arguments for 'set' command".to_owned()),
    }
}

fn set_key_value(
    key: &RespValue,
    value: &RespValue,
    db: &mut RedisDb,
    expiry: Option<Duration>,
) -> RespValue {
    let Some(key) = value_as_bytes(key) else {
        return RespValue::Error("ERR invalid SET argument: key".to_owned());
    };
    let Some(value) = value_as_bytes(value) else {
        return RespValue::Error("ERR invalid SET argument: value".to_owned());
    };

    db.set(key.to_owned(), value.to_owned());
    if let Some(duration) = expiry {
        db.expire(key, duration);
    }
    RespValue::SimpleString("OK".to_owned())
}
