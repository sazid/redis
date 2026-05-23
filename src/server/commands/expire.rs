use std::time::Duration;

use super::{value_as_bytes, value_as_i64};
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_expire(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key, ttl] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid EXPIRE argument: key".to_owned());
            };

            let Some(ttl) = value_as_i64(ttl) else {
                return RespValue::Error("ERR invalid EXPIRE argument: ttl".to_owned());
            };

            if !db.exists(key) {
                return RespValue::Integer(0);
            }

            if ttl <= 0 {
                db.delete(key);
                return RespValue::Integer(1);
            }

            let did_expire = db.expire(key, Duration::from_secs(ttl as u64));

            RespValue::Integer(if did_expire { 1 } else { 0 })
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'expire' command".to_owned()),
    }
}
