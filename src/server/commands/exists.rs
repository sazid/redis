use super::value_as_bytes;
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_exists(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    if items.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'exists' command".to_owned());
    }

    let count = match items[1..].iter().try_fold(0_i64, |count, key| {
        let Some(key) = value_as_bytes(key) else {
            return Err(RespValue::Error(
                "ERR invalid EXISTS argument: key".to_owned(),
            ));
        };

        Ok(count + if db.exists(key) { 1 } else { 0 })
    }) {
        Ok(count) => count,
        Err(error) => return error,
    };

    RespValue::Integer(count)
}
