mod echo;
mod exists;
mod expire;
mod get;
mod ping;
mod set;
mod ttl;

use self::{
    echo::handle_echo, exists::handle_exists, expire::handle_expire, get::handle_get,
    ping::handle_ping, set::handle_set, ttl::handle_ttl,
};
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_request(value: RespValue, db: &mut RedisDb) -> RespValue {
    let RespValue::Array(Some(items)) = value else {
        return RespValue::Error("ERR expected array command".to_owned());
    };

    if items.is_empty() {
        return RespValue::Error("ERR empty command".to_owned());
    }

    let Some(command_name) = value_as_bytes(&items[0]) else {
        return RespValue::Error("ERR command name must be a bulk string".to_owned());
    };

    if command_name.eq_ignore_ascii_case(b"PING") {
        handle_ping(&items)
    } else if command_name.eq_ignore_ascii_case(b"ECHO") {
        handle_echo(&items)
    } else if command_name.eq_ignore_ascii_case(b"SET") {
        handle_set(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"GET") {
        handle_get(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"EXPIRE") {
        handle_expire(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"EXISTS") {
        handle_exists(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"TTL") {
        handle_ttl(&items, db)
    } else {
        RespValue::Error("ERR unknown command".to_owned())
    }
}

pub(super) fn value_as_bytes(value: &RespValue) -> Option<&[u8]> {
    match value {
        RespValue::BulkString(Some(bytes)) => Some(bytes),
        RespValue::SimpleString(value) => Some(value.as_bytes()),
        _ => None,
    }
}

pub(super) fn value_as_i64(value: &RespValue) -> Option<i64> {
    match value {
        RespValue::Integer(value) => Some(*value),

        RespValue::BulkString(Some(bytes)) => {
            let text = std::str::from_utf8(bytes).ok()?;
            text.parse().ok()
        }

        RespValue::SimpleString(value) => value.parse().ok(),

        _ => None,
    }
}
