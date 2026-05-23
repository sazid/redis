use super::value_as_bytes;
use crate::resp::RespValue;

pub(super) fn handle_ping(items: &[RespValue]) -> RespValue {
    match items {
        [_command] => RespValue::SimpleString("PONG".to_owned()),

        [_command, message] => {
            let Some(bytes) = value_as_bytes(message) else {
                return RespValue::Error("ERR invalid PING argument".to_owned());
            };

            RespValue::BulkString(Some(bytes.to_vec()))
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'ping' command".to_owned()),
    }
}
