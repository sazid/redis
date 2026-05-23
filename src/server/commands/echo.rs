use super::value_as_bytes;
use crate::resp::RespValue;

pub(super) fn handle_echo(items: &[RespValue]) -> RespValue {
    match items {
        [_command, message] => {
            let Some(bytes) = value_as_bytes(message) else {
                return RespValue::Error("ERR invalid ECHO argument".to_owned());
            };

            RespValue::BulkString(Some(bytes.to_vec()))
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'echo' command".to_owned()),
    }
}
