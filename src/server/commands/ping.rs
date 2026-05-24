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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::RespValue;

    fn resp_bulk(s: &'static str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    fn resp_simple(s: &'static str) -> RespValue {
        RespValue::SimpleString(s.to_owned())
    }

    #[test]
    fn ping_without_message_returns_pong() {
        let result = handle_ping(&[resp_bulk("PING")]);
        assert_eq!(result, resp_simple("PONG"));
    }

    #[test]
    fn ping_with_message_returns_message() {
        let result = handle_ping(&[resp_bulk("PING"), resp_bulk("hello")]);
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[test]
    fn ping_with_simple_string_message_returns_message() {
        let result = handle_ping(&[resp_bulk("PING"), resp_simple("world")]);
        assert_eq!(result, RespValue::BulkString(Some(b"world".to_vec())));
    }

    #[test]
    fn ping_rejects_too_many_arguments() {
        let result = handle_ping(&[resp_bulk("PING"), resp_bulk("one"), resp_bulk("two")]);
        assert!(matches!(result, RespValue::Error(_)));
    }
}
