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
    fn echo_returns_bulk_string_message() {
        let result = handle_echo(&[resp_bulk("ECHO"), resp_bulk("hello")]);
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[test]
    fn echo_returns_simple_string_message() {
        let result = handle_echo(&[resp_bulk("ECHO"), resp_simple("world")]);
        assert_eq!(result, RespValue::BulkString(Some(b"world".to_vec())));
    }

    #[test]
    fn echo_rejects_no_message() {
        let result = handle_echo(&[resp_bulk("ECHO")]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn echo_rejects_too_many_arguments() {
        let result = handle_echo(&[resp_bulk("ECHO"), resp_bulk("one"), resp_bulk("two")]);
        assert!(matches!(result, RespValue::Error(_)));
    }
}
