/// A parsed RESP2 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// `+OK\r\n`
    SimpleString(String),

    /// `-ERR something went wrong\r\n`
    Error(String),

    /// `:123\r\n`
    Integer(i64),

    /// `$5\r\nhello\r\n` or `$-1\r\n`
    ///
    /// RESP bulk strings are binary-safe, so store bytes instead of `String`.
    /// `None` means a null bulk string.
    BulkString(Option<Vec<u8>>),

    /// `*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n` or `*-1\r\n`
    ///
    /// `None` means a null array.
    Array(Option<Vec<RespValue>>),
}

/// Things that can go wrong while parsing RESP data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespError {
    EmptyInput,
    UnknownTypeMarker(u8),
    MissingCrlf,
    InvalidUtf8,
    InvalidInteger,
    InvalidLength,
    IncompleteInput,
    TrailingData,
}

/// Parse exactly one RESP value.
///
/// This should fail with `RespError::TrailingData` if a valid value is followed
/// by extra bytes. For parsing one value from a larger buffer, use `decode_one`.
pub fn decode(data: &[u8]) -> Result<RespValue, RespError> {
    let (value, remaining) = decode_one(data)?;

    if remaining.is_empty() {
        Ok(value)
    } else {
        Err(RespError::TrailingData)
    }
}

/// Parse one RESP value from the beginning of `data`.
///
/// Return the parsed value plus the remaining unconsumed bytes.
pub fn decode_one(data: &[u8]) -> Result<(RespValue, &[u8]), RespError> {
    let Some((&marker, rest)) = data.split_first() else {
        return Err(RespError::EmptyInput);
    };

    match marker {
        b'+' => {
            let (value, remaining) = parse_utf8_line(rest)?;
            Ok((RespValue::SimpleString(value.to_owned()), remaining))
        }

        b'-' => {
            let (value, remaining) = parse_utf8_line(rest)?;
            Ok((RespValue::Error(value.to_owned()), remaining))
        }

        b':' => {
            let (value, remaining) = parse_i64_line(rest)?;
            Ok((RespValue::Integer(value), remaining))
        }

        b'$' => {
            let (len, remaining) = parse_i64_line(rest)?;

            if len == -1 {
                return Ok((RespValue::BulkString(None), remaining));
            }

            if len < 0 {
                return Err(RespError::InvalidLength);
            }

            let len = len as usize;

            // Need payload bytes plus trailing CRLF.
            if remaining.len() < len + 2 {
                return Err(RespError::IncompleteInput);
            }

            let (payload, after_payload) = remaining.split_at(len);

            if !after_payload.starts_with(b"\r\n") {
                return Err(RespError::MissingCrlf);
            }

            Ok((
                RespValue::BulkString(Some(payload.to_vec())),
                &after_payload[2..],
            ))
        }

        b'*' => {
            let (len, mut remaining) = parse_i64_line(rest)?;

            if len == -1 {
                return Ok((RespValue::Array(None), remaining));
            }

            if len < 0 {
                return Err(RespError::InvalidLength);
            }

            let mut values = Vec::with_capacity(len as usize);

            for _ in 0..len {
                let (value, rest) = decode_one(remaining)?;
                values.push(value);
                remaining = rest;
            }

            Ok((RespValue::Array(Some(values)), remaining))
        }

        other => Err(RespError::UnknownTypeMarker(other)),
    }
}

fn parse_i64_line(data: &[u8]) -> Result<(i64, &[u8]), RespError> {
    let (line, remaining) = parse_utf8_line(data)?;
    let value = line.parse().map_err(|_| RespError::InvalidInteger)?;
    Ok((value, remaining))
}

fn parse_utf8_line(data: &[u8]) -> Result<(&str, &[u8]), RespError> {
    let (line, remaining) = read_line(data)?;
    let value = std::str::from_utf8(line).map_err(|_| RespError::InvalidUtf8)?;
    Ok((value, remaining))
}

/// Read bytes until `\r\n`.
///
/// Return the line without `\r\n`, plus the bytes after the line ending.
fn read_line(data: &[u8]) -> Result<(&[u8], &[u8]), RespError> {
    let crlf_index = data
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or(RespError::MissingCrlf)?;

    Ok((&data[..crlf_index], &data[crlf_index + 2..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_line_returns_line_without_crlf_and_remaining_bytes() {
        let ans = read_line(b"OK\r\nextra");
        assert_eq!(ans, Ok((&b"OK"[..], &b"extra"[..])));
    }

    #[test]
    fn read_line_fails_when_crlf_is_missing() {
        assert_eq!(read_line(b"OK"), Err(RespError::MissingCrlf));
    }

    #[test]
    fn decode_fails_on_empty_input() {
        assert_eq!(decode(b""), Err(RespError::EmptyInput));
    }

    #[test]
    fn decode_fails_on_unknown_type_marker() {
        assert_eq!(decode(b"?wat\r\n"), Err(RespError::UnknownTypeMarker(b'?')));
    }

    #[test]
    fn decodes_simple_string() {
        assert_eq!(
            decode(b"+OK\r\n"),
            Ok(RespValue::SimpleString("OK".to_owned()))
        );
    }

    #[test]
    fn decodes_error() {
        assert_eq!(
            decode(b"-ERR unknown command\r\n"),
            Ok(RespValue::Error("ERR unknown command".to_owned()))
        );
    }

    #[test]
    fn decodes_positive_integer() {
        assert_eq!(decode(b":123\r\n"), Ok(RespValue::Integer(123)));
    }

    #[test]
    fn decodes_negative_integer() {
        assert_eq!(decode(b":-42\r\n"), Ok(RespValue::Integer(-42)));
    }

    #[test]
    fn integer_fails_when_not_a_number() {
        assert_eq!(decode(b":abc\r\n"), Err(RespError::InvalidInteger));
    }

    #[test]
    fn decodes_bulk_string() {
        assert_eq!(
            decode(b"$5\r\nhello\r\n"),
            Ok(RespValue::BulkString(Some(b"hello".to_vec())))
        );
    }

    #[test]
    fn decodes_empty_bulk_string() {
        assert_eq!(
            decode(b"$0\r\n\r\n"),
            Ok(RespValue::BulkString(Some(Vec::new())))
        );
    }

    #[test]
    fn decodes_null_bulk_string() {
        assert_eq!(decode(b"$-1\r\n"), Ok(RespValue::BulkString(None)));
    }

    #[test]
    fn bulk_string_fails_when_length_is_negative_but_not_null() {
        assert_eq!(decode(b"$-2\r\n"), Err(RespError::InvalidLength));
    }

    #[test]
    fn bulk_string_fails_when_payload_is_shorter_than_declared() {
        assert_eq!(decode(b"$5\r\nhel\r\n"), Err(RespError::IncompleteInput));
    }

    #[test]
    fn bulk_string_fails_when_payload_is_not_followed_by_crlf() {
        assert_eq!(decode(b"$5\r\nhelloXX"), Err(RespError::MissingCrlf));
    }

    #[test]
    fn decodes_empty_array() {
        assert_eq!(decode(b"*0\r\n"), Ok(RespValue::Array(Some(vec![]))));
    }

    #[test]
    fn decodes_null_array() {
        assert_eq!(decode(b"*-1\r\n"), Ok(RespValue::Array(None)));
    }

    #[test]
    fn decodes_array_of_bulk_strings() {
        assert_eq!(
            decode(b"*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n"),
            Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"PING".to_vec())),
                RespValue::BulkString(Some(b"test".to_vec())),
            ])))
        );
    }

    #[test]
    fn decodes_nested_array() {
        assert_eq!(
            decode(b"*2\r\n:1\r\n*2\r\n+OK\r\n$3\r\nhey\r\n"),
            Ok(RespValue::Array(Some(vec![
                RespValue::Integer(1),
                RespValue::Array(Some(vec![
                    RespValue::SimpleString("OK".to_owned()),
                    RespValue::BulkString(Some(b"hey".to_vec())),
                ])),
            ])))
        );
    }

    #[test]
    fn decode_one_returns_remaining_bytes() {
        assert_eq!(
            decode_one(b"+OK\r\n:123\r\n"),
            Ok((RespValue::SimpleString("OK".to_owned()), &b":123\r\n"[..]))
        );
    }

    #[test]
    fn decode_fails_when_valid_value_has_trailing_data() {
        assert_eq!(decode(b"+OK\r\n:123\r\n"), Err(RespError::TrailingData));
    }
}

impl RespValue {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            RespValue::SimpleString(value) => {
                out.extend_from_slice(b"+");
                out.extend_from_slice(value.as_bytes());
                out.extend_from_slice(b"\r\n");
            }

            RespValue::Error(value) => {
                out.extend_from_slice(b"-");
                out.extend_from_slice(value.as_bytes());
                out.extend_from_slice(b"\r\n");
            }

            RespValue::Integer(value) => {
                out.extend_from_slice(b":");
                out.extend_from_slice(value.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }

            RespValue::BulkString(Some(bytes)) => {
                out.extend_from_slice(b"$");
                out.extend_from_slice(bytes.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");

                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                out.extend_from_slice(b"$-1\r\n");
            }

            RespValue::Array(Some(values)) => {
                out.extend_from_slice(b"*");
                out.extend_from_slice(values.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");

                for value in values {
                    value.encode_into(out);
                }
            }
            RespValue::Array(None) => {
                out.extend_from_slice(b"*-1\r\n");
            }
        }
    }
}

#[test]
fn encodes_simple_string() {
    assert_eq!(
        RespValue::SimpleString("PONG".to_owned()).encode(),
        b"+PONG\r\n"
    );
}

#[test]
fn encodes_error() {
    assert_eq!(
        RespValue::Error("ERR unknown command".to_owned()).encode(),
        b"-ERR unknown command\r\n"
    );
}

#[test]
fn encodes_bulk_string() {
    assert_eq!(
        RespValue::BulkString(Some(b"hello".to_vec())).encode(),
        b"$5\r\nhello\r\n"
    );
}

#[test]
fn encode_into_appends_to_existing_buffer() {
    let mut out = b"prefix".to_vec();

    RespValue::SimpleString("OK".to_owned()).encode_into(&mut out);

    assert_eq!(out, b"prefix+OK\r\n");
}
