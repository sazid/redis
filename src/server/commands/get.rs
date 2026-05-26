use super::value_as_bytes;
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_get(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid GET argument: key".to_owned());
            };

            RespValue::BulkString(db.get(key))
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'get' command".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::RespValue;

    fn resp_bulk(s: &'static str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    #[test]
    fn get_returns_stored_value() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec()).unwrap();

        let result = handle_get(&[resp_bulk("GET"), resp_bulk("k")], &mut db);
        assert_eq!(result, RespValue::BulkString(Some(b"v".to_vec())));
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let mut db = RedisDb::new();

        let result = handle_get(&[resp_bulk("GET"), resp_bulk("missing")], &mut db);
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[test]
    fn get_returns_none_for_expired_key() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);
        db.set(b"k".to_vec(), b"v".to_vec()).unwrap();
        db.expire(b"k", std::time::Duration::from_secs(5));
        db.update_time(start + std::time::Duration::from_secs(5));

        let result = handle_get(&[resp_bulk("GET"), resp_bulk("k")], &mut db);
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[test]
    fn get_rejects_wrong_number_of_arguments() {
        let mut db = RedisDb::new();

        // too few
        let result = handle_get(&[resp_bulk("GET")], &mut db);
        assert!(matches!(result, RespValue::Error(_)));

        // too many
        let result = handle_get(
            &[resp_bulk("GET"), resp_bulk("k"), resp_bulk("extra")],
            &mut db,
        );
        assert!(matches!(result, RespValue::Error(_)));
    }
}
