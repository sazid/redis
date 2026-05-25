use super::value_as_bytes;
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_ttl(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid TTL argument: key".to_owned());
            };

            RespValue::Integer(db.ttl(key))
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'ttl' command".to_owned()),
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
    fn ttl_returns_remaining_ttl() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);
        db.set(b"k".to_vec(), b"v".to_vec());
        db.expire(b"k", std::time::Duration::from_secs(10));

        // Should be close to 10 (accounting for test execution time)
        let result = handle_ttl(&[resp_bulk("TTL"), resp_bulk("k")], &mut db);
        assert!(matches!(result, RespValue::Integer(n) if (9..=10).contains(&n)));
    }

    #[test]
    fn ttl_returns_minus_2_for_missing_key() {
        let mut db = RedisDb::new();

        let result = handle_ttl(&[resp_bulk("TTL"), resp_bulk("missing")], &mut db);
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn ttl_returns_minus_1_for_key_without_ttl() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());

        let result = handle_ttl(&[resp_bulk("TTL"), resp_bulk("k")], &mut db);
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn ttl_returns_0_for_expired_key() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);
        db.set(b"k".to_vec(), b"v".to_vec());
        db.expire(b"k", std::time::Duration::from_secs(5));
        db.update_time(start + std::time::Duration::from_secs(5));

        // Key is expired but not yet accessed, so exists returns false -> ttl returns -2
        let result = handle_ttl(&[resp_bulk("TTL"), resp_bulk("k")], &mut db);
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn ttl_rejects_wrong_number_of_arguments() {
        let mut db = RedisDb::new();

        // too few
        let result = handle_ttl(&[resp_bulk("TTL")], &mut db);
        assert!(matches!(result, RespValue::Error(_)));

        // too many
        let result = handle_ttl(
            &[resp_bulk("TTL"), resp_bulk("k"), resp_bulk("extra")],
            &mut db,
        );
        assert!(matches!(result, RespValue::Error(_)));
    }
}
