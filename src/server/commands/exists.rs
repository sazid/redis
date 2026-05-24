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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::RespValue;

    fn resp_bulk(s: &'static str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    #[test]
    fn exists_returns_count_of_existing_keys() {
        let mut db = RedisDb::new();
        db.set(b"k1".to_vec(), b"v1".to_vec());
        db.set(b"k2".to_vec(), b"v2".to_vec());
        db.set(b"k3".to_vec(), b"v3".to_vec());

        let result = handle_exists(
            &[
                resp_bulk("EXISTS"),
                resp_bulk("k1"),
                resp_bulk("k2"),
                resp_bulk("missing"),
            ],
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn exists_returns_zero_for_all_missing() {
        let mut db = RedisDb::new();

        let result = handle_exists(
            &[resp_bulk("EXISTS"), resp_bulk("a"), resp_bulk("b")],
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn exists_returns_zero_for_expired_key() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);
        db.set(b"k".to_vec(), b"v".to_vec());
        db.expire(b"k", std::time::Duration::from_secs(5));
        db.update_time(start + std::time::Duration::from_secs(5));

        let result = handle_exists(&[resp_bulk("EXISTS"), resp_bulk("k")], &mut db);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn exists_rejects_single_key_only() {
        let mut db = RedisDb::new();

        let result = handle_exists(&[resp_bulk("EXISTS")], &mut db);
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn exists_returns_one_for_single_existing_key() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());

        let result = handle_exists(&[resp_bulk("EXISTS"), resp_bulk("k")], &mut db);
        assert_eq!(result, RespValue::Integer(1));
    }
}
