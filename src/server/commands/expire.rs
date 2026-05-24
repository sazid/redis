use std::time::Duration;

use super::{value_as_bytes, value_as_i64};
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_expire(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key, ttl] => {
            let Some(key) = value_as_bytes(key) else {
                return RespValue::Error("ERR invalid EXPIRE argument: key".to_owned());
            };

            let Some(ttl) = value_as_i64(ttl) else {
                return RespValue::Error("ERR invalid EXPIRE argument: ttl".to_owned());
            };

            if !db.exists(key) {
                return RespValue::Integer(0);
            }

            if ttl <= 0 {
                db.delete(key);
                return RespValue::Integer(1);
            }

            let did_expire = db.expire(key, Duration::from_secs(ttl as u64));

            RespValue::Integer(if did_expire { 1 } else { 0 })
        }

        _ => RespValue::Error("ERR wrong number of arguments for 'expire' command".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::RespValue;

    fn resp_bulk(s: &'static str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    fn resp_int(i: i64) -> RespValue {
        RespValue::Integer(i)
    }

    #[test]
    fn expire_sets_ttl_and_returns_1() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());

        let result = handle_expire(
            &[resp_bulk("EXPIRE"), resp_bulk("k"), resp_int(10)],
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(db.ttl(b"k"), 10);
    }

    #[test]
    fn expire_returns_0_for_missing_key() {
        let mut db = RedisDb::new();

        let result = handle_expire(
            &[resp_bulk("EXPIRE"), resp_bulk("missing"), resp_int(10)],
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn expire_returns_1_and_deletes_key_for_zero_ttl() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());

        let result = handle_expire(&[resp_bulk("EXPIRE"), resp_bulk("k"), resp_int(0)], &mut db);
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(db.get(b"k"), None);
    }

    #[test]
    fn expire_returns_1_and_deletes_key_for_negative_ttl() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());

        let result = handle_expire(
            &[resp_bulk("EXPIRE"), resp_bulk("k"), resp_int(-5)],
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(db.get(b"k"), None);
    }

    #[test]
    fn expire_rejects_wrong_number_of_arguments() {
        let mut db = RedisDb::new();

        // too few
        let result = handle_expire(&[resp_bulk("EXPIRE"), resp_bulk("k")], &mut db);
        assert!(matches!(result, RespValue::Error(_)));

        // too many
        let result = handle_expire(
            &[
                resp_bulk("EXPIRE"),
                resp_bulk("k"),
                resp_int(10),
                resp_bulk("extra"),
            ],
            &mut db,
        );
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn expire_rejects_non_integer_ttl() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());

        let result = handle_expire(
            &[
                resp_bulk("EXPIRE"),
                resp_bulk("k"),
                resp_bulk("not_a_number"),
            ],
            &mut db,
        );
        assert!(matches!(result, RespValue::Error(_)));
    }
}
