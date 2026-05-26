use std::time::Duration;

use super::{value_as_bytes, value_as_i64};
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_set(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    match items {
        [_command, key, value] => set_key_value(key, value, db, None),

        [_command, key, value, ex, ttl] => {
            let Some(ex) = value_as_bytes(ex) else {
                return RespValue::Error("ERR invalid SET argument: ex".to_owned());
            };
            if !ex.eq_ignore_ascii_case(b"EX") {
                return RespValue::Error("ERR invalid SET argument: ex".to_owned());
            }
            let Some(ttl) = value_as_i64(ttl) else {
                return RespValue::Error("ERR invalid EXPIRE argument: ttl".to_owned());
            };
            if ttl <= 0 {
                return RespValue::Error("ERR ttl must be greater than 0".to_owned());
            }

            set_key_value(key, value, db, Some(Duration::from_secs(ttl as u64)))
        }
        _ => RespValue::Error("ERR wrong number of arguments for 'set' command".to_owned()),
    }
}

fn set_key_value(
    key: &RespValue,
    value: &RespValue,
    db: &mut RedisDb,
    expiry: Option<Duration>,
) -> RespValue {
    let Some(key) = value_as_bytes(key) else {
        return RespValue::Error("ERR invalid SET argument: key".to_owned());
    };
    let Some(value) = value_as_bytes(value) else {
        return RespValue::Error("ERR invalid SET argument: value".to_owned());
    };

    if let Err(err) = db.set(key.to_owned(), value.to_owned()) {
        return RespValue::Error(format!("ERR {err:?}"));
    }
    if let Some(duration) = expiry {
        db.expire(key, duration);
    }
    RespValue::SimpleString("OK".to_owned())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::resp::RespValue;

    fn resp_bulk(s: &'static str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    #[test]
    fn set_stores_value() {
        let mut db = RedisDb::new();
        let result = handle_set(&[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v")], &mut db);
        assert_eq!(result, RespValue::SimpleString("OK".to_owned()));
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn set_overwrites_existing_value() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"old".to_vec()).unwrap();

        let result = handle_set(
            &[resp_bulk("SET"), resp_bulk("k"), resp_bulk("new")],
            &mut db,
        );
        assert_eq!(result, RespValue::SimpleString("OK".to_owned()));
        assert_eq!(db.get(b"k"), Some(b"new".to_vec()));
    }

    #[test]
    fn set_clears_existing_ttl() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);
        db.set(b"k".to_vec(), b"v1".to_vec()).unwrap();
        db.expire(b"k", Duration::from_secs(10));

        // SET without EX should clear the TTL
        handle_set(
            &[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v2")],
            &mut db,
        );
        db.update_time(start + Duration::from_secs(20));

        // value should still be accessible since TTL was cleared
        assert_eq!(db.get(b"k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn set_with_ex_sets_ttl() {
        let start = Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        let result = handle_set(
            &[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("EX"),
                resp_bulk("10"),
            ],
            &mut db,
        );
        assert_eq!(result, RespValue::SimpleString("OK".to_owned()));
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));

        // TTL should be 10 seconds
        db.update_time(start + Duration::from_secs(9));
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
        db.update_time(start + Duration::from_secs(10));
        assert_eq!(db.get(b"k"), None);
    }

    #[test]
    fn set_with_ex_rejects_zero_ttl() {
        let mut db = RedisDb::new();

        let result = handle_set(
            &[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("EX"),
                resp_bulk("0"),
            ],
            &mut db,
        );
        assert!(matches!(result, RespValue::Error(_)));
        assert_eq!(db.get(b"k"), None);
    }

    #[test]
    fn set_with_ex_rejects_negative_ttl() {
        let mut db = RedisDb::new();

        let result = handle_set(
            &[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("EX"),
                resp_bulk("-5"),
            ],
            &mut db,
        );
        assert!(matches!(result, RespValue::Error(_)));
        assert_eq!(db.get(b"k"), None);
    }

    #[test]
    fn set_rejects_non_ex_option() {
        let mut db = RedisDb::new();

        let result = handle_set(
            &[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("NOTEX"),
                resp_bulk("10"),
            ],
            &mut db,
        );
        assert!(matches!(result, RespValue::Error(_)));
        assert_eq!(db.get(b"k"), None);
    }

    #[test]
    fn set_rejects_wrong_number_of_arguments() {
        let mut db = RedisDb::new();

        // too few
        let result = handle_set(&[resp_bulk("SET"), resp_bulk("k")], &mut db);
        assert!(matches!(result, RespValue::Error(_)));

        // too many (without EX/PX)
        let result = handle_set(
            &[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("extra"),
            ],
            &mut db,
        );
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn set_accepts_ex_case_insensitively() {
        let mut db = RedisDb::new();

        let result = handle_set(
            &[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("ex"),
                resp_bulk("10"),
            ],
            &mut db,
        );
        assert_eq!(result, RespValue::SimpleString("OK".to_owned()));
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
    }
}
