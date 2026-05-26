use super::value_as_bytes;
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_del(items: &[RespValue], db: &mut RedisDb) -> RespValue {
    if items.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'del' command".to_owned());
    }

    let count = match items[1..].iter().try_fold(0_i64, |count, key| {
        let Some(key) = value_as_bytes(key) else {
            return Err(RespValue::Error("ERR invalid DEL argument: key".to_owned()));
        };

        Ok(count + if db.delete(key) { 1 } else { 0 })
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
    fn del_returns_count_of_deleted_keys() {
        let mut db = RedisDb::new();
        db.set(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        db.set(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        db.set(b"k3".to_vec(), b"v3".to_vec()).unwrap();

        let result = handle_del(
            &[
                resp_bulk("DEL"),
                resp_bulk("k1"),
                resp_bulk("k2"),
                resp_bulk("missing"),
            ],
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn del_returns_zero_when_no_keys_exist() {
        let mut db = RedisDb::new();

        let result = handle_del(&[resp_bulk("DEL"), resp_bulk("missing")], &mut db);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn del_removes_existing_key() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec()).unwrap();

        handle_del(&[resp_bulk("DEL"), resp_bulk("k")], &mut db);
        assert_eq!(db.get(b"k"), None);
    }

    #[test]
    fn del_rejects_single_key_only() {
        let mut db = RedisDb::new();

        let result = handle_del(&[resp_bulk("DEL")], &mut db);
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn del_returns_count_for_single_existing_key() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec()).unwrap();

        let result = handle_del(&[resp_bulk("DEL"), resp_bulk("k")], &mut db);
        assert_eq!(result, RespValue::Integer(1));
    }
}
