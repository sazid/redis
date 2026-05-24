mod del;
mod echo;
mod exists;
mod expire;
mod get;
mod ping;
mod set;
mod ttl;

use self::{
    del::handle_del, echo::handle_echo, exists::handle_exists, expire::handle_expire,
    get::handle_get, ping::handle_ping, set::handle_set, ttl::handle_ttl,
};
use crate::{db::RedisDb, resp::RespValue};

pub(super) fn handle_request(value: RespValue, db: &mut RedisDb) -> RespValue {
    let RespValue::Array(Some(items)) = value else {
        return RespValue::Error("ERR expected array command".to_owned());
    };

    if items.is_empty() {
        return RespValue::Error("ERR empty command".to_owned());
    }

    let Some(command_name) = value_as_bytes(&items[0]) else {
        return RespValue::Error("ERR command name must be a bulk string".to_owned());
    };

    if command_name.eq_ignore_ascii_case(b"PING") {
        handle_ping(&items)
    } else if command_name.eq_ignore_ascii_case(b"ECHO") {
        handle_echo(&items)
    } else if command_name.eq_ignore_ascii_case(b"SET") {
        handle_set(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"GET") {
        handle_get(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"EXPIRE") {
        handle_expire(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"EXISTS") {
        handle_exists(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"TTL") {
        handle_ttl(&items, db)
    } else if command_name.eq_ignore_ascii_case(b"DEL") {
        handle_del(&items, db)
    } else {
        RespValue::Error("ERR unknown command".to_owned())
    }
}

pub(super) fn value_as_bytes(value: &RespValue) -> Option<&[u8]> {
    match value {
        RespValue::BulkString(Some(bytes)) => Some(bytes),
        RespValue::SimpleString(value) => Some(value.as_bytes()),
        _ => None,
    }
}

pub(super) fn value_as_i64(value: &RespValue) -> Option<i64> {
    match value {
        RespValue::Integer(value) => Some(*value),

        RespValue::BulkString(Some(bytes)) => {
            let text = std::str::from_utf8(bytes).ok()?;
            text.parse().ok()
        }

        RespValue::SimpleString(value) => value.parse().ok(),

        _ => None,
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

    fn command(items: &[RespValue]) -> RespValue {
        RespValue::Array(Some(items.to_vec()))
    }

    // --- PING ---

    #[test]
    fn ping_without_args() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[resp_bulk("PING")]), &mut db);
        assert_eq!(result, RespValue::SimpleString("PONG".to_owned()));
    }

    #[test]
    fn ping_with_message() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[resp_bulk("PING"), resp_bulk("hello")]), &mut db);
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[test]
    fn ping_case_insensitive() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[resp_bulk("ping")]), &mut db);
        assert_eq!(result, RespValue::SimpleString("PONG".to_owned()));
    }

    // --- ECHO ---

    #[test]
    fn echo_returns_message() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[resp_bulk("ECHO"), resp_bulk("world")]), &mut db);
        assert_eq!(result, RespValue::BulkString(Some(b"world".to_vec())));
    }

    // --- SET ---

    #[test]
    fn set_stores_and_retrieves_value() {
        let mut db = RedisDb::new();
        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v")]),
            &mut db,
        );
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn set_with_ex() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);
        handle_request(
            command(&[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("EX"),
                resp_bulk("10"),
            ]),
            &mut db,
        );
        db.update_time(start + std::time::Duration::from_secs(9));
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
        db.update_time(start + std::time::Duration::from_secs(10));
        assert_eq!(db.get(b"k"), None);
    }

    #[test]
    fn set_case_insensitive_ex() {
        let mut db = RedisDb::new();
        handle_request(
            command(&[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("ex"),
                resp_bulk("10"),
            ]),
            &mut db,
        );
        assert_eq!(db.ttl(b"k"), 10);
    }

    // --- GET ---

    #[test]
    fn get_returns_stored_value() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());
        let result = handle_request(command(&[resp_bulk("GET"), resp_bulk("k")]), &mut db);
        assert_eq!(result, RespValue::BulkString(Some(b"v".to_vec())));
    }

    #[test]
    fn get_returns_nil_for_missing() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[resp_bulk("GET"), resp_bulk("missing")]), &mut db);
        assert_eq!(result, RespValue::BulkString(None));
    }

    // --- EXPIRE ---

    #[test]
    fn expire_sets_ttl() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());
        let result = handle_request(
            command(&[resp_bulk("EXPIRE"), resp_bulk("k"), resp_int(10)]),
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(db.ttl(b"k"), 10);
    }

    #[test]
    fn expire_returns_0_for_missing() {
        let mut db = RedisDb::new();
        let result = handle_request(
            command(&[resp_bulk("EXPIRE"), resp_bulk("missing"), resp_int(10)]),
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn expire_with_zero_ttl_deletes_key() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());
        handle_request(
            command(&[resp_bulk("EXPIRE"), resp_bulk("k"), resp_int(0)]),
            &mut db,
        );
        assert_eq!(db.get(b"k"), None);
    }

    // --- EXISTS ---

    #[test]
    fn exists_returns_count() {
        let mut db = RedisDb::new();
        db.set(b"k1".to_vec(), b"v1".to_vec());
        db.set(b"k2".to_vec(), b"v2".to_vec());
        let result = handle_request(
            command(&[
                resp_bulk("EXISTS"),
                resp_bulk("k1"),
                resp_bulk("k2"),
                resp_bulk("missing"),
            ]),
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(2));
    }

    // --- TTL ---

    #[test]
    fn ttl_returns_remaining() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);
        db.set(b"k".to_vec(), b"v".to_vec());
        db.expire(b"k", std::time::Duration::from_secs(10));
        let result = handle_request(command(&[resp_bulk("TTL"), resp_bulk("k")]), &mut db);
        assert!(matches!(result, RespValue::Integer(n) if n <= 10 && n >= 9));
    }

    #[test]
    fn ttl_returns_minus_2_for_missing() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[resp_bulk("TTL"), resp_bulk("missing")]), &mut db);
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn ttl_returns_minus_1_for_no_expiry() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());
        let result = handle_request(command(&[resp_bulk("TTL"), resp_bulk("k")]), &mut db);
        assert_eq!(result, RespValue::Integer(-1));
    }

    // --- ERROR HANDLING ---

    #[test]
    fn non_array_request_returns_error() {
        let mut db = RedisDb::new();
        let result = handle_request(RespValue::BulkString(Some(b"SET".to_vec())), &mut db);
        assert!(matches!(result, RespValue::Error(e) if e.contains("expected array")));
    }

    #[test]
    fn empty_array_returns_error() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[]), &mut db);
        assert!(matches!(result, RespValue::Error(e) if e.contains("empty command")));
    }

    #[test]
    fn unknown_command_returns_error() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[resp_bulk("UNKNOWN")]), &mut db);
        assert!(matches!(result, RespValue::Error(e) if e.contains("unknown command")));
    }

    #[test]
    fn command_name_must_be_bulk_string() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[resp_int(123)]), &mut db);
        assert!(matches!(result, RespValue::Error(e) if e.contains("bulk string")));
    }

    // --- DEL ---

    #[test]
    fn del_returns_count_of_deleted_keys() {
        let mut db = RedisDb::new();
        db.set(b"k1".to_vec(), b"v1".to_vec());
        db.set(b"k2".to_vec(), b"v2".to_vec());
        let result = handle_request(
            command(&[
                resp_bulk("DEL"),
                resp_bulk("k1"),
                resp_bulk("k2"),
                resp_bulk("missing"),
            ]),
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn del_returns_zero_for_missing_keys() {
        let mut db = RedisDb::new();
        let result = handle_request(command(&[resp_bulk("DEL"), resp_bulk("missing")]), &mut db);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn del_case_insensitive() {
        let mut db = RedisDb::new();
        db.set(b"k".to_vec(), b"v".to_vec());
        let result = handle_request(command(&[resp_bulk("del"), resp_bulk("k")]), &mut db);
        assert_eq!(result, RespValue::Integer(1));
    }

    // --- MULTI-COMMAND SEQUENCES ---

    #[test]
    fn set_then_get_returns_value() {
        let mut db = RedisDb::new();
        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v")]),
            &mut db,
        );
        let result = handle_request(command(&[resp_bulk("GET"), resp_bulk("k")]), &mut db);
        assert_eq!(result, RespValue::BulkString(Some(b"v".to_vec())));
    }

    #[test]
    fn set_clears_existing_ttl_on_second_set() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        handle_request(
            command(&[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v1"),
                resp_bulk("EX"),
                resp_bulk("10"),
            ]),
            &mut db,
        );
        // Second SET without EX should clear the TTL
        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v2")]),
            &mut db,
        );
        db.update_time(start + std::time::Duration::from_secs(20));

        // Value should still be accessible since TTL was cleared
        assert_eq!(db.get(b"k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn set_with_ex_then_get_before_expiry() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        handle_request(
            command(&[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("EX"),
                resp_bulk("10"),
            ]),
            &mut db,
        );
        db.update_time(start + std::time::Duration::from_secs(5));

        // Key should still be accessible
        let result = handle_request(command(&[resp_bulk("GET"), resp_bulk("k")]), &mut db);
        assert_eq!(result, RespValue::BulkString(Some(b"v".to_vec())));
    }

    #[test]
    fn set_with_ex_then_get_after_expiry() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        handle_request(
            command(&[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("EX"),
                resp_bulk("5"),
            ]),
            &mut db,
        );
        db.update_time(start + std::time::Duration::from_secs(5));

        // Key should be expired (lazy deletion)
        let result = handle_request(command(&[resp_bulk("GET"), resp_bulk("k")]), &mut db);
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[test]
    fn set_ex_key_survives_until_expiry() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        handle_request(
            command(&[
                resp_bulk("SET"),
                resp_bulk("k"),
                resp_bulk("v"),
                resp_bulk("EX"),
                resp_bulk("10"),
            ]),
            &mut db,
        );

        // TTL should be ~10
        let result = handle_request(command(&[resp_bulk("TTL"), resp_bulk("k")]), &mut db);
        assert!(matches!(result, RespValue::Integer(n) if n <= 10 && n >= 9));

        // After 5 seconds, TTL should be ~5
        db.update_time(start + std::time::Duration::from_secs(5));
        let result = handle_request(command(&[resp_bulk("TTL"), resp_bulk("k")]), &mut db);
        assert!(matches!(result, RespValue::Integer(n) if n <= 5 && n >= 4));

        // After expiry, TTL should be gone
        db.update_time(start + std::time::Duration::from_secs(10));
        let result = handle_request(command(&[resp_bulk("TTL"), resp_bulk("k")]), &mut db);
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn expire_then_ttl_shows_remaining() {
        let start = std::time::Instant::now();
        let mut db = RedisDb::new();
        db.update_time(start);

        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v")]),
            &mut db,
        );
        handle_request(
            command(&[resp_bulk("EXPIRE"), resp_bulk("k"), resp_int(10)]),
            &mut db,
        );

        let result = handle_request(command(&[resp_bulk("TTL"), resp_bulk("k")]), &mut db);
        assert!(matches!(result, RespValue::Integer(n) if n <= 10 && n >= 9));
    }

    #[test]
    fn multiple_keys_exist_after_set() {
        let mut db = RedisDb::new();

        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k1"), resp_bulk("v1")]),
            &mut db,
        );
        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k2"), resp_bulk("v2")]),
            &mut db,
        );
        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k3"), resp_bulk("v3")]),
            &mut db,
        );

        let result = handle_request(
            command(&[
                resp_bulk("EXISTS"),
                resp_bulk("k1"),
                resp_bulk("k2"),
                resp_bulk("k3"),
                resp_bulk("missing"),
            ]),
            &mut db,
        );
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn set_get_set_get_sequence() {
        let mut db = RedisDb::new();

        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v1")]),
            &mut db,
        );
        let r1 = handle_request(command(&[resp_bulk("GET"), resp_bulk("k")]), &mut db);
        assert_eq!(r1, RespValue::BulkString(Some(b"v1".to_vec())));

        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v2")]),
            &mut db,
        );
        let r2 = handle_request(command(&[resp_bulk("GET"), resp_bulk("k")]), &mut db);
        assert_eq!(r2, RespValue::BulkString(Some(b"v2".to_vec())));
    }

    #[test]
    fn set_then_exists_then_del_then_exists() {
        let mut db = RedisDb::new();

        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v")]),
            &mut db,
        );

        let result = handle_request(command(&[resp_bulk("EXISTS"), resp_bulk("k")]), &mut db);
        assert_eq!(result, RespValue::Integer(1));

        handle_request(command(&[resp_bulk("DEL"), resp_bulk("k")]), &mut db);

        let result = handle_request(command(&[resp_bulk("EXISTS"), resp_bulk("k")]), &mut db);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn set_then_get_then_del_then_get() {
        let mut db = RedisDb::new();

        handle_request(
            command(&[resp_bulk("SET"), resp_bulk("k"), resp_bulk("v")]),
            &mut db,
        );
        let r1 = handle_request(command(&[resp_bulk("GET"), resp_bulk("k")]), &mut db);
        assert_eq!(r1, RespValue::BulkString(Some(b"v".to_vec())));

        handle_request(command(&[resp_bulk("DEL"), resp_bulk("k")]), &mut db);

        let r2 = handle_request(command(&[resp_bulk("GET"), resp_bulk("k")]), &mut db);
        assert_eq!(r2, RespValue::BulkString(None));
    }
}
