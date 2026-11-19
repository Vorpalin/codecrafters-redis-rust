use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Returned when an operation is applied to a key holding a different type
/// (e.g. `GET` on a list). Maps to Redis' `WRONGTYPE` error.
#[derive(Debug, PartialEq, Eq)]
pub struct WrongType;

enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
}

struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|t| t <= Instant::now())
    }
}

type Map = HashMap<Vec<u8>, Entry>;

/// Lazy expiration: drop the key if it has expired. Call before touching a key.
fn purge_if_expired(map: &mut Map, key: &[u8]) {
    if map.get(key).is_some_and(Entry::is_expired) {
        map.remove(key);
    }
}

/// Converts Redis-style inclusive indices (negative = from the end) into an
/// inclusive `(start, stop)` range clamped to the list, or `None` if empty.
fn normalize_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let start = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    let stop = if stop < 0 {
        len + stop
    } else {
        stop.min(len - 1)
    };
    if start >= len || stop < 0 || start > stop {
        None
    } else {
        Some((start as usize, stop as usize))
    }
}

/// Shared in-memory key/value store. Cheap to clone (it is an `Arc`).
///
/// One keyspace, one type per key: writing a string over a list replaces it,
/// and reading with the wrong operation yields `WrongType`.
///
/// A plain std `Mutex` is fine as long as no guard is held across an `.await`;
/// every method here locks, works, and returns.
#[derive(Clone, Default)]
pub struct Db {
    inner: Arc<Mutex<Map>>,
    list_changed: Arc<Condvar>,
}

// List operations are unused by the binary until their commands are added.
#[allow(dead_code)]
impl Db {
    // ---- strings -------------------------------------------------------

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongType> {
        let mut map = self.inner.lock().unwrap();
        purge_if_expired(&mut map, key);
        match map.get(key).map(|e| &e.value) {
            None => Ok(None),
            Some(Value::String(s)) => Ok(Some(s.clone())),
            Some(Value::List(_)) => Err(WrongType),
        }
    }

    /// Overwrites any existing value, whatever its type (same as Redis `SET`).
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.inner.lock().unwrap().insert(
            key,
            Entry {
                value: Value::String(value),
                expires_at,
            },
        );
    }

    // ---- lists ---------------------------------------------------------

    /// Appends to the tail. Returns the new length.
    pub fn rpush(&self, key: &[u8], items: Vec<Vec<u8>>) -> Result<usize, WrongType> {
        self.push(key, items, false)
    }

    /// Prepends to the head, one item at a time (so `LPUSH k a b` yields `b a`).
    /// Returns the new length.
    pub fn lpush(&self, key: &[u8], items: Vec<Vec<u8>>) -> Result<usize, WrongType> {
        self.push(key, items, true)
    }

    fn push(&self, key: &[u8], items: Vec<Vec<u8>>, front: bool) -> Result<usize, WrongType> {
        let should_notify = !items.is_empty();
        let mut map = self.inner.lock().unwrap();
        purge_if_expired(&mut map, key);

        let len = {
            let entry = map.entry(key.to_vec()).or_insert_with(|| Entry {
                value: Value::List(VecDeque::new()),
                expires_at: None,
            });

            let Value::List(list) = &mut entry.value else {
                return Err(WrongType);
            };

            for item in items {
                if front {
                    list.push_front(item);
                } else {
                    list.push_back(item);
                }
            }

            list.len()
        };

        drop(map);

        if should_notify {
            self.list_changed.notify_all();
        }

        Ok(len)
    }

    /// Inclusive range; negative indices count from the end. Missing key = empty.
    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>, WrongType> {
        let mut map = self.inner.lock().unwrap();
        purge_if_expired(&mut map, key);

        match map.get(key).map(|e| &e.value) {
            None => Ok(Vec::new()),
            Some(Value::String(_)) => Err(WrongType),
            Some(Value::List(list)) => Ok(match normalize_range(start, stop, list.len()) {
                None => Vec::new(),
                Some((s, e)) => list.iter().skip(s).take(e - s + 1).cloned().collect(),
            }),
        }
    }

    /// Missing key = 0.
    pub fn llen(&self, key: &[u8]) -> Result<usize, WrongType> {
        let mut map = self.inner.lock().unwrap();
        purge_if_expired(&mut map, key);

        match map.get(key).map(|e| &e.value) {
            None => Ok(0),
            Some(Value::String(_)) => Err(WrongType),
            Some(Value::List(list)) => Ok(list.len()),
        }
    }

    /// Pops up to `count` items (default 1) from the head.
    /// `Ok(None)` if the key doesn't exist. An emptied list removes its key.
    pub fn lpop(
        &self,
        key: &[u8],
        count: Option<usize>,
    ) -> Result<Option<Vec<Vec<u8>>>, WrongType> {
        let mut map = self.inner.lock().unwrap();
        purge_if_expired(&mut map, key);

        let Some(entry) = map.get_mut(key) else {
            return Ok(None);
        };
        let Value::List(list) = &mut entry.value else {
            return Err(WrongType);
        };

        let n = count.unwrap_or(1).min(list.len());
        let popped: Vec<_> = list.drain(..n).collect();
        if list.is_empty() {
            map.remove(key);
        }
        Ok(Some(popped))
    }

    pub fn blpop(
        &self,
        keys: &[Vec<u8>],
        timeout: Duration,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, WrongType> {
        let deadline = if timeout.is_zero() {
            None
        } else {
            Some(
                Instant::now()
                    .checked_add(timeout)
                    .expect("BLPOP timeout is too large"),
            )
        };

        let mut map = self.inner.lock().unwrap();

        loop {
            // Check every key in the requested order.
            for key in keys {
                purge_if_expired(&mut map, key);

                let popped = match map.get_mut(key) {
                    None => None,
                    Some(entry) => match &mut entry.value {
                        Value::String(_) => return Err(WrongType),
                        Value::List(list) => list.pop_front().map(|value| (value, list.is_empty())),
                    },
                };

                if let Some((value, is_empty)) = popped {
                    if is_empty {
                        map.remove(key);
                    }

                    return Ok(Some((key.clone(), value)));
                }
            }

            // No element was available.
            match deadline {
                None => {
                    // Infinite wait.
                    map = self.list_changed.wait(map).unwrap();
                }
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());

                    if remaining.is_zero() {
                        return Ok(None);
                    }

                    let (guard, result) = self.list_changed.wait_timeout(map, remaining).unwrap();

                    map = guard;

                    if result.timed_out() {
                        return Ok(None);
                    }
                }
            }
        }
    }

    pub fn r#type(&self, key: &[u8]) -> Result<String, WrongType> {
        let mut map = self.inner.lock().unwrap();
        purge_if_expired(&mut map, key);

        match map.get(key).map(|e| &e.value) {
            None => Ok("none".to_string()),
            Some(Value::String(_)) => Ok("string".to_string()),
            Some(Value::List(_)) => Ok("list".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn items(xs: &[&str]) -> Vec<Vec<u8>> {
        xs.iter().map(|s| b(s)).collect()
    }

    #[test]
    fn set_then_get() {
        let db = Db::default();
        db.set(b("k"), b("v"), None);
        assert_eq!(db.get(b"k"), Ok(Some(b("v"))));
        assert_eq!(db.get(b"missing"), Ok(None));
    }

    #[test]
    fn expired_keys_are_gone() {
        let db = Db::default();
        db.set(b("k"), b("v"), Some(Duration::from_millis(0)));
        assert_eq!(db.get(b"k"), Ok(None));
    }

    #[test]
    fn rpush_and_lrange() {
        let db = Db::default();
        assert_eq!(db.rpush(b"l", items(&["a", "b"])), Ok(2));
        assert_eq!(db.rpush(b"l", items(&["c"])), Ok(3));
        assert_eq!(db.lrange(b"l", 0, -1), Ok(items(&["a", "b", "c"])));
        assert_eq!(db.llen(b"l"), Ok(3));
    }

    #[test]
    fn lpush_reverses_argument_order() {
        let db = Db::default();
        assert_eq!(db.lpush(b"l", items(&["a", "b", "c"])), Ok(3));
        assert_eq!(db.lrange(b"l", 0, -1), Ok(items(&["c", "b", "a"])));
    }

    #[test]
    fn lrange_index_handling() {
        let db = Db::default();
        db.rpush(b"l", items(&["a", "b", "c", "d", "e"])).unwrap();
        assert_eq!(db.lrange(b"l", 1, 3), Ok(items(&["b", "c", "d"])));
        assert_eq!(db.lrange(b"l", -2, -1), Ok(items(&["d", "e"])));
        assert_eq!(
            db.lrange(b"l", 0, 100),
            Ok(items(&["a", "b", "c", "d", "e"]))
        );
        assert_eq!(db.lrange(b"l", -100, 1), Ok(items(&["a", "b"])));
        assert_eq!(db.lrange(b"l", 3, 1), Ok(vec![]));
        assert_eq!(db.lrange(b"l", 5, 10), Ok(vec![]));
        assert_eq!(db.lrange(b"l", 0, -10), Ok(vec![]));
        assert_eq!(db.lrange(b"nope", 0, -1), Ok(vec![]));
    }

    #[test]
    fn lpop_variants() {
        let db = Db::default();
        assert_eq!(db.lpop(b"l", None), Ok(None));

        db.rpush(b"l", items(&["a", "b", "c"])).unwrap();
        assert_eq!(db.lpop(b"l", None), Ok(Some(items(&["a"]))));
        assert_eq!(db.lpop(b"l", Some(5)), Ok(Some(items(&["b", "c"]))));

        // emptied list removes the key entirely
        assert_eq!(db.llen(b"l"), Ok(0));
        assert_eq!(db.lpop(b"l", None), Ok(None));
    }

    #[test]
    fn wrong_type_errors() {
        let db = Db::default();
        db.set(b("s"), b("v"), None);
        db.rpush(b"l", items(&["a"])).unwrap();

        assert_eq!(db.get(b"l"), Err(WrongType));
        assert_eq!(db.rpush(b"s", items(&["x"])), Err(WrongType));
        assert_eq!(db.lrange(b"s", 0, -1), Err(WrongType));
        assert_eq!(db.llen(b"s"), Err(WrongType));
        assert_eq!(db.lpop(b"s", None), Err(WrongType));
    }

    #[test]
    fn set_overwrites_a_list() {
        let db = Db::default();
        db.rpush(b"k", items(&["a"])).unwrap();
        db.set(b("k"), b("v"), None);
        assert_eq!(db.get(b"k"), Ok(Some(b("v"))));
    }
}
