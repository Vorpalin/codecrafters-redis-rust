use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub enum StreamIdInput {
    Auto,
    AutoSequence(u64),
    Explicit(StreamId),
}

/// Returned when an operation is applied to a key holding a different type
/// (e.g. `GET` on a list). Maps to Redis' `WRONGTYPE` error.
#[derive(Debug, PartialEq, Eq)]
pub struct WrongType;

struct Stream {
    entries: BTreeMap<StreamId, StreamEntry>,
    last_generated_id: StreamId,
}

/// Redis Stream entry identifier: `milliseconds-sequence`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub milliseconds: u64,
    pub sequence: u64,
}

impl StreamId {
    pub const MIN: Self = Self {
        milliseconds: 0,
        sequence: 0,
    };

    pub const MAX: Self = Self {
        milliseconds: u64::MAX,
        sequence: u64::MAX,
    };

    /// Parses a complete Redis stream ID (both components are required).
    pub fn parse(bytes: &[u8]) -> Result<Self, XAddError> {
        let text = std::str::from_utf8(bytes).map_err(|_| XAddError::InvalidId)?;
        let (milliseconds, sequence) = text.split_once('-').ok_or(XAddError::InvalidId)?;
        if milliseconds.is_empty() || sequence.is_empty() || sequence.contains('-') {
            return Err(XAddError::InvalidId);
        }

        let milliseconds = milliseconds.parse().map_err(|_| XAddError::InvalidId)?;
        let sequence = sequence.parse().map_err(|_| XAddError::InvalidId)?;
        Ok(Self {
            milliseconds,
            sequence,
        })
    }

    fn next_after(last: Self) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;

        if now > last.milliseconds {
            Self {
                milliseconds: now,
                sequence: 0,
            }
        } else {
            Self {
                milliseconds: last.milliseconds,
                sequence: last.sequence.saturating_add(1),
            }
        }
    }

    fn with_auto_sequence(milliseconds: u64, last: Self) -> Result<Self, XAddError> {
        let sequence = if milliseconds == last.milliseconds {
            last.sequence
                .checked_add(1)
                .ok_or(XAddError::IdNotGreater)?
        } else if milliseconds > last.milliseconds {
            0
        } else {
            return Err(XAddError::IdNotGreater);
        };

        Ok(Self {
            milliseconds,
            sequence,
        })
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.milliseconds, self.sequence)
    }
}

/// Errors specific to XADD.
#[derive(Debug, PartialEq, Eq)]
pub enum XAddError {
    WrongType,
    InvalidId,
    IdNotGreater,
    ZeroId,
}

struct StreamEntry {
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}

enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Stream(Stream),
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
            Some(Value::List(_)) | Some(Value::Stream(_)) => Err(WrongType),
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
            Some(Value::String(_)) | Some(Value::Stream(_)) => Err(WrongType),
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
            Some(Value::String(_)) | Some(Value::Stream(_)) => Err(WrongType),
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
                        Value::String(_) | Value::Stream(_) => return Err(WrongType),
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
            Some(Value::Stream(_)) => Ok("stream".to_string()),
        }
    }

    /// Adds one entry. Explicit IDs must be strictly increasing; `None` generates an ID.
    pub fn xadd(
        &self,
        key: &[u8],
        id: StreamIdInput,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<StreamId, XAddError> {
        // Redis reserves 0-0 and requires every explicit ID to be greater.
        if matches!(
            id,
            StreamIdInput::Explicit(id)
                if id.milliseconds == 0 && id.sequence == 0
        ) {
            return Err(XAddError::ZeroId);
        }

        let mut map = self.inner.lock().unwrap();
        purge_if_expired(&mut map, key);

        let entry = map.entry(key.to_vec()).or_insert_with(|| Entry {
            value: Value::Stream(Stream {
                entries: BTreeMap::new(),
                last_generated_id: StreamId {
                    milliseconds: 0,
                    sequence: 0,
                },
            }),
            expires_at: None,
        });

        let stream = match &mut entry.value {
            Value::Stream(stream) => stream,
            _ => return Err(XAddError::WrongType),
        };

        let new_id = match id {
            StreamIdInput::Explicit(id) => id,

            StreamIdInput::Auto => StreamId::next_after(stream.last_generated_id),

            StreamIdInput::AutoSequence(milliseconds) => {
                StreamId::with_auto_sequence(milliseconds, stream.last_generated_id)?
            }
        };

        if new_id <= stream.last_generated_id {
            return Err(XAddError::IdNotGreater);
        }

        stream.entries.insert(new_id, StreamEntry { fields });
        stream.last_generated_id = new_id;
        Ok(new_id)
    }

    pub fn xrange(
        &self,
        key: &[u8],
        start: StreamId,
        end: StreamId,
    ) -> Result<Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)>, WrongType> {
        if start > end {
            return Ok(Vec::new());
        }

        let mut map = self.inner.lock().unwrap();
        purge_if_expired(&mut map, key);

        let stream = match map.get(key).map(|e| &e.value) {
            None => return Ok(Vec::new()),
            Some(Value::Stream(stream)) => stream,
            _ => return Err(WrongType),
        };

        let range = stream.entries.range(start..=end);
        let result = range
            .map(|(&id, entry)| (id, entry.fields.clone()))
            .collect();

        Ok(result)
    }

    pub fn xlen(&self, key: &[u8]) -> Result<usize, WrongType> {
        let mut map = self.inner.lock().unwrap();
        purge_if_expired(&mut map, key);

        match map.get(key).map(|e| &e.value) {
            None => Ok(0),
            Some(Value::Stream(stream)) => Ok(stream.entries.len()),
            _ => Err(WrongType),
        }
    }

    pub fn xread(
        &self,
        keys: &[Vec<u8>],
        ids: &[StreamId],
        count: Option<usize>,
        block: Option<Duration>,
    ) -> Result<Vec<(Vec<u8>, Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)>)>, WrongType> {
        use std::ops::Bound::{Excluded, Unbounded};

        let deadline = block
            .map(|duration| if !duration.is_zero() {
                Instant::now() + duration
            } else {
                Instant::now() + Duration::from_secs(u64::MAX)
            });

        let mut map = self.inner.lock().unwrap();

        loop {
            let mut results = Vec::new();

            for (key, id) in keys.iter().zip(ids.iter()) {
                purge_if_expired(&mut map, key);

                let Some(entry) = map.get(key) else {
                    continue;
                };

                let Value::Stream(stream) = &entry.value else {
                    return Err(WrongType);
                };

                let entries: Vec<_> = stream
                    .entries
                    .range((Excluded(*id), Unbounded))
                    .take(count.unwrap_or(usize::MAX))
                    .map(|(&entry_id, entry)| (entry_id, entry.fields.clone()))
                    .collect();

                if !entries.is_empty() {
                    results.push((key.clone(), entries));
                }
            }

            if !results.is_empty() {
                return Ok(results);
            }

            let Some(block_duration) = block else {
                return Ok(Vec::new());
            };

            if block_duration.is_zero() {
                map = self.list_changed.wait(map).unwrap();
                continue;
            }

            let Some(deadline) = deadline else {
                return Ok(Vec::new());
            };

            let now = Instant::now();

            if now >= deadline {
                return Ok(Vec::new());
            }

            let remaining = deadline - now;

            let (guard, _) = self.list_changed.wait_timeout(map, remaining).unwrap();

            map = guard;
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

    #[test]
    fn xadd_generates_monotonic_ids_and_creates_stream() {
        let db = Db::default();
        let first = db
            .xadd(b"events", None, vec![(b("name"), b("Alice"))])
            .unwrap();
        let second = db
            .xadd(b"events", None, vec![(b("name"), b("Bob"))])
            .unwrap();
        assert!(second > first);
        assert_eq!(db.r#type(b"events").unwrap(), "stream");
    }

    #[test]
    fn xadd_accepts_increasing_explicit_ids_and_advances_generator() {
        let db = Db::default();
        let explicit = StreamId::parse(b"1000-7").unwrap();
        assert_eq!(
            db.xadd(b"events", Some(explicit), vec![(b("f"), b("v"))]),
            Ok(explicit)
        );
        let generated = db.xadd(b"events", None, vec![(b("f"), b("v"))]).unwrap();
        assert!(generated > explicit);
    }

    #[test]
    fn xadd_rejects_duplicate_or_decreasing_ids() {
        let db = Db::default();
        let id = StreamId::parse(b"1000-1").unwrap();
        db.xadd(b"events", Some(id), vec![(b("f"), b("v"))])
            .unwrap();
        assert_eq!(
            db.xadd(b"events", Some(id), vec![(b("f"), b("v"))]),
            Err(XAddError::IdNotGreater)
        );
        assert_eq!(
            db.xadd(
                b"events",
                Some(StreamId::parse(b"999-9").unwrap()),
                vec![(b("f"), b("v"))],
            ),
            Err(XAddError::IdNotGreater)
        );
    }

    #[test]
    fn xadd_rejects_wrong_type_and_zero_id() {
        let db = Db::default();
        db.set(b("key"), b("value"), None);
        assert_eq!(
            db.xadd(b"key", None, vec![(b("f"), b("v"))]),
            Err(XAddError::WrongType)
        );
        assert_eq!(
            db.xadd(
                b"stream",
                Some(StreamId::parse(b"0-0").unwrap()),
                vec![(b("f"), b("v"))],
            ),
            Err(XAddError::IdNotGreater)
        );
    }

    #[test]
    fn xadd_auto_sequence_starts_at_zero() {
        let db = Db::default();

        assert_eq!(
            run(&db, &["XADD", "s", "1000-*", "f", "v"]),
            Frame::Bulk(b"1000-0".to_vec())
        );
    }

    #[test]
    fn xadd_zero_timestamp_starts_at_one() {
        let db = Db::default();

        assert_eq!(
            run(&db, &["XADD", "s", "0-*", "f", "v"]),
            Frame::Bulk(b"0-1".to_vec())
        );
    }
}
