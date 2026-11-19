use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|t| t <= Instant::now())
    }
}

/// Shared in-memory key/value store. Cheap to clone (it is an `Arc`).
///
/// A plain std `Mutex` is fine as long as no guard is held across an `.await`;
/// every method here locks, works, and returns.
#[derive(Clone, Default)]
pub struct Db {
    inner: Arc<Mutex<HashMap<Vec<u8>, Entry>>>,
}

impl Db {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut map = self.inner.lock().unwrap();
        if map.get(key).is_some_and(Entry::is_expired) {
            map.remove(key); // lazy expiration
            return None;
        }
        map.get(key).map(|e| e.value.clone())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.inner
            .lock()
            .unwrap()
            .insert(key, Entry { value, expires_at });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_then_get() {
        let db = Db::default();
        db.set(b"k".to_vec(), b"v".to_vec(), None);
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(db.get(b"missing"), None);
    }

    #[test]
    fn expired_keys_are_gone() {
        let db = Db::default();
        db.set(b"k".to_vec(), b"v".to_vec(), Some(Duration::from_millis(0)));
        assert_eq!(db.get(b"k"), None);
    }
}
