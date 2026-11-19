use std::sync::{LazyLock, Mutex};
use std::collections::HashMap;

pub fn set(key: &str, value: &str, store: &LazyLock<Mutex<HashMap<String, String>>>) -> Vec<u8> {
    store.lock().unwrap().insert(key.to_string(), value.to_string());
    let mut response = Vec::new();
    response.extend_from_slice(b"+OK\r\n");
    response
}
