use std::sync::{LazyLock, Mutex};
use std::collections::HashMap;

pub fn get(key: &str, store: &LazyLock<Mutex<HashMap<String, String>>>) -> Vec<u8> {
    let mut response = Vec::new();
    response.extend_from_slice(b"$");
    if let Some(value) = store.lock().unwrap().get(key) {
        response.extend_from_slice(&(value.len() as u64).to_string().into_bytes());
        response.extend_from_slice(b"\r\n");
        response.extend_from_slice(value.as_bytes());
    } else {
        response.extend_from_slice(b"-1");
    }
    response.extend_from_slice(b"\r\n");
    response
}
