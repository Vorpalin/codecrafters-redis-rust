use std::sync::{LazyLock, Mutex};
use std::collections::HashMap;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn set(key: &str, value: &str, arguments: Vec<String>, store: &LazyLock<Mutex<HashMap<String, (String, i128)>>>) -> Vec<u8> {
    store.lock().unwrap().insert(key.to_string(), (value.to_string(), 0));

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .as_millis();


    for i in 0..arguments.len() {
        if arguments[i].to_uppercase() == "PX" {
            if i + 1 < arguments.len() {
                if let Ok(expiration) = arguments[i + 1].parse::<i128>() {
                    store.lock().unwrap().insert(key.to_string(), (value.to_string(), now as i128 + expiration));
                }
            }
        }
    }
    let mut response = Vec::new();
    response.extend_from_slice(b"+OK\r\n");
    response
}
