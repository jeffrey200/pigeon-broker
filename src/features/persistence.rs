use crate::PigeonState;
use actix_web::web::{self};
use sled::Db;
use std::{
    collections::{HashMap, VecDeque},
    str,
    time::Duration,
};

pub fn load_queue(db: &Db) -> HashMap<String, VecDeque<String>> {
    let mut queues = HashMap::new();
    for result in db.iter() {
        if let Ok((key, value)) = result {
            if let Ok(topic) = str::from_utf8(&key) {
                if topic.starts_with("queue_") {
                    if let Ok(messages_str) = str::from_utf8(&value) {
                        let messages = messages_str
                            .split('\n')
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .collect();
                        let topic_name = topic.trim_start_matches("queue_").to_string();
                        queues.insert(topic_name, messages);
                    }
                }
            }
        }
    }
    queues
}

pub fn load_keyvalue(db: &Db) -> HashMap<String, String> {
    let mut keyvalues = HashMap::new();
    for result in db.iter() {
        if let Ok((key, value)) = result {
            if let Ok(key_str) = str::from_utf8(&key) {
                if key_str.starts_with("kv_") {
                    if let Ok(value_str) = str::from_utf8(&value) {
                        keyvalues.insert(
                            key_str.trim_start_matches("kv_").to_string(),
                            value_str.to_string(),
                        );
                    }
                }
            }
        }
    }
    keyvalues
}

pub fn save_queue(db: &Db, topic: &str, queue: &VecDeque<String>) -> sled::Result<()> {
    let value = queue
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let key = format!("queue_{}", topic);
    db.insert(key, value.as_bytes())?;
    Ok(())
}

pub fn save_keyvalue(db: &Db, key: &str, value: &str) -> sled::Result<()> {
    let db_key = format!("kv_{}", key);
    db.insert(db_key, value.as_bytes())?;
    Ok(())
}

pub async fn periodic_flush(state: web::Data<PigeonState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        if let Err(e) = state.db.flush() {
            eprintln!("Failed to flush db: {:?}", e);
        }
    }
}
