use crate::PigeonState;
use actix_web::web::{self};
use sled::Db;
use std::{
    collections::{HashMap, VecDeque},
    str,
    time::Duration,
};
use tokio::time::interval;

pub fn load_queue(db: &Db) -> HashMap<String, VecDeque<String>> {
    let mut queues = HashMap::new();
    for result in db.iter() {
        if let Ok((key, value)) = result {
            if let Ok(topic) = str::from_utf8(&key) {
                if let Ok(messages_str) = str::from_utf8(&value) {
                    let messages = messages_str
                        .split('\n')
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .collect();
                    queues.insert(topic.to_string(), messages);
                }
            }
        }
    }
    queues
}

pub fn save_queue(db: &Db, topic: &str, queue: &VecDeque<String>) -> sled::Result<()> {
    let value = queue
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    db.insert(topic, value.as_bytes())?;
    Ok(())
}

pub async fn periodic_flush(state: web::Data<PigeonState>) {
    let mut interval = interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        let queues = state.queues.lock().unwrap();

        for (topic, queue) in queues.iter() {
            if let Err(e) = save_queue(&state.db, topic, queue) {
                eprintln!("Failed to flush {}: {:?}", topic, e);
            }
        }
    }
}
