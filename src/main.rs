use actix_web::{
    App, HttpResponse, HttpServer, Responder, get,
    middleware::Logger,
    post,
    web::{self},
};
use env_logger::Env;
use sled::Db;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    str,
    sync::Mutex,
    time::Duration,
};
use tokio::time::interval;

struct PigeonState {
    queues: Mutex<HashMap<String, VecDeque<String>>>,
    db: Db,
}

#[post("/publish/{topic}")]
async fn publish(
    data: web::Data<PigeonState>,
    path: web::Path<String>,
    body: String,
) -> impl Responder {
    let topic = path.into_inner();
    let mut queues = data.queues.lock().unwrap();
    let queue = queues.entry(topic.clone()).or_default();
    queue.push_back(body);

    match save_queue(&data.db, &topic, queue) {
        Ok(_) => HttpResponse::Ok().body("Successfully published"),
        Err(_) => HttpResponse::InternalServerError().body("Failed to persist"),
    }
}

#[post("/consume/{topic}")]
async fn consume(data: web::Data<PigeonState>, path: web::Path<String>) -> impl Responder {
    let topic = path.into_inner();
    let mut queues = data.queues.lock().unwrap();

    if let Some(queue) = queues.get_mut(&topic) {
        if let Some(message) = queue.pop_front() {
            if queue.is_empty() {
                queues.remove(&topic);
                let _ = data.db.remove(&topic);
            } else {
                let _ = save_queue(&data.db, &topic, queue);
            }
            return HttpResponse::Ok().body(message);
        }
    }

    HttpResponse::NotFound().body(String::new())
}

#[get("/length/{topic}")]
async fn length(data: web::Data<PigeonState>, path: web::Path<String>) -> impl Responder {
    let topic = path.into_inner();
    let queues = data.queues.lock().unwrap();

    if let Some(queue) = queues.get(&topic) {
        return HttpResponse::Ok().body(queue.len().to_string());
    }

    HttpResponse::Ok().body("0")
}

fn load_queue(db: &Db) -> HashMap<String, VecDeque<String>> {
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

fn save_queue(db: &Db, topic: &str, queue: &VecDeque<String>) -> sled::Result<()> {
    let value = queue
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    db.insert(topic, value.as_bytes())?;
    Ok(())
}

async fn periodic_flush(state: web::Data<PigeonState>) {
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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting server on http://127.0.0.1:8080");

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let exe_path = std::env::current_exe()?;
    let exe_dir = exe_path
        .parent()
        .expect("Executable does not have a directory");
    let mut db_path = PathBuf::from(exe_dir);
    db_path.push("queue_db");

    let db = sled::open(db_path).expect("Failed to open sled DB");

    let state = web::Data::new(PigeonState {
        queues: Mutex::new(load_queue(&db)),
        db,
    });

    let flush_state = state.clone();
    tokio::spawn(async move {
        periodic_flush(flush_state).await;
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .service(publish)
            .service(consume)
            .service(length)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
