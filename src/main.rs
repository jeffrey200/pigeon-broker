mod features;
mod handlers;
use crate::handlers::{
    kv_store::{kv_delete, kv_get, kv_set},
    queue::{consume, length, overview, publish},
};
use actix_web::{
    App, HttpServer,
    middleware::Logger,
    web::{self},
};
use env_logger::Env;
use features::persistence::{load_queue, periodic_flush};
use sled::Db;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Mutex,
};

struct PigeonState {
    queues: Mutex<HashMap<String, VecDeque<String>>>,
    keyvalues: Mutex<HashMap<String, String>>,
    db: Db,
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
    db_path.push("pigeon_db");

    let db = sled::open(db_path).expect("Failed to open sled DB");

    let state = web::Data::new(PigeonState {
        queues: Mutex::new(load_queue(&db)),
        keyvalues: Mutex::new(HashMap::new()),
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
            .service(overview)
            .service(kv_set)
            .service(kv_get)
            .service(kv_delete)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
