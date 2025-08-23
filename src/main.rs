use actix_web::{
    App, HttpResponse, HttpServer, Responder, get,
    middleware::Logger,
    post,
    web::{self},
};
use env_logger::Env;
use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
};

struct PigeonState {
    queues: Mutex<HashMap<String, VecDeque<String>>>,
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
    HttpResponse::Ok().body("Successful published")
}

#[post("/consume/{topic}")]
async fn consume(data: web::Data<PigeonState>, path: web::Path<String>) -> impl Responder {
    let topic = path.into_inner();
    let mut queues = data.queues.lock().unwrap();

    if let Some(queue) = queues.get_mut(&topic) {
        if let Some(message) = queue.pop_front() {
            if queue.is_empty() {
                queues.remove(&topic);
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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting server on http://127.0.0.1:8080");

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let state = web::Data::new(PigeonState {
        queues: Mutex::new(HashMap::new()),
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
