use crate::{PigeonState, features};
use actix_web::{
    HttpResponse, Responder, get, post,
    web::{self},
};
use features::persistence::save_queue;
use std::collections::HashMap;

#[post("/queues/{queue}/publish")]
pub async fn publish(
    data: web::Data<PigeonState>,
    path: web::Path<String>,
    body: String,
) -> impl Responder {
    let queue_name = path.into_inner();
    let mut queues = data.queues.lock().unwrap();
    let queue = queues.entry(queue_name.clone()).or_default();
    queue.push_back(body);

    match save_queue(&data.db, &queue_name, queue) {
        Ok(_) => HttpResponse::Ok().body("Successfully published"),
        Err(_) => HttpResponse::InternalServerError().body("Failed to persist"),
    }
}

#[post("/queues/{queue}/consume")]
pub async fn consume(data: web::Data<PigeonState>, path: web::Path<String>) -> impl Responder {
    let queue_name = path.into_inner();
    let mut queues = data.queues.lock().unwrap();

    if let Some(queue) = queues.get_mut(&queue_name) {
        if let Some(message) = queue.pop_front() {
            if queue.is_empty() {
                queues.remove(&queue_name);
                let db_key = format!("queue_{}", queue_name);
                let _ = data.db.remove(db_key);
            } else {
                let _ = save_queue(&data.db, &queue_name, queue);
            }
            return HttpResponse::Ok().body(message);
        }
    }

    HttpResponse::NotFound().body(String::new())
}

#[get("/queues/{queue}/length")]
pub async fn length(data: web::Data<PigeonState>, path: web::Path<String>) -> impl Responder {
    let queue_name = path.into_inner();
    let queues = data.queues.lock().unwrap();

    if let Some(queue) = queues.get(&queue_name) {
        return HttpResponse::Ok().body(queue.len().to_string());
    }

    HttpResponse::Ok().body("0")
}

#[get("/queues")]
pub async fn overview(data: web::Data<PigeonState>) -> impl Responder {
    let queues = data.queues.lock().unwrap();

    let lengths: HashMap<String, usize> = queues
        .iter()
        .map(|(key, queue)| (key.clone(), queue.len()))
        .collect();

    HttpResponse::Ok().json(lengths)
}
