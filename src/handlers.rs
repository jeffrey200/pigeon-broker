use crate::{PigeonState, features};
use actix_web::{
    HttpResponse, Responder, get, post,
    web::{self},
};
use features::persistence::save_queue;

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
