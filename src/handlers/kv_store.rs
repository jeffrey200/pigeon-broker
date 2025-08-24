use crate::PigeonState;
use actix_web::{
    HttpResponse, Responder, delete, get, post,
    web::{self},
};

#[post("/kv/{key}")]
pub async fn kv_set(
    data: web::Data<PigeonState>,
    path: web::Path<String>,
    body: String,
) -> impl Responder {
    let key = path.into_inner();
    let mut kvs = data.keyvalues.lock().unwrap();
    kvs.insert(key.clone(), body);

    HttpResponse::Ok().body("Successfully inserted")
}

#[get("/kv/{key}")]
pub async fn kv_get(data: web::Data<PigeonState>, path: web::Path<String>) -> impl Responder {
    let key = path.into_inner();
    let kvs = data.keyvalues.lock().unwrap();

    if let Some(value) = kvs.get(&key) {
        return HttpResponse::Ok().body(value.clone());
    }

    HttpResponse::NotFound().body("Key not found")
}

#[delete("/kv/{key}")]
pub async fn kv_delete(data: web::Data<PigeonState>, path: web::Path<String>) -> impl Responder {
    let key = path.into_inner();
    let mut kvs = data.keyvalues.lock().unwrap();

    if kvs.remove(&key).is_some() {
        return HttpResponse::Ok().body("Key deleted");
    }

    HttpResponse::NotFound().body("Key not found")
}
