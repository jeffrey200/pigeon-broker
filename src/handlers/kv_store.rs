use crate::{PigeonState, features::persistence::save_keyvalue};
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
    kvs.insert(key.clone(), body.clone());

    match save_keyvalue(&data.db, &key, &body) {
        Ok(_) => HttpResponse::Ok().body("Successfully inserted"),
        Err(_) => HttpResponse::InternalServerError().body("Failed to persist"),
    }
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
        let db_key = format!("kv_{}", key);
        let _ = data.db.remove(db_key);
        let _ = data.db.flush();
        return HttpResponse::Ok().body("Key deleted");
    }

    HttpResponse::NotFound().body("Key not found")
}
