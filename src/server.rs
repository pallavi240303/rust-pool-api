use axum::{routing::get, Router};
use std::net::SocketAddr;

use crate::api::{get_depth_history, get_earning_history, get_rune_pool_history, get_swaps_history};
pub async fn start_server() {
    let app = Router::new()
        .route("/depth", get(get_depth_history))
        .route("/swap",get(get_swaps_history))
        .route("/earnings",get(get_earning_history))
        .route("/rune",get(get_rune_pool_history));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("Server running at http://{}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}