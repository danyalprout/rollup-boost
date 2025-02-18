use std::net::SocketAddr;
use axum::extract::{ConnectInfo, State, WebSocketUpgrade};
use axum::extract::ws::WebSocket;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::{any, get};
use crate::registry::{Registry,ClientInfo};

#[derive(Clone)]
struct ServerState {
    registry: Registry,
}

pub struct Server {
    listen_addr: SocketAddr,
    registry: Registry,
}

impl Server {
    pub fn new(listen_addr: SocketAddr, registry: Registry) -> Self {
        Self {
            listen_addr,
            registry,
        }
    }

    pub async fn listen(&self) {
        let router = Router::new()
            .route("/healthz", get(healthz_handler))
            .route("/ws", any(websocket_handler))
            .with_state(ServerState { registry: self.registry.clone() });

        let listener = tokio::net::TcpListener::bind(self.listen_addr).await.unwrap();

        tracing::info!("listening on {}", listener.local_addr().unwrap());

        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        ).await.unwrap()
    }
}

async fn healthz_handler() -> impl IntoResponse {
    StatusCode::OK
}

async fn websocket_handler(
    State(state): State<ServerState>,
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    println!("client at {addr} connected");
    ws.on_upgrade(move |socket| handle_socket(socket, addr, state))
}

async fn handle_socket(
    ws: WebSocket,
    remote_addr: SocketAddr,
    state: ServerState,
) {
    state.registry.subscribe(ClientInfo{
        ip_addr: remote_addr,
        websocket: ws,
    }).await;
    
}