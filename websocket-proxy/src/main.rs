mod server;
mod notify;
#[cfg(all(feature = "integration", test))]
mod integration;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};
use tokio_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};
use http::{Request, Response, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Server};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

struct ClientInfo {
    sender: tokio::sync::mpsc::UnboundedSender<Message>,
    ip_addr: SocketAddr,
}

type Clients = Arc<Mutex<HashMap<usize, ClientInfo>>>;

#[tokio::main]
async fn main() {
    let clients: Clients = Arc::new(Mutex::new(HashMap::new()));
    let clients_clone = clients.clone();

    // Spawn the timestamp broadcaster
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            broadcast_timestamp(&clients_clone).await;
        }
    });

    let clients_service = clients.clone();

    let make_svc = make_service_fn(move |conn: &hyper::server::conn::AddrStream| {
        let clients = clients_service.clone();
        let remote_addr = conn.remote_addr();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                handle_request(req, clients.clone(), remote_addr)
            }))
        }
    });

    let addr = ([0, 0, 0, 0], 8545).into();
    let server = Server::bind(&addr).serve(make_svc);
    println!("Server running on 0.0.0.0:8545");

    if let Err(e) = server.await {
        eprintln!("Server error: {}", e);
    }
}

async fn broadcast_timestamp(clients: &Clients) {
    let timestamp = chrono::Local::now().to_rfc3339();
    let clients = clients.lock().await;

    for (_, client) in clients.iter() {
        if let Err(e) = client.sender.send(Message::Text(timestamp.clone())) {
            eprintln!("Error sending to {}: {}", client.ip_addr, e);
        }
    }
}

async fn handle_request(
    req: Request<Body>,
    clients: Clients,
    remote_addr: SocketAddr,
) -> Result<Response<Body>, Infallible> {
    match (req.method().as_str(), req.uri().path()) {
        ("GET", "/healthz") => {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("OK"))
                .unwrap())
        }
        ("GET", "/ws") => {
            // Check for the WebSocket upgrade headers
            if let Some(upgrade) = req.headers().get(hyper::header::UPGRADE) {
                if upgrade.as_bytes() == b"websocket" {
                    if let Some(key) = req.headers().get("Sec-WebSocket-Key") {
                        let accept_key = derive_accept_key(key.as_bytes());

                        // Create the upgrade response
                        let response = Response::builder()
                            .status(StatusCode::SWITCHING_PROTOCOLS)
                            .header(hyper::header::UPGRADE, "websocket")
                            .header(hyper::header::CONNECTION, "upgrade")
                            .header("Sec-WebSocket-Accept", accept_key)
                            .body(Body::empty())
                            .unwrap();

                        // Spawn WebSocket handler
                        let clients_clone = clients.clone();
                        tokio::spawn(async move {
                            if let Ok(upgraded) = hyper::upgrade::on(req).await {
                                handle_websocket_connection(upgraded, clients_clone, remote_addr).await;
                            }
                        });

                        return Ok(response);
                    }
                }
            }

            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Expected WebSocket upgrade request"))
                .unwrap())
        }
        _ => {
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Not Found"))
                .unwrap())
        }
    }
}

async fn handle_websocket_connection(
    upgraded: hyper::upgrade::Upgraded,
    clients: Clients,
    remote_addr: SocketAddr,
) {
    let ws_stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
        upgraded,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    ).await;

    static NEXT_CLIENT_ID: AtomicUsize = AtomicUsize::new(0);
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);

    println!("New WebSocket connection - ID: {}, IP: {}", client_id, remote_addr);

    let (mut ws_sender, _) = ws_stream.split();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    // Store the client information
    clients.lock().await.insert(client_id, ClientInfo {
        sender,
        ip_addr: remote_addr,
    });

    // Handle incoming messages from the broadcast channel
    let broadcast_task = tokio::spawn(async move {
        while let Some(msg) = receiver.recv().await {
            if let Err(e) = ws_sender.send(msg).await {
                eprintln!("WebSocket send error for client {} ({}): {}", client_id, remote_addr, e);
                break;
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = broadcast_task => {}
    }

    // Remove client when disconnected
    clients.lock().await.remove(&client_id);
    println!("Client {} ({}) disconnected", client_id, remote_addr);

    // Print current connected clients
    let connected_clients = clients.lock().await;
    println!("Current connected clients:");
    for (id, info) in connected_clients.iter() {
        println!("- Client ID: {}, IP: {}", id, info.ip_addr);
    }
}