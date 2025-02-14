use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};
use tokio_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};

type Clients = Arc<Mutex<HashMap<SocketAddr, tokio::sync::mpsc::UnboundedSender<Message>>>>;

#[tokio::main]
async fn main() {
    // Create a TCP listener bound to "127.0.0.1:8080"

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

    // Accept incoming connections
    let listener = TcpListener::bind("127.0.0.1:8080").await.expect("Failed to bind");
    println!("WebSocket server listening on ws://127.0.0.1:8080");
    while let Ok((stream, addr)) = listener.accept().await {
        let clients = clients.clone();
        tokio::spawn(handle_connection(stream, addr, clients));
    }
}

async fn broadcast_timestamp(clients: &Clients) {
    let timestamp = chrono::Local::now().to_rfc3339();
    let clients = clients.lock().await;

    for (_, sender) in clients.iter() {
        let _ = sender.send(Message::Text(timestamp.clone()));
    }
}

async fn handle_connection(stream: TcpStream, addr: SocketAddr, clients: Clients) {
    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during WebSocket handshake");
    println!("New WebSocket connection: {}", addr);

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    // Store the sender in clients map
    clients.lock().await.insert(addr, sender);

    // Handle incoming messages from the broadcast channel
    let broadcast_task = tokio::spawn(async move {
        while let Some(msg) = receiver.recv().await {
            ws_sender.send(msg).await.unwrap_or_else(|e| {
                eprintln!("WebSocket send error: {}", e);
            });
        }
    });

    // Handle incoming WebSocket messages
    let receive_task = tokio::spawn(async move {
        while let Some(result) = ws_receiver.next().await {
            match result {
                Ok(msg) => {
                    if msg.is_close() {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("WebSocket error: {}", e);
                    break;
                }
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = broadcast_task => {}
        _ = receive_task => {}
    }

    // Remove client when disconnected
    clients.lock().await.remove(&addr);
    println!("Client disconnected: {}", addr);
}
