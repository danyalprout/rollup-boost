#[cfg(all(feature = "integration", test))]
mod integration;
mod registry;
mod server;
mod subscriber;

use crate::registry::Registry;
use crate::server::Server;
use crate::subscriber::WebsocketSubscriber;
use axum::http::Uri;
use clap::Parser;
use dotenv::dotenv;
use std::net::SocketAddr;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(
        long,
        env,
        default_value = "0.0.0.0:8545",
        help = "The address and port to listen on for incoming connections"
    )]
    listen_addr: SocketAddr,

    #[arg(long, env, help = "WebSocket URI of the upstream server to connect to")]
    upstream_ws: Uri,

    #[arg(
        long,
        env,
        default_value = "20",
        help = "Number of messages to buffer for lagging clients"
    )]
    message_buffer_size: usize,

    #[arg(
        long,
        env,
        default_value = "100",
        help = "Maximum number of concurrently connected clients"
    )]
    maximum_concurrent_connections: usize,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    dotenv().ok();
    let args = Args::parse();

    // Channel with two blocks
    let (send, _rec) = broadcast::channel(args.message_buffer_size);
    let sender = send.clone();

    let listener = move |data: String| {
        debug!(message = "received data", data = data);
        match send.send(data) {
            Ok(_) => (),
            Err(e) => error!(message = "failed to send data", error = e.to_string()),
        }
    };

    let token = CancellationToken::new();

    let mut subscriber = WebsocketSubscriber::new(args.upstream_ws, listener);
    let subscriber_task = subscriber.run(token.clone());

    let registry = Registry::new(sender, args.maximum_concurrent_connections);

    let server = Server::new(args.listen_addr.into(), registry.clone());
    let server_task = server.listen(token.clone());

    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    let mut terminate = signal(SignalKind::terminate()).unwrap();

    tokio::select! {
        _ = subscriber_task => {
            info!("subscriber task terminated");
            token.cancel();
        },
        _ = server_task => {
            info!("server task terminated");
            token.cancel();
        }
        _ = interrupt.recv() => {
            info!("process interrupted, shutting down");
            token.cancel();
        }
        _ = terminate.recv() => {
            info!("process terminated, shutting down");
            token.cancel();
        }
    }
}
