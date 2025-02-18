mod server;
mod subscriber;
#[cfg(all(feature = "integration", test))]
mod integration;
mod registry;

use dotenv::dotenv;
use std::net::SocketAddr;
use axum::http::Uri;
use clap::Parser;
use tokio::sync::broadcast;
use tracing::{error, info, log};
use tracing::log::{trace};
use crate::registry::Registry;
use crate::server::Server;
use crate::subscriber::WebsocketSubscriber;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long, env, default_value = "0.0.0.0:8545")]
    listen_addr: SocketAddr,

    #[arg(long, env)]
    upstream_ws: Uri,
}


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    dotenv().ok();
    let args = Args::parse();

    // Channel with two blocks
    let (send, _rec) = broadcast::channel(20);
    let sender = send.clone();

    let listener = move |data: String| {
        info!("received data {}", data);
        match send.send(data) {
            Ok(_) => (),
            Err(e) => error!("failed to send data {}", e),
        }
    };

    let mut subscriber = WebsocketSubscriber::new(args.upstream_ws, listener);
    let subscriber_task = subscriber.run();

    let registry = Registry::new(sender);
    let registry_task = registry.run();

    let server = Server::new(args.listen_addr.into(), registry.clone());
    let server_task = server.listen();

    tokio::select! {
        _ = subscriber_task => {
            log::info!("subscriber task terminated");
            return
        },
        _ = server_task => {
            log::info!("server task terminated");
            return
        }
        _ = registry_task => {
            log::info!("registry task terminated");
            return
        }
    }
}
