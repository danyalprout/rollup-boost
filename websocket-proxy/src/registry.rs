use std::net::SocketAddr;
use std::time::Duration;
use axum::extract::ws::{WebSocket};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Sender;
use tokio::time::interval;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub struct ClientInfo {
    pub websocket: WebSocket,
    pub ip_addr: SocketAddr,
}

#[derive(Clone)]
pub struct Registry {
    sender: Sender<String>,
}

impl Registry {
    pub fn new(sender: Sender<String>) -> Self {
        Self {
            sender,
        }
    }

    pub async fn subscribe(&self, mut client: ClientInfo) {
        info!("subscribing client: {:?}", client.ip_addr);

        let mut receiver = self.sender.subscribe();

        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(msg) => {
                        match client.websocket.send(msg.into()).await {
                            Ok(_) => {}
                            Err(e) => {
                                warn!("disconnecting client, failed to send message to client {:?}, {}", client.ip_addr, e);
                                return
                            }
                        }
                    }
                    Err(e) => {
                        match e {
                            RecvError::Closed => {
                                debug!("client disconnected: {:?}", client);
                                break;
                            }
                            RecvError::Lagged(_) => {
                                println!("received for client lagging {:?}", client);
                                receiver = receiver.resubscribe();
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn run(&self) {
        let h = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                // todo: Maybe report metrics?
            }
        });

        h.await.unwrap();
    }
}