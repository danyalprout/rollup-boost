use axum::extract::ws::WebSocket;
use axum::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Sender;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::tungstenite::Error::ConnectionClosed;
use tracing::{debug, error, info, warn};

#[derive(Debug)]
pub struct ClientConnection {
    pub ip_addr: SocketAddr,
    _permit: OwnedSemaphorePermit,
    websocket: Option<WebSocket>,
}

impl ClientConnection {
    pub fn new(ip_addr: SocketAddr, permit: OwnedSemaphorePermit) -> Self {
        Self {
            ip_addr,
            _permit: permit,
            websocket: None,
        }
    }

    pub fn with_websocket(&mut self, ws: WebSocket) {
        self.websocket = Some(ws);
    }

    pub async fn send(&mut self, data: String) -> Result<(), Error> {
        match self.websocket {
            Some(ref mut wss) => wss.send(data.into_bytes().into()).await,
            None => {
                error!(
                    message = "websocket connection was not opened",
                    ip = self.ip_addr.to_string()
                );
                Err(Error::new(ConnectionClosed))
            }
        }
    }
}

#[derive(Clone)]
pub struct Registry {
    sender: Sender<String>,
    semaphore: Arc<Semaphore>,
}

impl Registry {
    pub fn new(sender: Sender<String>, max_concurrent_connections: usize) -> Self {
        Self {
            sender,
            semaphore: Arc::new(Semaphore::new(max_concurrent_connections)),
        }
    }

    pub fn try_register(&self, ip_addr: SocketAddr) -> Result<ClientConnection, ()> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(ClientConnection::new(ip_addr, permit)),
            Err(_) => Err(()),
        }
    }

    pub async fn subscribe(&self, mut client: ClientConnection) {
        info!(
            message = "subscribing client",
            ip = client.ip_addr.to_string()
        );

        let mut receiver = self.sender.subscribe();

        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(msg) => match client.send(msg.clone()).await {
                        Ok(_) => {}
                        Err(e) => {
                            warn!(
                                message = "failed to send data to client",
                                ip = client.ip_addr.to_string(),
                                error = e.to_string()
                            );
                            break;
                        }
                    },
                    Err(e) => match e {
                        RecvError::Closed => {
                            debug!(
                                message = "client closed connection",
                                ip = client.ip_addr.to_string()
                            );
                            break;
                        }
                        RecvError::Lagged(_) => {
                            info!(
                                message = "client is lagging",
                                ip = client.ip_addr.to_string()
                            );
                            receiver = receiver.resubscribe();
                        }
                    },
                }
            }

            info!(
                message = "client disconnected",
                ip = client.ip_addr.to_string()
            );
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_max_concurrent_connections() {
        let (tx, _rx) = tokio::sync::broadcast::channel(1);
        let registry = Registry::new(tx, 3);

        assert_eq!(registry.semaphore.available_permits(), 3);

        let c1 = registry
            .try_register(SocketAddr::from(([127, 0, 0, 1], 0)))
            .unwrap();
        let _c2 = registry
            .try_register(SocketAddr::from(([127, 0, 0, 1], 0)))
            .unwrap();
        let _c3 = registry
            .try_register(SocketAddr::from(([127, 0, 0, 1], 0)))
            .unwrap();

        assert_eq!(registry.semaphore.available_permits(), 0);

        let c4 = registry.try_register(SocketAddr::from(([127, 0, 0, 1], 0)));
        assert!(c4.is_err());

        drop(c1);
        assert_eq!(registry.semaphore.available_permits(), 1);

        let c4 = registry.try_register(SocketAddr::from(([127, 0, 0, 1], 0)));
        assert!(c4.is_ok());
    }
}
