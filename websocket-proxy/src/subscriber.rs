use std::time::Duration;
use axum::http::Uri;
use backoff::{ExponentialBackoff, backoff::Backoff};
use tokio_tungstenite::{connect_async, tungstenite::Error};
use tracing::{debug, error, info, warn};
use futures::{
    StreamExt,
};

pub struct WebsocketSubscriber<F> where
    F: Fn(String) + Send + Sync + 'static,
{
    uri: Uri,
    handler: F,
    backoff: ExponentialBackoff,
}

impl<F> WebsocketSubscriber<F>
where
    F: Fn(String) + Send + Sync + 'static,
{
    pub fn new(uri: Uri, handler: F) -> Self {
        let backoff = ExponentialBackoff {
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(60),
            max_elapsed_time: None, // Will retry indefinitely
            ..Default::default()
        };

        Self {
            uri,
            handler,
            backoff,
        }
    }

    pub async fn run(&mut self) {
        info!("subscriber run");
        loop {
            match self.connect_and_listen().await {
                Ok(()) => {
                    // Reset backoff on successful connection
                    self.backoff.reset();
                    info!("upstream connection closed");
                }
                Err(e) => {
                    error!("upstream websocket error: {}", e);

                    if let Some(duration) = self.backoff.next_backoff() {
                        warn!("Reconnecting in {} seconds", duration.as_secs());
                        tokio::time::sleep(duration).await;
                    }
                }
            }
        }
    }

    async fn connect_and_listen(&self) -> Result<(), Error> {
        info!("connecting to websocket at {}", self.uri);

        let (ws_stream, _) = connect_async(&self.uri).await?;
        info!("websocket connection established");

        let (_, mut read) = ws_stream.split();

        while let Some(message) = read.next().await {
            match message {
                Ok(msg) => {
                    debug!("Received message: {:?}", msg);
                    let text = msg.to_text()?;
                    (self.handler)(text.into());
                }
                Err(e) => {
                    error!("Error receiving message: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}