use axum::http::Uri;
use backoff::{backoff::Backoff, ExponentialBackoff};
use futures::StreamExt;
use std::time::Duration;
use tokio::select;
use tokio_tungstenite::{connect_async, tungstenite::Error};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub struct WebsocketSubscriber<F>
where
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

    pub async fn run(&mut self, token: CancellationToken) {
        info!("starting subscriber");
        loop {
            select! {
                _ = token.cancelled() => {
                    info!("cancelled subscriber");
                    return;
                }
                result = self.connect_and_listen() => {
                    match result {
                        Ok(()) => {
                            info!(message="upstream connection closed");
                        }
                        Err(e) => {
                            error!(message="upstream websocket error", error=e.to_string());
                            if let Some(duration) = self.backoff.next_backoff() {
                                warn!(message="recconecting", seconds=duration.as_secs());
                                select! {
                                    _ = token.cancelled() => {
                                        info!(message="cancelled subscriber during backoff");
                                        return
                                    }
                                    _ = tokio::time::sleep(duration) => {}

                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn connect_and_listen(&mut self) -> Result<(), Error> {
        info!(
            message = "connecting to websocket",
            uri = self.uri.to_string()
        );

        let (ws_stream, _) = connect_async(&self.uri).await?;
        info!(message = "websocket connection established");
        self.backoff.reset();

        let (_, mut read) = ws_stream.split();

        while let Some(message) = read.next().await {
            match message {
                Ok(msg) => {
                    let text = msg.to_text()?;
                    debug!(message = "received message", payload = text);
                    (self.handler)(text.into());
                }
                Err(e) => {
                    error!(message = "error receiving message", error = e.to_string());
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}
