mod test {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::sync::broadcast::error::RecvError;
    use tokio::sync::broadcast;
    
    #[tokio::test]
    async fn broadcast_channel_testing() {
        let (tx, mut rx1) = broadcast::channel(10); // Channel with one block
        let mut rx2 = tx.subscribe(); // Create second receiver
        let mut rx3 = tx.subscribe(); // Create third receiver

        // Get current timestamp in milliseconds
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let time_millis = move || {
            let later = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            later - now // Difference in milliseconds
        };

        // Spawn tasks for each receiver
        let h1 = tokio::spawn(async move {
            loop {
                match rx1.recv().await {
                    Ok(msg) => {
                        println!("Receiver 1 got: {} {}", msg, time_millis());
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(e) => {
                        match e {
                            RecvError::Closed => {
                                break;
                            }
                            RecvError::Lagged(_) => {
                                println!("Receiver 1 lagged");
                                rx1 = rx1.resubscribe();
                            }
                        }
                    },
                }
            }
        });

        let h2 = tokio::spawn(async move {
            loop {
                match rx2.recv().await {
                    Ok(msg) => {
                        println!("Receiver 2 got: {} {}", msg, time_millis());
                    }
                    Err(e) => {
                        match e {
                            RecvError::Closed => {
                                break;
                            }
                            RecvError::Lagged(_) => {
                                println!("Receiver 2 lagged");
                            }
                        }
                    }
                }
            }
        });

        let h3 = tokio::spawn(async move {
            loop {
                match rx3.recv().await {
                    Ok(msg) => {
                        println!("Receiver 3 got: {} {}", msg, time_millis());
                    }
                    Err(e) => {
                        match e {
                            RecvError::Closed => {
                                break;
                            }
                            RecvError::Lagged(_) => {
                                println!("Receiver 3 lagged");
                            }
                        }
                    }
                }
            }
        });

        let handler = tokio::spawn(async move {
            let mut count = 0;
            loop {
                // simulate block time
                tokio::time::sleep(Duration::from_millis(200)).await;
                let t = time_millis();
                let str = format!("Hello from sender {} {}", count, t);
                println!("sending={} time={} len={}", str, t, tx.len());
                tx.send(str).unwrap();
                count += 1;
                if count > 30 {
                    break;
                }
            }
        });

        // Wait for all receivers
        let _ = tokio::join!(handler, h1, h2, h3);

        assert_eq!(1, 1);
    }
}