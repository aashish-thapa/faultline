//! One idempotent Iggy producer, driven line by line over stdin/stdout.
//!
//! Each input line is `{"value": "..."}`; each output line reports the outcome.
//! On an unknown outcome the batch is resent with the same producer id, epoch
//! and sequence from a fresh session, the recovery the SDK README documents as
//! exactly-once: "This holds after a lost reply, a reconnect or a lost session."

use iggy::prelude::*;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Deserialize)]
struct Request {
    value: String,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum Outcome {
    Ok { offset: u64, attempts: u32 },
    Fail { error: String },
    Info { error: String, attempts: u32 },
}

struct Config {
    address: String,
    stream: Identifier,
    topic: Identifier,
    give_up_after: Duration,
    attempt_timeout: Duration,
}

/// Errors after which the batch definitely was not written.
fn is_not_applied(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::TransientNotAccepted | IggyError::NotPrimary | IggyError::NotConnected
    )
}

async fn connect(cfg: &Config) -> Result<IggyClient, IggyError> {
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(cfg.address.clone())
        .build()?;
    client.connect().await?;
    client.login_user("iggy", "iggy").await?;
    Ok(client)
}

async fn connect_retrying(cfg: &Config, deadline: Instant) -> Option<IggyClient> {
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), connect(cfg)).await {
            Ok(Ok(client)) => return Some(client),
            _ => tokio::time::sleep(Duration::from_millis(300)).await,
        }
    }
    None
}

struct Producer {
    cfg: Config,
    client: Option<IggyClient>,
    identity: Option<BatchProducer>,
    epoch: u16,
}

impl Producer {
    async fn send(&mut self, value: &str) -> Outcome {
        let deadline = Instant::now() + self.cfg.give_up_after;
        let mut attempts = 0;
        let mut uncertain = false;
        let mut last_error = String::from("no attempt made");
        while Instant::now() < deadline {
            if self.client.is_none() {
                self.client = connect_retrying(&self.cfg, deadline).await;
            }
            let Some(client) = self.client.as_ref() else { break };
            let producer = match self.identity {
                Some(p) => p,
                None => match client.next_producer_id().await {
                    Ok(id) => {
                        let p = BatchProducer::new(id, self.epoch, 0).expect("id within Kafka range");
                        self.identity = Some(p);
                        p
                    }
                    Err(e) => {
                        last_error = format!("next_producer_id: {e}");
                        self.client = None;
                        continue;
                    }
                },
            };
            attempts += 1;
            let mut batch = vec![IggyMessage::from_str(value).expect("non-empty payload")];
            let sent = tokio::time::timeout(
                self.cfg.attempt_timeout,
                client.send_producer_messages(&self.cfg.stream, &self.cfg.topic, 0, producer, &mut batch),
            )
            .await;
            match sent {
                Ok(Ok(resp)) => {
                    self.identity = Some(producer.next(1));
                    return match resp.confirmations.as_slice() {
                        [c] => Outcome::Ok { offset: c.base_offset, attempts },
                        other => Outcome::Info {
                            error: format!("acknowledged with {} confirmations", other.len()),
                            attempts,
                        },
                    };
                }
                Ok(Err(e)) if is_not_applied(&e) => last_error = e.to_string(),
                Ok(Err(e)) => {
                    uncertain = true;
                    last_error = e.to_string();
                }
                Err(_) => {
                    uncertain = true;
                    last_error = "client timeout".into();
                }
            }
            // Resend the same batch from a fresh session.
            self.client = None;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if uncertain {
            // We will never resend this value, so moving to a new epoch cannot duplicate it.
            self.epoch = self.epoch.wrapping_add(1);
            self.identity = self.identity.and_then(|p| BatchProducer::new(p.producer_id(), self.epoch, 0));
            Outcome::Info { error: last_error, attempts }
        } else {
            Outcome::Fail { error: last_error }
        }
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(address), Some(stream), Some(topic)) = (args.next(), args.next(), args.next()) else {
        eprintln!("usage: iggy-producer <tcp-address> <stream> <topic> [attempt-timeout-ms]");
        std::process::exit(2);
    };
    let cfg = Config {
        address,
        stream: Identifier::named(&stream).expect("valid stream name"),
        topic: Identifier::named(&topic).expect("valid topic name"),
        give_up_after: Duration::from_secs(30),
        attempt_timeout: Duration::from_millis(
            args.next().and_then(|ms| ms.parse().ok()).unwrap_or(10_000),
        ),
    };
    let mut producer = Producer { cfg, client: None, identity: None, epoch: 0 };
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Ok(Some(line)) = lines.next_line().await {
        let outcome = match serde_json::from_str::<Request>(&line) {
            Ok(req) => producer.send(&req.value).await,
            Err(e) => Outcome::Fail { error: format!("bad request: {e}") },
        };
        let mut out = serde_json::to_vec(&outcome).expect("outcome serializes");
        out.push(b'\n');
        if stdout.write_all(&out).await.is_err() || stdout.flush().await.is_err() {
            return;
        }
    }
}
