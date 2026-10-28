
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, SemaphorePermit};

const CALL_TIMEOUT: Duration = Duration::from_secs(30);
// Below Core 32's 16 HTTP clients (-rpcmaxconnections), leaving the watchdog's bitcoin-cli its slot.
const MAX_IN_FLIGHT: usize = 12;

#[derive(Debug)]
pub struct NodeError(pub Value);

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bitcoin RPC error: {}", self.0)
    }
}

impl std::error::Error for NodeError {}

#[derive(Debug)]
pub struct QueueFull(pub String);

impl std::fmt::Display for QueueFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bitcoin RPC queue full, {} not sent", self.0)
    }
}

impl std::error::Error for QueueFull {}

pub fn never_sent(e: &anyhow::Error) -> bool {
    e.downcast_ref::<QueueFull>().is_some()
        || e.downcast_ref::<reqwest::Error>().is_some_and(|r| r.is_connect())
}

#[derive(Clone)]
pub struct BtcRpc {
    client: reqwest::Client,
    url: String,
    user: String,
    pass: String,
    slots: Arc<Semaphore>,
}

impl BtcRpc {
    pub fn from_env(url: &str) -> Result<Self> {
        let user = std::env::var("BTC_RPC_USER")
            .context("BTC_RPC_USER not set (prod: /etc/indexd.env via EnvironmentFile)")?;
        let pass = std::env::var("BTC_RPC_PASS")
            .context("BTC_RPC_PASS not set (prod: /etc/indexd.env via EnvironmentFile)")?;
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(25))
            .build()
            .context("Failed to build Bitcoin RPC client")?;
        Ok(Self {
            client,
            url: url.to_string(),
            user,
            pass,
            slots: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        })
    }

    #[cfg(test)]
    pub fn for_test(url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.to_string(),
            user: "test".to_string(),
            pass: "test".to_string(),
            slots: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
    }

    
    async fn slot(&self, method: &str, timeout: Duration) -> Result<(SemaphorePermit<'_>, Duration)> {
        let started = Instant::now();
        let permit = tokio::time::timeout(timeout, self.slots.acquire())
            .await
            .map_err(|_| QueueFull(method.to_string()))?
            .context("Bitcoin RPC slots closed")?;
        let left = timeout.saturating_sub(started.elapsed());
        if left < timeout / 2 {
            return Err(QueueFull(method.to_string()).into());
        }
        Ok((permit, left))
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.call_with_timeout(method, params, CALL_TIMEOUT).await
    }

    pub async fn call_with_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value> {
        let (_slot, left) = self.slot(method, timeout).await?;
        let body = json!({ "jsonrpc": "2.0", "id": "relay", "method": method, "params": params });
        let resp = self
            .client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.pass))
            .timeout(left)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("Bitcoin RPC transport failed for {}", method))?;
        let v: Value = resp
            .json()
            .await
            .with_context(|| format!("Bitcoin RPC returned non-JSON for {}", method))?;
        if !v["error"].is_null() {
            return Err(NodeError(v["error"].clone()).into());
        }
        Ok(v["result"].clone())
    }

    pub async fn call_typed<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<T> {
        #[derive(serde::Deserialize)]
        struct Envelope<T> {
            result: Option<T>,
            #[serde(default)]
            error: Value,
        }
        let (_slot, left) = self.slot(method, timeout).await?;
        let body = json!({ "jsonrpc": "2.0", "id": "relay", "method": method, "params": params });
        let resp = self
            .client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.pass))
            .timeout(left)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("Bitcoin RPC transport failed for {}", method))?;
        let v: Envelope<T> = resp
            .json()
            .await
            .with_context(|| format!("Bitcoin RPC returned non-JSON for {}", method))?;
        if !v.error.is_null() {
            return Err(NodeError(v.error).into());
        }
        v.result
            .with_context(|| format!("Bitcoin RPC returned no result for {}", method))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_queue_counts_as_never_sent() {
        assert!(never_sent(&anyhow::Error::from(QueueFull("sendrawtransaction".into()))));
        assert!(!never_sent(&anyhow::anyhow!("something else")));
    }

    #[tokio::test]
    async fn a_thirteenth_call_waits_then_fails_as_queue_full() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let holder = {
            let accepted = Arc::clone(&accepted);
            tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((socket, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::Relaxed);
                    held.push(socket);
                }
            })
        };
        let rpc = BtcRpc::for_test(&url);
        let hung: Vec<_> = (0..MAX_IN_FLIGHT)
            .map(|_| {
                let rpc = rpc.clone();
                tokio::spawn(async move { rpc.call_with_timeout("getblockcount", json!([]), Duration::from_secs(1)).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let thirteenth = rpc.call_with_timeout("getblockcount", json!([]), Duration::from_millis(200)).await;
        let e = thirteenth.unwrap_err();
        assert!(e.downcast_ref::<QueueFull>().is_some(), "{e:?}");
        assert!(never_sent(&e));
        assert!(accepted.load(Ordering::Relaxed) <= MAX_IN_FLIGHT, "the queued call opened no connection");
        for h in hung {
            let e = h.await.unwrap().unwrap_err();
            assert!(e.downcast_ref::<QueueFull>().is_none(), "the twelve held slots and timed out on the socket: {e:?}");
        }
        let after = rpc.call_with_timeout("getblockcount", json!([]), Duration::from_secs(1)).await.unwrap_err();
        assert!(after.downcast_ref::<QueueFull>().is_none(), "timed-out calls release their slots: {after:?}");
        holder.abort();
    }
}
