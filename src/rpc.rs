
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::time::Duration;

const CALL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct NodeError(pub Value);

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bitcoin RPC error: {}", self.0)
    }
}

impl std::error::Error for NodeError {}

pub fn never_sent(e: &anyhow::Error) -> bool {
    e.downcast_ref::<reqwest::Error>().is_some_and(|r| r.is_connect())
}

#[derive(Clone)]
pub struct BtcRpc {
    client: reqwest::Client,
    url: String,
    user: String,
    pass: String,
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
        })
    }

    #[cfg(test)]
    pub fn for_test(url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.to_string(),
            user: "test".to_string(),
            pass: "test".to_string(),
        }
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
        let body = json!({ "jsonrpc": "2.0", "id": "relay", "method": method, "params": params });
        let resp = self
            .client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.pass))
            .timeout(timeout)
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
        let body = json!({ "jsonrpc": "2.0", "id": "relay", "method": method, "params": params });
        let resp = self
            .client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.pass))
            .timeout(timeout)
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
