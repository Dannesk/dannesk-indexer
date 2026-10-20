
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::rpc::BtcRpc;

const INTERVAL: Duration = Duration::from_secs(30);

const FAILURES_TO_DOWN: u32 = 2;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NodeState {
    Unknown,
    Up,
    Down,
}

impl NodeState {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeState::Unknown => "unknown",
            NodeState::Up => "up",
            NodeState::Down => "down",
        }
    }
}

pub fn spawn(rpc: BtcRpc, node_tx: watch::Sender<NodeState>) {
    tokio::spawn(run(rpc, node_tx));
}

async fn run(rpc: BtcRpc, node_tx: watch::Sender<NodeState>) {
    let mut tick = tokio::time::interval(INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut failures: u32 = 0;

    loop {
        tick.tick().await;

        match rpc.call("getblockcount", serde_json::json!([])).await {
            Ok(height) => {
                failures = 0;
                set(&node_tx, NodeState::Up, || {
                    info!("bitcoind reachable at height {}", height)
                });
            }
            Err(e) => {
                failures = failures.saturating_add(1);
                debug!("node probe failed ({} consecutive): {:?}", failures, e);
                if failures >= FAILURES_TO_DOWN {
                    set(&node_tx, NodeState::Down, || {
                        warn!("bitcoind marked DOWN after {} failed probes", failures)
                    });
                }
            }
        }
    }
}

fn set(node_tx: &watch::Sender<NodeState>, next: NodeState, log_on_change: impl FnOnce()) {
    let changed = node_tx.send_if_modified(|current| {
        if *current == next {
            return false;
        }
        *current = next;
        true
    });
    if changed {
        log_on_change();
    } else {
        debug!("node probe: still {}", next.as_str());
    }
}
