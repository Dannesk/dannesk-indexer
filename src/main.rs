
use anyhow::Result;
use tracing::{debug, error, info};

mod btc_index;
mod mempool;
mod node_probe;
mod node_stats;
mod rpc;
mod server;
mod watch;

fn init_logging() -> Option<tracing_appender::non_blocking::WorkerGuard> {
    #[cfg(debug_assertions)]
    {
        use tracing_subscriber::EnvFilter;
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env().add_directive(tracing::Level::DEBUG.into()),
            )
            .init();
        None
    }

    #[cfg(not(debug_assertions))]
    {
        use std::io::stdout;
        use tracing_appender::non_blocking;
        use tracing_subscriber::{EnvFilter, fmt, prelude::*};

        let (non_blocking_console, guard) = non_blocking(stdout());
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_writer(non_blocking_console)
                    .with_ansi(false)
                    .with_filter(filter),
            )
            .init();

        Some(guard)
    }
}

fn history_store_path(index_path: &str) -> String {
    sibling_store_path(index_path, "BTC_HISTORY_PATH", "history.redb")
}

fn sibling_store_path(index_path: &str, env: &str, name: &str) -> String {
    std::env::var(env).unwrap_or_else(|_| {
        std::path::Path::new(index_path)
            .parent()
            .filter(|d| !d.as_os_str().is_empty())
            .map(|d| d.join(name).display().to_string())
            .unwrap_or_else(|| name.to_string())
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("build-index") {
        return match (args.get(2), args.get(3)) {
            (Some(dump), Some(store)) => btc_index::build::run(dump, store),
            _ => {
                eprintln!("usage: indexd build-index <dump-path> <store-path>");
                std::process::exit(2);
            }
        };
    }
    if args.get(1).map(String::as_str) == Some("repair-same-block") {
        let usage = || -> ! {
            eprintln!("usage: indexd repair-same-block <store-path> [from-height] [until-height]");
            std::process::exit(2);
        };
        let height = |i: usize| match args.get(i).map(|h| h.parse::<u32>()) {
            None => None,
            Some(Ok(h)) => Some(h),
            Some(Err(_)) => usage(),
        };
        let (from, until) = (height(3), height(4));
        let Some(store) = args.get(2) else { usage() };
        let _guard = init_logging();
        return btc_index::repair::run_cli(store, from, until).await;
    }
    if args.get(1).map(String::as_str) == Some("query-index") {
        return match (args.get(2), args.get(3)) {
            (Some(store), Some(address)) => {
                let Some(spk) = btc_index::BtcIndex::script_pubkey_for(address) else {
                    eprintln!("not a valid mainnet address: {}", address);
                    std::process::exit(2);
                };
                let idx = btc_index::BtcIndex::open(store)?;
                println!(
                    "state: {:?}, tip: {:?}",
                    idx.state(),
                    idx.tip()?.map(|(h, _)| h)
                );
                let bal = idx.query_spk(&spk)?;
                println!(
                    "{}: {} sats across {} utxos",
                    address, bal.sats, bal.utxo_count
                );
                Ok(())
            }
            _ => {
                eprintln!("usage: indexd query-index <store-path> <address>");
                std::process::exit(2);
            }
        };
    }

    if args.get(1).map(String::as_str) == Some("wallet") {
        eprintln!("indexd keeps no wallet store");
        std::process::exit(2);
    }

    let probe = match args.get(1).map(String::as_str) {
        Some("state") => Some("state"),
        Some("height") => Some("height"),
        Some("history") => Some("history"),
        _ => None,
    };
    if let Some(field) = probe {
        let bind = std::env::var("INDEXD_BIND").unwrap_or_else(|_| "127.0.0.1:7500".to_string());
        let reply = async {
            use futures_util::{SinkExt, StreamExt};
            let timeout = std::time::Duration::from_secs(5);
            let (mut ws, _) = tokio::time::timeout(
                timeout,
                tokio_tungstenite::connect_async(format!("ws://{}", bind)),
            )
            .await
            .ok()?
            .ok()?;
            ws.send(tokio_tungstenite::tungstenite::Message::text(
                r#"{"method":"state"}"#,
            ))
            .await
            .ok()?;
            let reply = tokio::time::timeout(timeout, ws.next()).await.ok()??.ok()?;
            serde_json::from_str::<serde_json::Value>(reply.to_text().ok()?).ok()
        }
        .await;

        if field == "state" {
            let word = reply
                .as_ref()
                .and_then(|v| v["state"].as_str())
                .unwrap_or("unreachable")
                .to_string();
            println!("{}", word);
            if word != "live" {
                std::process::exit(1);
            }
        } else if field == "history" {
            let window = reply
                .as_ref()
                .map(|v| (v["history"]["from"].as_u64(), v["history"]["to"].as_u64()));
            match window {
                Some((Some(from), Some(to))) => println!("{} {}", from, to),
                _ => std::process::exit(1),
            }
        } else {
            match reply.as_ref().and_then(|v| v["height"].as_u64()) {
                Some(h) => println!("{}", h),
                None => std::process::exit(1),
            }
        }
        return Ok(());
    }

    let _guard = init_logging();

    let mut sigusr1 = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;
    tokio::spawn(async move {
        while sigusr1.recv().await.is_some() {
            info!("SIGUSR1 ignored: indexd keeps no wallet store");
        }
    });

    let store_path =
        std::env::var("BTC_INDEX_PATH").unwrap_or_else(|_| "btc-index.redb".to_string());
    let bind = std::env::var("INDEXD_BIND").unwrap_or_else(|_| "127.0.0.1:7500".to_string());

    let rpc = rpc::BtcRpc::from_env("http://127.0.0.1:8332")?;

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    info!("indexd listening on {} (opening store {})", bind, store_path);

    let (state_tx, state_rx) = tokio::sync::watch::channel(btc_index::IndexState::Opening);
    let (node_tx, node_rx) = tokio::sync::watch::channel(node_probe::NodeState::Unknown);
    node_probe::spawn(rpc.clone(), node_tx);
    let (ready_tx, ready_rx) = tokio::sync::watch::channel::<Option<server::Ready>>(None);

    let (blocks_tx, _) = tokio::sync::broadcast::channel::<String>(256);
    let (stats_tx, stats_rx) = tokio::sync::watch::channel(node_stats::NodeStats::default());
    node_stats::spawn(rpc.clone(), ready_rx.clone(), blocks_tx.clone(), stats_tx);
    let (txs_tx, _) = tokio::sync::broadcast::channel::<String>(256);
    let zmq = std::env::var("BTC_ZMQ").unwrap_or_else(|_| "tcp://127.0.0.1:28332".to_string());

    let (server_stop_tx, server_stop_rx) = tokio::sync::watch::channel(false);
    let mut server = tokio::spawn(server::run(
        listener,
        state_rx.clone(),
        node_rx,
        stats_rx,
        ready_rx,
        rpc.clone(),
        server_stop_rx,
    ));

    let idx = {
        let path = store_path.clone();
        tokio::task::spawn_blocking(move || btc_index::BtcIndex::open(&path)).await??
    };
    let history = {
        let path = history_store_path(&store_path);
        let opened =
            tokio::task::spawn_blocking(move || btc_index::history::HistoryIndex::open(&path)).await??;
        match opened.coverage()? {
            Some((low, high)) => info!("history index open (covers heights {}..{})", low, high),
            None => info!("history index is empty — the backfill builds it from the node's prune window once the index is live"),
        }
        opened
    };
    let (prune_tx, prune_rx) = tokio::sync::watch::channel(0u32);

    let watch_state =
        watch::WatchState::new(idx.clone()).with_enrichment(rpc.clone(), txs_tx.clone());
    mempool::spawn_walker(watch_state.mempool(), rpc.clone());

    btc_index::sync::spawn(
        idx.clone(),
        history.clone(),
        rpc.clone(),
        zmq.clone(),
        state_tx,
        btc_index::sync::SyncEvents {
            blocks: blocks_tx.clone(),
            txs: txs_tx.clone(),
            watched: watch_state.spks(),
            txids: watch_state.txids(),
        },
    );
    btc_index::audit::spawn(idx.clone(), rpc.clone(), state_rx.clone());
    btc_index::history::spawn(history.clone(), rpc.clone(), state_rx.clone(), prune_tx);

    watch::spawn_rawtx(watch_state.clone(), rpc, zmq, txs_tx.clone());
    watch::spawn_block_consumer(watch_state.clone(), blocks_tx.subscribe());

    ready_tx.send_replace(Some(server::Ready {
        idx,
        history,
        prune_rx,
        blocks: blocks_tx,
        txs: txs_tx,
        watch_state,
    }));
    info!("indexd ready: store {} bind {}", store_path, bind);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    tokio::select! {
        _ = sigterm.recv() => info!("SIGTERM — closing store and exiting"),
        _ = sigint.recv() => info!("SIGINT — closing store and exiting"),
        res = &mut server => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("query server exited: {:?}", e);
                return Err(e);
            }
            Err(e) => {
                error!("query server task failed: {:?}", e);
                return Err(e.into());
            }
        },
    }
    let _ = server_stop_tx.send(true);
    match tokio::time::timeout(std::time::Duration::from_secs(3), &mut server).await {
        Ok(_) => debug!("query socket closed"),
        Err(_) => debug!("query socket goodbyes timed out"),
    }
    Ok(())
}
