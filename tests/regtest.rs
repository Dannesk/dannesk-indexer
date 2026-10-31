use dannesk_btc_codec::address::Address;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const RPC_USER: &str = "harness";
const RPC_PASS: &str = "harness";
const WAIT: Duration = Duration::from_secs(90);
const MAX_HELD_FRAMES: usize = 10_000;
const FIRST_BLOCK: u64 = 111;

static SERIAL: Mutex<()> = Mutex::new(());

fn free_ports<const N: usize>() -> [u16; N] {
    let held: Vec<std::net::TcpListener> =
        (0..N).map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    std::array::from_fn(|i| held[i].local_addr().unwrap().port())
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("indexd-regtest-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // A failed scenario keeps its node and indexd logs.
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

struct Node {
    child: Option<Child>,
    datadir: PathBuf,
    rpc_port: u16,
    zmq_port: u16,
    http: reqwest::Client,
}

impl Node {
    async fn start(datadir: &Path, rpc_port: u16, zmq_port: u16, extra: &[&str]) -> Node {
        let bin = std::env::var("BITCOIND").expect("set BITCOIND to a bitcoind binary");
        std::fs::create_dir_all(datadir).unwrap();
        let zmq = format!("tcp://127.0.0.1:{}", zmq_port);
        let child = Command::new(bin)
            .args(["-regtest", "-server", "-listen=0", "-printtoconsole=0", "-dbcache=16", "-par=1"])
            .args(["-fallbackfee=0.0002", "-rpcbind=127.0.0.1", "-rpcallowip=127.0.0.1"])
            .arg(format!("-datadir={}", datadir.display()))
            .arg(format!("-rpcport={}", rpc_port))
            .arg(format!("-rpcuser={}", RPC_USER))
            .arg(format!("-rpcpassword={}", RPC_PASS))
            .arg(format!("-zmqpubsequence={}", zmq))
            .arg(format!("-zmqpubhashblock={}", zmq))
            .arg(format!("-zmqpubrawtx={}", zmq))
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("bitcoind did not start");
        let node = Node {
            child: Some(child),
            datadir: datadir.to_path_buf(),
            rpc_port,
            zmq_port,
            http: reqwest::Client::new(),
        };
        for _ in 0..300 {
            if node.try_call("", "getblockcount", json!([])).await.is_ok() {
                return node;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("bitcoind RPC never answered");
    }

    async fn restart(&mut self, extra: &[&str]) {
        self.stop().await;
        *self = Node::start(&self.datadir.clone(), self.rpc_port, self.zmq_port, extra).await;
    }

    async fn try_call(&self, wallet: &str, method: &str, params: Value) -> Result<Value, String> {
        let path = if wallet.is_empty() { String::new() } else { format!("wallet/{}", wallet) };
        let body = json!({ "jsonrpc": "2.0", "id": "harness", "method": method, "params": params });
        let reply = self
            .http
            .post(format!("http://127.0.0.1:{}/{}", self.rpc_port, path))
            .basic_auth(RPC_USER, Some(RPC_PASS))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let v: Value = reply.json().await.map_err(|e| e.to_string())?;
        match v.get("error") {
            Some(e) if !e.is_null() => Err(e.to_string()),
            _ => Ok(v["result"].clone()),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Value {
        self.try_call("", method, params.clone())
            .await
            .unwrap_or_else(|e| panic!("{} {} failed: {}", method, params, e))
    }

    async fn wallet(&self, wallet: &str, method: &str, params: Value) -> Value {
        self.try_call(wallet, method, params.clone())
            .await
            .unwrap_or_else(|e| panic!("{} {} on wallet {} failed: {}", method, params, wallet, e))
    }

    async fn stop(&mut self) {
        let _ = self.try_call("", "stop", json!([])).await;
        if let Some(mut child) = self.child.take() {
            for _ in 0..300 {
                if let Ok(Some(_)) = child.try_wait() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct Indexd {
    child: Option<Child>,
    bind: String,
    dir: PathBuf,
    rpc_port: u16,
    zmq_port: u16,
}

impl Indexd {
    fn command(dir: &Path, rpc_port: u16, zmq_port: u16) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_indexd"));
        cmd.env("BTC_INDEX_PATH", dir.join("btc-index.redb"))
            .env("BTC_RPC_URL", format!("http://127.0.0.1:{}", rpc_port))
            .env("BTC_RPC_USER", RPC_USER)
            .env("BTC_RPC_PASS", RPC_PASS)
            .env("BTC_ZMQ", format!("tcp://127.0.0.1:{}", zmq_port));
        cmd
    }

    async fn build(dir: &Path, node: &Node) {
        let dump = dir.join("utxo.dat").display().to_string();
        node.call("dumptxoutset", json!([dump, "latest"])).await;
        let store = dir.join("btc-index.redb").display().to_string();
        let out = Self::command(dir, node.rpc_port, node.zmq_port)
            .args(["build-index", &dump, &store])
            .output()
            .expect("the indexd CLI did not run");
        assert!(
            out.status.success(),
            "indexd build-index failed\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    async fn start(dir: &Path, rpc_port: u16, zmq_port: u16, port: u16) -> Indexd {
        let log = File::options().create(true).append(true).open(dir.join("indexd.log")).unwrap();
        let bind = format!("127.0.0.1:{}", port);
        let child = Self::command(dir, rpc_port, zmq_port)
            .env("INDEXD_BIND", &bind)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("indexd did not start");
        let indexd = Indexd { child: Some(child), bind, dir: dir.to_path_buf(), rpc_port, zmq_port };
        for _ in 0..600 {
            if let Some(reply) = try_query(&indexd.bind, json!({ "method": "state" })).await
                && reply["state"] == "live"
            {
                return indexd;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("indexd never went live");
    }

    async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = Command::new("kill").args(["-TERM", &child.id().to_string()]).status();
            for _ in 0..300 {
                if let Ok(Some(_)) = child.try_wait() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    async fn restart(&mut self) {
        self.stop().await;
        let port = self.bind.rsplit(':').next().unwrap().parse().unwrap();
        *self = Indexd::start(&self.dir.clone(), self.rpc_port, self.zmq_port, port).await;
    }
}

impl Drop for Indexd {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn try_query(bind: &str, req: Value) -> Option<Value> {
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}", bind)).await.ok()?;
    ws.send(Message::text(req.to_string())).await.ok()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await.ok()?? {
            Ok(Message::Text(text)) => return serde_json::from_str(&text).ok(),
            Ok(_) => continue,
            Err(_) => return None,
        }
    }
}

async fn utxos(bind: &str, address: &str) -> Vec<Value> {
    let req = json!({ "method": "utxos", "address": address });
    let reply = try_query(bind, req.clone()).await.unwrap_or_else(|| panic!("no answer to {}", req));
    assert_eq!(reply["state"], "live", "{}", reply);
    reply["utxos"].as_array().cloned().unwrap_or_else(|| panic!("no utxos in {}", reply))
}

struct Events {
    frames: mpsc::UnboundedReceiver<Value>,
    held: Vec<Value>,
    reader: tokio::task::JoinHandle<()>,
}

impl Events {
    async fn subscribe(bind: &str, addresses: &[&str]) -> Events {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}", bind)).await.unwrap();
        let (mut sink, mut stream) = ws.split();
        let req = json!({ "method": "subscribe_events", "addresses": addresses, "txids": [] });
        sink.send(Message::text(req.to_string())).await.unwrap();
        let (tx, frames) = mpsc::unbounded_channel();
        let reader = tokio::spawn(async move {
            let _sink = sink;
            while let Some(Ok(msg)) = stream.next().await {
                if let Message::Text(text) = msg
                    && let Ok(frame) = serde_json::from_str::<Value>(&text)
                    && tx.send(frame).is_err()
                {
                    return;
                }
            }
        });
        let mut events = Events { frames, held: Vec::new(), reader };
        events.expect("subscribed", |f| f["type"] == "subscribed").await;
        events
    }

    async fn expect(&mut self, what: &str, wanted: impl Fn(&Value) -> bool) -> Value {
        if let Some(i) = self.held.iter().position(&wanted) {
            return self.held.remove(i);
        }
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            match tokio::time::timeout_at(deadline, self.frames.recv()).await {
                Ok(Some(frame)) if wanted(&frame) => return frame,
                Ok(Some(frame)) => {
                    assert!(self.held.len() < MAX_HELD_FRAMES, "too many unmatched frames");
                    self.held.push(frame);
                }
                Ok(None) => panic!("the event stream closed while waiting for {}", what),
                Err(_) => panic!("no {} within {:?}, unmatched frames {:?}", what, WAIT, self.kinds()),
            }
        }
    }

    async fn none(&mut self, what: &str, within: Duration, unwanted: impl Fn(&Value) -> bool) {
        assert!(!self.held.iter().any(&unwanted), "unexpected {}", what);
        let deadline = tokio::time::Instant::now() + within;
        while let Ok(next) = tokio::time::timeout_at(deadline, self.frames.recv()).await {
            let Some(frame) = next else { return };
            assert!(!unwanted(&frame), "unexpected {}: {}", what, frame);
            assert!(self.held.len() < MAX_HELD_FRAMES, "too many unmatched frames");
            self.held.push(frame);
        }
    }

    fn kinds(&self) -> Vec<String> {
        self.held.iter().map(|f| f["type"].as_str().unwrap_or("?").to_string()).collect()
    }

    
    fn forget_held(&mut self) {
        self.held.clear();
    }
}

impl Drop for Events {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

fn mempool_tx(txid: &str) -> impl Fn(&Value) -> bool + '_ {
    move |f| f["type"] == "mempool_tx" && f["txid"] == txid
}

fn confirmed_tx(txid: &str) -> impl Fn(&Value) -> bool + '_ {
    move |f| f["type"] == "confirmed_tx" && f["txid"] == txid
}

fn dropped_tx(txid: &str) -> impl Fn(&Value) -> bool + '_ {
    move |f| f["type"] == "dropped_tx" && f["txid"] == txid
}


fn block_touching<'a>(address: &'a str, txid: &'a str) -> impl Fn(&Value) -> bool + 'a {
    move |f| {
        f["type"] == "block"
            && f["changes"].as_array().is_some_and(|changes| {
                changes.iter().any(|c| {
                    c["address"] == address
                        && ["created", "spent"].iter().any(|k| {
                            c[*k].as_array().is_some_and(|coins| coins.iter().any(|coin| coin["txid"] == txid))
                        })
                })
            })
    }
}

struct Addr {
    regtest: String,
    main: String,
}

struct Signed {
    hex: String,
    txid: String,
}

struct Env {
    node: Node,
    indexd: Indexd,
    miner: String,
    _scratch: Scratch,
    _serial: MutexGuard<'static, ()>,
}

async fn env(name: &str) -> Env {
    let serial = SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let scratch = Scratch::new(name);
    let [rpc, zmq, bind] = free_ports::<3>();
    let node = Node::start(&scratch.0.join("node"), rpc, zmq, &[]).await;
    for wallet in ["miner", "user"] {
        node.call("createwallet", json!([wallet, false, false, "", false, true, true])).await;
    }
    let miner = node.wallet("miner", "getnewaddress", json!([])).await.as_str().unwrap().to_string();
    node.call("generatetoaddress", json!([FIRST_BLOCK - 1, miner])).await;
    Indexd::build(&scratch.0, &node).await;
    let indexd = Indexd::start(&scratch.0, rpc, zmq, bind).await;
    Env { node, indexd, miner, _scratch: scratch, _serial: serial }
}

impl Env {
    fn bind(&self) -> &str {
        &self.indexd.bind
    }

    async fn watched(&self) -> Addr {
        let regtest = self.node.wallet("user", "getnewaddress", json!(["", "bech32"])).await;
        let regtest = regtest.as_str().unwrap().to_string();
        let info = self.node.wallet("user", "getaddressinfo", json!([regtest])).await;
        let main = Address::from_script(&hex(info["scriptPubKey"].as_str().unwrap())).unwrap().to_string();
        Addr { regtest, main }
    }

    async fn pay(&self, to: &Addr, btc: f64) -> String {
        let txid = self.node.wallet("miner", "sendtoaddress", json!([to.regtest, btc])).await;
        txid.as_str().unwrap().to_string()
    }

   
    async fn pay_raw(&self, to: &Addr, btc: f64) -> String {
        let mut outputs = Map::new();
        outputs.insert(to.regtest.clone(), json!(btc));
        let raw = self.node.call("createrawtransaction", json!([[], [outputs]])).await;
        let funded = self.node.wallet("miner", "fundrawtransaction", json!([raw])).await;
        let signed = self.node.wallet("miner", "signrawtransactionwithwallet", json!([funded["hex"]])).await;
        let txid = self.node.call("sendrawtransaction", json!([signed["hex"]])).await;
        txid.as_str().unwrap().to_string()
    }

    async fn mine(&self) -> String {
        let hashes = self.node.call("generatetoaddress", json!([1, self.miner])).await;
        hashes[0].as_str().unwrap().to_string()
    }

    async fn vout_paying(&self, txid: &str, to: &Addr) -> u64 {
        let tx = self.node.wallet("user", "gettransaction", json!([txid])).await;
        tx["details"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["address"] == to.regtest.as_str())
            .and_then(|d| d["vout"].as_u64())
            .expect("an output paying the address")
    }

    async fn sign(&self, inputs: &[(&str, u64)], outputs: Value) -> Signed {
        let inputs: Vec<Value> = inputs.iter().map(|(txid, vout)| json!({ "txid": txid, "vout": vout })).collect();
        let raw = self.node.call("createrawtransaction", json!([inputs, outputs])).await;
        let signed = self.node.wallet("user", "signrawtransactionwithwallet", json!([raw])).await;
        assert_eq!(signed["complete"], true, "{}", signed);
        let hex = signed["hex"].as_str().unwrap().to_string();
        let txid = self.node.call("decoderawtransaction", json!([hex])).await["txid"].as_str().unwrap().to_string();
        Signed { hex, txid }
    }

    async fn spend(&self, inputs: &[(&str, u64)], to: &str, btc: f64) -> String {
        let mut outputs = Map::new();
        outputs.insert(to.to_string(), json!(btc));
        let signed = self.sign(inputs, json!([outputs])).await;
        self.node.call("sendrawtransaction", json!([signed.hex])).await;
        signed.txid
    }
}

#[tokio::test]
#[ignore]
async fn a_payment_is_framed_then_confirmed() {
    let env = env("receive").await;
    let w = env.watched().await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    let txid = env.pay(&w, 1.0).await;
    let frame = events.expect("the payment", mempool_tx(&txid)).await;
    assert!(frame["time"].as_u64().is_some_and(|t| t > 0), "the node names when it first saw it: {frame}");
    let outputs = frame["outputs"].as_array().unwrap();
    assert!(outputs.iter().any(|o| o["address"] == w.main.as_str() && o["sats"] == 100_000_000), "{}", frame);
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!((coins[0]["txid"].as_str(), coins[0]["height"].as_u64()), (Some(txid.as_str()), Some(0)));

    env.mine().await;
    events.expect("the block paying it", block_touching(&w.main, &txid)).await;
    let confirmed = events.expect("its confirmation", confirmed_tx(&txid)).await;
    assert_eq!(confirmed["height"], FIRST_BLOCK);
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!(coins[0]["height"], FIRST_BLOCK);
    assert!(coins[0].get("spent_by").is_none(), "{:?}", coins);
}

#[tokio::test]
#[ignore]
async fn a_spend_marks_the_coin_then_removes_it() {
    let env = env("send").await;
    let w = env.watched().await;
    let funding = env.pay(&w, 1.0).await;
    env.mine().await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    let spend = env.node.wallet("user", "sendtoaddress", json!([env.miner, 0.5])).await;
    let spend = spend.as_str().unwrap().to_string();
    let frame = events.expect("the spend", mempool_tx(&spend)).await;
    let inputs = frame["inputs"].as_array().unwrap();
    assert!(
        inputs.iter().any(|i| i["txid"] == funding.as_str() && i["address"] == w.main.as_str() && i["sats"] == 100_000_000),
        "{}",
        frame
    );
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!(coins[0]["spent_by"], spend.as_str());

    env.mine().await;
    events.expect("the block spending it", block_touching(&w.main, &funding)).await;
    events.expect("the spend's confirmation", confirmed_tx(&spend)).await;
    assert!(utxos(env.bind(), &w.main).await.is_empty());
}

#[tokio::test]
#[ignore]
async fn a_child_of_an_unconfirmed_payment_is_framed_and_owns_the_coin() {
    let env = env("chain").await;
    let (w, w2) = (env.watched().await, env.watched().await);
    let mut events = Events::subscribe(env.bind(), &[&w.main, &w2.main]).await;
    let parent = env.pay(&w, 1.0).await;
    events.expect("the parent", mempool_tx(&parent)).await;
    let vout = env.vout_paying(&parent, &w).await;
    let child = env.spend(&[(&parent, vout)], &w2.regtest, 0.999).await;
    events.expect("the child", mempool_tx(&child)).await;
    assert!(utxos(env.bind(), &w.main).await.is_empty(), "the child spends the parent's output");
    let coins = utxos(env.bind(), &w2.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!((coins[0]["txid"].as_str(), coins[0]["height"].as_u64()), (Some(child.as_str()), Some(0)));

    env.mine().await;
    events.expect("the parent's confirmation", confirmed_tx(&parent)).await;
    events.expect("the child's confirmation", confirmed_tx(&child)).await;
    assert_eq!(utxos(env.bind(), &w2.main).await[0]["height"], FIRST_BLOCK);
}

#[tokio::test]
#[ignore]
async fn a_fee_bump_drops_the_original_and_frames_the_replacement() {
    let env = env("rbf").await;
    let w = env.watched().await;
    env.pay(&w, 1.0).await;
    env.mine().await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    let original = env.node.wallet("user", "sendtoaddress", json!([env.miner, 0.5, "", "", false, true])).await;
    let original = original.as_str().unwrap().to_string();
    events.expect("the original", mempool_tx(&original)).await;
    let bumped = env.node.wallet("user", "bumpfee", json!([original])).await;
    let replacement = bumped["txid"].as_str().unwrap().to_string();
    events.expect("the original's drop", dropped_tx(&original)).await;
    events.expect("the replacement", mempool_tx(&replacement)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!(coins[0]["spent_by"], replacement.as_str());

    env.mine().await;
    events.expect("the replacement's confirmation", confirmed_tx(&replacement)).await;
    assert!(utxos(env.bind(), &w.main).await.is_empty());
}

#[tokio::test]
#[ignore]
async fn a_spend_conflicted_by_a_block_is_reported_dropped() {
    let env = env("conflict").await;
    let w = env.watched().await;
    let funding = env.pay(&w, 1.0).await;
    env.mine().await;
    let vout = env.vout_paying(&funding, &w).await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    let ours = env.spend(&[(&funding, vout)], &env.miner, 0.999).await;
    events.expect("our spend", mempool_tx(&ours)).await;
    let mut outputs = Map::new();
    outputs.insert(env.miner.clone(), json!(0.998));
    let other = env.sign(&[(&funding, vout)], json!([outputs])).await;
    env.node.call("generateblock", json!([env.miner, [other.hex]])).await;
    events.expect("our spend's drop", dropped_tx(&ours)).await;
    events.expect("the conflicting spend's confirmation", confirmed_tx(&other.txid)).await;
    assert!(utxos(env.bind(), &w.main).await.is_empty());
}

#[tokio::test]
#[ignore]
async fn a_disconnected_block_returns_its_payment_to_the_mempool() {
    let env = env("reorg").await;
    let w = env.watched().await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    let txid = env.pay(&w, 1.0).await;
    events.expect("the payment", mempool_tx(&txid)).await;
    let block = env.mine().await;
    events.expect("its confirmation", confirmed_tx(&txid)).await;

    events.forget_held();
    env.node.call("invalidateblock", json!([block])).await;
    events.expect("the reorg", |f| f["type"] == "reorg" && f["height"] == FIRST_BLOCK).await;
    events.expect("the payment back in the mempool", mempool_tx(&txid)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!(coins[0]["height"], 0);

    env.node.call("reconsiderblock", json!([block])).await;
    events.expect("the block again", block_touching(&w.main, &txid)).await;
    events.expect("the confirmation again", confirmed_tx(&txid)).await;
    assert_eq!(utxos(env.bind(), &w.main).await[0]["height"], FIRST_BLOCK);
}

#[tokio::test]
#[ignore]
async fn a_spend_in_a_disconnected_block_is_matched_again() {
    let env = env("respend").await;
    let w = env.watched().await;
    let funding = env.pay(&w, 1.0).await;
    env.mine().await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    let spend = env.node.wallet("user", "sendtoaddress", json!([env.miner, 0.5])).await;
    let spend = spend.as_str().unwrap().to_string();
    events.expect("the spend", mempool_tx(&spend)).await;
    let block = env.mine().await;
    events.expect("the spend's confirmation", confirmed_tx(&spend)).await;
    assert!(utxos(env.bind(), &w.main).await.is_empty());

    events.forget_held();
    env.node.call("invalidateblock", json!([block])).await;
    events.expect("the reorg", |f| f["type"] == "reorg" && f["height"] == FIRST_BLOCK + 1).await;
    events.expect("the spend back in the mempool", mempool_tx(&spend)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!(coins[0]["txid"], funding.as_str());
    assert_eq!(coins[0]["spent_by"], spend.as_str());
}

#[tokio::test]
#[ignore]
async fn a_restart_frames_the_pending_payment_again() {
    let mut env = env("restart").await;
    let w = env.watched().await;
    let txid = {
        let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
        let txid = env.pay(&w, 1.0).await;
        events.expect("the payment", mempool_tx(&txid)).await;
        txid
    };
    env.indexd.restart().await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    events.expect("the payment framed at watch time", mempool_tx(&txid)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!(coins[0]["height"], 0);
}

#[tokio::test]
#[ignore]
async fn a_payment_lost_while_the_node_restarts_is_reported_dropped() {
    let mut env = env("gap").await;
    let w = env.watched().await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    let txid = env.pay(&w, 1.0).await;
    events.expect("the payment", mempool_tx(&txid)).await;
    env.node.restart(&["-persistmempool=0", "-walletbroadcast=0"]).await;
    env.mine().await;
    events.expect("the payment's drop", dropped_tx(&txid)).await;
    assert!(utxos(env.bind(), &w.main).await.is_empty());
    let again = env.pay_raw(&w, 0.5).await;
    events.expect("a payment after the restart", mempool_tx(&again)).await;
}

#[tokio::test]
#[ignore]
async fn a_confirmed_spend_with_no_address_output_is_never_reported_dropped() {
    let mut env = env("burn").await;
    let w = env.watched().await;
    let funding = env.pay(&w, 1.0).await;
    env.mine().await;
    let vout = env.vout_paying(&funding, &w).await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    let burn = env.sign(&[(&funding, vout)], json!([{ "data": "00".repeat(8) }])).await;
    env.node.call("sendrawtransaction", json!([burn.hex, 0])).await;
    events.expect("the burn", mempool_tx(&burn.txid)).await;
    env.mine().await;
    events.expect("the burn's confirmation", confirmed_tx(&burn.txid)).await;
    env.node.restart(&["-walletbroadcast=0"]).await;
    env.mine().await;
    events.none("a drop of the confirmed burn", Duration::from_secs(45), dropped_tx(&burn.txid)).await;
}


async fn pay_out(env: &Env, coin: (&str, u64), w: &Addr, paid: f64, back: f64) -> String {
    let mut to_them = Map::new();
    to_them.insert(env.miner.clone(), json!(paid));
    let mut to_us = Map::new();
    to_us.insert(w.regtest.clone(), json!(back));
    let signed = env.sign(&[coin], json!([to_them, to_us])).await;
    env.node.call("sendrawtransaction", json!([signed.hex])).await;
    signed.txid
}

fn coin_of<'a>(coins: &'a [Value], txid: &str) -> Option<&'a Value> {
    coins.iter().find(|c| c["txid"] == txid)
}

#[tokio::test]
#[ignore]
async fn two_payments_in_a_row_chain_on_the_leftover_and_settle_in_one_block() {
    let env = env("backtoback").await;
    let w = env.watched().await;
    let funding = env.pay(&w, 1.0).await;
    env.mine().await;
    let vout = env.vout_paying(&funding, &w).await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;

    let first = pay_out(&env, (&funding, vout), &w, 0.1, 0.8999).await;
    events.expect("the first payment", mempool_tx(&first)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coin_of(&coins, &funding).unwrap()["spent_by"], first.as_str(), "{:?}", coins);
    assert_eq!(coin_of(&coins, &first).unwrap()["height"], 0, "{:?}", coins);

    let second = pay_out(&env, (&first, 1), &w, 0.1, 0.7998).await;
    let frame = events.expect("the second payment", mempool_tx(&second)).await;
    assert_eq!(frame["inputs"][0]["address"], w.main.as_str(), "{}", frame);
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 2, "the spent coin and the newest leftover: {:?}", coins);
    assert!(coin_of(&coins, &first).is_none(), "{:?}", coins);
    assert_eq!(coin_of(&coins, &second).unwrap()["height"], 0, "{:?}", coins);

    env.mine().await;
    events.expect("the first confirmation", confirmed_tx(&first)).await;
    events.expect("the second confirmation", confirmed_tx(&second)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!((coins[0]["txid"].as_str(), coins[0]["vout"].as_u64()), (Some(second.as_str()), Some(1)));
    assert!(coins[0].get("spent_by").is_none(), "{:?}", coins);
}

#[tokio::test]
#[ignore]
async fn a_bump_of_a_payment_on_a_pending_leftover_keeps_the_coin_spent() {
    let env = env("chainbump").await;
    let w = env.watched().await;
    let funding = env.pay(&w, 1.0).await;
    env.mine().await;
    let vout = env.vout_paying(&funding, &w).await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;

    let first = pay_out(&env, (&funding, vout), &w, 0.1, 0.8999).await;
    events.expect("the first payment", mempool_tx(&first)).await;
    let second = pay_out(&env, (&first, 1), &w, 0.1, 0.7998).await;
    events.expect("the second payment", mempool_tx(&second)).await;
    let bumped = pay_out(&env, (&first, 1), &w, 0.1, 0.7990).await;
    events.expect("the second's drop", dropped_tx(&second)).await;
    let frame = events.expect("the bump", mempool_tx(&bumped)).await;
    assert_eq!(frame["inputs"][0]["address"], w.main.as_str(), "the row must read as a send: {}", frame);

 
    let first_hex = env.node.call("getrawtransaction", json!([first])).await;
    env.node.call("generateblock", json!([env.miner, [first_hex]])).await;
    events.expect("the first confirmation", confirmed_tx(&first)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coin_of(&coins, &first).unwrap()["spent_by"], bumped.as_str(), "{:?}", coins);
    assert_eq!(coin_of(&coins, &bumped).unwrap()["height"], 0, "{:?}", coins);

    env.mine().await;
    events.expect("the bump's confirmation", confirmed_tx(&bumped)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!(coins[0]["txid"], bumped.as_str());
}

#[tokio::test]
#[ignore]
async fn a_payment_undone_by_a_reorg_leaves_one_spendable_view() {
    let env = env("reorgleftover").await;
    let w = env.watched().await;
    let funding = env.pay(&w, 1.0).await;
    env.mine().await;
    let vout = env.vout_paying(&funding, &w).await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;

    let paid = pay_out(&env, (&funding, vout), &w, 0.1, 0.8999).await;
    events.expect("the payment", mempool_tx(&paid)).await;
    let block = env.mine().await;
    events.expect("its confirmation", confirmed_tx(&paid)).await;

    events.forget_held();
    env.node.call("invalidateblock", json!([block])).await;
    events.expect("the reorg", |f| f["type"] == "reorg" && f["height"] == FIRST_BLOCK + 1).await;
    events.expect("the payment back in the mempool", mempool_tx(&paid)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coin_of(&coins, &funding).unwrap()["spent_by"], paid.as_str(), "{:?}", coins);
    assert_eq!(coin_of(&coins, &paid).unwrap()["height"], 0, "{:?}", coins);

    env.node.call("reconsiderblock", json!([block])).await;
    events.expect("the confirmation again", confirmed_tx(&paid)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert_eq!(coins.len(), 1, "{:?}", coins);
    assert_eq!((coins[0]["txid"].as_str(), coins[0]["height"].as_u64()), (Some(paid.as_str()), Some(FIRST_BLOCK + 1)));
}

#[tokio::test]
#[ignore]
async fn a_pending_payment_found_at_watch_time_is_reported_when_it_drops() {
    let env = env("watchdrop").await;
    let w = env.watched().await;
    let funding = env.pay(&w, 1.0).await;
    env.mine().await;
    let vout = env.vout_paying(&funding, &w).await;
    let paid = pay_out(&env, (&funding, vout), &w, 0.1, 0.8999).await;
    let mut events = Events::subscribe(env.bind(), &[&w.main]).await;
    events.expect("the payment framed at watch time", mempool_tx(&paid)).await;

    let bumped = pay_out(&env, (&funding, vout), &w, 0.1, 0.8990).await;
    events.expect("the replaced payment's drop", dropped_tx(&paid)).await;
    events.expect("the replacement", mempool_tx(&bumped)).await;
    let coins = utxos(env.bind(), &w.main).await;
    assert!(coin_of(&coins, &paid).is_none(), "no phantom leftover: {:?}", coins);
    assert_eq!(coin_of(&coins, &funding).unwrap()["spent_by"], bumped.as_str(), "{:?}", coins);
}
