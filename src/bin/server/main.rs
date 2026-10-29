mod grpc;
mod log;
mod state;
mod store;
mod web;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use c2x::crypto::Cipher;
use c2x::pb::monitor_server::MonitorServer;
use tonic::transport::Server;

use state::{AppState, ClientInfo, Config};

#[tokio::main]
async fn main() -> Result<()> {
    let path: PathBuf = std::env::args().nth(1).unwrap_or_else(|| "server.toml".into()).into();
    let text = std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let cfg: Config = toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
    if cfg.tokens.is_empty() {
        bail!("tokens 不能为空");
    }

    let store = store::Store::new(cfg.data_dir.clone())?;
    // 已有历史数据的 client 先以离线状态列出, 重启后仍可查看历史曲线
    let mut clients = HashMap::new();
    for name in store.known_clients() {
        store.cleanup(&name);
        clients.insert(
            name.clone(),
            ClientInfo {
                id: name.clone(),
                ip: name.replace('_', ":"),
                name: store.load_name(&name).unwrap_or_else(|| name.clone()),
                online: false,
                addr: None,
                version: None,
                connected_at: None,
                last_seen: None,
                latest: None,
                services: vec![],
                conn_id: 0,
                cmd_tx: None,
            },
        );
    }
    let events = log::EventLog::new(cfg.log_dir.clone())?;
    let state = Arc::new(AppState { store, events, clients: Mutex::new(clients), next_conn_id: Mutex::new(0),
        next_cmd_id: AtomicU64::new(0),
        admin_password: cfg.admin_password.clone().filter(|p| !p.is_empty()),
        auth_gate: tokio::sync::Mutex::new(()),
        pending: Mutex::new(HashMap::new()),
    });

    let ciphers = Arc::new(cfg.tokens.iter().map(|t| Cipher::from_token(t)).collect::<Vec<_>>());
    let svc = MonitorServer::new(grpc::MonitorSvc { state: state.clone(), ciphers });

    let grpc_addr = cfg.grpc_listen.parse().context("grpc_listen")?;
    let grpc_task = tokio::spawn(async move {
        Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(10)))
            .http2_keepalive_timeout(Some(Duration::from_secs(5)))
            .tcp_nodelay(true)
            .add_service(svc)
            .serve(grpc_addr)
            .await
    });
    println!("gRPC listening on {}", cfg.grpc_listen);

    let listener = tokio::net::TcpListener::bind(&cfg.http_listen).await.context("http_listen")?;
    println!("Web listening on http://{}", cfg.http_listen);
    let http_task = tokio::spawn(async move { axum::serve(listener, web::router(state).into_make_service_with_connect_info::<std::net::SocketAddr>()).await });

    tokio::select! {
        r = grpc_task => r??,
        r = http_task => r??,
    }
    Ok(())
}
