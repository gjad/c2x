use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use c2x::crypto::Cipher;
use c2x::pb::monitor_server::Monitor;
use c2x::pb::{
    client_message, server_message, ClientMessage, Envelope, Ping, ServerMessage, ServiceCommand, Welcome,
};
use chrono::Utc;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep_until, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::log::{clean_message, LogEvent, KIND_GUARD, KIND_OFFLINE, KIND_ONLINE};
use crate::state::{valid_display_name, AppState, ClientInfo, DiskSample, Sample, ServiceOutcome, ServiceState};

/// 超过该时间没有收到 client 任何消息则判定掉线
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(3);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

pub struct MonitorSvc {
    pub state: Arc<AppState>,
    pub ciphers: Arc<Vec<Cipher>>,
}

type OutStream = Pin<Box<dyn Stream<Item = Result<Envelope, Status>> + Send>>;

#[tonic::async_trait]
impl Monitor for MonitorSvc {
    type SessionStream = OutStream;

    async fn session(&self, req: Request<Streaming<Envelope>>) -> Result<Response<OutStream>, Status> {
        let ip = req
            .remote_addr()
            .map(|a| a.ip().to_string())
            .ok_or_else(|| Status::internal("unknown remote address"))?;
        let mut inbound = req.into_inner();

        // 第一条消息必须是用合法 token 加密的 Hello
        let first = tokio::time::timeout(HELLO_TIMEOUT, inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("hello timeout"))??
            .ok_or_else(|| Status::invalid_argument("empty stream"))?;
        let (cipher_idx, hello) = self
            .ciphers
            .iter()
            .enumerate()
            .find_map(|(i, c)| c.open::<ClientMessage>(&first).map(|m| (i, m)))
            .ok_or_else(|| Status::unauthenticated("invalid token"))?;
        let Some(client_message::Body::Hello(hello)) = hello.body else {
            return Err(Status::invalid_argument("first message must be hello"));
        };
        let display = hello.name.trim().to_string();
        if !valid_display_name(&display) {
            return Err(Status::invalid_argument("invalid client name"));
        }
        // 以来源 IP 作为唯一标识 (同时也是数据目录名, IPv6 的 ':' 替换为 '_')
        let name = ip.replace(':', "_");
        self.state.store.save_name(&name, &display);

        let conn_id = {
            let mut n = self.state.next_conn_id.lock().unwrap();
            *n += 1;
            *n
        };
        let now = Utc::now().timestamp_millis();
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ServiceCommand>(8);
        let replaced = {
            let mut clients = self.state.clients.lock().unwrap();
            let info = clients.entry(name.clone()).or_insert_with(|| ClientInfo {
                id: name.clone(),
                name: display.clone(),
                ip: ip.clone(),
                online: false,
                addr: None,
                version: None,
                connected_at: None,
                last_seen: None,
                latest: None,
                services: vec![],
                conn_id: 0,
                cmd_tx: None,
            });
            let replaced = info.online;
            info.name = display.clone();
            info.online = true;
            info.addr = Some(ip.clone());
            info.version = Some(hello.version.clone());
            info.connected_at = Some(now);
            info.last_seen = Some(now);
            info.conn_id = conn_id;
            info.cmd_tx = Some(cmd_tx);
            replaced
        };
        let msg = if replaced { "上线 (取代旧连接)" } else { "上线" };
        self.state.events.append(LogEvent::new(&name, &display, KIND_ONLINE, true, msg.into()));
        println!("[+] client '{display}' connected from {ip}");

        let (tx, rx) = mpsc::channel::<Result<Envelope, Status>>(8);
        let state = self.state.clone();
        let ciphers = self.ciphers.clone();
        let display = display.clone();

        tokio::spawn(async move {
            let cipher = &ciphers[cipher_idx];
            let reason = serve(&state, cipher, &name, conn_id, &mut inbound, &tx, &mut cmd_rx).await;

            let was_current = {
                let mut clients = state.clients.lock().unwrap();
                match clients.get_mut(&name) {
                    // 已被同名新连接取代时不改变状态
                    Some(info) if info.conn_id == conn_id => {
                        info.online = false;
                        info.cmd_tx = None;
                        true
                    }
                    _ => false,
                }
            };
            if was_current {
                state.events.append(LogEvent::new(&name, &display, KIND_OFFLINE, true, format!("下线: {reason}")));
            }
            println!("[-] client {name} disconnected: {reason}");
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

async fn serve(
    state: &AppState,
    cipher: &Cipher,
    name: &str,
    conn_id: u64,
    inbound: &mut Streaming<Envelope>,
    tx: &mpsc::Sender<Result<Envelope, Status>>,
    cmd_rx: &mut mpsc::Receiver<ServiceCommand>,
) -> String {
    let welcome = ServerMessage { body: Some(server_message::Body::Welcome(Welcome {})) };
    if tx.send(Ok(cipher.seal(&welcome))).await.is_err() {
        return "发送失败".into();
    }

    let mut ping = interval(PING_INTERVAL);
    let mut deadline = Instant::now() + CLIENT_TIMEOUT;
    loop {
        tokio::select! {
            msg = inbound.message() => {
                let env = match msg {
                    Ok(Some(e)) => e,
                    Ok(None) => return "连接关闭".into(),
                    Err(e) => {
                        eprintln!("[!] client {name} stream error: {e}");
                        return "连接断开".into();
                    }
                };
                let Some(m) = cipher.open::<ClientMessage>(&env) else {
                    return "解密失败".into();
                };
                deadline = Instant::now() + CLIENT_TIMEOUT;
                let now = Utc::now().timestamp_millis();
                if let Some(client_message::Body::ServiceResult(r)) = m.body {
                    if let Some(w) = state.pending.lock().unwrap().remove(&r.id) {
                        let _ = w.send(ServiceOutcome { ok: r.ok, message: r.message });
                    }
                    if let Some(info) = state.clients.lock().unwrap().get_mut(name) {
                        if info.conn_id == conn_id {
                            info.last_seen = Some(now);
                        }
                    }
                } else if let Some(client_message::Body::GuardEvent(ev)) = m.body {
                    // 守护服务被 client 自动重启 (或重启失败), 记入日志
                    let display = {
                        let mut clients = state.clients.lock().unwrap();
                        clients.get_mut(name).map(|info| {
                            if info.conn_id == conn_id {
                                info.last_seen = Some(now);
                            }
                            info.name.clone()
                        })
                    };
                    if let Some(display) = display {
                        state.events.append(LogEvent::new(name, &display, KIND_GUARD, ev.ok, clean_message(&ev.message)));
                    }
                } else if let Some(client_message::Body::Report(r)) = m.body {
                    let services: Vec<ServiceState> = r
                        .services
                        .into_iter()
                        .map(|s| ServiceState {
                            name: s.name,
                            load: s.load_state,
                            active: s.active_state,
                            sub: s.sub_state,
                            guarded: s.guarded,
                            guard_paused: s.guard_paused,
                        })
                        .collect();
                    let sample = Sample {
                        ts: now,
                        cpu: r.cpu_percent,
                        mem_total: r.mem_total,
                        mem_used: r.mem_used,
                        disks: r
                            .disks
                            .into_iter()
                            .map(|d| DiskSample { mount: d.mount, fs: d.fs, total: d.total, used: d.used })
                            .collect(),
                    };
                    let current = {
                        let mut clients = state.clients.lock().unwrap();
                        match clients.get_mut(name) {
                            Some(info) if info.conn_id == conn_id => {
                                info.last_seen = Some(now);
                                info.services = services;
                                info.latest = Some(sample.clone());
                                true
                            }
                            _ => false,
                        }
                    };
                    if !current {
                        return "被新连接取代".into();
                    }
                    state.store.record(name, &sample);
                } else if let Some(info) = state.clients.lock().unwrap().get_mut(name) {
                    if info.conn_id == conn_id {
                        info.last_seen = Some(now);
                    }
                }
            }
            Some(cmd) = cmd_rx.recv() => {
                let m = ServerMessage { body: Some(server_message::Body::ServiceCommand(cmd)) };
                if tx.send(Ok(cipher.seal(&m))).await.is_err() {
                    return "发送失败".into();
                }
            }
            _ = ping.tick() => {
                let p = ServerMessage {
                    body: Some(server_message::Body::Ping(Ping { ts_ms: Utc::now().timestamp_millis() })),
                };
                if tx.send(Ok(cipher.seal(&p))).await.is_err() {
                    return "发送失败".into();
                }
            }
            _ = sleep_until(deadline) => {
                return format!("超时未收到消息 ({}s)", CLIENT_TIMEOUT.as_secs());
            }
        }
    }
}
