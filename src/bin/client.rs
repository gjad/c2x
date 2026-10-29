use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use c2x::crypto::Cipher;
use c2x::pb::monitor_client::MonitorClient;
use c2x::pb::{
    client_message, server_message, ClientMessage, Disk, Envelope, GuardEvent, Hello, Report, ServerMessage,
    ServiceAction, ServiceCommand, ServiceResult, ServiceStatus,
};
use serde::Deserialize;
use sysinfo::{Disks, MemoryRefreshKind, System, MINIMUM_CPU_UPDATE_INTERVAL};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;
use tonic::Request;

/// server 超过该时间没有任何消息(含 Ping)则认为连接已断开并重连
const SERVER_SILENCE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Deserialize)]
struct Config {
    /// 例如 http://1.2.3.4:50051
    server: String,
    /// 在 server 端显示的名称, 仅允许 字母/数字/./_/-
    name: String,
    /// 使用哪个 token 连接 (必须在 server 的 tokens 列表中)
    token: String,
    /// 允许 server 查看并控制(启动/停止/重启)的 systemd 服务, 例如 ["testser.service"]
    #[serde(default)]
    services: Vec<String>,
    /// 需要守护的服务: 发现它停止运行就自动重启. 必须是 services 的子集
    #[serde(default)]
    guard: Vec<String>,
}

/// 服务状态 / 守护相关的共享状态
#[derive(Default)]
struct Shared {
    /// 最近一次查询到的服务状态
    status: Mutex<Vec<ServiceStatus>>,
    /// 被手动停止的守护服务: 守护暂停, 直到再次被手动启动/重启, 避免守护把用户有意的停止又拉起来
    paused: Mutex<HashSet<String>>,
    /// 待上报给 server 的守护事件 (断线期间会暂存)
    events: Mutex<VecDeque<GuardEvent>>,
}

type SharedServices = Arc<Shared>;

/// 守护重启的退避: 连续重启尝试的间隔 5s, 10s, 20s, 40s, 之后固定 60s
fn guard_backoff(attempts: u32) -> Duration {
    Duration::from_secs((5u64 << attempts.saturating_sub(1).min(4)).min(60))
}

/// 服务持续运行超过该时间, 视为恢复稳定, 重启计数清零
const GUARD_STABLE: Duration = Duration::from_secs(60);

/// 查询并更新共享的服务状态 (带上守护标记)
async fn refresh(shared: &Shared, units: &[String], guard: &[String]) -> Vec<ServiceStatus> {
    let mut v = query_services(units).await;
    let paused = shared.paused.lock().unwrap().clone();
    for s in &mut v {
        s.guarded = guard.contains(&s.name);
        s.guard_paused = s.guarded && paused.contains(&s.name);
    }
    *shared.status.lock().unwrap() = v.clone();
    v
}

#[derive(Default)]
struct GuardState {
    attempts: u32,
    last_restart: Option<Instant>,
    next_attempt: Option<Instant>,
}

/// 守护检查: 对配置了守护的服务, 发现停止(inactive/failed)就自动重启, 并记录事件
async fn guard_check(
    shared: &Shared,
    status: &[ServiceStatus],
    guard: &[String],
    states: &mut HashMap<String, GuardState>,
) -> bool {
    let mut restarted = false;
    for st in status.iter().filter(|s| guard.contains(&s.name)) {
        let gs = states.entry(st.name.clone()).or_default();
        if st.active_state == "active" {
            if gs.last_restart.is_some_and(|t| t.elapsed() > GUARD_STABLE) {
                gs.attempts = 0;
            }
            continue;
        }
        // 只处理确定已停止的服务; 启动中/停止中/状态未知/未安装的服务不处理
        let stopped = st.load_state == "loaded" && matches!(st.active_state.as_str(), "inactive" | "failed");
        if !stopped || shared.paused.lock().unwrap().contains(&st.name) {
            continue;
        }
        if gs.next_attempt.is_some_and(|t| Instant::now() < t) {
            continue;
        }

        gs.attempts += 1;
        gs.last_restart = Some(Instant::now());
        gs.next_attempt = Some(Instant::now() + guard_backoff(gs.attempts));
        let res = control_service(&ServiceCommand {
            id: 0,
            service: st.name.clone(),
            action: ServiceAction::Restart as i32,
        })
        .await;
        let nth = if gs.attempts > 1 { format!(" (连续第 {} 次)", gs.attempts) } else { String::new() };
        let message = if res.ok {
            format!("检测到 {} 已停止 ({}/{}), 自动重启成功{nth}", st.name, st.active_state, st.sub_state)
        } else {
            format!("检测到 {} 已停止 ({}/{}), 自动重启失败{nth}: {}", st.name, st.active_state, st.sub_state, res.message)
        };
        println!("[guard] {message}");
        let mut q = shared.events.lock().unwrap();
        q.push_back(GuardEvent { service: st.name.clone(), ok: res.ok, message });
        while q.len() > 100 {
            q.pop_front();
        }
        restarted = true;
    }
    restarted
}

/// systemctl 单次调用的超时, 避免卡死
const SYSTEMCTL_TIMEOUT: Duration = Duration::from_secs(60);

/// 服务名只允许 systemd 单元名的常见字符且必须以 .service 结尾, 且不能以 '-' 开头(防止被当作命令行选项)
fn valid_unit(name: &str) -> bool {
    name.len() <= 128
        && name.ends_with(".service")
        && name.chars().next().is_some_and(|c| c.is_ascii_alphanumeric())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '@' | ':' | '\\'))
}

/// 一次 systemctl show 查询所有配置的服务状态
async fn query_services(units: &[String]) -> Vec<ServiceStatus> {
    let unknown = || {
        units
            .iter()
            .map(|u| ServiceStatus {
                name: u.clone(),
                load_state: "unknown".into(),
                active_state: "unknown".into(),
                sub_state: String::new(),
                guarded: false,
                guard_paused: false,
            })
            .collect::<Vec<_>>()
    };
    if units.is_empty() {
        return vec![];
    }
    let mut cmd = Command::new("systemctl");
    cmd.args(["show", "-p", "LoadState", "-p", "ActiveState", "-p", "SubState"])
        .args(units)
        .kill_on_drop(true);
    let Ok(Ok(out)) = tokio::time::timeout(Duration::from_secs(5), cmd.output()).await else {
        return unknown();
    };
    let text = String::from_utf8_lossy(&out.stdout);
    // 多个单元的输出以空行分隔, 顺序与参数一致
    let blocks: Vec<&str> = text.split("\n\n").filter(|b| !b.trim().is_empty()).collect();
    if blocks.len() != units.len() {
        return unknown();
    }
    units
        .iter()
        .zip(blocks)
        .map(|(name, block)| {
            let get = |key: &str| {
                block
                    .lines()
                    .find_map(|l| l.strip_prefix(key).and_then(|r| r.strip_prefix('=')))
                    .unwrap_or("")
                    .to_string()
            };
            ServiceStatus {
                name: name.clone(),
                load_state: get("LoadState"),
                active_state: get("ActiveState"),
                sub_state: get("SubState"),
                guarded: false,
                guard_paused: false,
            }
        })
        .collect()
}

/// 执行服务控制命令 (调用方需保证 service 在 config 白名单内)
async fn control_service(cmd: &ServiceCommand) -> ServiceResult {
    let fail = |m: String| ServiceResult { id: cmd.id, ok: false, message: m };
    let action = match ServiceAction::try_from(cmd.action) {
        Ok(ServiceAction::Start) => "start",
        Ok(ServiceAction::Stop) => "stop",
        Ok(ServiceAction::Restart) => "restart",
        _ => return fail("unsupported action".into()),
    };
    let mut c = Command::new("systemctl");
    c.arg(action).arg(&cmd.service).kill_on_drop(true);
    match tokio::time::timeout(SYSTEMCTL_TIMEOUT, c.output()).await {
        Err(_) => fail(format!("systemctl {action} timeout after {SYSTEMCTL_TIMEOUT:?}")),
        Ok(Err(e)) => fail(format!("run systemctl: {e}")),
        Ok(Ok(out)) => {
            let msg = String::from_utf8_lossy(&[out.stdout, out.stderr].concat()).trim().to_string();
            if out.status.success() {
                ServiceResult { id: cmd.id, ok: true, message: msg }
            } else {
                fail(if msg.is_empty() { format!("systemctl exit {}", out.status) } else { msg })
            }
        }
    }
}

struct Collector {
    sys: System,
    disks: Disks,
}

impl Collector {
    async fn new() -> Self {
        let mut sys = System::new().expect("init system info");
        sys.refresh_cpu_usage();
        // CPU 使用率需要两次刷新的间隔才有意义
        tokio::time::sleep(MINIMUM_CPU_UPDATE_INTERVAL).await;
        Collector { sys, disks: Disks::new_with_refreshed_list().expect("init disks") }
    }

    fn collect(&mut self) -> Report {
        self.sys.refresh_cpu_usage();
        self.sys.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
        self.disks.refresh(true);

        let mut seen = std::collections::HashSet::new();
        let mut disks = Vec::new();
        for d in self.disks.iter() {
            let fs = d.file_system().to_string_lossy().to_string();
            let mount = d.mount_point().to_string_lossy().to_string();
            if d.total_space() == 0
                || matches!(fs.as_str(), "tmpfs" | "devtmpfs" | "squashfs" | "overlay" | "efivarfs")
                || mount.starts_with("/snap")
                || mount.starts_with("/run")
            {
                continue;
            }
            // 同一设备被多次挂载(bind mount)只取第一个
            if !seen.insert(d.name().to_string_lossy().to_string()) {
                continue;
            }
            disks.push(Disk {
                mount,
                fs,
                total: d.total_space(),
                used: d.total_space().saturating_sub(d.available_space()),
            });
        }

        Report {
            ts_ms: chrono::Utc::now().timestamp_millis(),
            cpu_percent: self.sys.global_cpu_usage(),
            mem_total: self.sys.total_memory(),
            mem_used: self.sys.used_memory(),
            disks,
            services: vec![],
        }
    }
}

async fn run_once(cfg: &Config, cipher: &Cipher, collector: &mut Collector, services: &SharedServices) -> Result<()> {
    let channel = Endpoint::from_shared(cfg.server.clone())?
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(5))
        .keep_alive_while_idle(true)
        .tcp_nodelay(true)
        .connect()
        .await
        .context("connect")?;
    let mut grpc = MonitorClient::new(channel);

    let (tx, rx) = mpsc::channel::<Envelope>(16);
    tx.send(cipher.seal(&ClientMessage {
        body: Some(client_message::Body::Hello(Hello {
            name: cfg.name.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        })),
    }))
    .await?;

    let mut inbound = grpc
        .session(Request::new(ReceiverStream::new(rx)))
        .await
        .context("handshake (token 不正确或 name 非法?)")?
        .into_inner();
    println!("connected to {}", cfg.server);

    // 服务控制命令在独立任务中执行(可能耗时), 结果通过该通道回到主循环发送
    let (res_tx, mut res_rx) = mpsc::channel::<ServiceResult>(8);
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut deadline = Instant::now() + SERVER_SILENCE_TIMEOUT;

    loop {
        tokio::select! {
            _ = tick.tick() => {
                let mut report = collector.collect();
                report.services = services.status.lock().unwrap().clone();
                let env = cipher.seal(&ClientMessage {
                    body: Some(client_message::Body::Report(report)),
                });
                tx.send(env).await.context("send")?;
                // 上报守护事件 (发送失败则放回队列, 下次连接再发)
                loop {
                    let Some(ev) = services.events.lock().unwrap().pop_front() else { break };
                    let env = cipher.seal(&ClientMessage { body: Some(client_message::Body::GuardEvent(ev.clone())) });
                    if let Err(e) = tx.send(env).await {
                        services.events.lock().unwrap().push_front(ev);
                        return Err(anyhow::Error::from(e).context("send guard event"));
                    }
                }
            }
            msg = inbound.message() => {
                let env = msg.context("recv")?.context("server closed stream")?;
                let m: ServerMessage = cipher.open(&env).context("decrypt server message")?;
                match m.body {
                    Some(server_message::Body::ServiceCommand(cmd)) => {
                        let res_tx = res_tx.clone();
                        let shared = services.clone();
                        let units = cfg.services.clone();
                        let guard = cfg.guard.clone();
                        tokio::spawn(async move {
                            // 只允许控制 config 中列出的服务
                            let res = if units.contains(&cmd.service) {
                                println!("service command: {} {}", cmd.action, cmd.service);
                                // 手动停止守护服务 => 暂停守护; 手动启动/重启 => 恢复守护.
                                // 先于执行 systemctl 更新, 避免守护在停止完成的瞬间把它拉起来
                                let is_stop = cmd.action == ServiceAction::Stop as i32;
                                if is_stop {
                                    shared.paused.lock().unwrap().insert(cmd.service.clone());
                                } else {
                                    shared.paused.lock().unwrap().remove(&cmd.service);
                                }
                                let res = control_service(&cmd).await;
                                if is_stop && !res.ok {
                                    shared.paused.lock().unwrap().remove(&cmd.service);
                                }
                                res
                            } else {
                                ServiceResult { id: cmd.id, ok: false, message: "service not allowed by client config".into() }
                            };
                            // 立即刷新状态, 让 web 尽快看到变化
                            refresh(&shared, &units, &guard).await;
                            let _ = res_tx.send(res).await;
                        });
                    }
                    Some(server_message::Body::Welcome(_)) | Some(server_message::Body::Ping(_)) | None => {}
                }
                deadline = Instant::now() + SERVER_SILENCE_TIMEOUT;
            }
            Some(res) = res_rx.recv() => {
                let env = cipher.seal(&ClientMessage {
                    body: Some(client_message::Body::ServiceResult(res)),
                });
                tx.send(env).await.context("send result")?;
            }
            _ = tokio::time::sleep_until(deadline) => {
                anyhow::bail!("server silent for {:?}", SERVER_SILENCE_TIMEOUT);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let path: PathBuf = std::env::args().nth(1).unwrap_or_else(|| "client.toml".into()).into();
    let text = std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let cfg: Config = toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
    if let Some(bad) = cfg.services.iter().find(|u| !valid_unit(u)) {
        anyhow::bail!("invalid service name in config: {bad:?} (需形如 xxx.service)");
    }
    if let Some(bad) = cfg.guard.iter().find(|g| !cfg.services.contains(g)) {
        anyhow::bail!("guard 中的服务必须同时列在 services 中: {bad:?}");
    }
    let cipher = Cipher::from_token(&cfg.token);
    let mut collector = Collector::new().await;

    // 后台每 2 秒刷新一次服务状态, 并对守护的服务执行守护检查
    let services: SharedServices = Arc::new(Shared::default());
    refresh(&services, &cfg.services, &cfg.guard).await;
    if !cfg.services.is_empty() {
        let (shared, units, guard) = (services.clone(), cfg.services.clone(), cfg.guard.clone());
        tokio::spawn(async move {
            let mut states: HashMap<String, GuardState> = HashMap::new();
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let status = refresh(&shared, &units, &guard).await;
                if guard_check(&shared, &status, &guard, &mut states).await {
                    refresh(&shared, &units, &guard).await;
                }
            }
        });
    }

    let mut backoff = 1u64;
    loop {
        let started = Instant::now();
        if let Err(e) = run_once(&cfg, &cipher, &mut collector, &services).await {
            eprintln!("disconnected: {e:#}");
        }
        // 连接稳定过一段时间则重置退避
        if started.elapsed() > Duration::from_secs(30) {
            backoff = 1;
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(10);
    }
}
