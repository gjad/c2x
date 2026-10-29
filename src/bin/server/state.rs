use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

use c2x::pb::ServiceCommand;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::log::EventLog;
use crate::store::Store;

#[derive(Deserialize)]
pub struct Config {
    #[serde(default = "default_grpc")]
    pub grpc_listen: String,
    #[serde(default = "default_http")]
    pub http_listen: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// 事件日志目录 (client 上下线、服务启停操作), 每天一个文件, 保留 30 天
    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,
    /// 允许连接的 token 列表
    pub tokens: Vec<String>,
    /// 管理密码 (明文): 用于在网页上删除离线 client. 不配置则禁用删除功能
    #[serde(default)]
    pub admin_password: Option<String>,
}

fn default_grpc() -> String {
    "0.0.0.0:50051".into()
}
fn default_http() -> String {
    "0.0.0.0:8080".into()
}
fn default_data_dir() -> PathBuf {
    "data".into()
}
fn default_log_dir() -> PathBuf {
    "logs".into()
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DiskSample {
    pub mount: String,
    pub fs: String,
    pub total: u64,
    pub used: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Sample {
    /// server 接收时间 (unix ms)
    pub ts: i64,
    pub cpu: f32,
    pub mem_total: u64,
    pub mem_used: u64,
    pub disks: Vec<DiskSample>,
}

#[derive(Clone, Serialize)]
pub struct ServiceState {
    pub name: String,
    pub load: String,
    pub active: String,
    pub sub: String,
    /// 是否配置了守护
    pub guarded: bool,
    /// 守护已暂停 (被手动停止)
    pub guard_paused: bool,
}

/// 服务控制结果
pub struct ServiceOutcome {
    pub ok: bool,
    pub message: String,
}

#[derive(Clone, Serialize)]
pub struct ClientInfo {
    /// 唯一标识: client 来源 IP (用作数据目录名)
    pub id: String,
    /// client 配置的显示名称
    pub name: String,
    pub ip: String,
    pub online: bool,
    pub addr: Option<String>,
    pub version: Option<String>,
    /// 本次连接建立时间 (unix ms)
    pub connected_at: Option<i64>,
    /// 最近一次收到消息的时间 (unix ms)
    pub last_seen: Option<i64>,
    pub latest: Option<Sample>,
    /// client 配置中允许控制的服务及其最新状态
    pub services: Vec<ServiceState>,
    #[serde(skip)]
    pub conn_id: u64,
    /// 向该 client 当前连接下发服务控制命令
    #[serde(skip)]
    pub cmd_tx: Option<mpsc::Sender<ServiceCommand>>,
}

pub struct AppState {
    pub store: Store,
    pub events: EventLog,
    pub clients: Mutex<HashMap<String, ClientInfo>>,
    pub next_conn_id: Mutex<u64>,
    pub next_cmd_id: AtomicU64,
    pub admin_password: Option<String>,
    /// 串行化密码校验失败后的延迟, 使全局最多每秒一次失败尝试, 防止暴力猜测
    pub auth_gate: tokio::sync::Mutex<()>,
    /// 等待 client 返回结果的服务控制请求: 命令 id -> 结果通道
    pub pending: Mutex<HashMap<u64, oneshot::Sender<ServiceOutcome>>>,
}

/// id 会用作目录名, 只允许安全字符
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name != "."
        && name != ".."
        && name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// 显示名称: 非空, 不超过 64 个字符, 不含控制字符
pub fn valid_display_name(name: &str) -> bool {
    !name.is_empty() && name.chars().count() <= 64 && !name.chars().any(|c| c.is_control())
}
