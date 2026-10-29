use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use chrono::Utc;
use sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::{Stream, StreamExt};

use c2x::pb::{ServiceAction, ServiceCommand};

use crate::log::{clean_message, LogEvent, KIND_ADMIN, KIND_SERVICE, PAGE_SIZE};
use crate::state::{valid_name, AppState, ClientInfo};

const INDEX_HTML: &str = include_str!("index.html");
const DETAIL_HTML: &str = include_str!("detail.html");
const COMMON_JS: &str = include_str!("common.js");
const COMMON_CSS: &str = include_str!("common.css");

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/detail", get(|| async { Html(DETAIL_HTML) }))
        .route("/static/common.js", get(|| async { ([(CONTENT_TYPE, "text/javascript; charset=utf-8")], COMMON_JS) }))
        .route("/static/common.css", get(|| async { ([(CONTENT_TYPE, "text/css; charset=utf-8")], COMMON_CSS) }))
        .route("/api/stream", get(stream))
        .route("/api/history", get(history))
        .route("/api/events", get(events))
        .route("/api/clients/{id}/delete", post(delete_client))
        .route("/api/clients/{id}/services/{service}/{action}", post(service_action))
        .with_state(state)
}

#[derive(Serialize)]
struct ClientView {
    #[serde(flatten)]
    info: ClientInfo,
    /// 该 client 最近的几条日志, 最新的在前
    recent: Vec<LogEvent>,
}

#[derive(Serialize)]
struct Snapshot {
    now: i64,
    clients: Vec<ClientView>,
}

fn snapshot(state: &AppState) -> Snapshot {
    let mut infos: Vec<ClientInfo> = state.clients.lock().unwrap().values().cloned().collect();
    infos.sort_by(|a, b| a.id.cmp(&b.id));
    let clients = infos
        .into_iter()
        .map(|info| {
            let recent = state.events.recent(&info.id);
            ClientView { info, recent }
        })
        .collect();
    Snapshot { now: Utc::now().timestamp_millis(), clients }
}

/// SSE: 每秒推送一次所有 client 的实时状态
async fn stream(State(state): State<Arc<AppState>>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let s = IntervalStream::new(interval(Duration::from_secs(1))).map(move |_| {
        let json = serde_json::to_string(&snapshot(&state)).unwrap_or_default();
        Ok(Event::default().data(json))
    });
    Sse::new(s).keep_alive(KeepAlive::default())
}

#[derive(Deserialize)]
struct HistoryQuery {
    client: String,
    /// 起始时间 unix ms
    from: i64,
}

async fn history(State(state): State<Arc<AppState>>, Query(q): Query<HistoryQuery>) -> impl IntoResponse {
    if !valid_name(&q.client) {
        return (StatusCode::BAD_REQUEST, "invalid client").into_response();
    }
    let samples = tokio::task::spawn_blocking(move || state.store.history(&q.client, q.from))
        .await
        .unwrap_or_default();
    Json(samples).into_response()
}

#[derive(Deserialize)]
struct EventsQuery {
    client: Option<String>,
    page: Option<usize>,
}

#[derive(Serialize)]
struct EventsPage {
    total: usize,
    page: usize,
    pages: usize,
    page_size: usize,
    items: Vec<LogEvent>,
}

/// GET /api/events?client=<id>&page=<n>  日志按时间倒序, 每页 100 条
async fn events(State(state): State<Arc<AppState>>, Query(q): Query<EventsQuery>) -> impl IntoResponse {
    if q.client.as_deref().is_some_and(|c| !valid_name(c)) {
        return (StatusCode::BAD_REQUEST, "invalid client").into_response();
    }
    let page = q.page.unwrap_or(1).max(1);
    let (total, items) = state.events.page(q.client.as_deref(), page);
    let pages = total.div_ceil(PAGE_SIZE).max(1);
    Json(EventsPage { total, page, pages, page_size: PAGE_SIZE, items }).into_response()
}

/// client 执行 systemctl 的最长等待时间(client 自身 60s 超时, 这里留出余量)
const SERVICE_WAIT: Duration = Duration::from_secs(70);

#[derive(Serialize)]
struct ActionResult {
    ok: bool,
    message: String,
}

fn reply(code: StatusCode, message: &str) -> axum::response::Response {
    (code, Json(ActionResult { ok: false, message: message.to_string() })).into_response()
}

/// POST /api/clients/{id}/services/{service}/{start|stop|restart}
/// 要求自定义请求头 x-c2x, 浏览器跨站表单无法带上该头, 以此防止 CSRF.
async fn service_action(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path((id, service, action)): Path<(String, String, String)>,
) -> axum::response::Response {
    if !headers.contains_key("x-c2x") {
        return reply(StatusCode::FORBIDDEN, "missing x-c2x header");
    }
    let (action, label) = match action.as_str() {
        "start" => (ServiceAction::Start, "启动"),
        "stop" => (ServiceAction::Stop, "停止"),
        "restart" => (ServiceAction::Restart, "重启"),
        _ => return reply(StatusCode::BAD_REQUEST, "invalid action"),
    };
    if !valid_name(&id) {
        return reply(StatusCode::BAD_REQUEST, "invalid client");
    }

    let (cmd_tx, client_name) = {
        let clients = state.clients.lock().unwrap();
        let Some(info) = clients.get(&id) else {
            return reply(StatusCode::NOT_FOUND, "client not found");
        };
        if !info.services.iter().any(|s| s.name == service) {
            return reply(StatusCode::NOT_FOUND, "service not configured on client");
        }
        match (&info.cmd_tx, info.online) {
            (Some(tx), true) => (tx.clone(), info.name.clone()),
            _ => return reply(StatusCode::CONFLICT, "client offline"),
        }
    };

    let cmd_id = state.next_cmd_id.fetch_add(1, Ordering::Relaxed) + 1;
    let (tx, rx) = tokio::sync::oneshot::channel();
    state.pending.lock().unwrap().insert(cmd_id, tx);
    let cmd = ServiceCommand { id: cmd_id, service: service.clone(), action: action as i32 };
    if cmd_tx.send(cmd).await.is_err() {
        state.pending.lock().unwrap().remove(&cmd_id);
        return reply(StatusCode::CONFLICT, "client offline");
    }

    // 记录操作日志: 谁(来源 IP)对哪个服务做了什么, 结果如何
    let log = |ok: bool, detail: &str| {
        let msg = format!("{label} {service}: {detail} (操作来源 {})", peer.ip());
        state.events.append(LogEvent::new(&id, &client_name, KIND_SERVICE, ok, msg));
    };
    match tokio::time::timeout(SERVICE_WAIT, rx).await {
        Ok(Ok(out)) => {
            if out.ok {
                log(true, "成功");
            } else {
                log(false, &format!("失败 - {}", clean_message(&out.message)));
            }
            Json(ActionResult { ok: out.ok, message: out.message }).into_response()
        }
        _ => {
            state.pending.lock().unwrap().remove(&cmd_id);
            log(false, "失败 - client 未在规定时间内响应");
            reply(StatusCode::GATEWAY_TIMEOUT, "client did not respond in time")
        }
    }
}

#[derive(Deserialize)]
struct DeleteReq {
    password: String,
}

/// 比较密码: 先哈希再逐字节比较, 避免长度和内容相关的时间差
fn password_eq(a: &str, b: &str) -> bool {
    let (ha, hb) = (Sha256::digest(a.as_bytes()), Sha256::digest(b.as_bytes()));
    ha.iter().zip(hb.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// POST /api/clients/{id}/delete  body: {"password": "..."}
/// 只能删除已离线的 client; 需要 config 中配置的管理密码.
/// 删除其在列表中的记录和全部历史曲线数据 (操作日志保留).
async fn delete_client(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<DeleteReq>,
) -> axum::response::Response {
    if !headers.contains_key("x-c2x") {
        return reply(StatusCode::FORBIDDEN, "missing x-c2x header");
    }
    if !valid_name(&id) {
        return reply(StatusCode::BAD_REQUEST, "invalid client");
    }
    let Some(expected) = state.admin_password.as_deref() else {
        return reply(StatusCode::FORBIDDEN, "server 未配置 admin_password, 删除功能已禁用");
    };
    {
        // 持有闸门期间失败会延迟 1 秒, 全局最多每秒一次失败尝试
        let _gate = state.auth_gate.lock().await;
        if !password_eq(&req.password, expected) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            return reply(StatusCode::UNAUTHORIZED, "密码错误");
        }
    }

    // 在同一把锁内检查离线并移除, 避免与重新上线竞争
    let name = {
        let mut clients = state.clients.lock().unwrap();
        match clients.get(&id) {
            None => return reply(StatusCode::NOT_FOUND, "client not found"),
            Some(info) if info.online => return reply(StatusCode::CONFLICT, "client 在线, 不能删除"),
            Some(_) => clients.remove(&id).map(|i| i.name).unwrap_or_default(),
        }
    };
    let st = state.clone();
    let id2 = id.clone();
    let _ = tokio::task::spawn_blocking(move || st.store.delete_client(&id2)).await;
    state.events.append(LogEvent::new(&id, &name, KIND_ADMIN, true, format!("已删除该主机及其历史数据 (操作来源 {})", peer.ip())));
    Json(ActionResult { ok: true, message: String::new() }).into_response()
}
