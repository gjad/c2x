use std::collections::{HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use chrono::{Duration, Local, NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};

const KEEP_DAYS: i64 = 30;
/// 列表页每个 client 显示的最近日志条数
const RECENT: usize = 5;
/// 详情页每页日志条数
pub const PAGE_SIZE: usize = 100;

pub const KIND_ONLINE: &str = "online";
pub const KIND_OFFLINE: &str = "offline";
pub const KIND_SERVICE: &str = "service";
pub const KIND_GUARD: &str = "guard";
pub const KIND_ADMIN: &str = "admin";

/// 来自 client 的文本写入日志前清理: 控制字符(含换行)替换为空格, 并限制长度
pub fn clean_message(s: &str) -> String {
    let one_line: String = s.chars().map(|c| if c.is_control() { ' ' } else { c }).collect();
    let t = one_line.trim();
    if t.chars().count() > 300 { format!("{}...", t.chars().take(300).collect::<String>()) } else { t.to_string() }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LogEvent {
    /// unix ms
    pub ts: i64,
    /// client 唯一标识 (来源 IP)
    pub client: String,
    /// 记录时 client 的显示名称
    pub name: String,
    /// online / offline / service
    pub kind: String,
    /// 操作是否成功 (online/offline 恒为 true)
    pub ok: bool,
    pub message: String,
}

impl LogEvent {
    pub fn new(client: &str, name: &str, kind: &str, ok: bool, message: String) -> Self {
        LogEvent {
            ts: Utc::now().timestamp_millis(),
            client: client.to_string(),
            name: name.to_string(),
            kind: kind.to_string(),
            ok,
            message,
        }
    }
}

struct Inner {
    /// 保留期内的全部事件, 按时间升序
    events: Vec<LogEvent>,
    /// 每个 client 最近的几条事件, 按时间升序
    recent: HashMap<String, VecDeque<LogEvent>>,
    /// 上一次执行过期清理的日期
    cleaned_on: NaiveDate,
}

/// 事件日志: 记录 client 上下线和服务启停操作.
/// 每天一个文件 <log_dir>/events-YYYY-MM-DD.log (每行一个 JSON), 保留 30 天.
pub struct EventLog {
    dir: PathBuf,
    inner: Mutex<Inner>,
}

fn file_date(name: &str) -> Option<NaiveDate> {
    name.strip_prefix("events-")
        .and_then(|s| s.strip_suffix(".log"))
        .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
}

fn day_of(ts_ms: i64) -> NaiveDate {
    Local.timestamp_millis_opt(ts_ms).single().unwrap_or_else(Local::now).date_naive()
}

impl EventLog {
    pub fn new(dir: PathBuf) -> std::io::Result<Self> {
        fs::create_dir_all(&dir)?;
        let today = Local::now().date_naive();
        remove_expired(&dir, today);

        // 载入保留期内的历史日志, 按文件日期升序
        let mut files: Vec<(NaiveDate, PathBuf)> = fs::read_dir(&dir)?
            .flatten()
            .filter_map(|e| file_date(&e.file_name().to_string_lossy()).map(|d| (d, e.path())))
            .collect();
        files.sort();
        let mut events = Vec::new();
        for (_, path) in files {
            let text = fs::read_to_string(path).unwrap_or_default();
            events.extend(text.lines().filter_map(|l| serde_json::from_str::<LogEvent>(l).ok()));
        }
        events.sort_by_key(|e| e.ts);

        let mut recent: HashMap<String, VecDeque<LogEvent>> = HashMap::new();
        for e in &events {
            push_recent(&mut recent, e);
        }
        Ok(EventLog { dir, inner: Mutex::new(Inner { events, recent, cleaned_on: today }) })
    }

    pub fn append(&self, ev: LogEvent) {
        let mut inner = self.inner.lock().unwrap();

        let today = Local::now().date_naive();
        if inner.cleaned_on != today {
            remove_expired(&self.dir, today);
            let cutoff = (Utc::now() - Duration::days(KEEP_DAYS)).timestamp_millis();
            inner.events.retain(|e| e.ts >= cutoff);
            inner.cleaned_on = today;
        }

        let path = self.dir.join(format!("events-{}.log", day_of(ev.ts).format("%Y-%m-%d")));
        match serde_json::to_string(&ev) {
            Ok(line) => {
                let r = OpenOptions::new().create(true).append(true).open(&path).and_then(|mut f| writeln!(f, "{line}"));
                if let Err(e) = r {
                    eprintln!("write log {} failed: {e}", path.display());
                }
            }
            Err(e) => eprintln!("encode log failed: {e}"),
        }

        push_recent(&mut inner.recent, &ev);
        inner.events.push(ev);
    }

    /// 某个 client 最近的几条事件, 最新的在前
    pub fn recent(&self, client: &str) -> Vec<LogEvent> {
        let inner = self.inner.lock().unwrap();
        inner.recent.get(client).map(|q| q.iter().rev().cloned().collect()).unwrap_or_default()
    }

    /// 分页查询, 最新的在前. page 从 1 开始. 返回 (总条数, 本页事件)
    pub fn page(&self, client: Option<&str>, page: usize) -> (usize, Vec<LogEvent>) {
        let inner = self.inner.lock().unwrap();
        let matches = |e: &&LogEvent| client.is_none_or(|c| e.client == c);
        let total = inner.events.iter().filter(matches).count();
        let items = inner
            .events
            .iter()
            .rev()
            .filter(matches)
            .skip(page.saturating_sub(1) * PAGE_SIZE)
            .take(PAGE_SIZE)
            .cloned()
            .collect();
        (total, items)
    }
}

fn push_recent(recent: &mut HashMap<String, VecDeque<LogEvent>>, e: &LogEvent) {
    let q = recent.entry(e.client.clone()).or_default();
    q.push_back(e.clone());
    while q.len() > RECENT {
        q.pop_front();
    }
}

fn remove_expired(dir: &PathBuf, today: NaiveDate) {
    let limit = today - Duration::days(KEEP_DAYS - 1);
    let Ok(rd) = fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        if file_date(&e.file_name().to_string_lossy()).is_some_and(|d| d < limit) {
            let _ = fs::remove_file(e.path());
        }
    }
}
