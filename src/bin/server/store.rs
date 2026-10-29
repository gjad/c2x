use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use chrono::{Duration, Local, NaiveDate, TimeZone};

use crate::state::Sample;

const KEEP_DAYS: i64 = 30;

struct DayBuf {
    date: NaiveDate,
    samples: Vec<Sample>,
}

/// 每个 client 每分钟保存一个采样(该分钟内收到的第一个), 每天一个 json 文件,
/// 路径: <data_dir>/<client>/<YYYY-MM-DD>.json, 保留 30 天.
pub struct Store {
    dir: PathBuf,
    bufs: Mutex<HashMap<String, DayBuf>>,
}

fn day_of(ts_ms: i64) -> NaiveDate {
    Local.timestamp_millis_opt(ts_ms).single().unwrap_or_else(Local::now).date_naive()
}

fn read_day(path: &Path) -> Vec<Sample> {
    fs::read(path).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default()
}

impl Store {
    pub fn new(dir: PathBuf) -> std::io::Result<Self> {
        fs::create_dir_all(&dir)?;
        Ok(Store { dir, bufs: Mutex::new(HashMap::new()) })
    }

    fn day_path(&self, client: &str, date: NaiveDate) -> PathBuf {
        self.dir.join(client).join(format!("{}.json", date.format("%Y-%m-%d")))
    }

    /// 删除某个 client 的全部历史数据 (调用方保证 client 已通过 valid_name 校验)
    pub fn delete_client(&self, client: &str) {
        self.bufs.lock().unwrap().remove(client);
        let _ = fs::remove_dir_all(self.dir.join(client));
    }

    /// data_dir 下已有历史数据的 client 名称
    pub fn known_clients(&self) -> Vec<String> {
        let Ok(rd) = fs::read_dir(&self.dir) else { return vec![] };
        rd.flatten()
            .filter(|e| e.path().is_dir())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| crate::state::valid_name(n))
            .collect()
    }

    /// 记住 client 的显示名称, server 重启后离线状态仍能显示
    pub fn save_name(&self, client: &str, name: &str) {
        let dir = self.dir.join(client);
        if fs::create_dir_all(&dir).is_ok() {
            let _ = fs::write(dir.join("name.txt"), name);
        }
    }

    pub fn load_name(&self, client: &str) -> Option<String> {
        fs::read_to_string(self.dir.join(client).join("name.txt")).ok().map(|s| s.trim().to_string())
    }

    /// 若该采样属于新的一分钟则记录
    pub fn record(&self, client: &str, s: &Sample) {
        let date = day_of(s.ts);
        let minute = s.ts.div_euclid(60_000);
        let mut bufs = self.bufs.lock().unwrap();
        let path = self.day_path(client, date);
        let buf = bufs.entry(client.to_string()).or_insert_with(|| DayBuf {
            date,
            samples: read_day(&path),
        });
        let new_day = buf.date != date;
        if new_day {
            buf.date = date;
            buf.samples = read_day(&path);
        }
        if buf.samples.last().is_some_and(|l| l.ts.div_euclid(60_000) >= minute) {
            return;
        }
        buf.samples.push(s.clone());
        if let Err(e) = write_atomic(&path, &buf.samples) {
            eprintln!("save {} failed: {e}", path.display());
        }
        if new_day || buf.samples.len() == 1 {
            self.cleanup(client);
        }
    }

    pub fn cleanup(&self, client: &str) {
        let limit = Local::now().date_naive() - Duration::days(KEEP_DAYS - 1);
        let Ok(rd) = fs::read_dir(self.dir.join(client)) else { return };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some(d) = name.strip_suffix(".json").and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()) {
                if d < limit {
                    let _ = fs::remove_file(e.path());
                }
            }
        }
    }

    /// 读取 [from_ms, now] 内的历史采样
    pub fn history(&self, client: &str, from_ms: i64) -> Vec<Sample> {
        let today = Local::now().date_naive();
        let earliest = today - Duration::days(KEEP_DAYS - 1);
        let mut d = day_of(from_ms).max(earliest);
        let mut out = Vec::new();
        while d <= today {
            out.extend(read_day(&self.day_path(client, d)).into_iter().filter(|s| s.ts >= from_ms));
            d += Duration::days(1);
        }
        out
    }
}

fn write_atomic(path: &Path, samples: &[Sample]) -> std::io::Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec(samples)?)?;
    fs::rename(tmp, path)
}
