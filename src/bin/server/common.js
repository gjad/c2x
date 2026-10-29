const $ = id => document.getElementById(id);

const fmtBytes = n => {
  const u = ['B','KB','MB','GB','TB']; let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return n.toFixed(i ? 1 : 0) + ' ' + u[i];
};
const pct = (a, b) => b > 0 ? a / b * 100 : 0;
const pad = n => String(n).padStart(2, '0');
const fmtTime = ms => { const d = new Date(ms);
  return `${d.getMonth()+1}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`; };
const fmtDur = ms => { let s = Math.max(0, Math.floor(ms/1000));
  const d = Math.floor(s/86400); s %= 86400;
  return (d ? d + 'd ' : '') + `${pad(Math.floor(s/3600))}:${pad(Math.floor(s%3600/60))}:${pad(s%60)}`; };

// 服务状态只用颜色表达: 运行中(active)绿色, 其他(dead/inactive/failed 等)红色; 状态未知时为灰色
const svcClass = x => 'svc ' + (x.active === 'unknown' ? '' : x.active === 'active' ? 'up' : 'down');
const svcText = x => x.load === 'not-found' ? `${x.name} 未安装` : x.name;
// 被守护的服务显示盾牌图标 (守护暂停时变灰), 未守护的不显示
const setGuardIcon = (ic, x) => { ic.hidden = !x.guarded; ic.classList.toggle('paused', !!x.guard_paused); };

const fmtFull = ms => { const d = new Date(ms);
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`; };
// 日志颜色: 上线绿色, 下线红色, 服务操作失败红色, 服务操作成功橙色
const evClass = e => e.kind === 'online' ? 'good' : e.kind === 'offline' ? 'bad' : e.ok ? 'op' : 'bad';
const evKind = e => e.kind === 'online' ? '上线' : e.kind === 'offline' ? '下线' : e.kind === 'guard' ? '守护重启' : e.kind === 'admin' ? '管理操作' : '服务操作';

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}
function barCell(cls) {
  const cell = el('div', 'cell');
  const bar = el('div', 'bar ' + cls); const fill = el('i'); bar.appendChild(fill);
  const val = el('div', 'val');
  cell.append(bar, val);
  return { cell, fill, val };
}
function setBar(r, p, text) { r.fill.style.width = Math.min(100, p).toFixed(1) + '%'; r.val.textContent = text; }

// ---- 实时行: 每个 client 一行, 以 IP 为唯一标识 ----
function headRow() {
  const h = el('div', 'hostrow head');
  for (const t of ['主机 (IP)', '状态', 'CPU / 内存', '硬盘', '服务', '最近日志']) h.appendChild(el('div', '', t));
  return h;
}

function createRow() {
  const root = el('div', 'hostrow');
  const nm = el('div', 'nm'), meta = el('div', 'meta');
  const host = el('div'); host.append(nm, meta);
  const badge = el('span', 'badge', '离线');
  const st = el('div'); st.appendChild(badge);
  const cpu = barCell('cpu'), mem = barCell('mem');
  const disks = el('div');
  const svcs = el('div');
  const logs = el('div', 'logs');
  const cpumem = el('div', 'cpumem'); cpumem.append(cpu.cell, mem.cell);
  root.append(host, st, cpumem, disks, svcs, logs);
  return { root, nm, meta, badge, cpu, mem, disks, svcs, logs, logKey: null, diskRows: new Map() };
}

function updateRow(k, c, now) {
  k.root.classList.toggle('offline', !c.online);
  k.nm.textContent = c.name;
  k.badge.textContent = c.online ? '在线' : '离线';
  k.badge.classList.toggle('on', c.online);
  const parts = [c.ip];
  if (c.online && c.connected_at) parts.push('已连接 ' + fmtDur(now - c.connected_at));
  if (!c.online && c.last_seen) parts.push('最后在线 ' + fmtTime(c.last_seen));
  k.meta.textContent = parts.join('\n');
  // 服务状态: 每个服务一行 (小圆点颜色表示状态)
  const sv = c.services || [];
  if (!sv.length) {
    if (!k.svcs.firstChild || k.svcs.firstChild.className !== 'meta') {
      k.svcs.textContent = '';
      k.svcs.appendChild(el('div', 'meta', '-'));
    }
  } else {
    if (k.svcs.firstChild && k.svcs.firstChild.className === 'meta') k.svcs.textContent = '';
    while (k.svcs.children.length > sv.length) k.svcs.lastChild.remove();
    sv.forEach((x, i) => {
      let d = k.svcs.children[i];
      if (!d) { d = el('div', 'svc'); d.append(el('span', 'dot'), el('span', 'txt'), el('span', 'guard')); k.svcs.appendChild(d); }
      d.className = svcClass(x);
      d.children[1].textContent = svcText(x);
      setGuardIcon(d.children[2], x);
    });
  }
  // 最近 5 条日志 (内容变化时才重建, 避免每秒刷新打断文本选择)
  const recent = c.recent || [];
  const key = recent.map(e => e.ts + e.message).join('|');
  if (key !== k.logKey) {
    k.logKey = key;
    k.logs.replaceChildren(...(recent.length ? recent.map(e => {
      const d = el('div', 'ev ' + evClass(e));
      d.title = fmtFull(e.ts) + ' ' + e.message;
      d.append(el('span', 't', fmtTime(e.ts)), el('span', 'm', e.message));
      return d;
    }) : [el('div', 'meta', '-')]));
  }
  const s = c.latest;
  if (!s) return;
  setBar(k.cpu, s.cpu, 'CPU ' + s.cpu.toFixed(1) + '%');
  setBar(k.mem, pct(s.mem_used, s.mem_total), `内存 ${fmtBytes(s.mem_used)} / ${fmtBytes(s.mem_total)}`);
  const seen = new Set();
  for (const d of s.disks) {
    seen.add(d.mount);
    let r = k.diskRows.get(d.mount);
    if (!r) { r = barCell('disk'); k.diskRows.set(d.mount, r); k.disks.appendChild(r.cell); }
    setBar(r, pct(d.used, d.total), `硬盘 ${d.mount}  ${fmtBytes(d.used)} / ${fmtBytes(d.total)}`);
  }
  for (const [m, r] of k.diskRows) if (!seen.has(m)) { r.cell.remove(); k.diskRows.delete(m); }
}

// 实时流中断时, 页面上的数据不再可信: 把所有行标记为“状态未知”, 而不是继续显示在线
function setRowsStale(rows) {
  for (const r of rows) {
    r.root.classList.add('offline');
    r.badge.textContent = '未知'; r.badge.classList.remove('on');
  }
}

// ---- SSE: 每秒一次的实时快照 ----
function watchStream(onSnap, onStale) {
  let broken = false, lastMsg = Date.now();
  const es = new EventSource('/api/stream');
  es.onopen = () => {
    // 断开后重新连上(如 server 重启/升级), 直接刷新页面以获取最新代码和数据
    if (broken) { location.reload(); return; }
    $('conn').textContent = '实时已连接';
  };
  es.onmessage = e => { lastMsg = Date.now(); $('conn').textContent = '实时已连接'; onSnap(JSON.parse(e.data)); };
  es.onerror = () => { broken = true; $('conn').textContent = '实时连接断开, 重试中…'; onStale(); };
  // 连接看似存在但 server 长时间没有推送也视为中断
  setInterval(() => { if (Date.now() - lastMsg > 5000) { $('conn').textContent = '实时数据已停止更新'; onStale(); } }, 1000);
}
