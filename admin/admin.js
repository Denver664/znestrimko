import React, { useCallback, useEffect, useMemo, useState } from "https://esm.sh/react@18.3.1";
import { createRoot } from "https://esm.sh/react-dom@18.3.1/client";
import htm from "https://esm.sh/htm@3.1.1";

const html = htm.bind(React.createElement);
const tokenKey = "znestrumko-admin-token";
const filters = [
  ["all", "Усі події"],
  ["schedules", "Графіки"],
  ["notifications", "Оповіщення"],
  ["system", "Система"],
];

function prettyDate(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString("uk-UA", { dateStyle: "short", timeStyle: "medium" });
}

function duration(seconds = 0) {
  const minutes = Math.floor(Math.max(0, seconds) / 60);
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return hours ? `${hours} год ${rest} хв` : `${rest} хв`;
}

async function api(path, token, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(token ? { "X-Admin-Token": token } : {}),
      ...options.headers,
    },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail || `Запит не виконано (${response.status})`);
  return body;
}

function Login({ onLogin }) {
  const [token, setToken] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const submit = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      await api("/api/login", "", { method: "POST", body: JSON.stringify({ token }) });
      sessionStorage.setItem(tokenKey, token);
      onLogin(token);
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };
  return html`<main className="login-wrap">
    <form className="login-card" onSubmit=${submit}>
      <div className="brand-mark">⚡</div>
      <p className="eyebrow">КЕРУВАННЯ БОТОМ</p>
      <h1>Знеструмко</h1>
      <p className="muted">Увійдіть за токеном адміністратора, налаштованим на сервері.</p>
      <label className="field-label" htmlFor="admin-token">Токен адміністратора</label>
      <input id="admin-token" type="password" autoComplete="current-password" value=${token} onInput=${(e) => setToken(e.target.value)} placeholder="ADMIN_TOKEN" required />
      ${error && html`<p className="error-text">${error}</p>`}
      <button className="button primary full" disabled=${busy}>${busy ? "Перевіряю…" : "Увійти до панелі"}</button>
    </form>
  </main>`;
}

function App() {
  const [token, setToken] = useState(sessionStorage.getItem(tokenKey) || "");
  const [ready, setReady] = useState(false);
  const [status, setStatus] = useState(null);
  const [schedules, setSchedules] = useState([]);
  const [queues, setQueues] = useState([]);
  const [selectedQueue, setSelectedQueue] = useState("*");
  const [logs, setLogs] = useState([]);
  const [filter, setFilter] = useState("all");
  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState("");

  const load = useCallback(async (authToken) => {
    const [nextStatus, nextSchedules, nextLogs] = await Promise.all([
      api("/api/status", authToken),
      api("/api/schedules", authToken),
      api("/api/logs?limit=200", authToken),
    ]);
    setStatus(nextStatus);
    setSchedules(nextSchedules.items || []);
    setQueues(nextSchedules.queues || []);
    setLogs(nextLogs.items || []);
    setReady(true);
  }, []);

  useEffect(() => {
    if (!token) return;
    load(token).catch(() => {
      sessionStorage.removeItem(tokenKey);
      setToken("");
      setReady(false);
    });
  }, [token, load]);

  useEffect(() => {
    if (!token || !ready) return;
    const timer = window.setInterval(() => {
      Promise.all([api("/api/status", token), api("/api/schedules", token)])
        .then(([nextStatus, nextSchedules]) => {
          setStatus(nextStatus);
          setSchedules(nextSchedules.items || []);
          setQueues(nextSchedules.queues || []);
        })
        .catch((error) => setToast(error.message));
    }, 15000);
    const scheme = location.protocol === "https:" ? "wss:" : "ws:";
    let socket;
    let reconnectTimer;
    let closed = false;
    const connect = () => {
      socket = new WebSocket(`${scheme}//${location.host}/api/live`);
      socket.onopen = () => socket.send(JSON.stringify({ token }));
      socket.onmessage = (event) => {
        const item = JSON.parse(event.data);
        setLogs((current) => {
          if (current.some((entry) => entry.id === item.id)) return current;
          return [...current, item].slice(-500);
        });
      };
      socket.onclose = () => {
        if (!closed) reconnectTimer = window.setTimeout(connect, 2000);
      };
    };
    connect();
    return () => {
      closed = true;
      window.clearInterval(timer);
      window.clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, [token, ready]);

  useEffect(() => {
    if (!toast) return;
    const timer = window.setTimeout(() => setToast(""), 4000);
    return () => window.clearTimeout(timer);
  }, [toast]);

  const visibleLogs = useMemo(() => filter === "all" ? [...logs].reverse() : [...logs].filter((item) => item.category === filter).reverse(), [logs, filter]);

  const refresh = async () => {
    setBusy(true);
    try {
      await api("/api/refresh", token, { method: "POST", body: JSON.stringify({ queue: selectedQueue }) });
      setToast(selectedQueue === "*" ? "Примусове оновлення всіх черг поставлено в чергу" : `Оновлення черги ${selectedQueue} поставлено в чергу`);
    } catch (error) {
      setToast(error.message);
    } finally {
      setBusy(false);
    }
  };

  const logout = () => {
    sessionStorage.removeItem(tokenKey);
    setToken("");
    setReady(false);
  };

  if (!token) return html`<${Login} onLogin=${setToken} />`;
  if (!ready) return html`<main className="loading">Завантажую панель…</main>`;

  return html`<div className="app-shell">
    <aside className="sidebar">
      <div className="brand"><span className="brand-mark small">⚡</span><span><b>Знеструмко</b><small>АДМІНПАНЕЛЬ</small></span></div>
      <div className="side-label">МОНІТОРИНГ</div>
      <a className="side-link active" href="#overview"><span>▦</span> Огляд</a>
      <a className="side-link" href="#schedules"><span>◷</span> Графіки</a>
      <a className="side-link" href="#activity"><span>≋</span> Live журнал</a>
      <div className="sidebar-bottom"><span className=${`status-dot ${status?.bot_running ? "online" : "offline"}`}></span><span>${status?.bot_running ? "Бот працює" : "Бот зупинений"}</span><button className="logout" onClick=${logout} title="Вийти">↗</button></div>
    </aside>

    <main className="main-content">
      <header className="topbar"><div><p className="eyebrow">ЦЕНТР КЕРУВАННЯ</p><h1>Огляд системи</h1></div><div className="top-actions"><span className="live-pill"><i></i> LIVE</span><button className="button ghost" onClick=${() => load(token)}>↻ <span>Оновити огляд</span></button></div></header>

      <section id="overview" className="stats-grid">
        <article className="stat-card"><div className="stat-head"><span>Стан Telegram-бота</span><span className=${`stat-icon ${status.bot_running ? "green" : "red"}`}>●</span></div><strong className=${status.bot_running ? "green-text" : "red-text"}>${status.bot_running ? "Працює" : "Зупинений"}</strong><small>${status.bot_running ? "Моніторинг активний" : "Перевірте логи запуску"}</small></article>
        <article className="stat-card"><div className="stat-head"><span>Активні підписки</span><span className="stat-icon blue">♧</span></div><strong>${status.subscriptions_total}</strong><small>у ${status.queues_subscribed} чергах</small></article>
        <article className="stat-card"><div className="stat-head"><span>Остання перевірка</span><span className="stat-icon violet">◷</span></div><strong className="stat-date">${prettyDate(status.last_check_at)}</strong><small>перевірка графіків щохвилини</small></article>
        <article className="stat-card"><div className="stat-head"><span>Останнє успішне отримання</span><span className="stat-icon amber">↻</span></div><strong className="stat-date">${prettyDate(status.last_success_at)}</strong><small>${status.queues_total} доступних черг</small></article>
      </section>

      <section className="panel refresh-panel">
        <div className="panel-copy"><div className="panel-icon">↻</div><div><h2>Керування графіками</h2><p>Запросіть актуальний графік зараз. Для черги без підписників результат збережеться в кеші адмінпанелі.</p></div></div>
        <div className="refresh-controls"><select value=${selectedQueue} onChange=${(e) => setSelectedQueue(e.target.value)}><option value="*">Усі черги</option>${queues.map((queue) => html`<option key=${queue} value=${queue}>Черга ${queue}</option>`)}</select><button className="button primary" onClick=${refresh} disabled=${busy || !status.bot_running}>${busy ? "Надсилаю…" : "⟳  Оновити графік"}</button></div>
      </section>

      <section id="schedules" className="panel schedule-panel">
        <div className="section-heading"><div><p className="eyebrow">РОЗКЛАДИ</p><h2>Останні графіки</h2></div><span className="count-badge">${schedules.length} записів</span></div>
        ${schedules.length ? html`<div className="table-wrap"><table><thead><tr><th>Черга</th><th>Дата графіка</th><th>Періоди відключень</th><th>Разом за день</th><th>Підписники</th><th>Оновлено</th></tr></thead><tbody>${schedules.slice(0, 30).map((item) => html`<tr key=${`${item.queue}-${item.date}`}><td><span className="queue-tag">${item.queue}</span></td><td>${item.date}</td><td>${item.intervals.length ? item.intervals.join(", ") : html`<span className="muted">Немає</span>`}</td><td>${duration(item.total_seconds)}</td><td>${item.subscribers}</td><td className="muted">${prettyDate(item.fetched_at)}</td></tr>`)}</tbody></table></div>` : html`<div className="empty-state"><span>◷</span><b>Кеш графіків поки порожній</b><p>Натисніть «Оновити графік», щоб завантажити всі черги.</p></div>`}
      </section>

      <section id="activity" className="panel logs-panel">
        <div className="section-heading logs-heading"><div><p className="eyebrow">ПОДІЇ В РЕАЛЬНОМУ ЧАСІ</p><h2>Журнал активності <span className="live-pulse"></span></h2></div><span className="count-badge">${logs.length} у буфері</span></div>
        <nav className="log-filters">${filters.map(([id, label]) => html`<button className=${filter === id ? "filter active" : "filter"} onClick=${() => setFilter(id)}>${label}</button>`)}</nav>
        <div className="log-list">${visibleLogs.length ? visibleLogs.map((item) => html`<article className="log-row" key=${item.id}><span className=${`log-level ${item.level.toLowerCase()}`}>${item.level}</span><time>${prettyDate(item.time)}</time><span className=${`category ${item.category}`}>${item.category === "notifications" ? "Оповіщення" : item.category === "schedules" ? "Графіки" : "Система"}</span><p>${item.message}</p></article>`) : html`<div className="empty-state compact">Поки немає подій у цій категорії.</div>`}</div>
      </section>
      <footer className="footer">Знеструмко · Моніторинг оновлюється автоматично</footer>
    </main>
    ${toast && html`<div className="toast">${toast}</div>`}
  </div>`;
}

createRoot(document.getElementById("root")).render(html`<${App} />`);
