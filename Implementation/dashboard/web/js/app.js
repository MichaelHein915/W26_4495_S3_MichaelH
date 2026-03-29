const API_BASE = '';

let barChart = null;
let lineChart = null;
let donutChart = null;
let modalChart = null;
let volatilityChart = null;
let volumeChart = null;
let exchangeChart = null;
let heatmapChart = null;
let radarChart = null;
let bubbleChart = null;
let stackedVolumeChart = null;
let refreshTimer = null;
let metricsCache = [];
let tableSortKey = 'product_id';
let tableSortDir = 1;
let timeseriesCache = [];
let sparklinesCache = {};
let lineChartSelectedSymbols = null;
let prevKpiValues = {};
let activeAnimations = {};
let windowMinutes = 1;
let notificationsEnabled = localStorage.getItem('notif') === 'true';
let prevAlertKeys = new Set();
let currentModalSymbol = null;
let currentModalChartType = 'candlestick';
let exchangeFilter = '';
let directionFilter = 'all';
let favorites = JSON.parse(localStorage.getItem('favorites') || '[]');

const CHART_COLORS = [
  '#3b82f6', '#22c55e', '#eab308', '#ef4444', '#f97316',
  '#a855f7', '#06b6d4', '#ec4899', '#84cc16', '#6366f1',
];

const el = (id) => document.getElementById(id);

const kafkaStatusEl = el('kafkaStatus');
const freshnessStatusEl = el('freshnessStatus');
const latencyStatusEl = el('latencyStatus');
const totalTradesEl = el('totalTrades');
const liveSymbolsEl = el('liveSymbols');
const totalVolumeEl = el('totalVolume');
const liveExchangesEl = el('liveExchanges');
const dataFreshnessEl = el('dataFreshness');
const metricsBodyEl = el('metricsBody');
const alertsEl = el('alerts');
const alertsSectionEl = el('alertsSection');
const priceAlertsEl = el('priceAlerts');
const priceAlertsSectionEl = el('priceAlertsSection');
const updatedAtEl = el('updatedAt');
const eventCountEl = el('eventCount');
const windowMinutesEl = el('windowMinutes');
const liveToggleEl = el('liveToggle');
const refreshRateEl = el('refreshRate');
const symbolSearchEl = el('symbolSearch');
const exchangePillsEl = el('exchangePills');
const exchangeFilterEl = el('exchangeFilter');
const arbitrageListEl = el('arbitrageList');
const arbitrageSectionEl = el('arbitrageSection');
const anomalyAlertsEl = el('anomalyAlerts');
const anomalyAlertsSectionEl = el('anomalyAlertsSection');
const toastContainerEl = el('toastContainer');
const tickerTrackEl = el('tickerTrack');
const tickerSectionEl = el('tickerSection');
const newsFeedEl = el('newsFeed');
const sentimentGaugeNeedleEl = el('sentimentGaugeNeedle');
const sentimentScoreEl = el('sentimentScore');
const sentimentPosCountEl = el('sentimentPosCount');
const sentimentNeuCountEl = el('sentimentNeuCount');
const sentimentNegCountEl = el('sentimentNegCount');
const newsSentimentSectionEl = el('newsSentimentSection');
let sentimentBySymbolCache = {};
const watchlistGridEl = el('watchlistGrid');

function setStatusClass(elem, status) {
  elem.classList.remove('ok', 'warn', 'error');
  if (status) elem.classList.add(status);
}

function fmt(num) { return new Intl.NumberFormat().format(num); }
function fmtVol(val) { return parseFloat(val).toFixed(4); }
function fmtUsd(val) {
  if (val >= 1000) return `$${(val / 1000).toFixed(1)}k`;
  if (val >= 1) return `$${val.toFixed(2)}`;
  return `$${val.toFixed(4)}`;
}

// ─── Favorites ───────────────────────────────────────────────────────

function saveFavorites() {
  localStorage.setItem('favorites', JSON.stringify(favorites));
}

function toggleFavorite(symbol) {
  const idx = favorites.indexOf(symbol);
  if (idx >= 0) favorites.splice(idx, 1);
  else favorites.push(symbol);
  saveFavorites();
  renderMetricsTable(metricsCache);
  renderWatchlist(metricsCache);
}

function renderWatchlist(metrics) {
  if (!watchlistGridEl) return;
  if (!favorites.length) {
    watchlistGridEl.innerHTML = `
      <div class="watchlist-empty">
        <div class="watchlist-empty-icon">⭐</div>
        <div>Star symbols in the table to add them here</div>
      </div>`;
    return;
  }
  const favMetrics = favorites
    .map((s) => (metrics || []).find((m) => m.product_id === s))
    .filter(Boolean);
  if (!favMetrics.length) {
    watchlistGridEl.innerHTML = `
      <div class="watchlist-empty">
        <div class="watchlist-empty-icon">⏳</div>
        <div>Waiting for data for favorited symbols…</div>
      </div>`;
    return;
  }
  watchlistGridEl.innerHTML = favMetrics.map((m) => {
    const pct = m.price_change_pct;
    const cls = pct > 0 ? 'up' : pct < 0 ? 'down' : 'flat';
    const arrow = pct > 0 ? '▲' : pct < 0 ? '▼' : '–';
    return `<div class="watchlist-card" data-symbol="${m.product_id}">
      <div class="watchlist-card-header">
        <span class="watchlist-symbol">${m.product_id}</span>
        <span class="watchlist-change ${cls}">${arrow} ${Math.abs(pct).toFixed(2)}%</span>
      </div>
      <div class="watchlist-price">${fmtUsd(m.avg_price_usd)}</div>
      <div class="watchlist-meta">
        <span>${fmt(m.trade_count)} trades</span>
        <span>Vol: ${fmtVol(m.total_volume_qty)}</span>
      </div>
    </div>`;
  }).join('');

  watchlistGridEl.querySelectorAll('.watchlist-card').forEach((card) => {
    card.addEventListener('click', () => openModal(card.dataset.symbol));
  });
}

// ─── Market Insights ─────────────────────────────────────────────────

function renderMarketInsights(metrics) {
  if (!metrics?.length) return;

  const gainers = metrics.filter((m) => m.price_change_pct > 0).length;
  const losers = metrics.filter((m) => m.price_change_pct < 0).length;
  const total = metrics.length;
  const dirEl = el('insightDirection');
  if (dirEl) {
    const pctUp = total > 0 ? ((gainers / total) * 100).toFixed(0) : 0;
    dirEl.textContent = `${pctUp}% up · ${gainers}▲ ${losers}▼`;
    dirEl.style.color = gainers > losers ? 'var(--accent-green)' : gainers < losers ? 'var(--accent-red)' : 'var(--text-secondary)';
  }

  const avgChange = metrics.reduce((s, m) => s + m.price_change_pct, 0) / total;
  const avgEl = el('insightAvgChange');
  if (avgEl) {
    avgEl.textContent = `${avgChange >= 0 ? '+' : ''}${avgChange.toFixed(3)}%`;
    avgEl.style.color = avgChange >= 0 ? 'var(--accent-green)' : 'var(--accent-red)';
  }

  const mostActive = [...metrics].sort((a, b) => b.trade_count - a.trade_count)[0];
  const activeEl = el('insightMostActive');
  if (activeEl && mostActive) {
    activeEl.textContent = `${mostActive.product_id} (${fmt(mostActive.trade_count)})`;
  }

  const highVol = [...metrics].sort((a, b) => b.total_volume_qty - a.total_volume_qty)[0];
  const volEl = el('insightHighVol');
  if (volEl && highVol) {
    volEl.textContent = `${highVol.product_id} (${fmtVol(highVol.total_volume_qty)})`;
  }

  const mostVolatile = [...metrics].sort((a, b) => b.volatility_usd - a.volatility_usd)[0];
  const volatileEl = el('insightVolatile');
  if (volatileEl && mostVolatile) {
    volatileEl.textContent = `${mostVolatile.product_id} ($${mostVolatile.volatility_usd.toFixed(2)})`;
  }

  const totalVolume = metrics.reduce((s, m) => s + m.total_volume_qty, 0);
  const btcVol = metrics.find((m) => m.product_id === 'BTC-USD')?.total_volume_qty || 0;
  const domEl = el('insightBtcDom');
  if (domEl) {
    const dom = totalVolume > 0 ? ((btcVol / totalVolume) * 100).toFixed(1) : '0.0';
    domEl.textContent = `${dom}% by volume`;
  }
}

// ─── Theme toggle ───────────────────────────────────────────────────

function applyTheme(theme) {
  if (theme === 'light') {
    document.documentElement.setAttribute('data-theme', 'light');
    el('themeIcon').textContent = '🌙';
  } else {
    document.documentElement.removeAttribute('data-theme');
    el('themeIcon').textContent = '☀';
  }
  localStorage.setItem('theme', theme);

  [barChart, lineChart, donutChart, volatilityChart, volumeChart, exchangeChart, radarChart, bubbleChart, stackedVolumeChart].forEach((c) => {
    if (!c) return;
    const txtColor = theme === 'light' ? '#64748b' : '#64748b';
    const gridColor = theme === 'light' ? 'rgba(0,0,0,0.06)' : 'rgba(55,65,81,0.3)';
    Object.values(c.options.scales || {}).forEach((axis) => {
      if (axis.ticks) axis.ticks.color = txtColor;
      if (axis.grid) axis.grid.color = gridColor;
      if (axis.title) axis.title.color = txtColor;
    });
    if (c.options.plugins?.legend?.labels) c.options.plugins.legend.labels.color = txtColor;
    c.update('none');
  });
}

el('themeToggle').addEventListener('click', () => {
  const current = localStorage.getItem('theme') || 'dark';
  applyTheme(current === 'dark' ? 'light' : 'dark');
});

applyTheme(localStorage.getItem('theme') || 'dark');

// ─── Notification toggle ────────────────────────────────────────────

function updateNotifButton() {
  const btn = el('notifToggle');
  if (notificationsEnabled) btn.classList.add('active');
  else btn.classList.remove('active');
  el('notifIcon').textContent = notificationsEnabled ? '🔔' : '🔕';
}

el('notifToggle').addEventListener('click', () => {
  notificationsEnabled = !notificationsEnabled;
  localStorage.setItem('notif', notificationsEnabled);
  updateNotifButton();
  if (notificationsEnabled && 'Notification' in window && Notification.permission === 'default') {
    Notification.requestPermission();
  }
});

updateNotifButton();

function showToast(message) {
  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.textContent = message;
  toastContainerEl.appendChild(toast);
  setTimeout(() => toast.remove(), 5000);
}

function playAlertSound() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.frequency.value = 880;
    osc.type = 'sine';
    gain.gain.value = 0.15;
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.3);
    osc.start();
    osc.stop(ctx.currentTime + 0.3);
  } catch (_) { /* no audio context */ }
}

function notifyAlerts(alerts) {
  if (!notificationsEnabled || !alerts?.length) return;
  const newAlerts = alerts.filter((a) => {
    const key = `${a.product_id}:${a.spike_ratio}`;
    return !prevAlertKeys.has(key);
  });
  if (!newAlerts.length) return;

  prevAlertKeys = new Set(alerts.map((a) => `${a.product_id}:${a.spike_ratio}`));
  playAlertSound();

  newAlerts.forEach((a) => {
    const msg = `Volume spike: ${a.product_id} ${a.spike_ratio}x (${fmtVol(a.current_volume)})`;
    showToast(msg);
    if ('Notification' in window && Notification.permission === 'granted') {
      new Notification('Crypto Alert', { body: msg, icon: '⚡' });
    }
  });
}

// ─── Animated number counting ───────────────────────────────────────

function animateValue(element, end, duration, formatter) {
  const id = element.id || element.dataset?.key || Math.random().toString();
  if (activeAnimations[id]) cancelAnimationFrame(activeAnimations[id]);

  const startText = element.textContent.replace(/[^0-9.\-]/g, '');
  const start = parseFloat(startText) || 0;
  if (Math.abs(start - end) < 0.001) {
    element.textContent = formatter(end);
    return;
  }

  const t0 = performance.now();
  function step(now) {
    const elapsed = now - t0;
    const progress = Math.min(elapsed / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3);
    const current = start + (end - start) * eased;
    element.textContent = formatter(current);
    if (progress < 1) activeAnimations[id] = requestAnimationFrame(step);
    else delete activeAnimations[id];
  }
  activeAnimations[id] = requestAnimationFrame(step);
}

function flashKpi(cardId, newValue) {
  const key = cardId;
  if (prevKpiValues[key] !== undefined && prevKpiValues[key] !== newValue) {
    const card = el(cardId);
    if (card) {
      card.classList.add('flash');
      setTimeout(() => card.classList.remove('flash'), 600);
    }
  }
  prevKpiValues[key] = newValue;
}

// ─── CSV export ─────────────────────────────────────────────────────

function exportCsv() {
  const displayed = getFilteredAndSortedMetrics(metricsCache);
  if (!displayed.length) return;
  const headers = ['Symbol', 'Change %', 'Trades', 'Avg Price (USD)', 'VWAP (USD)', 'Volatility', 'Volume'];
  const rows = displayed.map((m) => [
    m.product_id,
    m.price_change_pct,
    m.trade_count,
    m.avg_price_usd,
    m.vwap_usd,
    m.volatility_usd,
    m.total_volume_qty,
  ]);
  const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `crypto_metrics_${windowMinutes}m_${new Date().toISOString().slice(0, 19)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

el('exportCsv')?.addEventListener('click', exportCsv);

// ─── Data fetching ──────────────────────────────────────────────────

async function fetchDashboard() {
  try {
    let url = `${API_BASE}/api/dashboard?window=${windowMinutes}`;
    if (exchangeFilter) url += `&exchange=${encodeURIComponent(exchangeFilter)}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(res.statusText);
    return await res.json();
  } catch (err) {
    console.error('Dashboard fetch failed:', err);
    return null;
  }
}

async function fetchCandles(symbol) {
  try {
    let url = `${API_BASE}/api/candles?symbol=${encodeURIComponent(symbol)}&window=${windowMinutes}`;
    if (exchangeFilter) url += `&exchange=${encodeURIComponent(exchangeFilter)}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(res.statusText);
    const data = await res.json();
    return data.candles || [];
  } catch (err) {
    console.error('Candles fetch failed:', err);
    return [];
  }
}

// ─── Rendering ──────────────────────────────────────────────────────

function renderStatus(data) {
  const s = data?.status || {};
  const kafka = s.kafka_status || '--';
  const kafkaErr = s.kafka_error;
  const freshness = s.freshness_seconds;
  const latency = s.latency_seconds;

  kafkaStatusEl.querySelector('.value').textContent = kafka;
  if (kafkaErr) setStatusClass(kafkaStatusEl, 'error');
  else if (kafka.includes('receiving')) setStatusClass(kafkaStatusEl, 'ok');
  else if (kafka.includes('waiting')) setStatusClass(kafkaStatusEl, 'warn');
  else setStatusClass(kafkaStatusEl, null);

  freshnessStatusEl.querySelector('.value').textContent =
    freshness != null ? `${freshness.toFixed(1)}s since last trade` : '--';
  setStatusClass(freshnessStatusEl,
    freshness != null && freshness <= 10 ? 'ok' :
    freshness != null && freshness <= 30 ? 'warn' :
    freshness != null ? 'error' : null);

  latencyStatusEl.querySelector('.value').textContent =
    latency != null ? `${latency.toFixed(2)}s` : '--';
  setStatusClass(latencyStatusEl,
    latency != null && latency <= 2 ? 'ok' :
    latency != null && latency <= 5 ? 'warn' :
    latency != null ? 'error' : null);
}

function renderExchangeBar(exchangeStats) {
  if (!exchangePillsEl) return;
  const exchanges = exchangeStats?.exchanges || [];
  const counts = exchangeStats?.exchange_counts || {};
  if (!exchanges.length) {
    exchangePillsEl.innerHTML = '<span style="color:var(--text-muted);font-size:0.8rem">waiting for data…</span>';
    return;
  }
  exchangePillsEl.innerHTML = exchanges.map((name) => {
    const active = !exchangeFilter || exchangeFilter === name;
    return `<span class="exchange-pill ${active ? 'active' : ''}" data-exchange="${name}" role="button" tabindex="0" title="Click to filter by ${name}">
      <span class="pill-dot"></span>${name}<span class="pill-count">${fmt(counts[name] || 0)} trades</span>
    </span>`;
  }).join('');

  exchangePillsEl.querySelectorAll('.exchange-pill').forEach((pill) => {
    pill.addEventListener('click', () => {
      const ex = pill.dataset.exchange;
      exchangeFilter = exchangeFilter === ex ? '' : ex;
      if (exchangeFilterEl) exchangeFilterEl.value = exchangeFilter;
      refresh();
    });
  });
}

function renderKPIs(data) {
  const s = data?.status || {};
  const trades = s.total_trades ?? 0;
  const symbols = s.live_symbols ?? 0;
  const volume = s.total_volume ?? 0;
  const freshness = s.freshness_seconds;
  const numExchanges = data?.exchange_stats?.exchanges?.length ?? 0;

  flashKpi('kpiTrades', trades);
  flashKpi('kpiSymbols', symbols);
  flashKpi('kpiVolume', volume);
  flashKpi('kpiExchanges', numExchanges);
  flashKpi('kpiFreshness', freshness);

  animateValue(totalTradesEl, trades, 400, (v) => fmt(Math.round(v)));
  animateValue(liveSymbolsEl, symbols, 400, (v) => fmt(Math.round(v)));
  animateValue(totalVolumeEl, volume, 400, (v) => fmtVol(v));
  animateValue(liveExchangesEl, numExchanges, 400, (v) => fmt(Math.round(v)));
  dataFreshnessEl.textContent = freshness != null ? `${freshness.toFixed(1)}s` : 'N/A';
  updatedAtEl.textContent = s.updated_at ?? '--:--:-- UTC';
  eventCountEl.textContent = fmt(s.event_count ?? 0);
  windowMinutesEl.textContent = s.window_minutes ?? windowMinutes;

  document.querySelectorAll('.kpi-window').forEach((span) => { span.textContent = windowMinutes; });
  const summaryEl = el('tableSummaryWindow');
  if (summaryEl) summaryEl.textContent = windowMinutes;
}

function renderTopMovers(metrics) {
  const section = el('topMoversSection');
  if (!metrics?.length) { section.style.display = 'none'; return; }
  const sorted = [...metrics].sort((a, b) => a.price_change_pct - b.price_change_pct);
  const loser = sorted[0];
  const gainer = sorted[sorted.length - 1];
  if (gainer.price_change_pct === 0 && loser.price_change_pct === 0) { section.style.display = 'none'; return; }
  section.style.display = 'flex';
  el('topGainer').querySelector('.mover-text').textContent =
    `${gainer.product_id} ${gainer.price_change_pct >= 0 ? '+' : ''}${gainer.price_change_pct}%`;
  el('topLoser').querySelector('.mover-text').textContent =
    `${loser.product_id} ${loser.price_change_pct >= 0 ? '+' : ''}${loser.price_change_pct}%`;
}

function drawSparkline(canvas, prices) {
  if (!prices || prices.length < 2) return;
  const ctx = canvas.getContext('2d');
  const w = canvas.width = 90;
  const h = canvas.height = 28;
  ctx.clearRect(0, 0, w, h);
  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const range = max - min || 1;
  const pad = 2;
  const up = prices[prices.length - 1] >= prices[0];
  const color = up ? '#22c55e' : '#ef4444';
  ctx.beginPath();
  prices.forEach((p, i) => {
    const x = (i / (prices.length - 1)) * (w - pad * 2) + pad;
    const y = h - pad - ((p - min) / range) * (h - pad * 2);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  ctx.stroke();
  ctx.lineTo(w - pad, h);
  ctx.lineTo(pad, h);
  ctx.closePath();
  const grad = ctx.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, up ? 'rgba(34,197,94,0.25)' : 'rgba(239,68,68,0.25)');
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  ctx.fillStyle = grad;
  ctx.fill();
}

function getFilteredAndSortedMetrics(metrics) {
  if (!metrics?.length) return [];
  let out = [...metrics];
  const q = (symbolSearchEl?.value || '').trim().toUpperCase();
  if (q) out = out.filter((m) => m.product_id.toUpperCase().includes(q));
  if (directionFilter === 'up') out = out.filter((m) => m.price_change_pct > 0);
  else if (directionFilter === 'down') out = out.filter((m) => m.price_change_pct < 0);
  out.sort((a, b) => {
    const va = a[tableSortKey], vb = b[tableSortKey];
    return tableSortDir * (typeof va === 'string' ? va.localeCompare(vb) : (va ?? 0) - (vb ?? 0));
  });
  return out;
}

function changeClass(val) { return val > 0 ? 'change-positive' : val < 0 ? 'change-negative' : 'change-neutral'; }
function changeArrow(val) { return val > 0 ? '▲' : val < 0 ? '▼' : '–'; }

function renderMetricsTable(metrics) {
  metricsCache = metrics || [];
  const displayed = getFilteredAndSortedMetrics(metricsCache);
  if (!displayed.length) {
    const searchActive = symbolSearchEl?.value?.trim() || directionFilter !== 'all';
    metricsBodyEl.innerHTML = searchActive
      ? '<tr class="empty-row"><td colspan="9"><div class="empty-state"><span class="empty-state-icon">🔍</span><div class="empty-state-title">No symbols match your filter</div><div class="empty-state-desc">Try a different search term or direction filter</div></div></td></tr>'
      : '<tr class="empty-row"><td colspan="9"><div class="empty-state"><span class="empty-state-icon">📊</span><div class="empty-state-title">Waiting for trades…</div><div class="empty-state-desc">Start the producer to see live metrics. Data appears as trades stream in.</div></div></td></tr>';
    return;
  }
  metricsBodyEl.innerHTML = displayed.map((m) => {
    const cls = changeClass(m.price_change_pct);
    const arrow = changeArrow(m.price_change_pct);
    const isFav = favorites.includes(m.product_id);
    return `<tr data-symbol="${m.product_id}">
      <td>
        <div class="symbol-cell">
          <span class="fav-star ${isFav ? 'active' : ''}" data-symbol="${m.product_id}" title="Toggle watchlist">★</span>
          <span class="symbol-badge">${m.product_id}</span>
        </div>
      </td>
      <td class="sparkline-cell"><canvas data-symbol="${m.product_id}"></canvas></td>
      <td class="${cls}">${arrow} ${Math.abs(m.price_change_pct).toFixed(2)}%</td>
      <td>${fmt(m.trade_count)}</td>
      <td>${fmtUsd(m.avg_price_usd)}</td>
      <td>${fmtUsd(m.vwap_usd)}</td>
      <td>${fmtUsd(m.volatility_usd)}</td>
      <td>${fmtVol(m.total_volume_qty)}</td>
      <td>${getSentimentBadge(m.product_id)}</td>
    </tr>`;
  }).join('');

  requestAnimationFrame(() => {
    metricsBodyEl.querySelectorAll('.sparkline-cell canvas').forEach((cvs) => {
      drawSparkline(cvs, sparklinesCache[cvs.dataset.symbol]);
    });
  });

  metricsBodyEl.querySelectorAll('.fav-star').forEach((star) => {
    star.addEventListener('click', (e) => {
      e.stopPropagation();
      toggleFavorite(star.dataset.symbol);
    });
  });

  metricsBodyEl.querySelectorAll('tr[data-symbol]').forEach((row) => {
    row.addEventListener('click', () => openModal(row.dataset.symbol));
  });
}

function updateSortIndicator() {
  document.querySelectorAll('.metrics-table th.sortable').forEach((th) => {
    const label = th.dataset.label || th.textContent;
    th.textContent = label + (th.dataset.sort === tableSortKey ? (tableSortDir === 1 ? ' ▽' : ' △') : '');
  });
}

function renderAlerts(alerts) {
  if (!alerts?.length) { alertsSectionEl.style.display = 'none'; return; }
  alertsSectionEl.style.display = 'block';
  alertsEl.innerHTML = alerts.map((a) =>
    `<div class="alert"><span class="alert-icon">⚡</span> <strong>${a.product_id}</strong>: volume ${a.current_volume.toFixed(4)} ` +
    `(baseline ${a.baseline_volume.toFixed(4)}, <strong>${a.spike_ratio}x</strong>)</div>`
  ).join('');
  notifyAlerts(alerts);
}

function renderPriceAlerts(priceAlerts) {
  if (!priceAlertsSectionEl || !priceAlertsEl) return;
  if (!priceAlerts?.length) {
    priceAlertsSectionEl.style.display = 'none';
    return;
  }
  priceAlertsSectionEl.style.display = 'block';
  priceAlertsEl.innerHTML = priceAlerts.map((a) =>
    `<div class="alert alert-price"><span class="alert-icon">💰</span> <strong>${a.product_id}</strong>: ` +
    `$${a.current_price.toLocaleString()} ${a.direction === 'above' ? '≥' : '≤'} $${a.threshold_price.toLocaleString()}</div>`
  ).join('');
}

function renderAnomalyAlerts(anomalies) {
  if (!anomalyAlertsSectionEl || !anomalyAlertsEl) return;
  if (!anomalies?.length) {
    anomalyAlertsSectionEl.style.display = 'none';
    return;
  }
  anomalyAlertsSectionEl.style.display = 'block';
  anomalyAlertsEl.innerHTML = anomalies.map((a) =>
    `<div class="alert alert-anomaly"><span class="alert-icon">🔮</span> <strong>${a.product_id}</strong>: ` +
    `score ${a.anomaly_score} · trades ${a.trade_count} · vol $${a.volatility_usd?.toFixed(2) ?? '-'} · ` +
    `change ${a.price_change_pct >= 0 ? '+' : ''}${a.price_change_pct?.toFixed(2) ?? '-'}%</div>`
  ).join('');
}

// ─── News & Sentiment ────────────────────────────────────────────────

function timeAgo(isoStr) {
  if (!isoStr) return '';
  try {
    const then = new Date(isoStr);
    const now = Date.now();
    const diff = Math.max(0, Math.floor((now - then) / 1000));
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
  } catch { return ''; }
}

function renderSentimentGauge(summary) {
  if (!sentimentGaugeNeedleEl || !sentimentScoreEl) return;
  const compound = summary?.avg_compound ?? 0;
  const pct = ((compound + 1) / 2) * 100;
  sentimentGaugeNeedleEl.style.left = `${pct}%`;

  const label = summary?.label || 'neutral';
  const color = label === 'positive' ? 'var(--accent-green)' :
                label === 'negative' ? 'var(--accent-red)' : 'var(--accent-yellow)';
  sentimentScoreEl.textContent = `${compound >= 0 ? '+' : ''}${compound.toFixed(3)}`;
  sentimentScoreEl.style.color = color;

  if (sentimentPosCountEl) sentimentPosCountEl.textContent = summary?.positive ?? 0;
  if (sentimentNeuCountEl) sentimentNeuCountEl.textContent = summary?.neutral ?? 0;
  if (sentimentNegCountEl) sentimentNegCountEl.textContent = summary?.negative ?? 0;
}

function renderNewsFeed(news) {
  if (!newsFeedEl) return;
  if (!news?.length) {
    newsFeedEl.innerHTML = '<div class="news-empty">No news articles yet. Enable NEWS_ENABLED and set CRYPTOPANIC_API_KEY.</div>';
    return;
  }
  newsFeedEl.innerHTML = news.map((n) => {
    const s = n.sentiment || {};
    const lbl = s.label || 'neutral';
    const currencies = (n.currencies || []).map(c =>
      `<span class="news-currency-tag">${c}</span>`
    ).join('');
    return `<div class="news-card">
      <span class="news-sentiment-pill ${lbl}">${lbl}</span>
      <div class="news-card-body">
        <div class="news-card-title"><a href="${n.url || '#'}" target="_blank" rel="noopener">${n.title || 'Untitled'}</a></div>
        <div class="news-card-meta">
          <span>${n.domain || ''}</span>
          <span>${timeAgo(n.published_at)}</span>
          <div class="news-card-currencies">${currencies}</div>
        </div>
      </div>
    </div>`;
  }).join('');
}

function renderNewsSentiment(data) {
  const summary = data?.sentiment_summary;
  const news = data?.news;
  const bySymbol = data?.sentiment_by_symbol || [];

  sentimentBySymbolCache = {};
  bySymbol.forEach(s => { sentimentBySymbolCache[s.currency] = s; });

  const hasNews = news && news.length > 0;
  if (newsSentimentSectionEl) {
    newsSentimentSectionEl.style.display = hasNews ? 'block' : 'none';
  }

  if (hasNews) {
    renderSentimentGauge(summary);
    renderNewsFeed(news);
  }
}

function getSentimentBadge(productId) {
  const base = productId ? productId.split('-')[0] : '';
  const s = sentimentBySymbolCache[base];
  if (!s) return '<span class="sentiment-badge neutral"><span class="sentiment-dot neutral"></span> --</span>';
  const lbl = s.label || 'neutral';
  const display = lbl === 'positive' ? '+' + s.avg_compound.toFixed(2) :
                  lbl === 'negative' ? s.avg_compound.toFixed(2) :
                  s.avg_compound.toFixed(2);
  return `<span class="sentiment-badge ${lbl}"><span class="sentiment-dot ${lbl}"></span> ${display}</span>`;
}

function renderArbitrage(arbitrage) {
  if (!arbitrageSectionEl || !arbitrageListEl) return;
  if (!arbitrage?.length) {
    arbitrageSectionEl.style.display = 'none';
    return;
  }
  arbitrageSectionEl.style.display = 'block';
  arbitrageListEl.innerHTML = arbitrage.map((a) =>
    `<div class="arbitrage-card">
      <div class="arbitrage-symbol">${a.product_id}</div>
      <div class="arbitrage-spread">${a.spread_pct}% spread</div>
      <div class="arbitrage-detail">
        Buy on <strong>${a.cheap_exchange}</strong> @ $${a.cheap_price.toLocaleString()} →
        Sell on <strong>${a.expensive_exchange}</strong> @ $${a.expensive_price.toLocaleString()}
      </div>
    </div>`
  ).join('');
}

function hexToRgb(hex) {
  const m = hex.replace('#', '').match(/.{2}/g);
  return m ? m.map((v) => parseInt(v, 16)).join(', ') : '255, 255, 255';
}

function getThemeColors() {
  const light = document.documentElement.getAttribute('data-theme') === 'light';
  return {
    text: '#64748b',
    grid: light ? 'rgba(0,0,0,0.06)' : 'rgba(55,65,81,0.3)',
    gridStrong: light ? 'rgba(0,0,0,0.1)' : 'rgba(55,65,81,0.5)',
    tooltipBg: light ? 'rgba(255,255,255,0.96)' : 'rgba(17,24,39,0.96)',
    tooltipTitle: light ? '#0f172a' : '#f1f5f9',
    tooltipBody: light ? '#64748b' : '#94a3b8',
    tooltipBorder: light ? '#e2e8f0' : '#374151',
  };
}

function updateBarChart(metrics) {
  const ctx = el('barChart').getContext('2d');
  const tc = getThemeColors();
  if (!metrics?.length) { if (barChart) barChart.data.datasets = []; return; }
  const labels = metrics.map((m) => m.product_id);
  const prices = metrics.map((m) => m.avg_price_usd);
  const trades = metrics.map((m) => m.trade_count);
  const priceColors = metrics.map((m) => m.price_change_pct >= 0 ? 'rgba(59, 130, 246, 0.75)' : 'rgba(239, 68, 68, 0.75)');

  if (!barChart) {
    barChart = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets: [
        { label: 'Avg Price (USD)', data: prices, backgroundColor: priceColors, yAxisID: 'y', borderRadius: 6, borderSkipped: false },
        { label: 'Trade Count', data: trades, backgroundColor: 'rgba(34, 197, 94, 0.6)', yAxisID: 'y1', borderRadius: 6, borderSkipped: false },
      ]},
      options: {
        responsive: true, maintainAspectRatio: true,
        plugins: {
          legend: { labels: { color: tc.text, usePointStyle: true, pointStyle: 'rectRounded', padding: 16 } },
          tooltip: { backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8, padding: 10 },
        },
        scales: {
          x: { ticks: { color: tc.text }, grid: { color: tc.gridStrong } },
          y: {
            type: 'logarithmic',
            position: 'left',
            title: { display: true, text: 'Avg Price (USD, log)', color: tc.text },
            ticks: { color: tc.text, callback: (v) => v >= 1000 ? `$${(v / 1000).toFixed(0)}k` : v >= 1 ? `$${v.toFixed(0)}` : `$${v.toFixed(2)}` },
            grid: { color: tc.grid },
            min: 0.01,
          },
          y1: { type: 'linear', position: 'right', title: { display: true, text: 'Trade Count', color: tc.text }, ticks: { color: tc.text }, grid: { drawOnChartArea: false } },
        },
      },
    });
  } else {
    barChart.data.labels = labels;
    barChart.data.datasets[0].data = prices;
    barChart.data.datasets[0].backgroundColor = priceColors;
    barChart.data.datasets[1].data = trades;
  }
  barChart.update();
}

function renderTicker(recentTrades) {
  if (!tickerTrackEl || !tickerSectionEl) return;
  tickerSectionEl.style.display = 'block';
  if (!recentTrades?.length) {
    tickerTrackEl.innerHTML = '<div class="empty-state"><span class="empty-state-icon">⏳</span><div class="empty-state-title">No recent trades yet</div><div class="empty-state-desc">Trades appear here as they stream in</div></div>';
    return;
  }
  tickerTrackEl.innerHTML = recentTrades.map((t) =>
    `<span class="ticker-item"><span class="ticker-symbol">${t.product_id}</span> ` +
    `$${t.price_usd.toLocaleString()} <span class="ticker-size">×${fmtVol(t.size_qty)}</span> ` +
    `<span class="ticker-exchange">${t.exchange}</span> <span class="ticker-time">${t.event_time}</span></span>`
  ).join('');
}

function updateVolatilityChart(metrics) {
  const ctx = el('volatilityChart')?.getContext('2d');
  const tc = getThemeColors();
  if (!metrics?.length) { if (volatilityChart) volatilityChart.data.datasets = []; return; }
  const labels = metrics.map((m) => m.product_id);
  const vols = metrics.map((m) => Math.max(0.01, m.volatility_usd ?? 0));
  const colors = metrics.map((_, i) => CHART_COLORS[i % CHART_COLORS.length] + 'bb');

  if (!volatilityChart) {
    volatilityChart = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets: [{ label: 'Volatility (USD)', data: vols, backgroundColor: colors, borderRadius: 6, borderSkipped: false }] },
      options: {
        indexAxis: 'y',
        responsive: true, maintainAspectRatio: true,
        plugins: {
          legend: { display: false },
          tooltip: { backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8 },
        },
        scales: {
          x: {
            type: 'logarithmic',
            ticks: { color: tc.text, callback: (v) => v >= 1 ? v.toFixed(0) : v.toFixed(2) },
            grid: { color: tc.grid },
            min: 0.01,
          },
          y: { ticks: { color: tc.text }, grid: { color: tc.grid } },
        },
      },
    });
  } else {
    volatilityChart.data.labels = labels;
    volatilityChart.data.datasets[0].data = vols;
    volatilityChart.data.datasets[0].backgroundColor = colors;
  }
  volatilityChart.update();
}

function updateVolumeChart(volumeTs) {
  const ctx = el('volumeChart')?.getContext('2d');
  const tc = getThemeColors();
  if (!volumeTs?.length) { if (volumeChart) volumeChart.data.datasets = []; return; }
  const labels = volumeTs.map((r) => r.event_time.split('T')[1] || r.event_time);
  const volumes = volumeTs.map((r) => r.total_volume_qty);

  if (!volumeChart) {
    volumeChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: 'Total Volume',
          data: volumes,
          borderColor: CHART_COLORS[2],
          backgroundColor: 'rgba(234, 179, 8, 0.1)',
          fill: true, tension: 0.4, pointRadius: 0, pointHitRadius: 6,
          borderWidth: 2,
        }],
      },
      plugins: [gradientFillPlugin],
      options: {
        responsive: true, maintainAspectRatio: true,
        plugins: {
          legend: { display: false },
          tooltip: { backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8 },
        },
        scales: {
          x: { ticks: { color: tc.text, maxTicksLimit: 8 }, grid: { color: tc.grid } },
          y: { ticks: { color: tc.text }, grid: { color: tc.grid } },
        },
      },
    });
  } else {
    volumeChart.data.labels = labels;
    volumeChart.data.datasets[0].data = volumes;
  }
  volumeChart.update();
}

function updateExchangeChart(exchangeMetrics) {
  const ctx = el('exchangeChart')?.getContext('2d');
  const tc = getThemeColors();
  if (!exchangeMetrics?.length) {
    if (exchangeChart) exchangeChart.data.datasets = [];
    el('exchangeChartSection')?.style.setProperty('display', 'none');
    return;
  }
  el('exchangeChartSection')?.style.setProperty('display', 'block');

  const bySymbol = {};
  exchangeMetrics.forEach((r) => {
    if (!bySymbol[r.product_id]) bySymbol[r.product_id] = {};
    bySymbol[r.product_id][r.exchange] = r.avg_price_usd;
  });
  const symbols = Object.keys(bySymbol).sort();
  const exchanges = [...new Set(exchangeMetrics.map((r) => r.exchange))].sort();
  if (exchanges.length < 2) {
    if (exchangeChart) exchangeChart.data.datasets = [];
    el('exchangeChartSection')?.style.setProperty('display', 'none');
    return;
  }

  const datasets = exchanges.map((ex, i) => ({
    label: ex,
    data: symbols.map((s) => {
      const v = bySymbol[s]?.[ex];
      return v != null ? Math.max(0.01, v) : null;
    }),
    backgroundColor: CHART_COLORS[i % CHART_COLORS.length] + 'bb',
    borderRadius: 6,
    borderSkipped: false,
  }));

  if (!exchangeChart) {
    exchangeChart = new Chart(ctx, {
      type: 'bar',
      data: { labels: symbols, datasets },
      options: {
        responsive: true, maintainAspectRatio: true,
        plugins: {
          legend: { labels: { color: tc.text, usePointStyle: true, pointStyle: 'rectRounded', padding: 16 } },
          tooltip: { backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8 },
        },
        scales: {
          x: { ticks: { color: tc.text }, grid: { color: tc.gridStrong } },
          y: {
            type: 'logarithmic',
            ticks: { color: tc.text, callback: (v) => v >= 1000 ? `$${(v / 1000).toFixed(0)}k` : v >= 1 ? `$${v.toFixed(0)}` : `$${v.toFixed(2)}` },
            grid: { color: tc.grid },
            min: 0.01,
          },
        },
      },
    });
  } else {
    exchangeChart.data.labels = symbols;
    exchangeChart.data.datasets = datasets;
  }
  exchangeChart.update();
}

function updateHeatmap(heatmapData) {
  const wrap = el('heatmapWrap');
  const canvas = el('heatmapChart');
  const legendEl = el('heatmapLegend');
  if (!canvas || !heatmapData?.labels?.length || !heatmapData?.matrix?.length) {
    if (wrap) wrap.style.display = 'none';
    return;
  }
  wrap.style.display = 'block';
  const { labels, times, matrix } = heatmapData;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const w = rect.width;
  const h = rect.height;
  const cellW = Math.max(20, (w - 80) / times.length);
  const cellH = Math.max(12, (h - 40) / labels.length);
  const labelW = 70;
  const labelH = 20;
  const allVals = matrix.flat().filter((v) => v != null);
  const minVal = allVals.length ? Math.min(...allVals) : 0;
  const maxVal = allVals.length ? Math.max(...allVals) : 0;
  const range = Math.max(Math.abs(minVal), Math.abs(maxVal), 0.01);

  function colorFor(val) {
    if (val == null) return 'rgba(128,128,128,0.2)';
    const t = (val + range) / (2 * range);
    if (t >= 0.5) {
      const g = Math.round(63 + (1 - t) * 2 * 192);
      return `rgb(34, ${g}, 94)`;
    }
    const r = Math.round(239 - t * 2 * 160);
    return `rgb(${r}, 68, 68)`;
  }

  ctx.clearRect(0, 0, w, h);
  ctx.font = '10px "Inter", sans-serif';
  ctx.textAlign = 'right';
  ctx.fillStyle = getThemeColors().text;
  labels.forEach((lbl, i) => {
    ctx.fillText(lbl, labelW - 4, labelH + i * cellH + cellH / 2 + 4);
  });
  ctx.textAlign = 'center';
  times.forEach((t, i) => {
    ctx.fillText(t, labelW + i * cellW + cellW / 2, labelH - 4);
  });
  matrix.forEach((row, ri) => {
    row.forEach((val, ci) => {
      ctx.fillStyle = colorFor(val);
      const x = labelW + ci * cellW + 1;
      const y = labelH + ri * cellH + 1;
      const cw = cellW - 2;
      const ch = cellH - 2;
      const r = 3;
      ctx.beginPath();
      ctx.moveTo(x + r, y);
      ctx.lineTo(x + cw - r, y);
      ctx.quadraticCurveTo(x + cw, y, x + cw, y + r);
      ctx.lineTo(x + cw, y + ch - r);
      ctx.quadraticCurveTo(x + cw, y + ch, x + cw - r, y + ch);
      ctx.lineTo(x + r, y + ch);
      ctx.quadraticCurveTo(x, y + ch, x, y + ch - r);
      ctx.lineTo(x, y + r);
      ctx.quadraticCurveTo(x, y, x + r, y);
      ctx.closePath();
      ctx.fill();
      if (val != null && cellW > 24) {
        ctx.fillStyle = '#f1f5f9';
        ctx.font = '9px "Inter", sans-serif';
        ctx.fillText(val.toFixed(1) + '%', labelW + ci * cellW + cellW / 2, labelH + ri * cellH + cellH / 2 + 3);
      }
    });
  });
  if (legendEl) {
    legendEl.innerHTML = `<span style="color:var(--accent-green)">▲ +${range.toFixed(1)}%</span> &nbsp; <span style="color:var(--accent-red)">▼ -${range.toFixed(1)}%</span>`;
  }
}

function updateRadarChart(metrics) {
  const ctx = el('radarChart')?.getContext('2d');
  const tc = getThemeColors();
  const top = (metrics || []).sort((a, b) => b.total_volume_qty - a.total_volume_qty).slice(0, 5);
  if (!top.length || !ctx) {
    if (radarChart) { radarChart.data.datasets = []; radarChart.update(); }
    return;
  }
  const maxPrice = Math.max(...top.map((m) => m.avg_price_usd), 1);
  const maxVol = Math.max(...top.map((m) => m.total_volume_qty), 1);
  const maxVolatility = Math.max(...top.map((m) => m.volatility_usd), 1);
  const maxTrades = Math.max(...top.map((m) => m.trade_count), 1);
  const maxChange = Math.max(...top.map((m) => Math.abs(m.price_change_pct)), 1);

  const datasets = top.map((m, i) => ({
    label: m.product_id,
    data: [
      (m.avg_price_usd / maxPrice) * 100,
      (m.total_volume_qty / maxVol) * 100,
      (m.volatility_usd / maxVolatility) * 100,
      (m.trade_count / maxTrades) * 100,
      (Math.abs(m.price_change_pct) / maxChange) * 100,
    ],
    borderColor: CHART_COLORS[i % CHART_COLORS.length],
    backgroundColor: CHART_COLORS[i % CHART_COLORS.length] + '22',
    pointBackgroundColor: CHART_COLORS[i % CHART_COLORS.length],
    pointBorderColor: 'transparent',
    pointHoverBackgroundColor: '#fff',
    borderWidth: 2,
  }));

  if (!radarChart) {
    radarChart = new Chart(ctx, {
      type: 'radar',
      data: {
        labels: ['Price', 'Volume', 'Volatility', 'Trades', '|Change|'],
        datasets,
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: { legend: { labels: { color: tc.text, usePointStyle: true, padding: 12 } } },
        scales: {
          r: { ticks: { color: tc.text, backdropColor: 'transparent' }, grid: { color: tc.grid }, pointLabels: { color: tc.text, font: { weight: 600 } } },
        },
      },
    });
  } else {
    radarChart.data.datasets = datasets;
  }
  radarChart.update();
}

function updateBubbleChart(metrics) {
  const ctx = el('bubbleChart')?.getContext('2d');
  const tc = getThemeColors();
  if (!metrics?.length || !ctx) {
    if (bubbleChart) { bubbleChart.data.datasets = []; bubbleChart.update(); }
    return;
  }
  const maxVolatility = Math.max(...metrics.map((m) => m.volatility_usd), 1);
  const data = metrics.map((m) => ({
    x: m.avg_price_usd,
    y: m.total_volume_qty,
    r: Math.max(8, Math.min(35, (m.volatility_usd / maxVolatility) * 30)),
    product_id: m.product_id,
  }));

  if (!bubbleChart) {
    bubbleChart = new Chart(ctx, {
      type: 'bubble',
      data: {
        datasets: [{
          label: 'Symbols',
          data,
          backgroundColor: metrics.map((_, i) => CHART_COLORS[i % CHART_COLORS.length] + '88'),
          borderColor: metrics.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]),
          borderWidth: 2,
        }],
      },
      options: {
        onClick: (evt, elements) => {
          if (elements.length && data[elements[0].index]?.product_id) {
            openModal(data[elements[0].index].product_id);
          }
        },
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (c) => {
                const d = c.raw;
                const m = metrics.find((x) => x.product_id === d.product_id);
                return d.product_id ? [`${d.product_id}`, `Price: $${d.x.toLocaleString()}`, `Vol: ${fmtVol(d.y)}`, `Volatility: $${m?.volatility_usd?.toFixed(2) ?? '-'}`] : [];
              },
            },
            backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8,
          },
        },
        scales: {
          x: { title: { display: true, text: 'Avg Price (USD)', color: tc.text }, ticks: { color: tc.text }, grid: { color: tc.grid } },
          y: { title: { display: true, text: 'Volume', color: tc.text }, ticks: { color: tc.text }, grid: { color: tc.grid } },
        },
      },
    });
  } else {
    bubbleChart.data.datasets[0].data = data;
    bubbleChart.data.datasets[0].backgroundColor = metrics.map((_, i) => CHART_COLORS[i % CHART_COLORS.length] + '88');
    bubbleChart.data.datasets[0].borderColor = metrics.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]);
  }
  bubbleChart.update();
}

function updateStackedVolumeChart(volumeByExchangeTs) {
  const ctx = el('stackedVolumeChart')?.getContext('2d');
  const tc = getThemeColors();
  const section = el('exchangeVolumeSection');
  if (!volumeByExchangeTs?.length || !ctx) {
    if (section) section.style.display = 'none';
    return;
  }
  const byTime = {};
  const exchanges = new Set();
  volumeByExchangeTs.forEach((r) => {
    exchanges.add(r.exchange);
    (byTime[r.event_time] ??= {})[r.exchange] = r.volume;
  });
  const labels = Object.keys(byTime).sort();
  const exchangesList = [...exchanges].sort();
  if (exchangesList.length < 2) {
    if (section) section.style.display = 'none';
    return;
  }
  if (section) section.style.display = 'block';
  const datasets = exchangesList.map((ex, i) => ({
    label: ex,
    data: labels.map((t) => byTime[t]?.[ex] ?? 0),
    backgroundColor: CHART_COLORS[i % CHART_COLORS.length] + '66',
    borderColor: CHART_COLORS[i % CHART_COLORS.length],
    fill: true,
    tension: 0.4,
    pointRadius: 0,
    borderWidth: 2,
  }));

  if (!stackedVolumeChart) {
    stackedVolumeChart = new Chart(ctx, {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { labels: { color: tc.text, usePointStyle: true, padding: 16 } },
          tooltip: { backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8 },
        },
        scales: {
          x: { stacked: true, ticks: { color: tc.text, maxTicksLimit: 8 }, grid: { color: tc.grid } },
          y: { stacked: true, ticks: { color: tc.text }, grid: { color: tc.grid } },
        },
      },
    });
  } else {
    stackedVolumeChart.data.labels = labels;
    stackedVolumeChart.data.datasets = datasets;
  }
  stackedVolumeChart.update();
}

function updateDonutChart(metrics) {
  const ctx = el('donutChart').getContext('2d');
  const tc = getThemeColors();
  if (!metrics?.length) { if (donutChart) { donutChart.data.datasets = []; donutChart.update(); } return; }
  const labels = metrics.map((m) => m.product_id);
  const volumes = metrics.map((m) => m.total_volume_qty);
  const colors = metrics.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]);
  const borderCol = document.documentElement.getAttribute('data-theme') === 'light' ? '#ffffff' : '#111827';

  if (!donutChart) {
    donutChart = new Chart(ctx, {
      type: 'doughnut',
      data: { labels, datasets: [{ data: volumes, backgroundColor: colors, borderColor: borderCol, borderWidth: 2, hoverOffset: 8 }] },
      options: {
        responsive: true, maintainAspectRatio: true, cutout: '65%',
        plugins: {
          legend: { position: 'right', labels: { color: tc.text, boxWidth: 14, padding: 12, font: { size: 11 }, usePointStyle: true, pointStyle: 'circle' } },
          tooltip: {
            callbacks: { label: (c) => { const t = c.dataset.data.reduce((a, b) => a + b, 0); return ` ${c.label}: ${fmtVol(c.parsed)} (${t > 0 ? ((c.parsed / t) * 100).toFixed(1) : 0}%)`; } },
            backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8,
          },
        },
      },
    });
  } else {
    donutChart.data.labels = labels;
    donutChart.data.datasets[0].data = volumes;
    donutChart.data.datasets[0].backgroundColor = colors;
    donutChart.data.datasets[0].borderColor = borderCol;
  }
  donutChart.update();
}

function getProductsFromTimeseries(ts) {
  return ts?.length ? [...new Set(ts.map((r) => r.product_id))].sort() : [];
}

function pivotTimeseries(timeseries, selectedSymbols = null) {
  if (!timeseries?.length) return { labels: [], datasets: [] };
  const byTime = {}, products = new Set();
  timeseries.forEach((row) => { products.add(row.product_id); (byTime[row.event_time] ??= {})[row.product_id] = row.avg_price_usd; });
  const labels = Object.keys(byTime).sort();
  let show = Array.from(products);
  if (selectedSymbols) show = show.filter((p) => selectedSymbols.has(p));
  return { labels, datasets: show.map((p, i) => ({
    label: p, data: labels.map((t) => byTime[t]?.[p] ?? null),
    borderColor: CHART_COLORS[i % CHART_COLORS.length], backgroundColor: 'transparent',
    tension: 0.4, pointRadius: 0, pointHitRadius: 6, fill: true, borderWidth: 2,
  }))};
}

function renderSymbolFilters(products) {
  const container = el('symbolChecks');
  if (!container || !products.length) return;
  const allSelected = lineChartSelectedSymbols === null;
  container.innerHTML = products.map((p) =>
    `<label><input type="checkbox" class="symbol-check" data-symbol="${p}" ${allSelected || lineChartSelectedSymbols?.has(p) ? 'checked' : ''}>${p}</label>`
  ).join('');
  container.querySelectorAll('.symbol-check').forEach((cb) => {
    cb.addEventListener('change', () => {
      if (lineChartSelectedSymbols === null) lineChartSelectedSymbols = new Set(products);
      cb.checked ? lineChartSelectedSymbols.add(cb.dataset.symbol) : lineChartSelectedSymbols.delete(cb.dataset.symbol);
      updateLineChart(timeseriesCache);
    });
  });
}

const gradientFillPlugin = {
  id: 'gradientFill',
  beforeDatasetsDraw(chart) {
    const { ctx, chartArea } = chart;
    if (!chartArea) return;
    chart.data.datasets.forEach((ds, i) => {
      if (ds.fill && ds.borderColor && ds.borderColor !== 'transparent') {
        const meta = chart.getDatasetMeta(i);
        if (!meta.visible || meta.hidden) return;
        const grad = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
        const rgb = hexToRgb(ds.borderColor);
        grad.addColorStop(0, `rgba(${rgb}, 0.15)`);
        grad.addColorStop(1, `rgba(${rgb}, 0.0)`);
        ds.backgroundColor = grad;
      }
    });
  },
};

function updateLineChart(timeseries) {
  timeseriesCache = timeseries || [];
  const products = getProductsFromTimeseries(timeseriesCache);
  const filtersEl = el('symbolChecks');
  if (filtersEl && products.length && !filtersEl.innerHTML) renderSymbolFilters(products);
  const { labels, datasets } = pivotTimeseries(timeseriesCache, lineChartSelectedSymbols);
  const ctx = el('lineChart')?.getContext('2d');
  const tc = getThemeColors();
  if (!ctx) return;
  if (!labels.length || !datasets.length) {
    if (lineChart) { lineChart.data.labels = []; lineChart.data.datasets = []; lineChart.update(); }
    return;
  }
  if (!lineChart) {
    lineChart = new Chart(ctx, {
      type: 'line', data: { labels, datasets }, plugins: [gradientFillPlugin],
      options: {
        responsive: true, maintainAspectRatio: true,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { labels: { color: tc.text, usePointStyle: true, padding: 12 } },
          tooltip: { mode: 'index', intersect: false, backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8 },
        },
        scales: {
          x: { ticks: { color: tc.text, maxTicksLimit: 10 }, grid: { color: tc.grid } },
          y: { ticks: { color: tc.text }, grid: { color: tc.grid } },
        },
      },
    });
  } else {
    lineChart.data.labels = labels;
    lineChart.data.datasets = datasets;
  }
  lineChart.update();
}

// ─── Candlestick drawing (custom Chart.js plugin) ───────────────────

const candlestickPlugin = {
  id: 'candlestick',
  afterDatasetsDraw(chart) {
    const meta = chart.getDatasetMeta(0);
    if (!meta?.data?.length || !chart.data.datasets[0]?._candles) return;
    const { ctx, chartArea, scales: { x, y } } = chart;
    const candles = chart.data.datasets[0]._candles;
    const barWidth = Math.max(4, Math.min(16, (chartArea.width / candles.length) * 0.6));

    candles.forEach((c, i) => {
      const cx = x.getPixelForValue(i);
      const oY = y.getPixelForValue(c.open);
      const cY = y.getPixelForValue(c.close);
      const hY = y.getPixelForValue(c.high);
      const lY = y.getPixelForValue(c.low);
      const bullish = c.close >= c.open;
      const color = bullish ? '#22c55e' : '#ef4444';

      ctx.beginPath();
      ctx.moveTo(cx, hY);
      ctx.lineTo(cx, lY);
      ctx.strokeStyle = color;
      ctx.lineWidth = 1;
      ctx.stroke();

      const top = Math.min(oY, cY);
      const bodyH = Math.max(1, Math.abs(oY - cY));
      ctx.fillStyle = color;
      ctx.beginPath();
      const r = Math.min(2, barWidth / 4);
      const bx = cx - barWidth / 2;
      ctx.moveTo(bx + r, top);
      ctx.lineTo(bx + barWidth - r, top);
      ctx.quadraticCurveTo(bx + barWidth, top, bx + barWidth, top + r);
      ctx.lineTo(bx + barWidth, top + bodyH - r);
      ctx.quadraticCurveTo(bx + barWidth, top + bodyH, bx + barWidth - r, top + bodyH);
      ctx.lineTo(bx + r, top + bodyH);
      ctx.quadraticCurveTo(bx, top + bodyH, bx, top + bodyH - r);
      ctx.lineTo(bx, top + r);
      ctx.quadraticCurveTo(bx, top, bx + r, top);
      ctx.closePath();
      ctx.fill();
    });
  },
};

async function drawCandlestickChart(symbol, color) {
  const candles = await fetchCandles(symbol);
  if (modalChart) modalChart.destroy();
  const ctx = el('modalChart').getContext('2d');
  const tc = getThemeColors();

  if (!candles.length) {
    modalChart = new Chart(ctx, {
      type: 'bar', data: { labels: ['No data'], datasets: [{ data: [0] }] },
      options: { responsive: true, plugins: { legend: { display: false } } },
    });
    return;
  }

  const labels = candles.map((c) => c.event_time.split('T')[1] || c.event_time);
  const midPoints = candles.map((c) => (c.open + c.close) / 2);
  const allPrices = candles.flatMap((c) => [c.high, c.low]);
  const yMin = Math.min(...allPrices);
  const yMax = Math.max(...allPrices);
  const yPad = (yMax - yMin) * 0.1 || 1;

  modalChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: `${symbol} OHLC`,
        data: midPoints,
        borderColor: 'transparent',
        backgroundColor: 'transparent',
        pointRadius: 0,
        _candles: candles,
      }],
    },
    plugins: [candlestickPlugin],
    options: {
      responsive: true, maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (c) => {
              const cd = candles[c.dataIndex];
              if (!cd) return '';
              return [`O: $${cd.open}  H: $${cd.high}`, `L: $${cd.low}  C: $${cd.close}`, `Vol: ${fmtVol(cd.volume)}`];
            },
          },
          backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8,
        },
      },
      scales: {
        x: { ticks: { color: tc.text, maxTicksLimit: 8 }, grid: { color: tc.grid } },
        y: { min: yMin - yPad, max: yMax + yPad, ticks: { color: tc.text }, grid: { color: tc.grid } },
      },
    },
  });
}

function drawLineModalChart(symbol, color) {
  if (modalChart) modalChart.destroy();
  const ctx = el('modalChart').getContext('2d');
  const tc = getThemeColors();
  const symbolTs = timeseriesCache.filter((r) => r.product_id === symbol);
  const labels = symbolTs.map((r) => r.event_time.split('T')[1] || r.event_time);
  const prices = symbolTs.map((r) => r.avg_price_usd);

  modalChart = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets: [{ label: `${symbol} Price`, data: prices, borderColor: color, backgroundColor: 'transparent', tension: 0.4, pointRadius: 2, pointHitRadius: 8, fill: true, borderWidth: 2 }] },
    plugins: [gradientFillPlugin],
    options: {
      responsive: true, maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        tooltip: { backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1, cornerRadius: 8 },
      },
      scales: {
        x: { ticks: { color: tc.text, maxTicksLimit: 8 }, grid: { color: tc.grid } },
        y: { ticks: { color: tc.text }, grid: { color: tc.grid } },
      },
    },
  });
}

// ─── Symbol detail modal ────────────────────────────────────────────

async function openModal(symbol) {
  currentModalSymbol = symbol;
  el('modalTitle').textContent = symbol;

  const m = metricsCache.find((r) => r.product_id === symbol);
  const statsEl = el('modalStats');
  if (m) {
    const cls = changeClass(m.price_change_pct);
    const arrow = changeArrow(m.price_change_pct);
    statsEl.innerHTML = [
      ['Avg Price', `$${m.avg_price_usd.toFixed(2)}`],
      ['VWAP', `$${m.vwap_usd.toFixed(2)}`],
      ['Change', `<span class="${cls}">${arrow} ${Math.abs(m.price_change_pct).toFixed(2)}%</span>`],
      ['Trades', fmt(m.trade_count)],
      ['Volume', fmtVol(m.total_volume_qty)],
      ['Volatility', `$${m.volatility_usd.toFixed(2)}`],
    ].map(([l, v]) => `<div class="modal-stat"><div class="modal-stat-label">${l}</div><div class="modal-stat-value">${v}</div></div>`).join('');
  }

  const color = CHART_COLORS[metricsCache.findIndex((r) => r.product_id === symbol) % CHART_COLORS.length] || '#3b82f6';

  document.querySelectorAll('.modal-chart-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.chartType === currentModalChartType);
  });

  if (currentModalChartType === 'candlestick') {
    await drawCandlestickChart(symbol, color);
  } else {
    drawLineModalChart(symbol, color);
  }

  el('modalOverlay').classList.add('open');
}

function closeModal() {
  el('modalOverlay').classList.remove('open');
  currentModalSymbol = null;
  if (modalChart) { modalChart.destroy(); modalChart = null; }
}

el('modalClose').addEventListener('click', closeModal);
el('modalOverlay').addEventListener('click', (e) => { if (e.target === el('modalOverlay')) closeModal(); });

document.querySelectorAll('.modal-chart-btn').forEach((btn) => {
  btn.addEventListener('click', () => {
    currentModalChartType = btn.dataset.chartType;
    document.querySelectorAll('.modal-chart-btn').forEach((b) => b.classList.toggle('active', b === btn));
    if (currentModalSymbol) openModal(currentModalSymbol);
  });
});

// ─── Fullscreen Chart ───────────────────────────────────────────────

let fullscreenChart = null;

function openFullscreen(chartId) {
  const overlay = el('fullscreenOverlay');
  const container = document.querySelector(`[data-chart-id="${chartId}"]`);
  if (!container || !overlay) return;

  const title = container.querySelector('h3')?.textContent || 'Chart';
  el('fullscreenTitle').textContent = title;

  const sourceCanvas = container.querySelector('canvas');
  if (!sourceCanvas) return;

  overlay.classList.add('open');

  requestAnimationFrame(() => {
    const fsCanvas = el('fullscreenCanvas');
    const fsCtx = fsCanvas.getContext('2d');
    const chartInstance = Chart.getChart(sourceCanvas);
    if (!chartInstance) return;

    const cfg = JSON.parse(JSON.stringify(chartInstance.config));
    cfg.options.maintainAspectRatio = false;
    cfg.options.responsive = true;

    if (chartInstance.config._config?.plugins) {
      cfg.plugins = chartInstance.config._config.plugins;
    }

    fullscreenChart = new Chart(fsCtx, cfg);
  });
}

function closeFullscreen() {
  const overlay = el('fullscreenOverlay');
  overlay.classList.remove('open');
  if (fullscreenChart) { fullscreenChart.destroy(); fullscreenChart = null; }
}

el('fullscreenClose')?.addEventListener('click', closeFullscreen);

document.querySelectorAll('.fullscreen-btn').forEach((btn) => {
  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    const container = btn.closest('.chart-container');
    const chartId = container?.dataset.chartId;
    if (chartId) openFullscreen(chartId);
  });
});

// ─── Direction Filter ───────────────────────────────────────────────

document.querySelectorAll('.direction-btn').forEach((btn) => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.direction-btn').forEach((b) => b.classList.remove('active'));
    btn.classList.add('active');
    directionFilter = btn.dataset.dir;
    renderMetricsTable(metricsCache);
  });
});

// ─── Keyboard Shortcuts ─────────────────────────────────────────────

function toggleShortcutsModal() {
  const modal = el('shortcutsModal');
  modal.classList.toggle('open');
}

el('shortcutsBtn')?.addEventListener('click', toggleShortcutsModal);
el('shortcutsModal')?.addEventListener('click', (e) => {
  if (e.target === el('shortcutsModal')) toggleShortcutsModal();
});

const windowBtnMap = { '1': 0, '2': 1, '3': 2, '4': 3 };

document.addEventListener('keydown', (e) => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' || e.target.tagName === 'TEXTAREA') return;

  if (e.key === 'Escape') {
    const pd = el('profileDropdown');
    if (pd && pd.classList.contains('open')) { pd.classList.remove('open'); el('profileBtn')?.classList.remove('active'); return; }
    if (el('shortcutsModal').classList.contains('open')) { toggleShortcutsModal(); return; }
    if (el('fullscreenOverlay').classList.contains('open')) { closeFullscreen(); return; }
    if (aiChatOpen) { toggleAiChat(); return; }
    closeModal();
    return;
  }

  if (e.key === '?' || (e.shiftKey && e.key === '/')) { toggleShortcutsModal(); return; }
  if (e.key === '/') { e.preventDefault(); symbolSearchEl?.focus(); return; }
  if (e.key.toLowerCase() === 'l') { liveToggleEl.checked = !liveToggleEl.checked; liveToggleEl.checked ? startPolling() : stopPolling(); return; }
  if (e.key.toLowerCase() === 't') { el('themeToggle').click(); return; }
  if (e.key.toLowerCase() === 'n') { el('notifToggle').click(); return; }
  if (e.key.toLowerCase() === 'e') { exportCsv(); return; }
  if (e.key.toLowerCase() === 'a') { toggleAiChat(); return; }

  if (windowBtnMap[e.key] !== undefined) {
    const btns = document.querySelectorAll('.window-btn');
    const idx = windowBtnMap[e.key];
    if (btns[idx]) btns[idx].click();
  }
});

// ─── Scroll to Top ──────────────────────────────────────────────────

const scrollTopBtn = el('scrollTopBtn');

window.addEventListener('scroll', () => {
  if (window.scrollY > 400) scrollTopBtn.classList.add('visible');
  else scrollTopBtn.classList.remove('visible');
}, { passive: true });

scrollTopBtn?.addEventListener('click', () => {
  window.scrollTo({ top: 0, behavior: 'smooth' });
});

// ─── Window selector ────────────────────────────────────────────────

document.querySelectorAll('.window-btn').forEach((btn) => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.window-btn').forEach((b) => b.classList.remove('active'));
    btn.classList.add('active');
    windowMinutes = parseInt(btn.dataset.window, 10);
    refresh();
  });
});

// ─── Main loop ──────────────────────────────────────────────────────

async function refresh() {
  if (!liveToggleEl.checked) return;
  const data = await fetchDashboard();
  if (!data) {
    kafkaStatusEl.querySelector('.value').textContent = 'API error';
    setStatusClass(kafkaStatusEl, 'error');
    return;
  }
  document.body.classList.remove('loading');
  sparklinesCache = data.sparklines || {};
  renderStatus(data);
  renderExchangeBar(data.exchange_stats);
  renderKPIs(data);
  renderMarketInsights(data.metrics);
  renderWatchlist(data.metrics);
  renderTopMovers(data.metrics);
  renderMetricsTable(data.metrics);
  updateSortIndicator();
  renderAlerts(data.alerts);
  renderPriceAlerts(data.price_alerts);
  renderAnomalyAlerts(data.anomalies);
  renderArbitrage(data.arbitrage);
  renderNewsSentiment(data);
  renderTicker(data.recent_trades);
  updateBarChart(data.metrics);
  updateDonutChart(data.metrics);
  updateVolatilityChart(data.metrics);
  updateVolumeChart(data.volume_timeseries);
  updateExchangeChart(data.exchange_metrics);
  updateLineChart(data.timeseries);
  updateHeatmap(data.heatmap_data);
  updateRadarChart(data.metrics);
  updateBubbleChart(data.metrics);
  updateStackedVolumeChart(data.volume_by_exchange_ts);
}

function startPolling() {
  if (refreshTimer) clearInterval(refreshTimer);
  const ms = parseInt(refreshRateEl.value, 10) * 1000;
  refresh();
  refreshTimer = setInterval(refresh, ms);
}

function stopPolling() {
  if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = null; }
}

liveToggleEl.addEventListener('change', () => { liveToggleEl.checked ? startPolling() : stopPolling(); });
refreshRateEl.addEventListener('change', () => { if (liveToggleEl.checked) startPolling(); });
symbolSearchEl?.addEventListener('input', () => renderMetricsTable(metricsCache));

document.querySelectorAll('.metrics-table th.sortable').forEach((th) => {
  th.addEventListener('click', () => {
    const key = th.dataset.sort;
    if (key === tableSortKey) tableSortDir *= -1;
    else { tableSortKey = key; tableSortDir = 1; }
    renderMetricsTable(metricsCache);
    updateSortIndicator();
  });
});

el('selectAllSymbols')?.addEventListener('click', () => {
  lineChartSelectedSymbols = null;
  const p = getProductsFromTimeseries(timeseriesCache);
  if (p.length) { renderSymbolFilters(p); updateLineChart(timeseriesCache); }
});

el('deselectAllSymbols')?.addEventListener('click', () => {
  lineChartSelectedSymbols = new Set();
  renderSymbolFilters(getProductsFromTimeseries(timeseriesCache));
  updateLineChart(timeseriesCache);
});

exchangeFilterEl?.addEventListener('change', () => {
  exchangeFilter = exchangeFilterEl.value || '';
  refresh();
});

// Collapsible chart sections
const collapsedSections = JSON.parse(localStorage.getItem('collapsedCharts') || '[]');
collapsedSections.forEach((id) => {
  const section = document.querySelector(`.collapsible-section[data-section="${id}"]`);
  if (section) {
    section.classList.add('collapsed');
    section.querySelector('.collapsible-trigger')?.setAttribute('aria-expanded', 'false');
  }
});

document.querySelectorAll('.collapsible-trigger').forEach((btn) => {
  btn.addEventListener('click', () => {
    const section = btn.closest('.collapsible-section');
    const expanded = !section.classList.toggle('collapsed');
    btn.setAttribute('aria-expanded', expanded);
    const ids = Array.from(document.querySelectorAll('.collapsible-section.collapsed'))
      .map((s) => s.dataset.section)
      .filter(Boolean);
    localStorage.setItem('collapsedCharts', JSON.stringify(ids));
  });
});

startPolling();

// ─── AI Chat Assistant ──────────────────────────────────────────────

const aiChatFab = el('aiChatFab');
const aiChatPanel = el('aiChatPanel');
const aiChatClose = el('aiChatClose');
const aiChatInput = el('aiChatInput');
const aiChatSend = el('aiChatSend');
const aiChatMessages = el('aiChatMessages');
const aiClearBtn = el('aiClearBtn');
const aiStatusEl = el('aiStatus');
const aiInsightBody = el('aiInsightBody');
const aiInsightTime = el('aiInsightTime');
const aiFabBadge = el('aiFabBadge');

let aiChatOpen = false;
let aiHistory = [];
let aiStreaming = false;
let aiEnabled = false;

function toggleAiChat() {
  aiChatOpen = !aiChatOpen;
  aiChatPanel.classList.toggle('open', aiChatOpen);
  aiChatFab.classList.toggle('open', aiChatOpen);
  if (aiChatOpen) {
    aiChatInput.focus();
    checkAiHealth();
    fetchAiInsight();
  }
}

aiChatFab?.addEventListener('click', toggleAiChat);
aiChatClose?.addEventListener('click', toggleAiChat);

function checkAiHealth() {
  fetch(`${API_BASE}/api/ai/health`)
    .then((r) => r.json())
    .then((data) => {
      aiEnabled = data.enabled !== false;
      if (!aiEnabled) {
        aiStatusEl.textContent = 'AI disabled';
        return;
      }
      if (data.status === 'ok' && data.model_ready) {
        aiStatusEl.textContent = 'Online';
        aiFabBadge.style.display = 'block';
      } else if (data.status === 'ok') {
        aiStatusEl.textContent = 'Model loading...';
      } else {
        aiStatusEl.textContent = 'Connecting...';
      }
    })
    .catch(() => {
      aiStatusEl.textContent = 'Offline';
      aiFabBadge.style.display = 'none';
    });
}

function fetchAiInsight() {
  fetch(`${API_BASE}/api/ai/insights`)
    .then((r) => r.json())
    .then((data) => {
      if (!data.enabled) return;
      if (data.insight) {
        aiInsightBody.textContent = data.insight;
        aiInsightTime.textContent = data.updated_at || '';
      } else if (data.error) {
        aiInsightBody.textContent = `Error: ${data.error}`;
      }
    })
    .catch(() => {});
}

function renderAiMarkdown(text) {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\n/g, '<br>');
}

function addAiMessage(role, content) {
  const welcome = aiChatMessages.querySelector('.ai-chat-welcome');
  if (welcome) welcome.remove();

  const div = document.createElement('div');
  div.className = `ai-msg ${role}`;
  div.innerHTML = renderAiMarkdown(content);
  aiChatMessages.appendChild(div);
  aiChatMessages.scrollTop = aiChatMessages.scrollHeight;
  return div;
}

function showTypingIndicator() {
  const div = document.createElement('div');
  div.className = 'ai-typing';
  div.id = 'aiTyping';
  div.innerHTML = '<span class="ai-typing-dot"></span><span class="ai-typing-dot"></span><span class="ai-typing-dot"></span>';
  aiChatMessages.appendChild(div);
  aiChatMessages.scrollTop = aiChatMessages.scrollHeight;
}

function removeTypingIndicator() {
  const t = el('aiTyping');
  if (t) t.remove();
}

async function sendAiMessage(text) {
  if (!text?.trim() || aiStreaming) return;
  const message = text.trim();

  addAiMessage('user', message);
  aiHistory.push({ role: 'user', content: message });

  aiChatInput.value = '';
  aiChatSend.disabled = true;
  aiStreaming = true;
  showTypingIndicator();

  try {
    const resp = await fetch(`${API_BASE}/api/ai/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message,
        history: aiHistory.slice(-20),
        stream: true,
        window: windowMinutes,
      }),
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ error: resp.statusText }));
      throw new Error(err.error || resp.statusText);
    }

    removeTypingIndicator();
    const assistantDiv = addAiMessage('assistant', '');
    let fullContent = '';

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let sseBuffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      sseBuffer += decoder.decode(value, { stream: true });
      const lines = sseBuffer.split('\n');
      sseBuffer = lines.pop() || '';

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data: ')) continue;
        const payload = trimmed.slice(6);
        if (payload === '[DONE]') continue;
        try {
          const chunk = JSON.parse(payload);
          if (chunk.content) {
            fullContent += chunk.content;
            assistantDiv.innerHTML = renderAiMarkdown(fullContent);
            aiChatMessages.scrollTop = aiChatMessages.scrollHeight;
          }
        } catch (_) {}
      }
    }

    aiHistory.push({ role: 'assistant', content: fullContent });
  } catch (err) {
    removeTypingIndicator();
    addAiMessage('error', `Failed: ${err.message}`);
  } finally {
    aiStreaming = false;
    aiChatSend.disabled = false;
    aiChatInput.focus();
  }
}

aiChatSend?.addEventListener('click', () => sendAiMessage(aiChatInput.value));
aiChatInput?.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    sendAiMessage(aiChatInput.value);
  }
});

aiClearBtn?.addEventListener('click', () => {
  aiHistory = [];
  aiChatMessages.innerHTML = `
    <div class="ai-chat-welcome">
      <div class="ai-welcome-icon">AI</div>
      <p>Ask me about the live crypto market data.</p>
      <div class="ai-suggestions">
        <button class="ai-suggestion" data-q="What's the market sentiment right now?">Market sentiment?</button>
        <button class="ai-suggestion" data-q="Which coin has the highest volatility?">Highest volatility?</button>
        <button class="ai-suggestion" data-q="Are there any arbitrage opportunities?">Arbitrage opps?</button>
        <button class="ai-suggestion" data-q="Compare BTC price across exchanges">BTC cross-exchange</button>
      </div>
    </div>`;
  bindSuggestions();
});

function bindSuggestions() {
  document.querySelectorAll('.ai-suggestion').forEach((btn) => {
    btn.addEventListener('click', () => {
      const q = btn.dataset.q;
      if (q) sendAiMessage(q);
    });
  });
}

bindSuggestions();

setInterval(() => { if (aiChatOpen) fetchAiInsight(); }, 60000);

setTimeout(checkAiHealth, 3000);

/* ═══════════════════════════════════════════════════════════════════
   User Profile
   ═══════════════════════════════════════════════════════════════════ */

const PROFILE_STORAGE_KEY = 'cryptostream_profile';

const defaultProfile = {
  displayName: '',
  email: '',
  avatarColor: '#3b82f6',
  currency: 'USD',
  defaultExchange: '',
  joinedAt: new Date().toISOString(),
  sessionCount: 1,
};

function loadProfile() {
  try {
    const raw = localStorage.getItem(PROFILE_STORAGE_KEY);
    if (raw) {
      const p = JSON.parse(raw);
      p.sessionCount = (p.sessionCount || 0) + 1;
      localStorage.setItem(PROFILE_STORAGE_KEY, JSON.stringify(p));
      return p;
    }
  } catch (_) {}
  localStorage.setItem(PROFILE_STORAGE_KEY, JSON.stringify(defaultProfile));
  return { ...defaultProfile };
}

function saveProfile(profile) {
  localStorage.setItem(PROFILE_STORAGE_KEY, JSON.stringify(profile));
}

function getInitials(name) {
  if (!name || !name.trim()) return 'CS';
  const parts = name.trim().split(/\s+/);
  if (parts.length >= 2) return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
  return parts[0].substring(0, 2).toUpperCase();
}

function formatJoinDate(iso) {
  try {
    const d = new Date(iso);
    return 'Member since ' + d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
  } catch (_) {
    return 'Member';
  }
}

function applyProfileToUI(profile) {
  const initials = getInitials(profile.displayName);
  const initialsEl = el('profileInitials');
  const initialsLgEl = el('profileInitialsLg');
  const avatarBtn = el('profileBtn');
  const avatarLg = el('profileAvatarLg');

  if (initialsEl) initialsEl.textContent = initials;
  if (initialsLgEl) initialsLgEl.textContent = initials;

  if (avatarBtn) avatarBtn.style.background = profile.avatarColor || '#3b82f6';
  if (avatarLg) avatarLg.style.background = profile.avatarColor || '#3b82f6';

  const displayNameEl = el('profileDisplayName');
  if (displayNameEl) displayNameEl.textContent = profile.displayName || 'CryptoStream User';

  const joinedEl = el('profileJoined');
  if (joinedEl) joinedEl.textContent = formatJoinDate(profile.joinedAt);

  const nameInput = el('profileNameInput');
  if (nameInput) nameInput.value = profile.displayName || '';

  const emailInput = el('profileEmailInput');
  if (emailInput) emailInput.value = profile.email || '';

  const currencySelect = el('profileCurrency');
  if (currencySelect) currencySelect.value = profile.currency || 'USD';

  const exchSelect = el('profileDefaultExchange');
  if (exchSelect) exchSelect.value = profile.defaultExchange || '';

  document.querySelectorAll('.color-swatch').forEach((sw) => {
    sw.classList.toggle('active', sw.dataset.color === profile.avatarColor);
  });

  const watchlistCount = el('profileWatchlistCount');
  if (watchlistCount) watchlistCount.textContent = favorites.length;

  const sessionCount = el('profileSessionCount');
  if (sessionCount) sessionCount.textContent = profile.sessionCount || 1;

  const themeLabel = el('profileThemeLabel');
  if (themeLabel) {
    const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
    themeLabel.textContent = isDark ? 'Dark' : 'Light';
  }
}

const userProfile = loadProfile();

(function initProfile() {
  const profileBtn = el('profileBtn');
  const profileDropdown = el('profileDropdown');
  if (!profileBtn || !profileDropdown) return;

  applyProfileToUI(userProfile);

  profileBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    const isOpen = profileDropdown.classList.contains('open');
    profileDropdown.classList.toggle('open', !isOpen);
    profileBtn.classList.toggle('active', !isOpen);
    if (!isOpen) {
      const watchlistCount = el('profileWatchlistCount');
      if (watchlistCount) watchlistCount.textContent = favorites.length;
      const themeLabel = el('profileThemeLabel');
      if (themeLabel) {
        const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
        themeLabel.textContent = isDark ? 'Dark' : 'Light';
      }
    }
  });

  document.addEventListener('click', (e) => {
    if (!profileDropdown.contains(e.target) && e.target !== profileBtn) {
      profileDropdown.classList.remove('open');
      profileBtn.classList.remove('active');
    }
  });

  document.querySelectorAll('.color-swatch').forEach((sw) => {
    sw.addEventListener('click', () => {
      document.querySelectorAll('.color-swatch').forEach((s) => s.classList.remove('active'));
      sw.classList.add('active');
    });
  });

  const saveBtn = el('profileSaveBtn');
  if (saveBtn) {
    saveBtn.addEventListener('click', () => {
      const nameInput = el('profileNameInput');
      const emailInput = el('profileEmailInput');
      const currencySelect = el('profileCurrency');
      const exchSelect = el('profileDefaultExchange');
      const activeSwatch = document.querySelector('.color-swatch.active');

      userProfile.displayName = nameInput?.value.trim() || '';
      userProfile.email = emailInput?.value.trim() || '';
      userProfile.currency = currencySelect?.value || 'USD';
      userProfile.defaultExchange = exchSelect?.value || '';
      if (activeSwatch) userProfile.avatarColor = activeSwatch.dataset.color;

      saveProfile(userProfile);
      applyProfileToUI(userProfile);

      if (userProfile.defaultExchange && exchangeFilterEl) {
        exchangeFilterEl.value = userProfile.defaultExchange;
        exchangeFilter = userProfile.defaultExchange;
      }

      showToast('Profile saved');
    });
  }

  const resetBtn = el('profileResetBtn');
  if (resetBtn) {
    resetBtn.addEventListener('click', () => {
      Object.assign(userProfile, {
        displayName: '',
        email: '',
        avatarColor: '#3b82f6',
        currency: 'USD',
        defaultExchange: '',
      });
      saveProfile(userProfile);
      applyProfileToUI(userProfile);
      showToast('Profile reset');
    });
  }

  if (userProfile.defaultExchange && exchangeFilterEl) {
    exchangeFilterEl.value = userProfile.defaultExchange;
    exchangeFilter = userProfile.defaultExchange;
  }
})();
