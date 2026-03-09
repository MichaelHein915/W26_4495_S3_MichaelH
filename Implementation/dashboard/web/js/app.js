const API_BASE = '';

let barChart = null;
let lineChart = null;
let donutChart = null;
let modalChart = null;
let volatilityChart = null;
let volumeChart = null;
let exchangeChart = null;
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

const CHART_COLORS = [
  '#58a6ff', '#3fb950', '#d29922', '#f85149', '#db6d28',
  '#a371f7', '#79c0ff', '#7ee787', '#f0883e', '#d2a8ff',
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
const toastContainerEl = el('toastContainer');
const tickerTrackEl = el('tickerTrack');
const tickerSectionEl = el('tickerSection');

function setStatusClass(elem, status) {
  elem.classList.remove('ok', 'warn', 'error');
  if (status) elem.classList.add(status);
}

function fmt(num) { return new Intl.NumberFormat().format(num); }
function fmtVol(val) { return parseFloat(val).toFixed(4); }

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

  [barChart, lineChart, donutChart, volatilityChart, volumeChart, exchangeChart].forEach((c) => {
    if (!c) return;
    const txtColor = theme === 'light' ? '#656d76' : '#8b949e';
    const gridColor = theme === 'light' ? 'rgba(0,0,0,0.08)' : 'rgba(48,54,61,0.3)';
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
  const w = canvas.width = 80;
  const h = canvas.height = 24;
  ctx.clearRect(0, 0, w, h);
  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const range = max - min || 1;
  const pad = 2;
  const up = prices[prices.length - 1] >= prices[0];
  const color = up ? '#3fb950' : '#f85149';
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
  grad.addColorStop(0, up ? 'rgba(63,185,80,0.25)' : 'rgba(248,81,73,0.25)');
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  ctx.fillStyle = grad;
  ctx.fill();
}

function getFilteredAndSortedMetrics(metrics) {
  if (!metrics?.length) return [];
  let out = [...metrics];
  const q = (symbolSearchEl?.value || '').trim().toUpperCase();
  if (q) out = out.filter((m) => m.product_id.toUpperCase().includes(q));
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
    metricsBodyEl.innerHTML = '<tr><td colspan="8">No data yet' + (symbolSearchEl?.value?.trim() ? ' (no match)' : '') + '</td></tr>';
    return;
  }
  metricsBodyEl.innerHTML = displayed.map((m) => {
    const cls = changeClass(m.price_change_pct);
    const arrow = changeArrow(m.price_change_pct);
    return `<tr data-symbol="${m.product_id}">
      <td>${m.product_id}</td>
      <td class="sparkline-cell"><canvas data-symbol="${m.product_id}"></canvas></td>
      <td class="${cls}">${arrow} ${Math.abs(m.price_change_pct).toFixed(2)}%</td>
      <td>${fmt(m.trade_count)}</td>
      <td>${m.avg_price_usd.toFixed(2)}</td>
      <td>${m.vwap_usd.toFixed(2)}</td>
      <td>${m.volatility_usd.toFixed(2)}</td>
      <td>${fmtVol(m.total_volume_qty)}</td>
    </tr>`;
  }).join('');

  requestAnimationFrame(() => {
    metricsBodyEl.querySelectorAll('.sparkline-cell canvas').forEach((cvs) => {
      drawSparkline(cvs, sparklinesCache[cvs.dataset.symbol]);
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
    `<div class="alert"><span class="alert-icon">⚡</span> ${a.product_id}: volume ${a.current_volume.toFixed(4)} ` +
    `(baseline ${a.baseline_volume.toFixed(4)}, ${a.spike_ratio}x)</div>`
  ).join('');
  notifyAlerts(alerts);
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
    text: light ? '#656d76' : '#8b949e',
    grid: light ? 'rgba(0,0,0,0.08)' : 'rgba(48,54,61,0.3)',
    gridStrong: light ? 'rgba(0,0,0,0.12)' : 'rgba(48,54,61,0.5)',
    tooltipBg: light ? 'rgba(255,255,255,0.95)' : 'rgba(22,27,34,0.95)',
    tooltipTitle: light ? '#1f2328' : '#e6edf3',
    tooltipBody: light ? '#656d76' : '#8b949e',
    tooltipBorder: light ? '#d0d7de' : '#30363d',
  };
}

function updateBarChart(metrics) {
  const ctx = el('barChart').getContext('2d');
  const tc = getThemeColors();
  if (!metrics?.length) { if (barChart) barChart.data.datasets = []; return; }
  const labels = metrics.map((m) => m.product_id);
  const prices = metrics.map((m) => m.avg_price_usd);
  const trades = metrics.map((m) => m.trade_count);
  const priceColors = metrics.map((m) => m.price_change_pct >= 0 ? 'rgba(88, 166, 255, 0.7)' : 'rgba(248, 81, 73, 0.7)');

  if (!barChart) {
    barChart = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets: [
        { label: 'Avg Price (USD)', data: prices, backgroundColor: priceColors, yAxisID: 'y', borderRadius: 4 },
        { label: 'Trade Count', data: trades, backgroundColor: 'rgba(63, 185, 80, 0.6)', yAxisID: 'y1', borderRadius: 4 },
      ]},
      options: {
        responsive: true, maintainAspectRatio: true,
        plugins: { legend: { labels: { color: tc.text } } },
        scales: {
          x: { ticks: { color: tc.text }, grid: { color: tc.gridStrong } },
          y: { type: 'linear', position: 'left', title: { display: true, text: 'Avg Price (USD)', color: tc.text }, ticks: { color: tc.text }, grid: { color: tc.grid } },
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
  if (!recentTrades?.length) {
    tickerSectionEl.style.display = 'none';
    return;
  }
  tickerSectionEl.style.display = 'block';
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
  const vols = metrics.map((m) => m.volatility_usd);
  const colors = metrics.map((m) => m.price_change_pct >= 0 ? 'rgba(63, 185, 80, 0.7)' : 'rgba(248, 81, 73, 0.7)');

  if (!volatilityChart) {
    volatilityChart = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets: [{ label: 'Volatility (USD)', data: vols, backgroundColor: colors, borderRadius: 4 }] },
      options: {
        indexAxis: 'y',
        responsive: true, maintainAspectRatio: true,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: tc.text }, grid: { color: tc.grid } },
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
          backgroundColor: 'rgba(210, 153, 34, 0.15)',
          fill: true, tension: 0.3, pointRadius: 0, pointHitRadius: 6,
        }],
      },
      plugins: [gradientFillPlugin],
      options: {
        responsive: true, maintainAspectRatio: true,
        plugins: { legend: { display: false } },
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
    data: symbols.map((s) => bySymbol[s]?.[ex] ?? null),
    backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
    borderRadius: 4,
  }));

  if (!exchangeChart) {
    exchangeChart = new Chart(ctx, {
      type: 'bar',
      data: { labels: symbols, datasets },
      options: {
        responsive: true, maintainAspectRatio: true,
        plugins: { legend: { labels: { color: tc.text } } },
        scales: {
          x: { ticks: { color: tc.text }, grid: { color: tc.gridStrong } },
          y: { ticks: { color: tc.text }, grid: { color: tc.grid } },
        },
      },
    });
  } else {
    exchangeChart.data.labels = symbols;
    exchangeChart.data.datasets = datasets;
  }
  exchangeChart.update();
}

function updateDonutChart(metrics) {
  const ctx = el('donutChart').getContext('2d');
  const tc = getThemeColors();
  if (!metrics?.length) { if (donutChart) { donutChart.data.datasets = []; donutChart.update(); } return; }
  const labels = metrics.map((m) => m.product_id);
  const volumes = metrics.map((m) => m.total_volume_qty);
  const colors = metrics.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]);
  const borderCol = document.documentElement.getAttribute('data-theme') === 'light' ? '#ffffff' : '#21262d';

  if (!donutChart) {
    donutChart = new Chart(ctx, {
      type: 'doughnut',
      data: { labels, datasets: [{ data: volumes, backgroundColor: colors, borderColor: borderCol, borderWidth: 2, hoverOffset: 6 }] },
      options: {
        responsive: true, maintainAspectRatio: true, cutout: '60%',
        plugins: {
          legend: { position: 'right', labels: { color: tc.text, boxWidth: 12, padding: 10, font: { size: 11 } } },
          tooltip: { callbacks: { label: (c) => { const t = c.dataset.data.reduce((a, b) => a + b, 0); return ` ${c.label}: ${fmtVol(c.parsed)} (${t > 0 ? ((c.parsed / t) * 100).toFixed(1) : 0}%)`; } } },
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
    tension: 0.3, pointRadius: 0, pointHitRadius: 6, fill: true,
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
        grad.addColorStop(0, `rgba(${rgb}, 0.18)`);
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
          legend: { labels: { color: tc.text } },
          tooltip: { mode: 'index', intersect: false, backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1 },
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
      const color = bullish ? '#3fb950' : '#f85149';

      ctx.beginPath();
      ctx.moveTo(cx, hY);
      ctx.lineTo(cx, lY);
      ctx.strokeStyle = color;
      ctx.lineWidth = 1;
      ctx.stroke();

      const top = Math.min(oY, cY);
      const bodyH = Math.max(1, Math.abs(oY - cY));
      ctx.fillStyle = color;
      ctx.fillRect(cx - barWidth / 2, top, barWidth, bodyH);
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
            label: (ctx) => {
              const c = candles[ctx.dataIndex];
              if (!c) return '';
              return [`O: $${c.open}  H: $${c.high}`, `L: $${c.low}  C: $${c.close}`, `Vol: ${fmtVol(c.volume)}`];
            },
          },
          backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1,
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
    data: { labels, datasets: [{ label: `${symbol} Price`, data: prices, borderColor: color, backgroundColor: 'transparent', tension: 0.3, pointRadius: 2, pointHitRadius: 8, fill: true }] },
    plugins: [gradientFillPlugin],
    options: {
      responsive: true, maintainAspectRatio: true,
      plugins: { legend: { display: false }, tooltip: { backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder, borderWidth: 1 } },
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

  const color = CHART_COLORS[metricsCache.findIndex((r) => r.product_id === symbol) % CHART_COLORS.length] || '#58a6ff';

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
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeModal(); });

document.querySelectorAll('.modal-chart-btn').forEach((btn) => {
  btn.addEventListener('click', () => {
    currentModalChartType = btn.dataset.chartType;
    document.querySelectorAll('.modal-chart-btn').forEach((b) => b.classList.toggle('active', b === btn));
    if (currentModalSymbol) openModal(currentModalSymbol);
  });
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
  sparklinesCache = data.sparklines || {};
  renderStatus(data);
  renderExchangeBar(data.exchange_stats);
  renderKPIs(data);
  renderTopMovers(data.metrics);
  renderMetricsTable(data.metrics);
  updateSortIndicator();
  renderAlerts(data.alerts);
  renderArbitrage(data.arbitrage);
  renderTicker(data.recent_trades);
  updateBarChart(data.metrics);
  updateDonutChart(data.metrics);
  updateVolatilityChart(data.metrics);
  updateVolumeChart(data.volume_timeseries);
  updateExchangeChart(data.exchange_metrics);
  updateLineChart(data.timeseries);
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

startPolling();
