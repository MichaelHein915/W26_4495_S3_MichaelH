const API_BASE = '';

let barChart = null;
let lineChart = null;
let refreshTimer = null;
let metricsCache = [];
let tableSortKey = 'product_id';
let tableSortDir = 1; // 1 = asc, -1 = desc
let timeseriesCache = [];
let lineChartSelectedSymbols = null; // null = all, Set = filter (empty Set = none)

const kafkaStatusEl = document.getElementById('kafkaStatus');
const freshnessStatusEl = document.getElementById('freshnessStatus');
const latencyStatusEl = document.getElementById('latencyStatus');
const totalTradesEl = document.getElementById('totalTrades');
const liveSymbolsEl = document.getElementById('liveSymbols');
const totalVolumeEl = document.getElementById('totalVolume');
const dataFreshnessEl = document.getElementById('dataFreshness');
const metricsBodyEl = document.getElementById('metricsBody');
const alertsEl = document.getElementById('alerts');
const alertsSectionEl = document.getElementById('alertsSection');
const updatedAtEl = document.getElementById('updatedAt');
const eventCountEl = document.getElementById('eventCount');
const windowMinutesEl = document.getElementById('windowMinutes');
const liveToggleEl = document.getElementById('liveToggle');
const refreshRateEl = document.getElementById('refreshRate');
const symbolSearchEl = document.getElementById('symbolSearch');

function setStatusClass(el, status) {
  el.classList.remove('ok', 'warn', 'error');
  if (status) el.classList.add(status);
}

function formatNumber(num) {
  return new Intl.NumberFormat().format(num);
}

function formatVolume(val) {
  return parseFloat(val).toFixed(4);
}

async function fetchDashboard() {
  try {
    const res = await fetch(`${API_BASE}/api/dashboard`);
    if (!res.ok) throw new Error(res.statusText);
    return await res.json();
  } catch (err) {
    console.error('Dashboard fetch failed:', err);
    return null;
  }
}

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
  setStatusClass(freshnessStatusEl, freshness != null && freshness <= 10 ? 'ok' : freshness != null && freshness <= 30 ? 'warn' : freshness != null ? 'error' : null);

  latencyStatusEl.querySelector('.value').textContent =
    latency != null ? `${latency.toFixed(2)}s` : '--';
  setStatusClass(latencyStatusEl, latency != null && latency <= 2 ? 'ok' : latency != null && latency <= 5 ? 'warn' : latency != null ? 'error' : null);
}

function renderKPIs(data) {
  const s = data?.status || {};
  totalTradesEl.textContent = formatNumber(s.total_trades ?? 0);
  liveSymbolsEl.textContent = formatNumber(s.live_symbols ?? 0);
  totalVolumeEl.textContent = formatVolume(s.total_volume ?? 0);
  dataFreshnessEl.textContent =
    s.freshness_seconds != null ? `${s.freshness_seconds.toFixed(1)}s` : 'N/A';
  updatedAtEl.textContent = s.updated_at ?? '--:--:-- UTC';
  eventCountEl.textContent = formatNumber(s.event_count ?? 0);
  windowMinutesEl.textContent = s.window_minutes ?? 3;
}

function getFilteredAndSortedMetrics(metrics) {
  if (!metrics?.length) return [];
  let out = [...metrics];
  const q = (symbolSearchEl?.value || '').trim().toUpperCase();
  if (q) out = out.filter((m) => m.product_id.toUpperCase().includes(q));
  out.sort((a, b) => {
    const va = a[tableSortKey];
    const vb = b[tableSortKey];
    const cmp = typeof va === 'string' ? va.localeCompare(vb) : (va ?? 0) - (vb ?? 0);
    return tableSortDir * cmp;
  });
  return out;
}

function renderMetricsTable(metrics) {
  metricsCache = metrics || [];
  const displayed = getFilteredAndSortedMetrics(metricsCache);
  if (!displayed.length) {
    metricsBodyEl.innerHTML = '<tr><td colspan="6">No data yet' +
      (symbolSearchEl?.value?.trim() ? ' (no match)' : '') + '</td></tr>';
    return;
  }
  metricsBodyEl.innerHTML = displayed
    .map(
      (m) =>
        `<tr>
          <td>${m.product_id}</td>
          <td>${formatNumber(m.trade_count)}</td>
          <td>${m.avg_price_usd.toFixed(2)}</td>
          <td>${m.vwap_usd.toFixed(2)}</td>
          <td>${m.volatility_usd.toFixed(2)}</td>
          <td>${formatVolume(m.total_volume_qty)}</td>
        </tr>`
    )
    .join('');
}

function updateSortIndicator() {
  document.querySelectorAll('.metrics-table th.sortable').forEach((th) => {
    const label = th.dataset.label || th.textContent;
    const arrow = th.dataset.sort === tableSortKey ? (tableSortDir === 1 ? ' ▽' : ' △') : '';
    th.textContent = label + arrow;
  });
}

function renderAlerts(alerts) {
  if (!alerts?.length) {
    alertsSectionEl.style.display = 'none';
    return;
  }
  alertsSectionEl.style.display = 'block';
  alertsEl.innerHTML = alerts
    .map(
      (a) =>
        `<div class="alert">${a.product_id}: volume ${a.current_volume.toFixed(4)} ` +
        `(baseline ${a.baseline_volume.toFixed(4)}, ${a.spike_ratio}x)</div>`
    )
    .join('');
}

function updateBarChart(metrics) {
  const ctx = document.getElementById('barChart').getContext('2d');
  if (!metrics?.length) {
    if (barChart) barChart.data.datasets = [];
    return;
  }
  const labels = metrics.map((m) => m.product_id);
  const prices = metrics.map((m) => m.avg_price_usd);
  const trades = metrics.map((m) => m.trade_count);

  if (!barChart) {
    barChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label: 'Avg Price (USD)', data: prices, backgroundColor: 'rgba(88, 166, 255, 0.6)', yAxisID: 'y' },
          { label: 'Trade Count', data: trades, backgroundColor: 'rgba(63, 185, 80, 0.6)', yAxisID: 'y1' },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: { legend: { labels: { color: '#8b949e' } } },
        scales: {
          x: { ticks: { color: '#8b949e' } },
          y: {
            type: 'linear',
            position: 'left',
            title: { display: true, text: 'Avg Price (USD)', color: '#58a6ff' },
            ticks: { color: '#8b949e' },
          },
          y1: {
            type: 'linear',
            position: 'right',
            title: { display: true, text: 'Trade Count', color: '#3fb950' },
            ticks: { color: '#8b949e' },
            grid: { drawOnChartArea: false },
          },
        },
      },
    });
  } else {
    barChart.data.labels = labels;
    barChart.data.datasets[0].data = prices;
    barChart.data.datasets[1].data = trades;
  }
  barChart.update();
}

function getProductsFromTimeseries(timeseries) {
  if (!timeseries?.length) return [];
  const products = new Set();
  timeseries.forEach((row) => products.add(row.product_id));
  return Array.from(products).sort();
}

function pivotTimeseries(timeseries, selectedSymbols = null) {
  if (!timeseries?.length) return { labels: [], datasets: [] };
  const byTime = {};
  const products = new Set();
  timeseries.forEach((row) => {
    products.add(row.product_id);
    if (!byTime[row.event_time]) byTime[row.event_time] = {};
    byTime[row.event_time][row.product_id] = row.avg_price_usd;
  });
  const labels = Object.keys(byTime).sort();
  const colors = [
    '#58a6ff', '#3fb950', '#d29922', '#f85149', '#db6d28',
    '#a371f7', '#79c0ff', '#7ee787',
  ];
  let productsToShow = Array.from(products);
  if (selectedSymbols !== null && selectedSymbols !== undefined) {
    productsToShow = productsToShow.filter((p) => selectedSymbols.has(p));
  }
  const datasets = productsToShow.map((p, i) => ({
    label: p,
    data: labels.map((t) => byTime[t]?.[p] ?? null),
    borderColor: colors[i % colors.length],
    backgroundColor: 'transparent',
    tension: 0.2,
  }));
  return { labels, datasets };
}

function renderSymbolFilters(products) {
  const container = document.getElementById('symbolChecks');
  if (!container || !products.length) return;
  const allSelected = lineChartSelectedSymbols === null;
  container.innerHTML = products
    .map(
      (p) =>
        `<label><input type="checkbox" class="symbol-check" data-symbol="${p}" ${allSelected || (lineChartSelectedSymbols && lineChartSelectedSymbols.has(p)) ? 'checked' : ''}>${p}</label>`
    )
    .join('');
  container.querySelectorAll('.symbol-check').forEach((cb) => {
    cb.addEventListener('change', () => {
      if (lineChartSelectedSymbols === null) lineChartSelectedSymbols = new Set(products);
      if (cb.checked) lineChartSelectedSymbols.add(cb.dataset.symbol);
      else lineChartSelectedSymbols.delete(cb.dataset.symbol);
      timeseriesCache.length && updateLineChart(timeseriesCache);
    });
  });
}

function updateLineChart(timeseries) {
  timeseriesCache = timeseries || [];
  const products = getProductsFromTimeseries(timeseriesCache);
  const filtersEl = document.getElementById('symbolChecks');
  if (filtersEl && products.length && !filtersEl.innerHTML) {
    renderSymbolFilters(products);
  }
  const selected = lineChartSelectedSymbols;
  const { labels, datasets } = pivotTimeseries(timeseriesCache, selected);
  const ctx = document.getElementById('lineChart')?.getContext('2d');
  if (!ctx) return;
  if (!labels.length || !datasets.length) {
    if (lineChart) {
      lineChart.data.labels = [];
      lineChart.data.datasets = [];
      lineChart.update();
    }
    return;
  }
  if (!lineChart) {
    lineChart = new Chart(ctx, {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        interaction: { mode: 'index', intersect: false },
        plugins: { legend: { labels: { color: '#8b949e' } } },
        scales: {
          x: { ticks: { color: '#8b949e' } },
          y: { ticks: { color: '#8b949e' } },
        },
      },
    });
  } else {
    lineChart.data.labels = labels;
    lineChart.data.datasets = datasets;
  }
  lineChart.update();
}

async function refresh() {
  if (!liveToggleEl.checked) return;
  const data = await fetchDashboard();
  if (!data) {
    kafkaStatusEl.querySelector('.value').textContent = 'API error';
    setStatusClass(kafkaStatusEl, 'error');
    return;
  }
  renderStatus(data);
  renderKPIs(data);
  renderMetricsTable(data.metrics);
  updateSortIndicator();
  renderAlerts(data.alerts);
  updateBarChart(data.metrics);
  updateLineChart(data.timeseries);
}

function startPolling() {
  if (refreshTimer) clearInterval(refreshTimer);
  const ms = parseInt(refreshRateEl.value, 10) * 1000;
  refresh();
  refreshTimer = setInterval(refresh, ms);
}

function stopPolling() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

liveToggleEl.addEventListener('change', () => {
  if (liveToggleEl.checked) startPolling();
  else stopPolling();
});

refreshRateEl.addEventListener('change', () => {
  if (liveToggleEl.checked) startPolling();
});

symbolSearchEl?.addEventListener('input', () => {
  renderMetricsTable(metricsCache);
});

document.querySelectorAll('.metrics-table th.sortable').forEach((th) => {
  th.addEventListener('click', () => {
    const key = th.dataset.sort;
    if (key === tableSortKey) tableSortDir *= -1;
    else { tableSortKey = key; tableSortDir = 1; }
    renderMetricsTable(metricsCache);
    updateSortIndicator();
  });
});

document.getElementById('selectAllSymbols')?.addEventListener('click', () => {
  lineChartSelectedSymbols = null;
  const products = getProductsFromTimeseries(timeseriesCache);
  if (products.length) {
    renderSymbolFilters(products);
    updateLineChart(timeseriesCache);
  }
});

document.getElementById('deselectAllSymbols')?.addEventListener('click', () => {
  lineChartSelectedSymbols = new Set();
  const products = getProductsFromTimeseries(timeseriesCache);
  renderSymbolFilters(products);
  updateLineChart(timeseriesCache);
});

startPolling();
