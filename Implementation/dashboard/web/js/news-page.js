/* ═══════════════════════════════════════════════════════════════════
   News Page — fetch, filter, render
   ═══════════════════════════════════════════════════════════════════ */

let allArticles = [];
let sentimentData = {};
let sentimentTimeChart = null;
let activeFilter = 'all';
let activeCurrency = '';
let searchQuery = '';
let autoRefreshId = null;

const el = (id) => document.getElementById(id);

// ── Helpers ──────────────────────────────────────────────────────────

function timeAgo(iso) {
  if (!iso) return '';
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function formatTime(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
}

// ── API ──────────────────────────────────────────────────────────────

async function fetchNews() {
  try {
    const res = await fetch('/api/news?limit=200');
    const data = await res.json();
    allArticles = data.articles || [];
  } catch (e) {
    console.error('Failed to fetch news:', e);
  }
}

async function fetchSentiment() {
  try {
    const res = await fetch('/api/sentiment');
    sentimentData = await res.json();
  } catch (e) {
    console.error('Failed to fetch sentiment:', e);
  }
}

async function refreshAll() {
  await Promise.all([fetchNews(), fetchSentiment()]);
  renderSentimentOverview();
  renderSymbolChips();
  renderFeed();
  populateCurrencyFilter();
  el('lastUpdated').textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
}

// ── Sentiment Overview ───────────────────────────────────────────────

function renderSentimentOverview() {
  const summary = sentimentData.summary || {};
  const compound = summary.avg_compound ?? 0;
  const pct = ((compound + 1) / 2) * 100;

  const needle = el('sentimentGaugeNeedle');
  const score = el('sentimentScore');
  if (needle) needle.style.left = `${pct}%`;

  const label = summary.label || 'neutral';
  const color = label === 'positive' ? 'var(--accent-green)' :
                label === 'negative' ? 'var(--accent-red)' : 'var(--accent-yellow)';
  if (score) {
    score.textContent = `${compound >= 0 ? '+' : ''}${compound.toFixed(3)}`;
    score.style.color = color;
  }

  const pos = summary.positive ?? 0;
  const neu = summary.neutral ?? 0;
  const neg = summary.negative ?? 0;
  el('statPos').textContent = pos;
  el('statNeu').textContent = neu;
  el('statNeg').textContent = neg;
  el('statTotal').textContent = pos + neu + neg;

  renderSentimentTimeChart();
}

function renderSentimentTimeChart() {
  const timeseries = sentimentData.timeseries || [];
  if (!timeseries.length) return;

  const labels = timeseries.map(t => formatTime(t.bucket));
  const posData = timeseries.map(t => t.positive || 0);
  const neuData = timeseries.map(t => t.neutral || 0);
  const negData = timeseries.map(t => t.negative || 0);

  const ctx = el('sentimentTimeChart');
  if (!ctx) return;

  if (sentimentTimeChart) sentimentTimeChart.destroy();

  sentimentTimeChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'Positive',
          data: posData,
          backgroundColor: 'rgba(34, 197, 94, 0.7)',
          borderRadius: 3,
        },
        {
          label: 'Neutral',
          data: neuData,
          backgroundColor: 'rgba(234, 179, 8, 0.5)',
          borderRadius: 3,
        },
        {
          label: 'Negative',
          data: negData,
          backgroundColor: 'rgba(239, 68, 68, 0.7)',
          borderRadius: 3,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 11 } } },
      },
      scales: {
        x: {
          stacked: true,
          grid: { display: false },
          ticks: { font: { size: 10 }, color: '#64748b' },
        },
        y: {
          stacked: true,
          beginAtZero: true,
          grid: { color: 'rgba(55,65,81,0.3)' },
          ticks: { font: { size: 10 }, color: '#64748b', stepSize: 1 },
        },
      },
    },
  });
}

// ── Symbol Chips ─────────────────────────────────────────────────────

function renderSymbolChips() {
  const bySymbol = sentimentData.by_symbol || [];
  const container = el('symbolChips');
  if (!container) return;

  if (!bySymbol.length) {
    container.innerHTML = '<span style="color:var(--text-muted);font-size:0.8rem;">No symbol sentiment data yet</span>';
    return;
  }

  container.innerHTML = bySymbol.map(s => {
    const lbl = s.label || 'neutral';
    const compound = s.avg_compound ?? 0;
    const total = (s.positive || 0) + (s.neutral || 0) + (s.negative || 0);
    const posPct = total > 0 ? ((s.positive || 0) / total) * 100 : 50;
    const barColor = lbl === 'positive' ? 'var(--accent-green)' :
                     lbl === 'negative' ? 'var(--accent-red)' : 'var(--accent-yellow)';
    const isActive = activeCurrency === s.currency;
    return `<div class="np-symbol-chip${isActive ? ' active' : ''}" data-currency="${s.currency}">
      <span class="np-chip-name">${s.currency}</span>
      <span class="np-chip-score ${lbl}">${compound >= 0 ? '+' : ''}${compound.toFixed(2)}</span>
      <div class="np-chip-bar">
        <div class="np-chip-bar-fill" style="width:${posPct}%;background:${barColor}"></div>
      </div>
      <span class="np-chip-count">${total} articles</span>
    </div>`;
  }).join('');

  container.querySelectorAll('.np-symbol-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      const cur = chip.dataset.currency;
      if (activeCurrency === cur) {
        activeCurrency = '';
        el('currencyFilter').value = '';
      } else {
        activeCurrency = cur;
        el('currencyFilter').value = cur;
      }
      renderSymbolChips();
      renderFeed();
    });
  });
}

// ── Currency Filter Populate ─────────────────────────────────────────

function populateCurrencyFilter() {
  const select = el('currencyFilter');
  if (!select) return;

  const currencies = new Set();
  allArticles.forEach(a => (a.currencies || []).forEach(c => currencies.add(c)));

  const current = select.value;
  const opts = ['<option value="">All Currencies</option>'];
  [...currencies].sort().forEach(c => {
    opts.push(`<option value="${c}"${c === current ? ' selected' : ''}>${c}</option>`);
  });
  select.innerHTML = opts.join('');
}

// ── News Feed ────────────────────────────────────────────────────────

function getFilteredArticles() {
  let filtered = [...allArticles];

  if (activeFilter !== 'all') {
    filtered = filtered.filter(a => {
      const lbl = a.sentiment?.label || 'neutral';
      return lbl === activeFilter;
    });
  }

  if (activeCurrency) {
    filtered = filtered.filter(a => (a.currencies || []).includes(activeCurrency));
  }

  if (searchQuery) {
    const q = searchQuery.toLowerCase();
    filtered = filtered.filter(a => (a.title || '').toLowerCase().includes(q));
  }

  return filtered;
}

function renderFeed() {
  const feed = el('newsFeed');
  if (!feed) return;

  const articles = getFilteredArticles();
  el('articleCount').textContent = `${articles.length} article${articles.length !== 1 ? 's' : ''}`;

  if (!articles.length) {
    feed.innerHTML = `<div class="np-feed-empty">
      <div class="np-feed-empty-icon">📭</div>
      <div class="np-feed-empty-text">No articles match your filters</div>
    </div>`;
    return;
  }

  feed.innerHTML = articles.map(a => {
    const s = a.sentiment || {};
    const lbl = s.label || 'neutral';
    const compound = s.compound ?? 0;
    const currencies = (a.currencies || []).map(c =>
      `<span class="np-currency-tag">${c}</span>`
    ).join('');

    return `<article class="np-article">
      <div class="np-article-sentiment">
        <span class="np-sentiment-label ${lbl}">${lbl}</span>
        <span class="np-compound-score ${lbl}">${compound >= 0 ? '+' : ''}${compound.toFixed(2)}</span>
      </div>
      <div class="np-article-body">
        <div class="np-article-title">
          <a href="${a.url || '#'}" target="_blank" rel="noopener">${a.title || 'Untitled'}</a>
        </div>
        <div class="np-article-meta">
          <span class="np-article-source">${a.domain || a.source || ''}</span>
          <span>${timeAgo(a.published_at)}</span>
          <div class="np-article-currencies">${currencies}</div>
        </div>
      </div>
    </article>`;
  }).join('');
}

// ── Event Listeners ──────────────────────────────────────────────────

function setupListeners() {
  // Sentiment filter pills
  document.querySelectorAll('.np-pill[data-filter]').forEach(pill => {
    pill.addEventListener('click', () => {
      document.querySelectorAll('.np-pill[data-filter]').forEach(p => p.classList.remove('active'));
      pill.classList.add('active');
      activeFilter = pill.dataset.filter;
      renderFeed();
    });
  });

  // Currency select
  const currSelect = el('currencyFilter');
  if (currSelect) {
    currSelect.addEventListener('change', () => {
      activeCurrency = currSelect.value;
      renderSymbolChips();
      renderFeed();
    });
  }

  // Search
  const searchInput = el('newsSearch');
  if (searchInput) {
    searchInput.addEventListener('input', () => {
      searchQuery = searchInput.value.trim();
      renderFeed();
    });
  }

  // Refresh
  const refreshBtn = el('refreshBtn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      refreshBtn.textContent = '↻ Loading...';
      refreshAll().then(() => { refreshBtn.textContent = '↻ Refresh'; });
    });
  }

  // Theme toggle
  const themeToggle = el('themeToggle');
  const themeIcon = el('themeIcon');
  if (themeToggle) {
    themeToggle.addEventListener('click', () => {
      const isLight = document.documentElement.getAttribute('data-theme') === 'light';
      if (isLight) {
        document.documentElement.removeAttribute('data-theme');
        localStorage.setItem('theme', 'dark');
        if (themeIcon) themeIcon.textContent = '☀';
      } else {
        document.documentElement.setAttribute('data-theme', 'light');
        localStorage.setItem('theme', 'light');
        if (themeIcon) themeIcon.textContent = '🌙';
      }
    });

    if (document.documentElement.getAttribute('data-theme') === 'light' && themeIcon) {
      themeIcon.textContent = '🌙';
    }
  }
}

// ── Init ─────────────────────────────────────────────────────────────

async function init() {
  setupListeners();
  await refreshAll();
  autoRefreshId = setInterval(refreshAll, 30000);
}

init();
