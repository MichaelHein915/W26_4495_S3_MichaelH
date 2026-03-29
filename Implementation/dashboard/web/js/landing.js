/* ═══════════════════════════════════════════════════════════════════
   CryptoStream Landing Page — Particles, Highcharts, Animations
   ═══════════════════════════════════════════════════════════════════ */

/* ─── Particle System ──────────────────────────────────────────────── */

(function initParticles() {
  const canvas = document.getElementById('particleCanvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  let particles = [];
  let mouse = { x: null, y: null };
  const PARTICLE_COUNT = 80;
  const CONNECT_DISTANCE = 140;
  const MOUSE_RADIUS = 180;

  function resize() {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
  }

  resize();
  window.addEventListener('resize', resize);

  document.addEventListener('mousemove', (e) => {
    mouse.x = e.clientX;
    mouse.y = e.clientY;
  });

  class Particle {
    constructor() {
      this.x = Math.random() * canvas.width;
      this.y = Math.random() * canvas.height;
      this.vx = (Math.random() - 0.5) * 0.4;
      this.vy = (Math.random() - 0.5) * 0.4;
      this.radius = Math.random() * 2 + 0.5;
      this.baseAlpha = Math.random() * 0.4 + 0.1;
      this.alpha = this.baseAlpha;
    }

    update() {
      this.x += this.vx;
      this.y += this.vy;

      if (this.x < 0 || this.x > canvas.width) this.vx *= -1;
      if (this.y < 0 || this.y > canvas.height) this.vy *= -1;

      if (mouse.x !== null) {
        const dx = this.x - mouse.x;
        const dy = this.y - mouse.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < MOUSE_RADIUS) {
          const force = (MOUSE_RADIUS - dist) / MOUSE_RADIUS;
          this.alpha = this.baseAlpha + force * 0.5;
          this.x += dx * force * 0.02;
          this.y += dy * force * 0.02;
        } else {
          this.alpha = this.baseAlpha;
        }
      }
    }

    draw() {
      ctx.beginPath();
      ctx.arc(this.x, this.y, this.radius, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(59, 130, 246, ${this.alpha})`;
      ctx.fill();
    }
  }

  for (let i = 0; i < PARTICLE_COUNT; i++) {
    particles.push(new Particle());
  }

  function connectParticles() {
    for (let i = 0; i < particles.length; i++) {
      for (let j = i + 1; j < particles.length; j++) {
        const dx = particles[i].x - particles[j].x;
        const dy = particles[i].y - particles[j].y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < CONNECT_DISTANCE) {
          const alpha = (1 - dist / CONNECT_DISTANCE) * 0.15;
          ctx.beginPath();
          ctx.moveTo(particles[i].x, particles[i].y);
          ctx.lineTo(particles[j].x, particles[j].y);
          ctx.strokeStyle = `rgba(59, 130, 246, ${alpha})`;
          ctx.lineWidth = 0.6;
          ctx.stroke();
        }
      }
    }
  }

  function animate() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    particles.forEach((p) => {
      p.update();
      p.draw();
    });
    connectParticles();
    requestAnimationFrame(animate);
  }

  animate();
})();

/* ─── Floating Crypto Icons ────────────────────────────────────────── */

(function initFloatingIcons() {
  const container = document.getElementById('floatingIcons');
  if (!container) return;
  const icons = ['₿', 'Ξ', '◎', '₮', '✕', '◆', '₳', '⬡', '⟐', '⬟'];
  for (let i = 0; i < 15; i++) {
    const el = document.createElement('span');
    el.className = 'floating-icon';
    el.textContent = icons[i % icons.length];
    el.style.left = Math.random() * 100 + '%';
    el.style.animationDuration = (15 + Math.random() * 25) + 's';
    el.style.animationDelay = (-Math.random() * 30) + 's';
    el.style.fontSize = (1 + Math.random() * 2) + 'rem';
    container.appendChild(el);
  }
})();

/* ─── Scroll Reveal (IntersectionObserver) ─────────────────────────── */

(function initReveal() {
  const reveals = document.querySelectorAll('.reveal');
  if (!reveals.length) return;

  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          const delay = parseInt(entry.target.dataset.delay || '0', 10);
          setTimeout(() => entry.target.classList.add('visible'), delay);
          observer.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.15, rootMargin: '0px 0px -40px 0px' }
  );

  reveals.forEach((el) => observer.observe(el));
})();

/* ─── Nav Scroll Effect ────────────────────────────────────────────── */

(function initNav() {
  const nav = document.getElementById('nav');
  const toggle = document.getElementById('navMobileToggle');
  const menu = document.getElementById('mobileMenu');

  window.addEventListener('scroll', () => {
    if (!nav) return;
    nav.classList.toggle('scrolled', window.scrollY > 50);
  });

  if (toggle && menu) {
    toggle.addEventListener('click', () => {
      menu.classList.toggle('open');
    });
    menu.querySelectorAll('a').forEach((a) => {
      a.addEventListener('click', () => menu.classList.remove('open'));
    });
  }
})();

/* ─── Animated Counters ────────────────────────────────────────────── */

(function initCounters() {
  const counters = document.querySelectorAll('[data-count]');
  if (!counters.length) return;

  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        const el = entry.target;
        const target = parseFloat(el.dataset.count);
        const suffix = el.dataset.suffix || '';
        const isDecimal = target % 1 !== 0;
        const duration = 2000;
        const start = performance.now();

        function tick(now) {
          const progress = Math.min((now - start) / duration, 1);
          const eased = 1 - Math.pow(1 - progress, 3);
          const current = eased * target;

          if (target >= 10000) {
            el.textContent = Math.floor(current).toLocaleString() + suffix;
          } else if (isDecimal) {
            el.textContent = current.toFixed(1) + suffix;
          } else {
            el.textContent = Math.floor(current) + suffix;
          }

          if (progress < 1) requestAnimationFrame(tick);
        }

        requestAnimationFrame(tick);
        observer.unobserve(el);
      });
    },
    { threshold: 0.5 }
  );

  counters.forEach((el) => observer.observe(el));
})();

/* ─── Ticker Marquee ───────────────────────────────────────────────── */

(function initTicker() {
  const track = document.getElementById('landingTickerTrack');
  if (!track) return;

  const tickers = [
    { symbol: 'BTC-USD', price: 67432.18, change: 2.34 },
    { symbol: 'ETH-USD', price: 3521.45, change: 1.87 },
    { symbol: 'SOL-USD', price: 142.67, change: -0.92 },
    { symbol: 'ADA-USD', price: 0.4521, change: 3.12 },
    { symbol: 'DOT-USD', price: 7.34, change: -1.45 },
    { symbol: 'AVAX-USD', price: 35.82, change: 4.21 },
    { symbol: 'LINK-USD', price: 14.56, change: 1.03 },
    { symbol: 'MATIC-USD', price: 0.712, change: -0.58 },
    { symbol: 'UNI-USD', price: 9.87, change: 2.76 },
    { symbol: 'ATOM-USD', price: 8.92, change: -1.21 },
    { symbol: 'XRP-USD', price: 0.5234, change: 0.89 },
    { symbol: 'DOGE-USD', price: 0.0823, change: 5.43 },
  ];

  function buildTicker() {
    return tickers.map((t) => {
      const dir = t.change >= 0 ? 'up' : 'down';
      const sign = t.change >= 0 ? '+' : '';
      return `<span class="ticker-item">
        <span class="ticker-symbol">${t.symbol}</span>
        <span class="ticker-price">$${t.price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
        <span class="ticker-change ${dir}">${sign}${t.change}%</span>
      </span>`;
    }).join('');
  }

  track.innerHTML = buildTicker() + buildTicker();
})();

/* ─── Chart Data Generation ────────────────────────────────────────── */

function generateOHLC(count, basePrice) {
  const data = [];
  let price = basePrice;
  const now = Date.now();
  const interval = 60 * 1000;

  for (let i = count; i >= 0; i--) {
    const time = now - i * interval;
    const open = price;
    const volatility = price * 0.003;
    const high = open + Math.random() * volatility * 2;
    const low = open - Math.random() * volatility * 2;
    const close = low + Math.random() * (high - low);
    data.push([time, +open.toFixed(2), +high.toFixed(2), +low.toFixed(2), +close.toFixed(2)]);
    price = close + (Math.random() - 0.48) * volatility;
  }
  return data;
}

function generateAreaData(ohlcData) {
  return ohlcData.map((d) => [d[0], d[4]]);
}

/* Shared dark-theme config for all Highcharts on this page */
function darkStockConfig(overrides) {
  const base = {
    chart: {
      backgroundColor: 'transparent',
      style: { fontFamily: '"Inter", sans-serif' },
    },
    title: { text: '' },
    credits: { enabled: false },
    exporting: { enabled: false },
    accessibility: { enabled: false },
    navigator: { enabled: false },
    scrollbar: { enabled: false },
    rangeSelector: { enabled: false },
    xAxis: {
      lineColor: 'rgba(55,70,100,0.3)',
      tickColor: 'rgba(55,70,100,0.3)',
      labels: { style: { color: '#64748b', fontSize: '10px' } },
      gridLineColor: 'rgba(55,70,100,0.1)',
    },
    yAxis: {
      gridLineColor: 'rgba(55,70,100,0.15)',
      labels: { style: { color: '#64748b', fontSize: '10px' } },
      title: { text: '' },
    },
    tooltip: {
      backgroundColor: 'rgba(15, 20, 35, 0.9)',
      borderColor: 'rgba(59, 130, 246, 0.3)',
      style: { color: '#f1f5f9', fontSize: '12px' },
      borderRadius: 8,
    },
  };
  return Highcharts.merge(base, overrides);
}

/* ─── Canvas fallback (if Highcharts CDN fails to load) ────────────── */

function drawCanvasFallback(containerId) {
  const container = document.getElementById(containerId);
  if (!container) return;
  const canvas = document.createElement('canvas');
  canvas.width = container.clientWidth || 500;
  canvas.height = container.clientHeight || 300;
  canvas.style.width = '100%';
  canvas.style.height = '100%';
  container.appendChild(canvas);
  const ctx = canvas.getContext('2d');

  const points = [];
  let price = 67000;
  for (let i = 0; i < 80; i++) {
    price += (Math.random() - 0.48) * 200;
    points.push(price);
  }
  const minP = Math.min(...points);
  const maxP = Math.max(...points);
  const range = maxP - minP || 1;
  const pad = 30;
  const w = canvas.width - pad * 2;
  const h = canvas.height - pad * 2;

  const grad = ctx.createLinearGradient(0, pad, 0, canvas.height - pad);
  grad.addColorStop(0, 'rgba(59, 130, 246, 0.25)');
  grad.addColorStop(1, 'rgba(59, 130, 246, 0)');

  ctx.beginPath();
  ctx.moveTo(pad, pad + h);
  points.forEach((p, i) => {
    const x = pad + (i / (points.length - 1)) * w;
    const y = pad + h - ((p - minP) / range) * h;
    ctx.lineTo(x, y);
  });
  ctx.lineTo(pad + w, pad + h);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();

  ctx.beginPath();
  points.forEach((p, i) => {
    const x = pad + (i / (points.length - 1)) * w;
    const y = pad + h - ((p - minP) / range) * h;
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = '#3b82f6';
  ctx.lineWidth = 2;
  ctx.stroke();
}

/* ─── Highcharts: Hero Chart ───────────────────────────────────────── */

(function initHeroChart() {
  if (typeof Highcharts === 'undefined' || !Highcharts.stockChart) {
    drawCanvasFallback('heroChart');
    return;
  }

  try {
    const ohlc = generateOHLC(60, 67000);
    const areaData = generateAreaData(ohlc);

    Highcharts.stockChart('heroChart', darkStockConfig({
      chart: { height: null, animation: true },
      series: [{
        type: 'areaspline',
        name: 'BTC-USD',
        data: areaData,
        color: '#3b82f6',
        fillColor: {
          linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
          stops: [
            [0, 'rgba(59, 130, 246, 0.25)'],
            [1, 'rgba(59, 130, 246, 0)'],
          ],
        },
        lineWidth: 2,
        threshold: null,
      }],
      plotOptions: {
        series: { animation: { duration: 1500 } },
      },
    }));
  } catch (e) {
    console.warn('Hero chart init failed, using canvas fallback:', e);
    drawCanvasFallback('heroChart');
  }
})();

/* ─── Highcharts: Live Preview Chart ───────────────────────────────── */

let previewChart = null;
let previewOHLC = null;
let previewLiveTimer = null;
let previewCurrentType = 'area';

function buildPreviewSeries(type) {
  if (type === 'candlestick') {
    return {
      type: 'candlestick',
      name: 'BTC-USD',
      data: previewOHLC.slice(),
      color: '#ef4444',
      upColor: '#22c55e',
      lineColor: '#ef4444',
      upLineColor: '#22c55e',
    };
  }
  if (type === 'line') {
    return {
      type: 'spline',
      name: 'BTC-USD',
      data: generateAreaData(previewOHLC),
      color: '#a855f7',
      lineWidth: 2,
      threshold: null,
    };
  }
  return {
    type: 'areaspline',
    name: 'BTC-USD',
    data: generateAreaData(previewOHLC),
    color: '#3b82f6',
    fillColor: {
      linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
      stops: [
        [0, 'rgba(59, 130, 246, 0.2)'],
        [1, 'rgba(59, 130, 246, 0)'],
      ],
    },
    lineWidth: 2,
    threshold: null,
  };
}

function createPreviewChart(type) {
  if (previewLiveTimer) { clearInterval(previewLiveTimer); previewLiveTimer = null; }
  if (previewChart) { previewChart.destroy(); previewChart = null; }

  previewCurrentType = type;

  previewChart = Highcharts.stockChart('previewChart', darkStockConfig({
    chart: { height: 418, animation: true },
    series: [buildPreviewSeries(type)],
    plotOptions: {
      candlestick: {
        color: '#ef4444',
        upColor: '#22c55e',
        lineColor: '#ef4444',
        upLineColor: '#22c55e',
      },
      series: { animation: { duration: 600 } },
    },
  }));

  previewLiveTimer = setInterval(() => {
    if (!previewChart || !previewChart.series || !previewChart.series[0]) return;
    const series = previewChart.series[0];
    const lastPoint = previewOHLC[previewOHLC.length - 1];
    const lastClose = lastPoint[4];
    const vol = lastClose * 0.002;
    const newTime = lastPoint[0] + 60000;
    const open = lastClose;
    const high = open + Math.random() * vol * 2;
    const low = open - Math.random() * vol * 2;
    const close = low + Math.random() * (high - low);
    const newPoint = [newTime, +open.toFixed(2), +high.toFixed(2), +low.toFixed(2), +close.toFixed(2)];
    previewOHLC.push(newPoint);

    try {
      if (previewCurrentType === 'candlestick') {
        series.addPoint(newPoint, true, series.data.length > 120);
      } else {
        series.addPoint([newTime, +close.toFixed(2)], true, series.data.length > 120);
      }
    } catch (_) {}
  }, 2000);
}

(function initPreviewChart() {
  if (typeof Highcharts === 'undefined' || !Highcharts.stockChart) {
    drawCanvasFallback('previewChart');
    return;
  }

  try {
    previewOHLC = generateOHLC(120, 67000);
    createPreviewChart('area');

    document.querySelectorAll('.toolbar-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.toolbar-btn').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        const type = btn.dataset.series;
        if (type && type !== previewCurrentType) {
          createPreviewChart(type);
        }
      });
    });
  } catch (e) {
    console.warn('Preview chart init failed, using canvas fallback:', e);
    drawCanvasFallback('previewChart');
  }
})();

/* ─── Smooth scroll for anchor links ───────────────────────────────── */

document.querySelectorAll('a[href^="#"]').forEach((anchor) => {
  anchor.addEventListener('click', (e) => {
    e.preventDefault();
    const target = document.querySelector(anchor.getAttribute('href'));
    if (target) {
      target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  });
});
