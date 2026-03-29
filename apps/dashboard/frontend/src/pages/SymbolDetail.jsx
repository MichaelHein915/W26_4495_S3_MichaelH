import { useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import {
  Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";

import { useDashboard } from "../hooks/useDashboard";
import { InfoTip, SeverityPill } from "../components/Tooltip";
import { Pagination, usePagination } from "../components/Pagination";
import {
  formatAxisLabel, formatFreshness, formatNumber, formatPercent,
  formatPrice, formatWindowLabel,
} from "../utils/format";

const CANDLES_PAGE_SIZE = 10;
const ALERTS_PAGE_SIZE  = 8;

const TIMEFRAMES = [
  { label: "1m",  minutes: 1  },
  { label: "5m",  minutes: 5  },
  { label: "15m", minutes: 15 },
];

/* ── Candle aggregation ──────────────────────────────────────────────────── */

/**
 * Roll up 1-minute candles into N-minute buckets.
 * OHLC is correct (open from first, close from last, high/low from extremes).
 * VWAP is volume-weighted across all constituent candles.
 */
function aggregateCandles(candles1m, windowMinutes) {
  if (windowMinutes === 1 || candles1m.length === 0) return candles1m;

  const bucketMs = windowMinutes * 60 * 1000;
  const grouped  = new Map();

  for (const c of candles1m) {
    const ts       = new Date(c.window_start).getTime();
    const bucketTs = Math.floor(ts / bucketMs) * bucketMs;

    if (!grouped.has(bucketTs)) {
      grouped.set(bucketTs, {
        window_start: new Date(bucketTs).toISOString(),
        window_end:   new Date(bucketTs + bucketMs).toISOString(),
        open_price:   c.open_price,
        high_price:   c.high_price,
        low_price:    c.low_price,
        close_price:  c.close_price,
        volume_qty:   c.volume_qty  ?? 0,
        trade_count:  c.trade_count ?? 0,
        _notional:    (c.vwap_usd ?? c.close_price) * (c.volume_qty ?? 0),
      });
    } else {
      const b         = grouped.get(bucketTs);
      b.high_price    = Math.max(b.high_price,  c.high_price);
      b.low_price     = Math.min(b.low_price,   c.low_price);
      b.close_price   = c.close_price;
      b.volume_qty   += c.volume_qty  ?? 0;
      b.trade_count  += c.trade_count ?? 0;
      b._notional    += (c.vwap_usd ?? c.close_price) * (c.volume_qty ?? 0);
    }
  }

  return [...grouped.values()]
    .sort((a, b) => new Date(a.window_start) - new Date(b.window_start))
    .map((b) => ({
      window_start: b.window_start,
      window_end:   b.window_end,
      open_price:   b.open_price,
      high_price:   b.high_price,
      low_price:    b.low_price,
      close_price:  b.close_price,
      volume_qty:   b.volume_qty,
      trade_count:  b.trade_count,
      vwap_usd:     b.volume_qty > 0 ? b._notional / b.volume_qty : b.open_price,
    }));
}

/* ── Candlestick chart ───────────────────────────────────────────────────── */

function CandlestickChart({ data }) {
  if (data.length === 0) {
    return <p className="empty-state">No candles yet.</p>;
  }

  const width      = 1000;
  const height     = 280;
  const padding    = { top: 16, right: 18, bottom: 36, left: 58 };
  const plotWidth  = width  - padding.left - padding.right;
  const plotHeight = height - padding.top  - padding.bottom;

  const lows      = data.map((p) => p.low);
  const highs     = data.map((p) => p.high);
  const minPrice  = Math.min(...lows);
  const maxPrice  = Math.max(...highs);
  const rawRange  = maxPrice - minPrice;
  const buffer    = rawRange === 0 ? Math.max(maxPrice * 0.01, 1) : rawRange * 0.08;
  const chartMin  = minPrice - buffer;
  const chartMax  = maxPrice + buffer;
  const chartRange = Math.max(chartMax - chartMin, 1);

  const xStep       = data.length > 1 ? plotWidth / (data.length - 1) : 0;
  const candleWidth = Math.min(Math.max(plotWidth / Math.max(data.length * 2, 2), 6), 16);
  const labelEvery  = Math.max(1, Math.ceil(data.length / 6));

  const xForIndex = (i)     => padding.left + (data.length === 1 ? plotWidth / 2 : i * xStep);
  const yForPrice = (price) => padding.top  + ((chartMax - price) / chartRange) * plotHeight;

  const gridValues = Array.from({ length: 5 }, (_, i) => chartMin + (chartRange / 4) * i).reverse();
  const vwapPath   = data
    .map((p, i) => `${i === 0 ? "M" : "L"} ${xForIndex(i)} ${yForPrice(p.vwap)}`)
    .join(" ");

  return (
    <div className="candlestick-chart">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="candlestick-chart__svg"
        role="img"
        aria-label="Candlestick price chart"
      >
        {/* Grid lines + price labels */}
        {gridValues.map((value) => {
          const y = yForPrice(value);
          return (
            <g key={value}>
              <line x1={padding.left} x2={width - padding.right} y1={y} y2={y} className="candlestick-grid" />
              <text x={padding.left - 10} y={y + 4} textAnchor="end" className="candlestick-axis-text">
                {formatPrice(value)}
              </text>
            </g>
          );
        })}

        {/* VWAP line */}
        <path d={vwapPath} className="candlestick-vwap-line" />

        {/* Candles */}
        {data.map((point, i) => {
          const x          = xForIndex(i);
          const wickTop    = yForPrice(point.high);
          const wickBottom = yForPrice(point.low);
          const bodyTop    = yForPrice(Math.max(point.open, point.close));
          const bodyBottom = yForPrice(Math.min(point.open, point.close));
          const bodyHeight = Math.max(bodyBottom - bodyTop, 2);
          const toneClass  = point.close >= point.open ? "candlestick-body--up" : "candlestick-body--down";

          return (
            <g key={point.windowLabel}>
              <title>{`${point.windowLabel}\nOpen ${formatPrice(point.open)}\nHigh ${formatPrice(point.high)}\nLow ${formatPrice(point.low)}\nClose ${formatPrice(point.close)}\nVWAP ${formatPrice(point.vwap)}`}</title>
              {/* Wick */}
              <line x1={x} x2={x} y1={wickTop} y2={wickBottom} className={`candlestick-wick ${toneClass}`} />
              {/* Body */}
              <rect
                x={x - candleWidth / 2}
                y={bodyTop}
                width={candleWidth}
                height={bodyHeight}
                rx="2"
                className={`candlestick-body ${toneClass}`}
              />
              {/* Alert marker — orange triangle below the wick */}
              {point.hasAlert && (
                <g>
                  <title>Volume spike ({point.alertSeverity})</title>
                  <polygon
                    points={`${x},${wickBottom + 13} ${x - 5},${wickBottom + 5} ${x + 5},${wickBottom + 5}`}
                    className={`candle-alert-marker candle-alert-marker--${point.alertSeverity ?? "high"}`}
                  />
                </g>
              )}
            </g>
          );
        })}

        {/* X-axis time labels */}
        {data.map((point, i) =>
          i % labelEvery === 0 || i === data.length - 1 ? (
            <text
              key={`${point.axisLabel}-${i}`}
              x={xForIndex(i)}
              y={height - 10}
              textAnchor="middle"
              className="candlestick-axis-text"
            >
              {point.axisLabel}
            </text>
          ) : null,
        )}
      </svg>

      <div className="candlestick-chart__legend">
        <span><span className="legend-swatch legend-swatch--up" /> Bullish</span>
        <span><span className="legend-swatch legend-swatch--down" /> Bearish</span>
        <span><span className="legend-swatch legend-swatch--vwap" /> VWAP</span>
        <span><span className="legend-swatch legend-swatch--alert" /> Volume alert</span>
      </div>
    </div>
  );
}

/* ── Metric card ─────────────────────────────────────────────────────────── */
function MetricCard({ label, value, tone = "neutral", helper, tooltip }) {
  return (
    <section className={`metric-card metric-card--${tone}`}>
      <p className="eyebrow">{label} {tooltip && <InfoTip text={tooltip} />}</p>
      <h3>{value}</h3>
      {helper && <span>{helper}</span>}
    </section>
  );
}

/* ── Anomaly signal panel ────────────────────────────────────────────────── */
const DIRECTION_ICONS = { positive: "↑", negative: "↓", neutral: "↔" };

function SignalCard({ signal }) {
  const icon = DIRECTION_ICONS[signal.direction] ?? "↔";
  return (
    <div className={`signal-card signal-card--${signal.severity}`}>
      <div className="signal-card__header">
        <span className={`signal-icon signal-icon--${signal.direction}`}>{icon}</span>
        <strong className="signal-label">{signal.label}</strong>
        <span className="signal-value">{signal.value_str}</span>
      </div>
      <p className="signal-desc">{signal.description}</p>
    </div>
  );
}

function AnomalyPanel({ signals }) {
  if (!signals) {
    return (
      <section className="panel anomaly-panel">
        <div className="panel-header"><h2>Market Signals</h2><span>loading…</span></div>
        <p className="empty-state">Waiting for candle data…</p>
      </section>
    );
  }

  if (signals.length === 0) {
    return (
      <section className="panel anomaly-panel">
        <div className="panel-header">
          <h2>Market Signals <InfoTip text="Deterministic signals computed from live candle and volume data. No model — pure math on real numbers." /></h2>
          <span>all clear</span>
        </div>
        <div className="signal-all-clear">
          <span className="signal-icon signal-icon--positive" style={{ fontSize: "2rem" }}>✓</span>
          <div>
            <strong>Nothing unusual detected</strong>
            <p>Price near VWAP, volume within normal range, momentum not accelerating. Normal conditions.</p>
          </div>
        </div>
      </section>
    );
  }

  const warnings = signals.filter((s) => s.severity === "warning");
  const infos    = signals.filter((s) => s.severity !== "warning");

  return (
    <section className="panel anomaly-panel">
      <div className="panel-header">
        <h2>Market Signals <InfoTip text="Deterministic signals: VWAP deviation, price/volume divergence, volume z-score, and momentum acceleration. All computed from completed candles — no AI model." /></h2>
        <span>{signals.length} signal{signals.length !== 1 ? "s" : ""}</span>
      </div>
      {warnings.length > 0 && (
        <div className="signal-group">
          <p className="signal-group__label">⚠ Needs attention</p>
          {warnings.map((s) => <SignalCard key={s.id} signal={s} />)}
        </div>
      )}
      {infos.length > 0 && (
        <div className="signal-group">
          {warnings.length > 0 && <p className="signal-group__label">ℹ Observations</p>}
          {infos.map((s) => <SignalCard key={s.id} signal={s} />)}
        </div>
      )}
    </section>
  );
}

/* ── Main page ───────────────────────────────────────────────────────────── */
export default function SymbolDetail() {
  const { symbolId }    = useParams();
  const navigate        = useNavigate();
  const symbol          = decodeURIComponent(symbolId || "");
  const [tfMinutes, setTfMinutes] = useState(1);

  const { dashboard, loading, error } = useDashboard(symbol);

  const overview       = dashboard?.overview;
  const candles        = dashboard?.candles ?? [];
  const alerts         = dashboard?.alerts  ?? [];
  const anomalySignals = dashboard?.anomaly_signals ?? null;

  // Aggregate candles to selected timeframe
  const displayCandles = aggregateCandles(candles, tfMinutes);

  // Session stats always computed over raw 1m candles (full session picture)
  const sessionHigh = candles.length > 0 ? Math.max(...candles.map((c) => c.high_price)) : null;
  const sessionLow  = candles.length > 0 ? Math.min(...candles.map((c) => c.low_price))  : null;
  const lastPrice   = overview?.last_price_usd;
  const fromHighPct = sessionHigh && lastPrice ? ((lastPrice - sessionHigh) / sessionHigh * 100) : null;
  const fromLowPct  = sessionLow  && lastPrice ? ((lastPrice - sessionLow)  / sessionLow  * 100) : null;

  // Build chart data — mark candles that have alerts in their window
  const chartData = displayCandles.map((c) => {
    const start = new Date(c.window_start).getTime();
    const end   = new Date(c.window_end  ?? new Date(start + tfMinutes * 60000)).getTime();
    const candleAlerts = alerts.filter((a) => {
      const t = new Date(a.window_start).getTime();
      return t >= start && t < end;
    });
    const worstSeverity = ["high", "medium", "low"].find((s) => candleAlerts.some((a) => a.severity === s));
    return {
      axisLabel:     formatAxisLabel(c.window_start),
      windowLabel:   formatWindowLabel(c.window_start),
      open:          c.open_price,
      high:          c.high_price,
      low:           c.low_price,
      close:         c.close_price,
      volume:        c.volume_qty,
      vwap:          c.vwap_usd,
      hasAlert:      candleAlerts.length > 0,
      alertSeverity: worstSeverity ?? null,
    };
  });

  // Candles table: newest first, based on display candles
  const candlesReversed = [...displayCandles].reverse();

  const {
    page: candlesPage, setPage: setCandlesPage,
    pageItems: candlesPageItems, totalPages: candlesTotalPages,
  } = usePagination(candlesReversed, CANDLES_PAGE_SIZE);

  const {
    page: alertsPage, setPage: setAlertsPage,
    pageItems: alertsPageItems, totalPages: alertsTotalPages,
  } = usePagination(alerts, ALERTS_PAGE_SIZE);

  return (
    <>
      {/* Back + header */}
      <div className="page-header">
        <button className="nav-link" onClick={() => navigate("/")}>← Market Overview</button>
        <h1 className="page-title">{symbol}</h1>
        <span className="muted">{formatFreshness(dashboard?.summary?.freshness_seconds)}</span>
      </div>

      {error   && <section className="banner banner--error">{error}</section>}
      {loading && !dashboard && <section className="banner">Loading {symbol} data…</section>}

      {/* Key metrics — 8 cards (added session high/low) */}
      <section className="metrics-grid" style={{ marginTop: 24 }}>
        <MetricCard
          label="Last Price"
          value={formatPrice(lastPrice)}
          tone={overview?.price_change_pct >= 0 ? "positive" : "negative"}
          helper="Most recent trade price"
          tooltip="The price of the most recent completed trade. Updates every time Spark processes a new batch from Kafka."
        />
        <MetricCard
          label="1m Change"
          value={formatPercent(overview?.price_change_pct)}
          tone={overview?.price_change_pct >= 0 ? "positive" : "negative"}
          helper="Open → close of latest window"
          tooltip="How much the price moved from start to end of the last 1-minute candle. Not a 24h change — it's a single window."
        />
        <MetricCard
          label="Session High"
          value={formatPrice(sessionHigh)}
          tone={fromHighPct !== null && fromHighPct > -0.5 ? "positive" : "neutral"}
          helper={fromHighPct !== null ? `${formatPercent(fromHighPct)} from high` : "computing…"}
          tooltip="The highest price reached across all 1-minute candles in the current view. Near the high = price is at the top of its recent range. Far below = price has pulled back."
        />
        <MetricCard
          label="Session Low"
          value={formatPrice(sessionLow)}
          tone={fromLowPct !== null && fromLowPct < 0.5 ? "negative" : "neutral"}
          helper={fromLowPct !== null ? `+${fromLowPct.toFixed(2)}% from low` : "computing…"}
          tooltip="The lowest price reached across all 1-minute candles in the current view. Near the low = price is at the bottom of its recent range. Far above = price has recovered."
        />
        <MetricCard
          label="VWAP"
          value={formatPrice(overview?.vwap_usd)}
          helper="Volume-weighted average price"
          tooltip="Volume Weighted Average Price — the average price of all trades weighted by trade size. If current price is above VWAP, buyers are in control. Below VWAP = sellers."
        />
        <MetricCard
          label="1m Volume"
          value={formatNumber(overview?.volume_qty, 4)}
          helper={`${formatNumber(overview?.trade_count, 0)} trades`}
          tooltip="Coins traded in the latest 1-minute window. Compare against the volume chart bars to see if this is normal or elevated."
        />
        <MetricCard
          label="Volatility"
          value={overview?.volatility_usd != null ? `$${formatNumber(overview.volatility_usd, 2)}` : "--"}
          helper="Price swing within window"
          tooltip="How much price jumped around inside this 1-minute window. Always read relative to price — $15 volatility on a $97k BTC is tiny; same $15 on a $0.30 altcoin is extreme."
        />
        <MetricCard
          label="Alerts"
          value={formatNumber(alerts.length, 0)}
          tone={alerts.length > 0 ? "negative" : "neutral"}
          helper="Volume spikes"
          tooltip="Number of volume spike alerts for this symbol. A spike means trading volume was significantly above the rolling baseline."
        />
      </section>

      {/* Charts — with timeframe toggle */}
      <section className="chart-grid" style={{ marginTop: 24 }}>
        <section className="panel chart-panel">
          <div className="panel-header">
            <h2>
              Candles + VWAP{" "}
              <InfoTip text="Each candle shows open, high, low, and close. The wick is the full price range. Blue dashed line is VWAP. Orange triangles below candles mark volume spike alerts." />
            </h2>
            {/* Timeframe toggle */}
            <div className="timeframe-toggle">
              {TIMEFRAMES.map((tf) => (
                <button
                  key={tf.minutes}
                  type="button"
                  className={`timeframe-btn${tfMinutes === tf.minutes ? " timeframe-btn--active" : ""}`}
                  onClick={() => setTfMinutes(tf.minutes)}
                >
                  {tf.label}
                </button>
              ))}
            </div>
          </div>
          <div className="chart-shell">
            <CandlestickChart data={chartData} />
          </div>
        </section>

        <section className="panel chart-panel">
          <div className="panel-header">
            <h2>
              Volume{tfMinutes > 1 ? ` (${TIMEFRAMES.find((t) => t.minutes === tfMinutes)?.label})` : " per Minute"}{" "}
              <InfoTip text="Each bar = coins traded in that window. A bar much taller than others is a volume spike. Orange triangles on the candle chart mark which candles had alerts." />
            </h2>
          </div>
          <div className="chart-shell">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                <XAxis dataKey="axisLabel" stroke="#a1a1aa" />
                <YAxis stroke="#a1a1aa" />
                <Tooltip labelFormatter={(_, payload) => payload?.[0]?.payload?.windowLabel ?? _} />
                <Bar dataKey="volume" fill="#22c55e" radius={[6, 6, 0, 0]} name="Volume" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </section>
      </section>

      {/* Anomaly signals + OHLCV table */}
      <section className="secondary-grid" style={{ marginTop: 24 }}>
        <AnomalyPanel signals={anomalySignals} />

        <section className="panel">
          <div className="panel-header">
            <h2>
              OHLCV Candles{" "}
              <InfoTip text="Open, High, Low, Close, Volume for each window. Toggle the timeframe above the chart to see 5m or 15m aggregated rows here too." />
            </h2>
            <span>
              {displayCandles.length} × {TIMEFRAMES.find((t) => t.minutes === tfMinutes)?.label}
            </span>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Open</th>
                  <th>High</th>
                  <th>Low</th>
                  <th>Close</th>
                  <th>Volume</th>
                </tr>
              </thead>
              <tbody>
                {displayCandles.length === 0 ? (
                  <tr><td colSpan="6" className="empty-state">No candles yet.</td></tr>
                ) : candlesPageItems.map((c) => (
                  <tr key={c.window_start}>
                    <td>{formatWindowLabel(c.window_start)}</td>
                    <td>{formatPrice(c.open_price)}</td>
                    <td className="positive">{formatPrice(c.high_price)}</td>
                    <td className="negative">{formatPrice(c.low_price)}</td>
                    <td className={c.close_price >= c.open_price ? "positive" : "negative"}>
                      {formatPrice(c.close_price)}
                    </td>
                    <td>{formatNumber(c.volume_qty, 4)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <Pagination
            page={candlesPage}
            totalPages={candlesTotalPages}
            onPage={setCandlesPage}
            totalItems={displayCandles.length}
            pageSize={CANDLES_PAGE_SIZE}
          />
        </section>
      </section>

      {/* Alerts table */}
      {alerts.length > 0 && (
        <section className="panel" style={{ marginTop: 24 }}>
          <div className="panel-header">
            <h2>{symbol} Volume Alerts</h2>
            <span>{alerts.length} alerts</span>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Severity</th>
                  <th>Spike Ratio <InfoTip text="Current volume ÷ baseline. 3× means 3 times normal." /></th>
                  <th>Volume</th>
                  <th>Baseline</th>
                </tr>
              </thead>
              <tbody key={`alerts-page-${alertsPage}`}>
                {alertsPageItems.map((a, idx) => (
                  <tr key={`${alertsPage}-${idx}-${a.product_id}-${a.window_start}`}>
                    <td>{formatWindowLabel(a.window_start)}</td>
                    <td><SeverityPill severity={a.severity} /></td>
                    <td>{formatNumber(a.spike_ratio, 2)}×</td>
                    <td>{formatNumber(a.volume_qty, 4)}</td>
                    <td>{formatNumber(a.baseline_volume_qty, 4)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <Pagination
            page={alertsPage}
            totalPages={alertsTotalPages}
            onPage={setAlertsPage}
            totalItems={alerts.length}
            pageSize={ALERTS_PAGE_SIZE}
          />
        </section>
      )}
    </>
  );
}
