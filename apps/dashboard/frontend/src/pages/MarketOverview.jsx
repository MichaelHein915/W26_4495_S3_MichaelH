import { startTransition, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Bar, BarChart, CartesianGrid, Line, LineChart,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";

import { useDashboard } from "../hooks/useDashboard";
import { InfoTip, SeverityPill } from "../components/Tooltip";
import { Pagination, usePagination } from "../components/Pagination";
import { Sparkline, TrendArrow } from "../components/Sparkline";
import {
  formatAxisLabel, formatFreshness, formatNumber, formatPercent, formatPrice, formatUsdAmount,
  formatWindowLabel, freshnessTone,
} from "../utils/format";

const SYMBOLS_PAGE_SIZE = 10;
const ALERTS_PAGE_SIZE = 8;
const ALERT_SEVERITY_PRIORITY = { high: 0, medium: 1, low: 2 };
const FLAT_PRICE_MOVE_THRESHOLD_PCT = 0.15;

const DEFAULT_SORT = { key: "notional_volume_usd", direction: "desc" };

function sortRows(rows, sortConfig) {
  const multiplier = sortConfig.direction === "asc" ? 1 : -1;

  return [...rows].sort((left, right) => {
    const leftValue = left?.[sortConfig.key];
    const rightValue = right?.[sortConfig.key];

    if (sortConfig.key === "product_id") {
      return multiplier * String(leftValue ?? "").localeCompare(String(rightValue ?? ""));
    }

    if (sortConfig.key === "window_start") {
      return multiplier * (new Date(leftValue ?? 0).getTime() - new Date(rightValue ?? 0).getTime());
    }

    return multiplier * ((Number(leftValue) || 0) - (Number(rightValue) || 0));
  });
}

function SortableHeader({ label, sortKey, sortConfig, onSort, defaultDirection = "desc", tooltip }) {
  const isActive = sortConfig.key === sortKey;
  const direction = isActive ? sortConfig.direction : defaultDirection;
  const icon = direction === "asc" ? "↑" : "↓";

  return (
    <button
      type="button"
      className={`table-sort-button${isActive ? " table-sort-button--active" : ""}`}
      onClick={() => onSort(sortKey, defaultDirection)}
    >
      <span>{label}</span>
      {tooltip ? <InfoTip text={tooltip} /> : null}
      <span aria-hidden="true">{icon}</span>
    </button>
  );
}

function AlertActionGuide() {
  return (
    <div className="alert-guide">
      <strong>What to check next</strong>
      <p>Start with `high` severity rows, compare `spike ratio` with `1m change`, then open the symbol chart to confirm whether the move held above/below VWAP or got rejected.</p>
    </div>
  );
}

function alertMoveLabel(priceChangePct) {
  const value = Number(priceChangePct) || 0;
  if (Math.abs(value) < FLAT_PRICE_MOVE_THRESHOLD_PCT) {
    return { label: "Flat reaction", tone: "neutral" };
  }
  if (value > 0) {
    return { label: "Bullish follow-through", tone: "positive" };
  }
  return { label: "Bearish follow-through", tone: "negative" };
}

function alertVwapLabel(lastPrice, vwap) {
  const price = Number(lastPrice) || 0;
  const weightedAverage = Number(vwap) || 0;

  if (!price || !weightedAverage) {
    return { label: "VWAP n/a", tone: "neutral" };
  }

  const deviationPct = ((price - weightedAverage) / weightedAverage) * 100;
  if (Math.abs(deviationPct) < 0.15) {
    return { label: "At VWAP", tone: "neutral" };
  }
  if (deviationPct > 0) {
    return { label: "Above VWAP", tone: "positive" };
  }
  return { label: "Below VWAP", tone: "negative" };
}

function MarketStatusBanner({ summary, marketOverview }) {
  const freshness = summary?.freshness_seconds;
  const activeSymbols = marketOverview?.length ?? 0;

  if (!summary) return null;

  let tone = "neutral";
  let message = `Tracking ${activeSymbols} symbols · pipeline fresh ${formatFreshness(freshness)}`;

  if (freshnessTone(freshness) === "negative") {
    tone = "error";
    message = `Pipeline stale — last batch ${formatFreshness(freshness)} · data may be outdated`;
  } else if (freshnessTone(freshness) === "positive" && activeSymbols > 0) {
    tone = "positive";
    message = `Pipeline live · ${activeSymbols} symbols updating · last batch ${formatFreshness(freshness)}`;
  } else if (activeSymbols > 0) {
    tone = "neutral";
    message = `Pipeline updating · ${activeSymbols} symbols tracked · last batch ${formatFreshness(freshness)}`;
  }

  return (
    <div className={`status-banner status-banner--${tone}`}>
      <span className="status-dot" />
      {message}
    </div>
  );
}

function LeaderRow({ rank, row, metricLabel, formattedValue }) {
  const navigate = useNavigate();
  return (
    <div
      className="leader-row leader-row--clickable"
      onClick={() => navigate(`/symbol/${encodeURIComponent(row.product_id)}`)}
    >
      <div>
        <p className="leader-rank">#{rank}</p>
        <strong>{row.product_id}</strong>
      </div>
      <div className="leader-metric">
        <span>{metricLabel}</span>
        <strong>{formattedValue}</strong>
      </div>
    </div>
  );
}

export default function MarketOverview() {
  const [selectedSymbol, setSelectedSymbol] = useState("");
  const [sortConfig, setSortConfig] = useState(DEFAULT_SORT);
  const { dashboard, loading, error } = useDashboard(selectedSymbol || undefined);
  const navigate = useNavigate();

  const summary = dashboard?.summary;
  const marketOverview = dashboard?.market_overview ?? [];
  const topMovers = dashboard?.market_leaders?.top_movers ?? [];
  const topNotional = dashboard?.market_leaders?.top_volume ?? [];
  const recentAlerts = dashboard?.recent_alerts ?? [];
  const candles = dashboard?.candles ?? [];
  const symbols = dashboard?.symbols ?? [];
  const sparklines = dashboard?.sparklines ?? {};

  // Count recent alerts per symbol for the badge
  const alertCountBySymbol = recentAlerts.reduce((acc, a) => {
    acc[a.product_id] = (acc[a.product_id] ?? 0) + 1;
    return acc;
  }, {});
  const sortedMarketOverview = sortRows(marketOverview, sortConfig);
  const marketOverviewBySymbol = new Map(marketOverview.map((row) => [row.product_id, row]));
  const sortedRecentAlerts = [...recentAlerts].sort((left, right) => {
    const severityDelta = (ALERT_SEVERITY_PRIORITY[left.severity] ?? 99) - (ALERT_SEVERITY_PRIORITY[right.severity] ?? 99);
    if (severityDelta !== 0) return severityDelta;
    return new Date(right.window_start).getTime() - new Date(left.window_start).getTime();
  });

  const chartData = candles.map((c) => ({
    axisLabel: formatAxisLabel(c.window_start),
    windowLabel: formatWindowLabel(c.window_start),
    close_price: c.close_price,
    volume_qty: c.volume_qty,
  }));

  // Auto-select first symbol if none chosen yet
  if (!selectedSymbol && dashboard?.selected_symbol) {
    startTransition(() => setSelectedSymbol(dashboard.selected_symbol));
  }

  function handleSort(nextKey, defaultDirection = "desc") {
    setSortConfig((current) => (
      current.key === nextKey
        ? { key: nextKey, direction: current.direction === "desc" ? "asc" : "desc" }
        : { key: nextKey, direction: defaultDirection }
    ));
  }

  // Pagination for the all-symbols table
  const {
    page: symbolsPage,
    setPage: setSymbolsPage,
    pageItems: symbolsPage_items,
    totalPages: symbolsTotalPages,
  } = usePagination(sortedMarketOverview, SYMBOLS_PAGE_SIZE);

  // Pagination for the recent alerts table
  const {
    page: alertsPage,
    setPage: setAlertsPage,
    pageItems: alertsPageItems,
    totalPages: alertsTotalPages,
  } = usePagination(sortedRecentAlerts, ALERTS_PAGE_SIZE);

  return (
    <>
      {/* Header */}
      <section className="hero">
        <div>
          <p className="eyebrow">Local-First Crypto Analytics</p>
          <h1>Live market signals.</h1>
          <p className="hero-copy">
            Real-time pipeline data — candles, volume spikes, and market leaders
            computed by Spark and served fresh via SSE.
          </p>
        </div>
        <div className="hero-controls">
          <label htmlFor="symbol-select">Focus symbol</label>
          <select
            id="symbol-select"
            value={selectedSymbol}
            onChange={(e) => setSelectedSymbol(e.target.value)}
            disabled={!symbols.length}
          >
            {symbols.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <span>Live via SSE</span>
          <strong>{formatFreshness(summary?.freshness_seconds)}</strong>
        </div>
      </section>

      {/* Status banner */}
      <MarketStatusBanner summary={summary} marketOverview={marketOverview} />

      {error && <section className="banner banner--error">{error}</section>}
      {loading && !dashboard && <section className="banner">Connecting to live data stream…</section>}

      {/* Market leaders */}
      <section className="insight-grid" style={{ marginTop: 24 }}>
        <section className="panel">
          <div className="panel-header">
            <h2>Top Movers <InfoTip text="Symbols with the largest 1-minute price change % in the latest processed window. High % = strong momentum in that direction." /></h2>
            <span>Latest window</span>
          </div>
          <div style={{ padding: "0 8px" }}>
            {topMovers.length === 0 ? (
              <p className="empty-state">No processed data yet.</p>
            ) : topMovers.map((row, i) => (
              <LeaderRow
                key={row.product_id}
                rank={i + 1}
                row={row}
                metricLabel="1m change"
                formattedValue={formatPercent(row.price_change_pct)}
              />
            ))}
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <h2>Top Notional Volume <InfoTip text="Symbols with the highest USD value traded in the latest 1-minute window. This compares economic activity, not raw coin count, so BTC and ADA are measured on the same scale." /></h2>
            <span>Latest window</span>
          </div>
          <div style={{ padding: "0 8px" }}>
            {topNotional.length === 0 ? (
              <p className="empty-state">No processed data yet.</p>
            ) : topNotional.map((row, i) => (
              <LeaderRow
                key={row.product_id}
                rank={i + 1}
                row={row}
                metricLabel="1m notional"
                formattedValue={formatUsdAmount(row.notional_volume_usd)}
              />
            ))}
          </div>
        </section>
      </section>

      {/* Price + volume charts for selected symbol */}
      {chartData.length > 0 && (
        <section className="chart-grid" style={{ marginTop: 24 }}>
          <section className="panel chart-panel">
            <div className="panel-header">
              <h2>{selectedSymbol} Price Trend</h2>
              <button
                className="nav-link"
                onClick={() => navigate(`/symbol/${encodeURIComponent(selectedSymbol)}`)}
              >
                Full detail →
              </button>
            </div>
            <div className="chart-shell">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="axisLabel" stroke="#a1a1aa" />
                  <YAxis stroke="#a1a1aa" domain={["auto", "auto"]} />
                  <Tooltip labelFormatter={(_, payload) => payload?.[0]?.payload?.windowLabel ?? _} />
                  <Line type="monotone" dataKey="close_price" stroke="#f97316" strokeWidth={3} dot={false} name="Close" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </section>

          <section className="panel chart-panel">
            <div className="panel-header">
              <h2>{selectedSymbol} Volume <InfoTip text="How many coins were traded each minute. A sudden tall bar = volume spike. Compare bar heights — a spike significantly taller than the rest is worth investigating." /></h2>
            </div>
            <div className="chart-shell">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="axisLabel" stroke="#a1a1aa" />
                  <YAxis stroke="#a1a1aa" />
                  <Tooltip labelFormatter={(_, payload) => payload?.[0]?.payload?.windowLabel ?? _} />
                  <Bar dataKey="volume_qty" fill="#22c55e" radius={[8, 8, 0, 0]} name="Volume" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </section>
        </section>
      )}

      {/* All symbols table — paginated */}
      <section className="panel" style={{ marginTop: 24 }}>
        <div className="panel-header">
          <h2>All Symbols <InfoTip text="Click any row to drill into detailed candles, alerts, and the AI assistant for that symbol." /></h2>
          <span>Sorted by {sortConfig.key === "notional_volume_usd" ? "notional volume" : sortConfig.key === "window_start" ? "latest window" : sortConfig.key.replaceAll("_", " ")}</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>
                  <SortableHeader
                    label="Symbol"
                    sortKey="product_id"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    defaultDirection="asc"
                  />
                </th>
                <th>
                  <SortableHeader
                    label="Last Price"
                    sortKey="last_price_usd"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    tooltip="Most recent trade price."
                  />
                </th>
                <th>
                  <SortableHeader
                    label="1m Change"
                    sortKey="price_change_pct"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    tooltip="How much the price moved in the last 1-minute window. Click to sort biggest gainers or losers."
                  />
                </th>
                <th>
                  <SortableHeader
                    label="Notional"
                    sortKey="notional_volume_usd"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    tooltip="USD value traded in the latest 1-minute window. Best cross-symbol activity comparison."
                  />
                </th>
                <th>
                  <SortableHeader
                    label="Volume"
                    sortKey="volume_qty"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    tooltip="Coins traded in the latest 1-minute window. Useful inside one symbol, but not ideal across different coins."
                  />
                </th>
                <th>
                  <SortableHeader
                    label="Trades"
                    sortKey="trade_count"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    tooltip="Number of individual transactions in the window. Many small trades vs one large trade can have the same volume but different meaning."
                  />
                </th>
                <th>
                  Trend{" "}
                  <InfoTip text="Last 8 candles plotted as a mini chart. Arrow shows direction: ↑ up, ↓ down, → flat. A steady upward line = consistent buying. A jagged line = choppy, less reliable signals." />
                </th>
                <th>
                  <SortableHeader
                    label="Window"
                    sortKey="window_start"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    tooltip="Latest processed 1-minute window for this symbol."
                  />
                </th>
              </tr>
            </thead>
            <tbody>
              {sortedMarketOverview.length === 0 ? (
                <tr><td colSpan="8" className="empty-state">Waiting for processed data…</td></tr>
              ) : symbolsPage_items.map((row) => {
                const prices     = sparklines[row.product_id];
                const alertCount = alertCountBySymbol[row.product_id] ?? 0;
                return (
                  <tr
                    key={row.product_id}
                    className="is-clickable"
                    onClick={() => navigate(`/symbol/${encodeURIComponent(row.product_id)}`)}
                  >
                    <td>
                      <div className="symbol-cell">
                        <strong>{row.product_id}</strong>
                        {alertCount > 0 && (
                          <span className="alert-badge" title={`${alertCount} recent alert${alertCount > 1 ? "s" : ""}`}>
                            {alertCount}
                          </span>
                        )}
                      </div>
                    </td>
                    <td>{formatPrice(row.last_price_usd)}</td>
                    <td
                      className={row.price_change_pct >= 0 ? "positive" : "negative"}
                      style={{
                        background: row.price_change_pct >= 0
                          ? `rgba(34,197,94,${Math.min(Math.abs(row.price_change_pct ?? 0) / 3, 0.4)})`
                          : `rgba(239,68,68,${Math.min(Math.abs(row.price_change_pct ?? 0) / 3, 0.4)})`,
                      }}
                    >
                      {formatPercent(row.price_change_pct)}
                    </td>
                    <td>{formatUsdAmount(row.notional_volume_usd)}</td>
                    <td>{formatNumber(row.volume_qty, 4)}</td>
                    <td>{formatNumber(row.trade_count, 0)}</td>
                    <td>
                      <div className="spark-cell">
                        <TrendArrow prices={prices} />
                        <Sparkline prices={prices} />
                      </div>
                    </td>
                    <td>{formatWindowLabel(row.window_start)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <Pagination
          page={symbolsPage}
          totalPages={symbolsTotalPages}
          onPage={setSymbolsPage}
          totalItems={sortedMarketOverview.length}
          pageSize={SYMBOLS_PAGE_SIZE}
        />
      </section>

      {/* Recent alerts — paginated */}
      {recentAlerts.length > 0 && (
        <section className="panel" style={{ marginTop: 24 }}>
          <div className="panel-header">
            <h2>Recent Volume Spikes <InfoTip text="A spike means the trading volume was significantly higher than the rolling average for that symbol. Spike ratio = current volume ÷ baseline. 3x means 3 times the normal volume — something unusual is happening." /></h2>
            <span>{recentAlerts.length} alerts</span>
          </div>
          <AlertActionGuide />
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Time</th>
                  <th>Severity <InfoTip text="Low = mildly above average. Medium = notably above average. High = extreme spike, pay attention." /></th>
                  <th>Spike Ratio <InfoTip text="How many times above normal volume. 2x = double the usual. 5x = extremely unusual." /></th>
                  <th>1m Change <InfoTip text="Use this with severity to see whether the spike came with follow-through up, down, or basically flat price." /></th>
                  <th>Read</th>
                  <th>VWAP</th>
                  <th>Last Price</th>
                  <th>Volume</th>
                  <th>Baseline <InfoTip text="The rolling average volume for this symbol. Your current volume is compared against this." /></th>
                  <th className="col-sticky-right">Next Step</th>
                </tr>
              </thead>
              <tbody key={`alerts-page-${alertsPage}`}>
                {alertsPageItems.map((a, idx) => {
                  const symbolMetrics = marketOverviewBySymbol.get(a.product_id);
                  const move = alertMoveLabel(symbolMetrics?.price_change_pct);
                  const vwapRead = alertVwapLabel(symbolMetrics?.last_price_usd, symbolMetrics?.vwap_usd);
                  return (
                    <tr
                      key={`${alertsPage}-${idx}-${a.product_id}-${a.window_start}`}
                      className="is-clickable"
                      onClick={() => navigate(`/symbol/${encodeURIComponent(a.product_id)}`)}
                    >
                      <td>{a.product_id}</td>
                      <td>{formatWindowLabel(a.window_start)}</td>
                      <td><SeverityPill severity={a.severity} /></td>
                      <td>{formatNumber(a.spike_ratio, 2)}x</td>
                      <td className={(symbolMetrics?.price_change_pct ?? 0) >= 0 ? "positive" : "negative"}>
                        {formatPercent(symbolMetrics?.price_change_pct)}
                      </td>
                      <td>
                        <span className={`signal-read signal-read--${move.tone}`}>
                          {move.label}
                        </span>
                      </td>
                      <td>
                        <span className={`signal-read signal-read--${vwapRead.tone}`}>
                          {vwapRead.label}
                        </span>
                      </td>
                      <td>{formatPrice(symbolMetrics?.last_price_usd)}</td>
                      <td>{formatNumber(a.volume_qty, 4)}</td>
                      <td>{formatNumber(a.baseline_volume_qty, 4)}</td>
                      <td className="col-sticky-right">
                        <button
                          type="button"
                          className="table-action-button"
                          onClick={(event) => {
                            event.stopPropagation();
                            navigate(`/symbol/${encodeURIComponent(a.product_id)}`);
                          }}
                        >
                          Open chart
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <Pagination
            page={alertsPage}
            totalPages={alertsTotalPages}
            onPage={setAlertsPage}
            totalItems={sortedRecentAlerts.length}
            pageSize={ALERTS_PAGE_SIZE}
          />
        </section>
      )}
    </>
  );
}
