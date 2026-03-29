import { useDashboard } from "../hooks/useDashboard";
import { InfoTip } from "../components/Tooltip";
import { formatFreshness, formatNumber, formatWindowLabel, freshnessTone } from "../utils/format";

function HealthCard({ row }) {
  const tone = freshnessTone(
    row.latest_timestamp
      ? Math.floor((Date.now() - new Date(row.latest_timestamp).getTime()) / 1000)
      : null
  );
  return (
    <article className="health-card">
      <p className="eyebrow">{row.table_name}</p>
      <h3>{formatNumber(row.row_count, 0)}</h3>
      <span className={`health-badge health-badge--${tone}`}>
        {row.latest_timestamp ? formatWindowLabel(row.latest_timestamp) : "No rows yet"}
      </span>
    </article>
  );
}

export default function PipelinePage() {
  const { dashboard, loading, error } = useDashboard();
  const health = dashboard?.pipeline_health ?? [];
  const summary = dashboard?.summary;

  return (
    <>
      <div className="page-header">
        <h1 className="page-title">Pipeline Health</h1>
        <span className="muted">Developer view — internal data flow status</span>
      </div>

      {error && <section className="banner banner--error">{error}</section>}
      {loading && !dashboard && <section className="banner">Loading pipeline data…</section>}

      {/* Freshness summary */}
      <section className="panel" style={{ marginTop: 24 }}>
        <div className="panel-header">
          <h2>Pipeline Freshness <InfoTip text="How long ago the last Spark micro-batch finished writing analytics into ClickHouse. Under 2 minutes is healthy. Over 10 minutes means something is wrong." /></h2>
        </div>
        <div className="health-grid">
          <article className={`health-card health-card--large metric-card--${freshnessTone(summary?.freshness_seconds)}`}>
            <p className="eyebrow">Last batch completed</p>
            <h3>{formatFreshness(summary?.freshness_seconds)}</h3>
            <span>
              {summary?.freshness_seconds != null
                ? summary.freshness_seconds <= 120
                  ? "Pipeline is healthy"
                  : summary.freshness_seconds <= 600
                    ? "Pipeline is slightly delayed"
                    : "Pipeline may be stalled — check Spark logs"
                : "No processed data yet"}
            </span>
          </article>
          <article className="health-card">
            <p className="eyebrow">Tracked symbols</p>
            <h3>{formatNumber(summary?.tracked_symbols, 0)}</h3>
            <span>Symbols with live metrics</span>
          </article>
          <article className="health-card">
            <p className="eyebrow">Recent alerts</p>
            <h3>{formatNumber(summary?.recent_alert_count, 0)}</h3>
            <span>Volume spikes detected</span>
          </article>
        </div>
      </section>

      {/* Table row counts */}
      <section className="panel" style={{ marginTop: 24 }}>
        <div className="panel-header">
          <h2>ClickHouse Table Stats <InfoTip text="Row counts for each analytics table. raw_trades = incoming events from Kafka. candles_1m = 1-minute OHLCV windows computed by Spark. live_metrics = latest market metrics per symbol. alerts = detected spike events." /></h2>
          <span>Updated every Spark batch</span>
        </div>
        <div className="health-grid">
          {health.length === 0 ? (
            <p className="empty-state">No pipeline data yet.</p>
          ) : health.map((row) => (
            <HealthCard key={row.table_name} row={row} />
          ))}
        </div>
      </section>

      {/* Architecture explanation */}
      <section className="panel" style={{ marginTop: 24 }}>
        <div className="panel-header">
          <h2>Data Flow</h2>
        </div>
        <div style={{ padding: "16px 20px", color: "var(--muted)", lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px", fontFamily: "monospace", fontSize: "0.88rem" }}>
            Coinbase WebSocket
            → Python Producer
            → Kafka
            → Spark Streaming
            → ClickHouse
            → FastAPI SSE
            → Dashboard
          </p>
          <p style={{ margin: 0 }}>
            Spark reads raw trades from Kafka every micro-batch, computes 1-minute OHLCV candles,
            live metrics, and volume spike alerts, then writes the curated analytics tables into ClickHouse.
            The dashboard reads fresh snapshots from the API, which queries ClickHouse directly.
            Updates arrive via Server-Sent Events within ~1 second of each Spark batch completing.
          </p>
        </div>
      </section>
    </>
  );
}
