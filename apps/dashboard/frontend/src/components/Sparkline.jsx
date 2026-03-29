/**
 * Inline mini-chart showing the last N close prices for a symbol.
 * Renders as a tiny SVG polyline — green if trending up, red if down.
 *
 * Usage:
 *   <Sparkline prices={[100, 101.2, 100.8, 102.4, 103.1]} />
 */
export function Sparkline({ prices }) {
  if (!prices || prices.length < 2) {
    return <span className="sparkline sparkline--empty">–</span>;
  }

  const W = 56;
  const H = 22;
  const PAD = 1; // pixel padding so line doesn't clip at edges

  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const range = max - min || Math.max(max * 0.001, 0.01); // avoid flat line on zero range

  const pts = prices.map((p, i) => {
    const x = PAD + (i / (prices.length - 1)) * (W - PAD * 2);
    const y = H - PAD - ((p - min) / range) * (H - PAD * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  const isUp   = prices[prices.length - 1] >= prices[0];
  const stroke = isUp ? "#4ade80" : "#f87171";

  return (
    <svg
      width={W}
      height={H}
      viewBox={`0 0 ${W} ${H}`}
      className="sparkline"
      aria-hidden="true"
    >
      <polyline
        points={pts.join(" ")}
        fill="none"
        stroke={stroke}
        strokeWidth="1.8"
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      {/* Dot at the last price */}
      <circle
        cx={pts[pts.length - 1].split(",")[0]}
        cy={pts[pts.length - 1].split(",")[1]}
        r="2.2"
        fill={stroke}
      />
    </svg>
  );
}

/**
 * Compact trend arrow showing direction across the sparkline window.
 *
 * Usage:
 *   <TrendArrow prices={[100, 101, 102]} />
 */
export function TrendArrow({ prices }) {
  if (!prices || prices.length < 2) return null;

  const first = prices[0];
  const last  = prices[prices.length - 1];
  const pct   = ((last - first) / Math.max(Math.abs(first), 0.0001)) * 100;

  if (Math.abs(pct) < 0.05) {
    return <span className="trend-arrow trend-arrow--flat" title="Flat over recent candles">→</span>;
  }
  if (pct > 0) {
    return (
      <span className="trend-arrow trend-arrow--up" title={`+${pct.toFixed(2)}% over recent candles`}>
        ↑
      </span>
    );
  }
  return (
    <span className="trend-arrow trend-arrow--down" title={`${pct.toFixed(2)}% over recent candles`}>
      ↓
    </span>
  );
}
