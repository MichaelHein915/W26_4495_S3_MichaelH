import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";

vi.mock("react", async () => {
  const actual = await vi.importActual("react");
  return {
    ...actual,
    startTransition: (callback) => callback(),
  };
});

vi.mock("recharts", () => {
  const MockComponent = ({ children }) => <div>{children}</div>;
  return {
    ResponsiveContainer: MockComponent,
    BarChart: MockComponent,
    Bar: MockComponent,
    CartesianGrid: MockComponent,
    LineChart: MockComponent,
    Line: MockComponent,
    Tooltip: MockComponent,
    XAxis: MockComponent,
    YAxis: MockComponent,
  };
});

vi.mock("./hooks/useDashboard", () => ({
  useDashboard: vi.fn(),
}));

import App from "./App";
import { useDashboard } from "./hooks/useDashboard";

const dashboardPayload = {
  symbols: ["BTC-USD", "ETH-USD"],
  selected_symbol: "BTC-USD",
  summary: {
    tracked_symbols: 2,
    symbols_with_live_metrics: 2,
    selected_symbol_alerts: 1,
    recent_alert_count: 1,
    last_updated_at: "2026-03-19T10:01:00Z",
    freshness_seconds: 45,
  },
  overview: {
    product_id: "BTC-USD",
    last_price_usd: 102.5,
    price_change_pct: 2.5,
    volume_qty: 4.5,
    notional_volume_usd: 461.25,
    trade_count: 4,
    vwap_usd: 101.7,
    volatility_usd: 1.0,
  },
  market_overview: [
    {
      product_id: "ETH-USD",
      last_price_usd: 2400.0,
      price_change_pct: 1.1,
      volume_qty: 2.0,
      notional_volume_usd: 4800.0,
      trade_count: 7,
      window_start: "2026-03-19T10:00:00Z",
      vwap_usd: 2395.0,
      volatility_usd: 8.0,
    },
    {
      product_id: "BTC-USD",
      last_price_usd: 102.5,
      price_change_pct: 2.5,
      volume_qty: 4.5,
      notional_volume_usd: 461.25,
      trade_count: 4,
      window_start: "2026-03-19T10:00:00Z",
      vwap_usd: 101.7,
      volatility_usd: 1.0,
    },
  ],
  market_leaders: {
    top_movers: [
      {
        product_id: "BTC-USD",
        price_change_pct: 2.5,
      },
    ],
    top_volume: [
      {
        product_id: "ETH-USD",
        notional_volume_usd: 4800.0,
      },
    ],
  },
  pipeline_health: [
    {
      table_name: "live_metrics",
      row_count: 2,
      latest_timestamp: "2026-03-19T10:01:00Z",
    },
  ],
  candles: [
    {
      product_id: "BTC-USD",
      window_start: "2026-03-19T10:00:00Z",
      window_end: "2026-03-19T10:01:00Z",
      open_price: 100.0,
      high_price: 103.0,
      low_price: 99.0,
      close_price: 102.5,
      volume_qty: 4.5,
      trade_count: 4,
      vwap_usd: 101.7,
    },
  ],
  alerts: [
    {
      product_id: "BTC-USD",
      window_start: "2026-03-19T10:00:00Z",
      severity: "high",
      spike_ratio: 3.0,
      volume_qty: 4.5,
      baseline_volume_qty: 1.5,
    },
  ],
  recent_alerts: [
    {
      product_id: "BTC-USD",
      window_start: "2026-03-19T10:00:00Z",
      severity: "high",
      spike_ratio: 3.0,
      volume_qty: 4.5,
      baseline_volume_qty: 1.5,
    },
  ],
  anomaly_signals: [
    {
      id: "vwap_deviation",
      label: "Above VWAP",
      value_str: "+0.79%",
      description: "Buyers are slightly in control this window.",
      severity: "info",
      direction: "positive",
    },
  ],
};

function renderApp(route = "/") {
  return render(
    <MemoryRouter initialEntries={[route]}>
      <App />
    </MemoryRouter>,
  );
}

describe("App", () => {
  beforeEach(() => {
    vi.mocked(useDashboard).mockImplementation(() => ({
      dashboard: dashboardPayload,
      loading: false,
      error: "",
    }));
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it("renders the market overview route", () => {
    renderApp("/");

    expect(screen.getByText("Live market signals.")).toBeInTheDocument();
    expect(screen.getByText("Top Movers")).toBeInTheDocument();
    expect(screen.getByText("Top Notional Volume")).toBeInTheDocument();
    expect(screen.getByText("All Symbols")).toBeInTheDocument();
    expect(screen.getByText("What to check next")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Open chart" })).toBeInTheDocument();
    expect(screen.getByText("Bullish follow-through")).toBeInTheDocument();
    expect(screen.getByText("Above VWAP")).toBeInTheDocument();
    expect(screen.getAllByText("$4,800.00")).toHaveLength(2);
  });

  it("renders the symbol detail route", () => {
    renderApp("/symbol/BTC-USD");

    expect(screen.getByText("BTC-USD")).toBeInTheDocument();
    expect(screen.getByText("Market Signals")).toBeInTheDocument();
    expect(screen.getByText("OHLCV Candles")).toBeInTheDocument();
    expect(screen.getByText("Volume per Minute")).toBeInTheDocument();
  });

  it("renders the pipeline route", () => {
    renderApp("/pipeline");

    expect(screen.getByText("Pipeline Health")).toBeInTheDocument();
    expect(screen.getByText("ClickHouse Table Stats")).toBeInTheDocument();
    expect(screen.getByText("Data Flow")).toBeInTheDocument();
  });

  it("falls back to the market overview for unknown routes", () => {
    renderApp("/missing");

    expect(screen.getByText("Live market signals.")).toBeInTheDocument();
    expect(screen.getByText("All Symbols")).toBeInTheDocument();
  });
});
