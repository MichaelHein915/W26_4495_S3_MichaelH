// Shared formatting helpers used across all pages

export function formatPrice(value) {
  if (value === null || value === undefined) return "--";
  if (Math.abs(value) >= 1000) {
    return new Intl.NumberFormat("en-US", {
      style: "currency", currency: "USD", maximumFractionDigits: 2,
    }).format(value);
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 4,
  }).format(value);
}

export function formatNumber(value, maximumFractionDigits = 2) {
  if (value === null || value === undefined) return "--";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits }).format(value);
}

export function formatUsdAmount(value) {
  if (value === null || value === undefined) return "--";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: Math.abs(value) >= 10000 ? "compact" : "standard",
    maximumFractionDigits: Math.abs(value) >= 10000 ? 1 : 2,
  }).format(value);
}

export function formatPercent(value) {
  if (value === null || value === undefined) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

export function formatFreshness(seconds) {
  if (seconds === null || seconds === undefined) return "No recent batch";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ago`;
}

export function formatWindowLabel(isoValue) {
  if (!isoValue) return "--";
  return new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(new Date(isoValue));
}

export function formatAxisLabel(isoValue) {
  if (!isoValue) return "--";
  return new Intl.DateTimeFormat("en-US", {
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(isoValue));
}

export function freshnessTone(seconds) {
  if (seconds === null || seconds === undefined) return "negative";
  if (seconds <= 120) return "positive";
  if (seconds <= 600) return "neutral";
  return "negative";
}
