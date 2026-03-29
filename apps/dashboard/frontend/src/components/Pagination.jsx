import { useEffect, useMemo, useState } from "react";

/**
 * Generic pagination hook.
 *
 * Usage:
 *   const { page, setPage, pageItems, totalPages } = usePagination(rows, 10);
 *
 * - `items`    full array to paginate
 * - `pageSize` rows per page (default 10)
 *
 * Returns:
 *   page       current page number (1-based)
 *   setPage    navigate to a specific page (clamped to valid range)
 *   pageItems  slice of items for the current page
 *   totalPages total number of pages
 */
export function usePagination(items, pageSize = 10) {
  const [page, setPageRaw] = useState(1);

  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));

  // Build a lightweight fingerprint of item identity so we detect
  // content changes even when items.length stays the same.
  const fingerprint = useMemo(() => {
    if (items.length === 0) return "";
    // Sample first, middle, last items to keep it cheap
    const first = items[0];
    const mid = items[Math.floor(items.length / 2)];
    const last = items[items.length - 1];
    const pick = (o) =>
      o ? `${o.product_id ?? ""}|${o.window_start ?? ""}` : "";
    return `${items.length}:${pick(first)}:${pick(mid)}:${pick(last)}`;
  }, [items]);

  // Reset to page 1 when item identity changes (length, order, or content)
  const prevFingerprintRef = useMemo(() => ({ current: fingerprint }), []); // eslint-disable-line
  useEffect(() => {
    if (fingerprint !== prevFingerprintRef.current) {
      prevFingerprintRef.current = fingerprint;
      setPageRaw(1);
    }
  }, [fingerprint]); // eslint-disable-line

  // Clamp page to valid range
  useEffect(() => {
    if (page > totalPages) setPageRaw(totalPages);
  }, [totalPages, page]);

  const setPage = (n) => setPageRaw(Math.max(1, Math.min(n, totalPages)));

  const pageItems = useMemo(
    () => items.slice((page - 1) * pageSize, page * pageSize),
    [items, page, pageSize],
  );

  return { page, setPage, pageItems, totalPages };
}

/**
 * Renders page navigation controls.
 *
 * Props:
 *   page        current page (1-based)
 *   totalPages  total number of pages
 *   onPage      callback(n) when the user clicks a page button
 *   totalItems  optional – renders "start–end of N" summary on the left
 *   pageSize    rows per page (used for the summary line)
 */
export function Pagination({ page, totalPages, onPage, totalItems, pageSize = 10 }) {
  if (totalPages <= 1) return null;

  const start = (page - 1) * pageSize + 1;
  const end = Math.min(page * pageSize, totalItems ?? page * pageSize);
  const pages = buildPageList(page, totalPages);

  return (
    <div className="pagination">
      {totalItems != null && (
        <span className="pagination__summary">
          {start}–{end} of {totalItems}
        </span>
      )}
      <div className="pagination__controls">
        <button
          className="pagination__btn"
          onClick={() => onPage(page - 1)}
          disabled={page === 1}
          aria-label="Previous page"
        >
          ‹
        </button>

        {pages.map((p, i) =>
          p === "…" ? (
            <span key={`gap-${i}`} className="pagination__gap">…</span>
          ) : (
            <button
              key={p}
              className={`pagination__btn${p === page ? " pagination__btn--active" : ""}`}
              onClick={() => onPage(p)}
              aria-current={p === page ? "page" : undefined}
            >
              {p}
            </button>
          ),
        )}

        <button
          className="pagination__btn"
          onClick={() => onPage(page + 1)}
          disabled={page === totalPages}
          aria-label="Next page"
        >
          ›
        </button>
      </div>
    </div>
  );
}

/** Returns a compact page list with "…" gaps: [1, "…", 4, 5, 6, "…", 12] */
function buildPageList(current, total) {
  if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);

  const keep = new Set(
    [1, total, current, current - 1, current + 1].filter((p) => p >= 1 && p <= total),
  );
  const sorted = [...keep].sort((a, b) => a - b);

  const result = [];
  for (let i = 0; i < sorted.length; i++) {
    if (i > 0 && sorted[i] - sorted[i - 1] > 1) result.push("…");
    result.push(sorted[i]);
  }
  return result;
}
