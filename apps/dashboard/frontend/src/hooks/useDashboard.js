import { startTransition, useEffect, useState } from "react";

/**
 * Opens an SSE connection to /api/dashboard/stream and keeps dashboard
 * state fresh. Shared by all pages so only one SSE connection is open at a time.
 */
export function useDashboard(selectedSymbol) {
  const [dashboard, setDashboard] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    const params = new URLSearchParams();
    if (selectedSymbol) params.set("symbol", selectedSymbol);
    const url = `/api/dashboard/stream?${params.toString()}`;
    const evtSource = new EventSource(url);

    setLoading(true);

    evtSource.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.sse_error) {
          startTransition(() => {
            setError(`Server error: ${payload.sse_error}`);
            setLoading(false);
          });
          return;
        }
        startTransition(() => {
          setDashboard(payload);
          setError("");
          setLoading(false);
        });
      } catch (e) {
        console.error("SSE parse error:", e);
      }
    };

    evtSource.onerror = () => {
      startTransition(() => setError("Live connection interrupted — reconnecting…"));
    };

    return () => evtSource.close();
  }, [selectedSymbol]);

  return { dashboard, loading, error };
}
