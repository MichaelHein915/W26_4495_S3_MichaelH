import { describe, expect, it } from "vitest";

import { formatAxisLabel, formatWindowLabel } from "./format";

describe("format helpers", () => {
  it("formats absolute timestamps with calendar context", () => {
    const formatted = formatWindowLabel("2026-03-19T10:00:00Z");

    expect(formatted).toContain("2026");
    expect(formatted.length).toBeGreaterThan(formatAxisLabel("2026-03-19T10:00:00Z").length);
  });

  it("formats axis labels as compact intraday times", () => {
    expect(formatAxisLabel("2026-03-19T10:00:00Z")).toMatch(/\d{1,2}:\d{2}/);
  });
});
