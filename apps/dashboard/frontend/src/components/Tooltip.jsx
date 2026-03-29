import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

/**
 * Wraps any element with a tooltip that shows on hover OR click.
 * Clicking the host stops propagation so row-level navigation doesn't fire.
 *
 * Usage: <Tooltip text="What this metric means">…children…</Tooltip>
 */
export function Tooltip({ text, children }) {
  const [visible, setVisible] = useState(false);
  const [position, setPosition] = useState({ left: 0, top: 0, placement: "top" });
  const hostRef = useRef(null);

  // Close when the user clicks anywhere outside this tooltip
  useEffect(() => {
    if (!visible) return;
    function handleOutside(e) {
      if (hostRef.current && !hostRef.current.contains(e.target)) {
        setVisible(false);
      }
    }
    document.addEventListener("mousedown", handleOutside);
    return () => document.removeEventListener("mousedown", handleOutside);
  }, [visible]);

  useLayoutEffect(() => {
    if (!visible || !hostRef.current) return;

    function updatePosition() {
      const rect = hostRef.current.getBoundingClientRect();
      const tooltipWidth = 240;
      const margin = 12;
      const left = Math.min(
        Math.max(rect.left + rect.width / 2 - tooltipWidth / 2, margin),
        window.innerWidth - tooltipWidth - margin,
      );
      const placeBelow = rect.top < 96;

      setPosition({
        left,
        top: placeBelow ? rect.bottom + 10 : rect.top - 10,
        placement: placeBelow ? "bottom" : "top",
      });
    }

    updatePosition();
    window.addEventListener("resize", updatePosition);
    window.addEventListener("scroll", updatePosition, true);

    return () => {
      window.removeEventListener("resize", updatePosition);
      window.removeEventListener("scroll", updatePosition, true);
    };
  }, [visible]);

  function handleClick(e) {
    e.stopPropagation(); // prevent row-click navigation from firing
    setVisible((v) => !v);
  }

  return (
    <>
      <span
        ref={hostRef}
        className="tooltip-host"
        onMouseEnter={() => setVisible(true)}
        onMouseLeave={() => setVisible(false)}
        onClick={handleClick}
      >
        {children}
      </span>
      {visible && text && typeof document !== "undefined" && createPortal(
        <span
          className={`tooltip-bubble tooltip-bubble--${position.placement}`}
          role="tooltip"
          style={{
            left: `${position.left}px`,
            top: `${position.top}px`,
          }}
        >
          {text}
        </span>,
        document.body,
      )}
    </>
  );
}

/**
 * Small ⓘ icon that shows a tooltip on hover or click.
 * Safe to place inside clickable table rows — click is intercepted.
 */
export function InfoTip({ text }) {
  return (
    <Tooltip text={text}>
      <span className="info-tip" aria-label="More information">ⓘ</span>
    </Tooltip>
  );
}

const SEVERITY_EXPLANATIONS = {
  low: "Low — volume is somewhat above normal. Not alarming, but worth a glance. Typically 1.5–2× the rolling baseline.",
  medium: "Medium — volume is notably elevated. Something is moving this market. Usually 2–4× the baseline. Keep an eye on it.",
  high: "High — extreme volume spike, well above the rolling baseline (4×+). Significant market activity is happening right now. This is the signal that warrants immediate attention.",
};

/**
 * Severity pill with a built-in click-to-explain tooltip.
 * Click the pill itself to see a plain-English explanation of the severity level.
 */
export function SeverityPill({ severity }) {
  const level = (severity ?? "").toLowerCase();
  const explanation = SEVERITY_EXPLANATIONS[level] ?? "Volume spike detected.";
  return (
    <Tooltip text={explanation}>
      <span className={`pill pill--${level}`} style={{ cursor: "help" }}>
        {severity}
      </span>
    </Tooltip>
  );
}
