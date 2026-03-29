from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any


ALLOWED_TOPIC_KEYWORDS = {
    "alert",
    "alerts",
    "analytics",
    "batch",
    "candle",
    "candles",
    "change",
    "current",
    "dashboard",
    "fresh",
    "freshness",
    "health",
    "leader",
    "leaders",
    "market",
    "metric",
    "metrics",
    "move",
    "mover",
    "overview",
    "pipeline",
    "price",
    "signal",
    "spike",
    "state",
    "summarize",
    "summary",
    "symbol",
    "trade",
    "trades",
    "volume",
    "window",
}

REFUSAL_KEYWORDS = {
    "code",
    "fibonacci",
    "python",
    "recipe",
    "world cup",
    "weather",
    "capital",
    "essay",
}

# Financial advice keywords — even if the question contains dashboard terms,
# any phrasing that solicits a recommendation must be refused.
# A 0.8B model will happily give buy/sell advice using real dashboard numbers
# which looks credible but is dangerous. Block these before Qwen sees the question.
FINANCIAL_ADVICE_KEYWORDS = {
    "should i buy",
    "should i sell",
    "should i invest",
    "should i hold",
    "good time to buy",
    "good time to sell",
    "worth buying",
    "worth investing",
    "buying opportunity",
    "selling opportunity",
    "recommend buying",
    "recommend selling",
    "what should i",
    "tell me to buy",
    "tell me to sell",
    "is it safe to invest",
    "is it a good investment",
}


@lru_cache(maxsize=2)
def _load_mlx_model(model_source: str) -> tuple[Any, Any]:
    from mlx_lm import load

    return load(model_source)


def _format_price(value: float | None) -> str:
    if value is None:
        return "unknown price"
    if abs(value) >= 1000:
        return f"${value:,.2f}"
    return f"${value:,.4f}"


def _format_percent(value: float | None) -> str:
    if value is None:
        return "0.00%"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def _format_volume(value: float | None) -> str:
    if value is None:
        return "0"
    return f"{value:,.4f}".rstrip("0").rstrip(".")


def _safe_float(value: Any, default: float | None = None) -> float | None:
    """Convert a value to float safely, returning default on failure or NaN."""
    if value is None:
        return default
    try:
        result = float(value)
        # Reject NaN and Inf
        if result != result or result == float("inf") or result == float("-inf"):
            return default
        return result
    except (ValueError, TypeError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    """Convert a value to int safely, returning default on failure."""
    if value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


@dataclass(frozen=True)
class DashboardChatReply:
    answer: str
    refusal: bool
    citations: list[str]
    backend: str | None = None
    model_name: str | None = None
    warning: str | None = None

    def to_dict(self, backend: str, model_name: str) -> dict[str, Any]:
        payload = {
            "type": "chat",
            "backend": self.backend or backend,
            "model_name": self.model_name or model_name,
            "answer": self.answer,
            "refusal": self.refusal,
            "citations": self.citations,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        if self.warning:
            payload["warning"] = self.warning
        return payload


class DashboardChatRuntimeUnavailable(RuntimeError):
    pass


class DashboardChatService:
    def __init__(
        self,
        model_backend: str,
        model_name: str,
        model_source: str = "",
        max_tokens: int = 160,
    ):
        self.model_backend = model_backend
        self.model_name = model_name
        self.model_source = model_source.strip()
        self.max_tokens = max_tokens

    def answer_question(
        self,
        question: str,
        snapshot: dict[str, Any],
        history: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        history = history or []
        guardrail_reply = self._answer_from_guardrails(question=question, snapshot=snapshot)
        if guardrail_reply is not None:
            return guardrail_reply.to_dict(backend=self.model_backend, model_name=self.model_name)

        try:
            reply = self._answer_with_backend(
                question=question,
                snapshot=snapshot,
                history=history,
            )
        except DashboardChatRuntimeUnavailable as exc:
            fallback = self._answer_with_local_stub(question=question, snapshot=snapshot)
            reply = DashboardChatReply(
                answer=fallback.answer,
                refusal=fallback.refusal,
                citations=fallback.citations,
                backend="local_stub",
                model_name=self.model_name,
                warning=str(exc),
            )

        return reply.to_dict(backend=self.model_backend, model_name=self.model_name)

    def _answer_from_guardrails(
        self,
        question: str,
        snapshot: dict[str, Any],
    ) -> DashboardChatReply | None:
        cleaned_question = question.strip()
        if not cleaned_question:
            return DashboardChatReply(
                answer=(
                    "Ask me about this dashboard only, for example price, volume, alerts, "
                    "market leaders, or pipeline freshness."
                ),
                refusal=False,
                citations=[],
            )

        lowered = cleaned_question.lower()
        if any(keyword in lowered for keyword in REFUSAL_KEYWORDS) and not any(
            keyword in lowered for keyword in ALLOWED_TOPIC_KEYWORDS
        ):
            return DashboardChatReply(
                answer=(
                    "I can only answer questions about the current dashboard data, such as "
                    "prices, volume, alerts, market leaders, and pipeline health."
                ),
                refusal=True,
                citations=[],
            )

        # Financial advice guardrail — checked independently of REFUSAL_KEYWORDS
        # because these phrases often contain allowed dashboard keywords
        # (e.g. "should I buy based on the current market volume?").
        # Block before Qwen ever sees the question.
        if any(phrase in lowered for phrase in FINANCIAL_ADVICE_KEYWORDS):
            return DashboardChatReply(
                answer=(
                    "I can only report what the dashboard shows — prices, volume, and alerts. "
                    "I cannot give investment advice or buying/selling recommendations."
                ),
                refusal=True,
                citations=[],
            )

        if not any(keyword in lowered for keyword in ALLOWED_TOPIC_KEYWORDS) and not self._extract_symbol(
            lowered,
            snapshot,
        ):
            return DashboardChatReply(
                answer=(
                    "That question looks outside this dashboard. Ask me about the current "
                    "symbols, latest prices, alerts, market leaders, or pipeline freshness."
                ),
                refusal=True,
                citations=[],
            )

        if any(keyword in lowered for keyword in {"what can you answer", "help", "what can you do"}):
            return DashboardChatReply(
                answer=(
                    "I can help with the current dashboard only: latest symbol prices, 1-minute "
                    "changes, volume leaders, alerts, and pipeline freshness."
                ),
                refusal=False,
                citations=["summary", "market_overview", "pipeline_health"],
            )

        return None

    def _answer_with_backend(
        self,
        question: str,
        snapshot: dict[str, Any],
        history: list[dict[str, str]],
    ) -> DashboardChatReply:
        backend = self.model_backend.strip().lower()
        if backend in {"local_stub", "stub"}:
            return self._answer_with_local_stub(question=question, snapshot=snapshot)

        if backend in {"mlx", "mlx_qwen", "mlx_lm"}:
            return self._answer_with_mlx_qwen(
                question=question,
                snapshot=snapshot,
                history=history,
            )

        raise DashboardChatRuntimeUnavailable(
            f"Unsupported chat backend '{self.model_backend}'. Falling back to local_stub."
        )

    def _answer_with_local_stub(
        self,
        question: str,
        snapshot: dict[str, Any],
    ) -> DashboardChatReply:
        lowered = question.strip().lower()

        selected_symbol = self._extract_symbol(lowered, snapshot) or snapshot.get("selected_symbol")
        selected_overview = self._select_overview(snapshot, selected_symbol)

        if any(keyword in lowered for keyword in {"summarize", "summary", "overview", "current state", "what's happening", "what is happening"}):
            return self._answer_summary(snapshot)

        if any(keyword in lowered for keyword in {"alert", "alerts", "spike"}):
            return self._answer_alerts(snapshot, selected_symbol)

        if any(keyword in lowered for keyword in {"fresh", "freshness", "pipeline", "health", "stale", "batch"}):
            return self._answer_pipeline(snapshot)

        if any(keyword in lowered for keyword in {"volume leader", "top volume", "most volume"}):
            return self._answer_top_volume(snapshot)

        if any(keyword in lowered for keyword in {"mover", "move", "top move", "change leader"}):
            return self._answer_top_mover(snapshot)

        if any(keyword in lowered for keyword in {"price", "change", "volume", "trade", "trades"}) or selected_overview:
            return self._answer_symbol_metrics(selected_symbol, selected_overview)

        return DashboardChatReply(
            answer=(
                "I could not find enough dashboard context for that question. Try asking about "
                "a symbol price, alerts, market leaders, or pipeline freshness."
            ),
            refusal=False,
            citations=[],
        )

    def _answer_with_mlx_qwen(
        self,
        question: str,
        snapshot: dict[str, Any],
        history: list[dict[str, str]],
    ) -> DashboardChatReply:
        if not self.model_source:
            raise DashboardChatRuntimeUnavailable(
                "CHAT_MODEL_SOURCE is not set. Point it at a local MLX Qwen model path or model id."
            )

        try:
            from mlx_lm import generate
        except ImportError as exc:
            raise DashboardChatRuntimeUnavailable(
                "MLX chat runtime is not installed. Install `mlx-lm` with uv before using the Qwen chat backend."
            ) from exc

        try:
            model, tokenizer = _load_mlx_model(self.model_source)
        except Exception as exc:
            raise DashboardChatRuntimeUnavailable(
                f"Could not load the local chat model from '{self.model_source}'. "
                "Check that the MLX Qwen model exists locally and is readable."
            ) from exc

        prompt = self._build_model_prompt(
            question=question,
            snapshot=snapshot,
            history=history,
            tokenizer=tokenizer,
        )
        citations = self._select_citations(question=question, snapshot=snapshot)

        try:
            raw_response = generate(
                model,
                tokenizer,
                prompt=prompt,
                max_tokens=self.max_tokens,
                verbose=False,
            )
        except Exception as exc:
            raise DashboardChatRuntimeUnavailable(
                "The local MLX chat runtime failed during generation. Falling back to local_stub."
            ) from exc

        # Check for garbage on the RAW string before any cleanup strips the evidence
        raw_str = str(getattr(raw_response, "text", raw_response))
        if self._looks_like_garbage(raw_str):
            raise DashboardChatRuntimeUnavailable(
                "The local MLX model returned garbled or repetitive output. Falling back to local_stub."
            )

        answer = self._clean_model_answer(raw_response)
        if not answer:
            raise DashboardChatRuntimeUnavailable(
                "The local MLX model returned an empty answer. Falling back to local_stub."
            )

        # Post-generation hallucination check: if the model cited a price or numeric
        # value that doesn't exist anywhere in the snapshot, it made it up.
        # Fall back to local_stub which reads directly from the data.
        if self._answer_contains_hallucinated_numbers(answer, snapshot):
            raise DashboardChatRuntimeUnavailable(
                "The local MLX model cited numbers not present in the dashboard snapshot. "
                "Falling back to local_stub to ensure accuracy."
            )

        return DashboardChatReply(
            answer=answer,
            refusal=self._looks_like_refusal(answer),
            citations=citations,
            backend="mlx_qwen",
            model_name=self.model_name,
        )

    def _extract_symbol(self, lowered_question: str, snapshot: dict[str, Any]) -> str | None:
        for symbol in snapshot.get("symbols", []):
            if symbol.lower() in lowered_question:
                return symbol
        return None

    def _select_overview(self, snapshot: dict[str, Any], symbol: str | None) -> dict[str, Any] | None:
        if symbol is None:
            return snapshot.get("overview")

        for row in snapshot.get("market_overview", []):
            if row.get("product_id") == symbol:
                return row
        return None

    def _answer_symbol_metrics(
        self,
        symbol: str | None,
        overview: dict[str, Any] | None,
    ) -> DashboardChatReply:
        if not symbol or not overview:
            return DashboardChatReply(
                answer="I do not have processed market metrics for that symbol yet.",
                refusal=False,
                citations=["market_overview"],
            )

        price = _safe_float(overview.get("last_price_usd"))
        change_pct = _safe_float(overview.get("price_change_pct"), default=0.0)
        volume = _safe_float(overview.get("volume_qty"), default=0.0)
        trade_count = _safe_int(overview.get("trade_count"))

        return DashboardChatReply(
            answer=(
                f"{symbol} last traded at {_format_price(price)}, "
                f"with a 1-minute change of {_format_percent(change_pct)}, "
                f"volume of {_format_volume(volume)}, and "
                f"{trade_count} trades in the latest processed window."
            ),
            refusal=False,
            citations=["market_overview", "overview"],
        )

    def _answer_top_volume(self, snapshot: dict[str, Any]) -> DashboardChatReply:
        leaders = snapshot.get("market_leaders", {}).get("top_volume", [])
        if not leaders:
            return DashboardChatReply(
                answer="There is no processed top-volume leader yet.",
                refusal=False,
                citations=["market_leaders.top_volume"],
            )

        leader = leaders[0]
        return DashboardChatReply(
            answer=(
                f"{leader.get('product_id', 'Unknown')} is leading 1-minute volume with "
                f"{_format_volume(_safe_float(leader.get('volume_qty'), default=0.0))} units traded."
            ),
            refusal=False,
            citations=["market_leaders.top_volume"],
        )

    def _answer_top_mover(self, snapshot: dict[str, Any]) -> DashboardChatReply:
        leaders = snapshot.get("market_leaders", {}).get("top_movers", [])
        if not leaders:
            return DashboardChatReply(
                answer="There is no processed top mover yet.",
                refusal=False,
                citations=["market_leaders.top_movers"],
            )

        leader = leaders[0]
        return DashboardChatReply(
            answer=(
                f"{leader.get('product_id', 'Unknown')} is the strongest short-term mover at "
                f"{_format_percent(_safe_float(leader.get('price_change_pct'), default=0.0))} "
                f"in the latest processed window."
            ),
            refusal=False,
            citations=["market_leaders.top_movers"],
        )

    def _answer_pipeline(self, snapshot: dict[str, Any]) -> DashboardChatReply:
        summary = snapshot.get("summary", {})
        freshness_seconds = summary.get("freshness_seconds")
        pipeline_health = snapshot.get("pipeline_health", [])
        live_metrics_health = next(
            (row for row in pipeline_health if row.get("table_name") == "live_metrics"),
            None,
        )

        if freshness_seconds is None:
            return DashboardChatReply(
                answer="The pipeline does not have a fresh processed batch yet.",
                refusal=False,
                citations=["summary", "pipeline_health"],
            )

        return DashboardChatReply(
            answer=(
                f"The latest processed batch is {int(freshness_seconds)} seconds old, and the "
                f"live_metrics table currently has "
                f"{int(live_metrics_health['row_count']) if live_metrics_health else 0} rows."
            ),
            refusal=False,
            citations=["summary", "pipeline_health"],
        )

    def _answer_alerts(self, snapshot: dict[str, Any], symbol: str | None) -> DashboardChatReply:
        selected_alerts = snapshot.get("alerts", [])
        recent_alerts = snapshot.get("recent_alerts", [])

        if symbol and selected_alerts:
            latest = selected_alerts[0]
            spike = _safe_float(latest.get("spike_ratio"), default=0.0)
            return DashboardChatReply(
                answer=(
                    f"The latest alert for {symbol} is {latest.get('severity', 'unknown')} severity "
                    f"with a spike ratio of {spike:.2f}x."
                ),
                refusal=False,
                citations=["alerts"],
            )

        if recent_alerts:
            latest = recent_alerts[0]
            spike = _safe_float(latest.get("spike_ratio"), default=0.0)
            return DashboardChatReply(
                answer=(
                    f"The most recent market alert is for {latest.get('product_id', 'Unknown')}: "
                    f"{latest.get('severity', 'unknown')} severity with a {spike:.2f}x spike."
                ),
                refusal=False,
                citations=["recent_alerts"],
            )

        return DashboardChatReply(
            answer="There are no recent processed volume alerts in the dashboard right now.",
            refusal=False,
            citations=["alerts", "recent_alerts"],
        )

    def _answer_summary(self, snapshot: dict[str, Any]) -> DashboardChatReply:
        """Return a grounded text summary of the full current dashboard snapshot."""
        summary = snapshot.get("summary", {})
        market_leaders = snapshot.get("market_leaders", {})
        overview = snapshot.get("overview")

        tracked = _safe_int(summary.get("tracked_symbols"))
        freshness = _safe_int(summary.get("freshness_seconds")) if summary.get("freshness_seconds") is not None else None
        alert_count = _safe_int(summary.get("recent_alert_count"))
        top_movers = market_leaders.get("top_movers", [])
        top_volume = market_leaders.get("top_volume", [])

        parts: list[str] = []

        if tracked:
            parts.append(f"The dashboard is tracking {tracked} symbols")

        if freshness is not None:
            parts.append(f"the latest processed batch is {freshness}s old")

        if overview:
            sym = overview.get("product_id", "")
            price = _safe_float(overview.get("last_price_usd"))
            change = _safe_float(overview.get("price_change_pct"), default=0.0)
            if sym and price is not None:
                parts.append(
                    f"the selected symbol {sym} is at {_format_price(price)} "
                    f"({_format_percent(change)} 1-minute change)"
                )

        if top_movers:
            m = top_movers[0]
            m_pct = _safe_float(m.get("price_change_pct"), default=0.0)
            parts.append(
                f"the top mover is {m.get('product_id', 'Unknown')} at {_format_percent(m_pct)}"
            )

        if top_volume:
            v = top_volume[0]
            v_qty = _safe_float(v.get("volume_qty"), default=0.0)
            parts.append(
                f"the top volume is {v.get('product_id', 'Unknown')} "
                f"with {_format_volume(v_qty)} traded"
            )

        if alert_count:
            parts.append(f"there are {alert_count} recent volume alerts")

        if not parts:
            return DashboardChatReply(
                answer="No processed data is available in the dashboard yet.",
                refusal=False,
                citations=["summary"],
            )

        # Capitalise first sentence, join the rest with semicolons for readability
        first = parts[0][0].upper() + parts[0][1:]
        if len(parts) == 1:
            answer = first + "."
        else:
            answer = first + "; " + "; ".join(parts[1:]) + "."

        return DashboardChatReply(
            answer=answer,
            refusal=False,
            citations=["summary", "market_overview", "market_leaders"],
        )

    def _build_model_prompt(
        self,
        question: str,
        snapshot: dict[str, Any],
        history: list[dict[str, str]],
        tokenizer: Any,
    ) -> str:
        context = self._build_context(snapshot)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a dashboard-only crypto analytics assistant. "
                    "Answer ONLY from the provided dashboard context JSON. "
                    "Allowed topics: symbol prices, 1-minute changes, volume, trades, "
                    "market leaders, recent alerts, and pipeline freshness or health. "
                    "Refuse anything outside this dashboard. "
                    "Rules: answer in 1-3 plain English sentences. "
                    "Format prices as $X.XX (max 4 decimal places). "
                    "Format percentages as +X.XX% or -X.XX%. "
                    "Never output raw long decimal numbers. "
                    "Never use markdown, lists, or code. "
                    "End with a period. If data is missing say it is not available yet."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Dashboard context:\n"
                    f"{json.dumps(context, indent=2, sort_keys=True)}\n\n"
                    f"Question: {question}\n\n"
                    "Answer (1-3 sentences, plain text only):"
                ),
            },
        ]
        if hasattr(tokenizer, "apply_chat_template"):
            return tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True,
            )

        return "\n\n".join(f"{message['role'].upper()}: {message['content']}" for message in messages)

    def _build_context(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            "selected_symbol": snapshot.get("selected_symbol"),
            "symbols": snapshot.get("symbols", []),
            "summary": snapshot.get("summary", {}),
            "overview": snapshot.get("overview"),
            "market_overview": snapshot.get("market_overview", [])[:10],
            "market_leaders": {
                "top_movers": snapshot.get("market_leaders", {}).get("top_movers", [])[:3],
                "top_volume": snapshot.get("market_leaders", {}).get("top_volume", [])[:3],
            },
            "alerts": snapshot.get("alerts", [])[:5],
            "recent_alerts": snapshot.get("recent_alerts", [])[:5],
            "pipeline_health": snapshot.get("pipeline_health", [])[:5],
        }

    def _format_history(self, history: list[dict[str, str]]) -> str:
        cleaned_lines = []
        for message in history[-6:]:
            role = message.get("role", "user").strip().lower()
            if role not in {"user", "assistant"}:
                continue
            content = message.get("content", "").strip()
            if not content:
                continue
            cleaned_lines.append(f"{role}: {content}")

        if not cleaned_lines:
            return "No prior conversation."

        return "\n".join(cleaned_lines)

    def _clean_model_answer(self, raw_response: Any) -> str:
        if hasattr(raw_response, "text"):
            answer = str(raw_response.text)
        else:
            answer = str(raw_response)

        cleaned = answer.strip()

        # Strip common LLM response prefixes
        for prefix in ("assistant:", "Assistant:", "answer:", "Answer:"):
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix) :].strip()

        # Strip garbage: long zero-decimal tails like 0.000000000000 or 1.0000000000
        # These are loop artifacts from small LLMs getting stuck on numeric tokens
        cleaned = re.sub(r"\b(\d+\.\d{1,4})0{5,}\d*", r"\1", cleaned)
        cleaned = re.sub(r"\b0\.0{5,}\d*", "0.00", cleaned)

        # Strip parenthesised garbage tail like "(0.0000000000000000000...)"
        cleaned = re.sub(r"\s*\(\s*0+\.0+[.]*\s*\)", "", cleaned)

        # Strip trailing "..." that ends with digits
        cleaned = re.sub(r"\d[.]{2,}$", ".", cleaned.rstrip())

        # Truncate to a hard maximum of 500 characters at a sentence boundary
        if len(cleaned) > 500:
            boundary = re.search(r"[.!?]\s", cleaned[200:500])
            if boundary:
                cleaned = cleaned[: 200 + boundary.start() + 1]
            else:
                cleaned = cleaned[:500].rstrip()

        return cleaned.strip()

    def _looks_like_garbage(self, answer: str) -> bool:
        """Return True if the LLM output appears to be repetitive junk."""
        # Detect long runs of zero-decimal digits e.g. 0.000000000 or 1.000000000000
        if re.search(r"\d\.\d{0,2}0{7,}", answer):
            return True
        # Detect obvious character repetition (same char 12+ times consecutively)
        if re.search(r"(.)\1{11,}", answer):
            return True
        # Answer is suspiciously short after cleaning (model got stuck immediately)
        if len(answer.strip()) < 8:
            return True
        # Detect raw JSON structure leaking out of the model
        if re.search(r"\{.*\}", answer, re.DOTALL):
            return True
        # Detect numbered list loops: model stuck repeating "1. 2. 3." or "1. ... 1. ... 1."
        # These appear when the model tries to write a list but loops back to the start.
        numbered_items = re.findall(r"\b1\.\s", answer)
        if len(numbered_items) >= 3:
            return True
        # Detect phrase-level repetition: same sentence fragment repeated 3+ times
        # Split into short chunks and look for repeated segments
        words = answer.split()
        if len(words) >= 12:
            chunk_size = 6
            chunks = [" ".join(words[i: i + chunk_size]) for i in range(0, len(words) - chunk_size, chunk_size)]
            if len(chunks) != len(set(chunks)) and len(chunks) - len(set(chunks)) >= 2:
                return True
        return False

    def _extract_numbers_from_text(self, text: str) -> list[float]:
        """Extract all numeric values (prices, percentages, counts) from a text string."""
        raw = re.findall(r"\$?([\d,]+(?:\.\d+)?)", text)
        results = []
        for token in raw:
            try:
                results.append(float(token.replace(",", "")))
            except ValueError:
                pass
        return results

    def _build_allowed_numbers(self, snapshot: dict[str, Any]) -> set[float]:
        """Collect all numeric values present in the snapshot that the model is
        allowed to reference. Any number the model cites must appear here
        (within a small tolerance) or it's a hallucination."""
        allowed: set[float] = set()

        def _collect(obj: Any) -> None:
            if isinstance(obj, dict):
                for value in obj.values():
                    _collect(value)
            elif isinstance(obj, list):
                for item in obj:
                    _collect(item)
            elif isinstance(obj, (int, float)):
                f = float(obj)
                if f == f and f != float("inf") and f != float("-inf"):
                    allowed.add(round(f, 4))

        _collect(snapshot)
        return allowed

    def _answer_contains_hallucinated_numbers(
        self,
        answer: str,
        snapshot: dict[str, Any],
    ) -> bool:
        """Return True if the model's answer contains dollar/numeric values that
        do not appear anywhere in the snapshot data.

        Only prices >= $0.01 are checked — single-digit counts and percentages
        near zero are too common to validate meaningfully.
        Strategy: for each number in the answer, check if any allowed snapshot
        number is within 0.5% of it. If a significant price is totally absent
        from the snapshot, the model made it up.
        """
        answer_numbers = [n for n in self._extract_numbers_from_text(answer) if n >= 0.01]
        if not answer_numbers:
            return False

        allowed = self._build_allowed_numbers(snapshot)
        if not allowed:
            return False

        for num in answer_numbers:
            # Skip small counts / percentage-like values (< 1000) that are
            # likely trade counts or % figures where exact match isn't critical
            if num < 1.0:
                continue
            # Check if any allowed number is within 0.5% of this one
            close_match = any(
                abs(num - a) / max(abs(a), 1e-9) < 0.005
                for a in allowed
            )
            if not close_match:
                return True

        return False

    def _looks_like_refusal(self, answer: str) -> bool:
        lowered = answer.lower()
        return any(
            phrase in lowered
            for phrase in {
                "only answer questions about the current dashboard",
                "outside this dashboard",
                "outside the provided dashboard context",
                "not available in the current processed dashboard data",
            }
        )

    def _select_citations(self, question: str, snapshot: dict[str, Any]) -> list[str]:
        del snapshot
        lowered = question.lower()
        if any(keyword in lowered for keyword in {"alert", "alerts", "spike"}):
            return ["alerts", "recent_alerts"]
        if any(keyword in lowered for keyword in {"fresh", "freshness", "pipeline", "health", "stale", "batch"}):
            return ["summary", "pipeline_health"]
        if any(keyword in lowered for keyword in {"volume leader", "top volume", "most volume"}):
            return ["market_leaders.top_volume"]
        if any(keyword in lowered for keyword in {"mover", "move", "top move", "change leader"}):
            return ["market_leaders.top_movers"]
        if any(keyword in lowered for keyword in {"price", "change", "volume", "trade", "trades"}):
            return ["market_overview", "overview"]
        return ["summary", "market_overview", "pipeline_health"]
