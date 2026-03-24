"""
Flask Blueprint for CryptoStream AI assistant endpoints.

Provides /health, /chat (SSE streaming), /insights, and /query routes.
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone

from flask import Blueprint, Response, current_app, jsonify, request

from utils.config import get_config
from utils.context_builder import build_market_context, build_insight_prompt, build_query_prompt
from utils.llm_service import MLXClient

log = logging.getLogger(__name__)

ai_bp = Blueprint("ai", __name__)

_ai_state = {
    "client": None,
    "latest_insight": None,
    "insight_updated_at": None,
    "insight_error": None,
}
_ai_lock = threading.Lock()


def _get_mlx_client() -> MLXClient | None:
    """Lazy-init the MLX LLM client."""
    with _ai_lock:
        if _ai_state["client"] is not None:
            return _ai_state["client"]
    config = get_config()
    if not config.ai_enabled:
        return None
    client = MLXClient(
        base_url=config.mlx_server_url,
        model=config.mlx_model,
        timeout=config.ai_request_timeout_sec,
    )
    with _ai_lock:
        _ai_state["client"] = client
    return client


def start_insight_loop(get_dashboard_payload, is_running, window_minutes):
    """Launch the background insight-generation thread. Call once at startup."""
    config = get_config()
    if not config.ai_enabled:
        return

    def _insight_loop():
        interval = config.ai_insight_interval_sec
        time.sleep(min(interval, 30))

        while is_running():
            try:
                client = _get_mlx_client()
                if client is None:
                    time.sleep(interval)
                    continue

                payload = get_dashboard_payload(window_minutes, "")
                if not payload.get("metrics"):
                    time.sleep(10)
                    continue

                prompt = build_insight_prompt(payload)
                insight = client.generate(prompt)

                with _ai_lock:
                    _ai_state["latest_insight"] = insight
                    _ai_state["insight_updated_at"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                    _ai_state["insight_error"] = None
            except Exception:
                log.exception("Insight generation failed")
                with _ai_lock:
                    _ai_state["insight_error"] = str(Exception)

            time.sleep(interval)

    t = threading.Thread(target=_insight_loop, daemon=True)
    t.start()
    log.info(
        "AI assistant enabled (model=%s, insight_interval=%ds)",
        config.mlx_model,
        config.ai_insight_interval_sec,
    )
    return t


@ai_bp.route("/health")
def ai_health():
    """Check AI assistant availability."""
    config = get_config()
    if not config.ai_enabled:
        return jsonify({"enabled": False, "status": "disabled"}), 200

    client = _get_mlx_client()
    if client is None:
        return jsonify({"enabled": True, "status": "client_init_failed"}), 503

    health = client.health_check()
    code = 200 if health.get("status") == "ok" else 503
    return jsonify({"enabled": True, **health}), code


@ai_bp.route("/chat", methods=["POST"])
def ai_chat():
    """Conversational AI with live market context. Supports SSE streaming."""
    config = get_config()
    if not config.ai_enabled:
        return jsonify({"error": "AI assistant is disabled"}), 503

    client = _get_mlx_client()
    if client is None:
        return jsonify({"error": "AI service unavailable"}), 503

    body = request.get_json(silent=True) or {}
    user_message = body.get("message", "").strip()
    if not user_message:
        return jsonify({"error": "message is required"}), 400

    history = body.get("history", [])
    stream = body.get("stream", True)

    window_minutes = current_app.config["WINDOW_MINUTES"]
    window = int(body.get("window", window_minutes))
    window = max(1, min(window, 30))
    get_payload = current_app.config["get_dashboard_payload"]
    payload = get_payload(window, "")
    context = build_market_context(payload)

    messages = []
    for msg in history[-20:]:
        role = msg.get("role", "user")
        if role in ("user", "assistant"):
            messages.append({"role": role, "content": msg.get("content", "")})
    messages.append({"role": "user", "content": user_message})

    if stream:

        def generate():
            for chunk in client.chat(messages, context=context, stream=True):
                yield f"data: {json.dumps({'content': chunk})}\n\n"
            yield "data: [DONE]\n\n"

        return Response(generate(), mimetype="text/event-stream")

    result = client.chat(messages, context=context, stream=False)
    return jsonify(result)


@ai_bp.route("/insights")
def ai_insights():
    """Return the latest auto-generated market insight summary."""
    config = get_config()
    if not config.ai_enabled:
        return jsonify({"enabled": False}), 200

    with _ai_lock:
        insight = _ai_state["latest_insight"]
        updated_at = _ai_state["insight_updated_at"]
        error = _ai_state["insight_error"]

    return jsonify(
        {
            "enabled": True,
            "insight": insight,
            "updated_at": updated_at,
            "error": error,
        }
    )


@ai_bp.route("/query", methods=["POST"])
def ai_query():
    """Natural-language data query — LLM parses intent and returns structured data."""
    config = get_config()
    if not config.ai_enabled:
        return jsonify({"error": "AI assistant is disabled"}), 503

    client = _get_mlx_client()
    if client is None:
        return jsonify({"error": "AI service unavailable"}), 503

    body = request.get_json(silent=True) or {}
    query = body.get("query", "").strip()
    if not query:
        return jsonify({"error": "query is required"}), 400

    window_minutes = current_app.config["WINDOW_MINUTES"]
    window = int(body.get("window", window_minutes))
    window = max(1, min(window, 30))
    get_payload = current_app.config["get_dashboard_payload"]
    payload = get_payload(window, "")
    prompt = build_query_prompt(query, payload)

    raw = client.generate(prompt)

    try:
        result = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        result = {"answer": raw, "symbols": [], "exchanges": [], "data": []}

    return jsonify(result)
