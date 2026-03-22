"""
MLX LLM service using the OpenAI-compatible API provided by mlx_lm.server.
Provides chat, generate, streaming, and health-check capabilities.
"""

import json
import logging
from typing import Generator

import requests

log = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are CryptoStream AI, an expert cryptocurrency market analyst embedded in a "
    "real-time trading dashboard. You have access to live market data from Coinbase, "
    "Binance, and Kraken exchanges.\n\n"
    "Guidelines:\n"
    "- Be concise and data-driven. Cite specific numbers from the provided market data.\n"
    "- When analyzing price movements, consider volume, volatility, and cross-exchange spreads.\n"
    "- Flag anomalies, arbitrage opportunities, and unusual volume spikes when relevant.\n"
    "- Use markdown formatting: **bold** for emphasis, tables for comparisons, bullet points for lists.\n"
    "- If you don't have enough data to answer confidently, say so.\n"
    "- Never give financial advice. Always clarify that your analysis is informational only."
)


class MLXClient:
    """HTTP client for the MLX LM server (OpenAI-compatible API)."""

    def __init__(self, base_url: str, model: str, timeout: int = 120):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout

    def health_check(self) -> dict:
        """Check if the MLX server is reachable and serving a model."""
        try:
            resp = requests.get(f"{self.base_url}/v1/models", timeout=5)
            resp.raise_for_status()
            data = resp.json()
            models = [m.get("id", "") for m in data.get("data", [])]
            model_ready = len(models) > 0
            return {"status": "ok", "models": models, "model_ready": model_ready}
        except requests.RequestException as exc:
            return {"status": "error", "error": str(exc), "model_ready": False}

    def chat(
        self,
        messages: list[dict],
        context: str = "",
        stream: bool = False,
    ) -> dict | Generator[str, None, None]:
        """
        Send a chat completion request.

        messages: list of {"role": "user"|"assistant", "content": "..."}
        context: market data context injected into the system prompt
        stream: if True, returns a generator yielding content chunks
        """
        system_content = SYSTEM_PROMPT
        if context:
            system_content += f"\n\n--- LIVE MARKET DATA ---\n{context}\n--- END MARKET DATA ---"

        full_messages = [{"role": "system", "content": system_content}] + messages

        payload = {
            "model": self.model,
            "messages": full_messages,
            "stream": stream,
        }

        if stream:
            return self._stream_chat(payload)

        try:
            resp = requests.post(
                f"{self.base_url}/v1/chat/completions",
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            content = ""
            choices = data.get("choices", [])
            if choices:
                content = choices[0].get("message", {}).get("content", "")
            return {
                "content": content,
                "model": data.get("model", self.model),
                "done": True,
            }
        except requests.RequestException as exc:
            log.error("MLX chat error: %s", exc)
            return {"content": f"Error communicating with AI: {exc}", "error": True}

    def _stream_chat(self, payload: dict) -> Generator[str, None, None]:
        """Stream chat response token by token via SSE."""
        try:
            with requests.post(
                f"{self.base_url}/v1/chat/completions",
                json=payload,
                stream=True,
                timeout=self.timeout,
            ) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines():
                    if not line:
                        continue
                    decoded = line.decode("utf-8") if isinstance(line, bytes) else line
                    if not decoded.startswith("data: "):
                        continue
                    data_str = decoded[6:]
                    if data_str.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data_str)
                        delta = chunk.get("choices", [{}])[0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            yield content
                    except (json.JSONDecodeError, IndexError):
                        continue
        except requests.RequestException as exc:
            log.error("MLX stream error: %s", exc)
            yield f"\n\n[Error: {exc}]"

    def generate(self, prompt: str, stream: bool = False) -> str:
        """Single-shot generation via chat completions. Used for insight summaries."""
        messages = [{"role": "user", "content": prompt}]
        result = self.chat(messages, context="", stream=False)
        if isinstance(result, dict):
            return result.get("content", "")
        return ""
