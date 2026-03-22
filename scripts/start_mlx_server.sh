#!/usr/bin/env bash
#
# Start the MLX LM server for the CryptoStream AI assistant.
# Requires Apple Silicon (M1/M2/M3/M4) and Python 3.11+.
#
# Usage:
#   ./scripts/start_mlx_server.sh
#
# The server exposes an OpenAI-compatible API at http://localhost:8080
# The dashboard container connects to it via host.docker.internal:8080

set -euo pipefail

MODEL="${MLX_MODEL:-mlx-community/Llama-3.2-3B-Instruct-4bit}"
PORT="${MLX_PORT:-8080}"

# Check Apple Silicon
if [[ "$(uname -m)" != "arm64" ]]; then
    echo "ERROR: MLX requires Apple Silicon (arm64). Detected: $(uname -m)"
    exit 1
fi

# Install mlx-lm if not available
if ! python3 -c "import mlx_lm" 2>/dev/null; then
    echo "Installing mlx-lm..."
    pip3 install mlx-lm
fi

echo "============================================"
echo "  CryptoStream AI — MLX LM Server"
echo "============================================"
echo "  Model : ${MODEL}"
echo "  Port  : ${PORT}"
echo "  URL   : http://localhost:${PORT}"
echo "============================================"
echo ""
echo "The model will be downloaded on first run (~4.5 GB)."
echo "Press Ctrl+C to stop."
echo ""

python3 -m mlx_lm.server --model "${MODEL}" --host 0.0.0.0 --port "${PORT}"
