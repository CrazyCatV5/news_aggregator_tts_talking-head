#!/bin/sh
set -e

MODEL="${LLM_MODEL:-qwen3:8b}"

echo "[ollama-init] starting local ollama serve ..."
ollama serve >/tmp/ollama.log 2>&1 &
pid=$!

echo "[ollama-init] waiting for local server ..."
n=0
until ollama list >/dev/null 2>&1; do
  n=$((n+1))
  if [ "$n" -ge 180 ]; then
    echo "[ollama-init] local ollama not ready"
    echo "---- ollama log ----"
    tail -n 200 /tmp/ollama.log || true
    kill "$pid" 2>/dev/null || true
    exit 1
  fi
  sleep 1
done

echo "[ollama-init] ensure model: ${MODEL}"
if ! ollama list 2>/dev/null | grep -q "^${MODEL}[[:space:]]"; then
  ollama pull "${MODEL}"
fi

echo "[ollama-init] done"
kill "$pid" 2>/dev/null || true
