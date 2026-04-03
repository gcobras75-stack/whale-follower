# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build deps only in builder
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy source
COPY . .

# Create log directory
RUN mkdir -p /app/logs

# Non-root user for security
RUN useradd -m -u 1001 botuser && chown -R botuser:botuser /app
USER botuser

# Railway injects PORT automatically; default 8080
ENV PORT=8080

EXPOSE 8080

# Healthcheck — Railway will probe /health
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT}/health')"

CMD ["python", "main.py"]
