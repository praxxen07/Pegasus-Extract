###############################################################################
# Pegasus Extract — Backend Dockerfile
# Python 3.11 + Playwright + Chromium on Debian Bookworm
#
# Multi-stage build:
#   Stage 1 (builder): install Python deps + Playwright browsers
#   Stage 2 (runtime): lean production image with non-root user
###############################################################################

# ── Stage 1: Builder ────────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm AS builder

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# Install build dependencies needed by some pip packages (curl-cffi, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Install Playwright browsers (Chromium only) into a known path
ENV PLAYWRIGHT_BROWSERS_PATH=/install/pw-browsers
RUN pip install --no-cache-dir playwright && \
    python -m playwright install chromium


# ── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm AS runtime

LABEL maintainer="Pegasus Extract Team" \
      description="Pegasus Extract backend — AI-powered universal web scraper" \
      version="1.0"

# ── System dependencies for Playwright Chromium ────────────────────────────
# These are the exact libraries Chromium needs at runtime on Debian Bookworm.
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Chromium core dependencies
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    # Wayland support
    libwayland-client0 \
    # Fonts — needed for proper text rendering
    fonts-noto-color-emoji \
    fonts-liberation \
    fonts-dejavu-core \
    # Utilities
    curl \
    dumb-init \
    && rm -rf /var/lib/apt/lists/*

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Copy installed Python packages from builder
COPY --from=builder /install/lib /usr/local/lib
COPY --from=builder /install/bin /usr/local/bin

# Copy Playwright browsers from builder
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers
COPY --from=builder /install/pw-browsers /opt/pw-browsers

# ── Non-root user ──────────────────────────────────────────────────────────
RUN groupadd -r pegasus && useradd -r -g pegasus -m -s /bin/bash pegasus

# Create output directory with correct permissions
RUN mkdir -p /app/output && chown -R pegasus:pegasus /app

WORKDIR /app

# Copy application code
COPY --chown=pegasus:pegasus . .

# Remove files not needed in production
RUN rm -rf test_*.py tmp_*.json __pycache__ .env .DS_Store

# Switch to non-root user
USER pegasus

# ── Environment defaults ───────────────────────────────────────────────────
# These can be overridden at runtime via docker run -e or docker-compose
ENV PORT=8001 \
    BACKEND_PORT=8001 \
    FRONTEND_URL="" \
    ANTHROPIC_API_KEY="" \
    GROQ_API_KEY="" \
    GEMINI_API_KEY="" \
    DEEPSEEK_API_KEY="" \
    OPENROUTER_API_KEY=""

EXPOSE 8001

# ── Health check ───────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8001}/ || exit 1

# ── Entrypoint ─────────────────────────────────────────────────────────────
# dumb-init ensures proper signal handling (SIGTERM → graceful shutdown)
ENTRYPOINT ["dumb-init", "--"]
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8001} --workers 1 --timeout-keep-alive 120"]
