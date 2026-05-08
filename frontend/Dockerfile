###############################################################################
# Pegasus Extract — Frontend Dockerfile
# Next.js 14 on Node 20 Alpine
#
# Multi-stage build:
#   Stage 1 (deps):    install node_modules
#   Stage 2 (builder): build the Next.js production bundle
#   Stage 3 (runner):  lean production image with standalone output
###############################################################################

# ── Stage 1: Dependencies ──────────────────────────────────────────────────
FROM node:20-alpine AS deps

WORKDIR /app

# Copy package files
COPY package.json package-lock.json ./

# Install production dependencies only
RUN npm ci --omit=dev


# ── Stage 2: Builder ───────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

# Copy all deps (including dev deps for build)
COPY package.json package-lock.json ./
RUN npm ci

# Copy source code
COPY . .

# Build-time env var — will be baked into the client bundle
# Override at build time: docker build --build-arg NEXT_PUBLIC_API_URL=https://...
ARG NEXT_PUBLIC_API_URL=https://pegasus-extract.onrender.com
ENV NEXT_PUBLIC_API_URL=${NEXT_PUBLIC_API_URL}

# Enable Next.js standalone output for minimal Docker image
# This is injected at build time so we don't need to modify next.config.js permanently
RUN echo "const nextConfig = { reactStrictMode: true, output: 'standalone' }; module.exports = nextConfig;" > next.config.js

# Build the production bundle
RUN npm run build


# ── Stage 3: Runner ────────────────────────────────────────────────────────
FROM node:20-alpine AS runner

LABEL maintainer="Pegasus Extract Team" \
      description="Pegasus Extract frontend — Next.js UI" \
      version="1.0"

WORKDIR /app

ENV NODE_ENV=production \
    PORT=3000

# Non-root user
RUN addgroup -S pegasus && adduser -S pegasus -G pegasus

# Copy the standalone build output
COPY --from=builder --chown=pegasus:pegasus /app/.next/standalone ./
COPY --from=builder --chown=pegasus:pegasus /app/.next/static ./.next/static
COPY --from=builder --chown=pegasus:pegasus /app/public ./public 2>/dev/null || true

USER pegasus

EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:3000/ || exit 1

CMD ["node", "server.js"]
