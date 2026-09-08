# Multi-stage build for mcp-tg MCP Server
# Production-ready Docker image with security and optimization

# Stage 1: Builder
FROM python:3.11-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim

# Security: Create non-root user
RUN useradd -m -u 1000 -s /bin/bash mcp && \
    mkdir -p /app /data /logs && \
    chown -R mcp:mcp /app /data /logs

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder
COPY --from=builder /root/.local /home/mcp/.local

# Set environment variables
ENV PATH=/home/mcp/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=UTC

# Switch to non-root user
USER mcp
WORKDIR /app

# Copy application files
COPY --chown=mcp:mcp . .

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Volume for persistent data
VOLUME ["/data", "/logs"]

# Expose MCP server port
EXPOSE 8765

# Entry point
ENTRYPOINT ["python", "-m", "mcp_telegram"]
CMD ["run", "--host", "0.0.0.0", "--port", "8765"]