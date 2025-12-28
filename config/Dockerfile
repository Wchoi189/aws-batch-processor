# AWS Batch-compatible Dockerfile for pseudo-label processing
# Supports both AWS Lambda and AWS Batch environments

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (minimal - only what's needed for API calls)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast package management
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    /root/.local/bin/uv --version

ENV PATH="/root/.local/bin:${PATH}"

# Copy dependency files first (for layer caching)
COPY config/requirements.txt /tmp/requirements.txt

# Install minimal Python dependencies
RUN /root/.local/bin/uv pip install --system -r /tmp/requirements.txt

# Copy application code
COPY src/ /app/src/

# Create directories for data
RUN mkdir -p /app/data/processed /app/data/checkpoints

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command (can be overridden)
CMD ["python", "-m", "src.processor"]
