FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        && rm -rf /var/lib/apt/lists/*

# Create non-root user first
RUN useradd --create-home --shell /bin/bash app

# Set working directory
WORKDIR /app

# Copy source code (we'll install dependencies directly)
COPY . .

# Install Python dependencies - ALL required packages
RUN pip install --retries 10 --timeout 300 --upgrade pip setuptools wheel \
    && pip install --retries 10 --timeout 300 \
        fastapi==0.104.1 \
        uvicorn[standard]==0.24.0 \
        pydantic==2.5.0 \
        python-multipart==0.0.6 \
        pyjwt==2.8.0 \
        python-dotenv==1.0.0 \
        prometheus-client==0.19.0 \
        fastapi-limiter==0.1.6 \
        redis==5.0.1 \
        aio-pika==9.5.7 \
        aiohttp==3.9.1 \
        etcd3==0.12.0

# Fix permissions for the app directory
RUN chown -R app:app /app

# Switch to app user
USER app

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]