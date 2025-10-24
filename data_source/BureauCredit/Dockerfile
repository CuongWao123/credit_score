FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY Bureau_API.py .

# Expose port
EXPOSE 6666

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:6666/health')"

# Run application
CMD ["uvicorn", "Bureau_API:app", "--host", "0.0.0.0", "--port", "6666"]
