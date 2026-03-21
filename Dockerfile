FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .
COPY forecaster/ ./forecaster/

# Railway sets PORT env var automatically
CMD ["python", "server.py"]
