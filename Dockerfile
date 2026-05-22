FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .
COPY forecaster/ ./forecaster/

# Railway sets PORT env var automatically; EXPOSE is needed so Railway's
# public-domain proxy can find the bound port. Without it the proxy can't
# route public traffic to the container (502), even though private/internal
# networking still reaches the app fine. (Diagnosed 2026-05-22.)
EXPOSE 8000

CMD ["python", "server.py"]
