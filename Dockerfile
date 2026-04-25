FROM python:3.11-slim

WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser

RUN mkdir -p /app/data && chown -R appuser:appuser /app/data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY tests/ ./tests/

USER appuser

EXPOSE 8080

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]