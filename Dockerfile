FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY manifest.json .

ENV PORT=8000
ENV LOG_LEVEL=INFO
ENV STORAGE_DIR=/data/installations
ENV ENCRYPTION_KEY=""

RUN mkdir -p /data/installations

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
