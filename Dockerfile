FROM python:3.11-slim

WORKDIR /app

# Install system deps needed for building wheels + git for VCS requirements
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential libpq-dev git \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080 \
    PYTHONUNBUFFERED=1

# Tune these if needed
ENV WEB_CONCURRENCY=2 \
    WEB_THREADS=8

CMD exec gunicorn --bind :$PORT --workers ${WEB_CONCURRENCY} --threads ${WEB_THREADS} --timeout 0 wsgi:app
