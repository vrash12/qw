# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (if needed for psycopg2/mysqlclient/etc.)
RUN apt-get update && apt-get install -y build-essential libpq-dev

# Copy dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose port
ENV PORT 8080

# Command to run with Gunicorn (4 workers, auto-bind to Cloud Run port)
CMD exec gunicorn --bind :$PORT --workers 4 --threads 8 --timeout 0 wsgi:app
