# Use an official Python runtime as the base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies for PostgreSQL and PostGIS
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    g++ \
    postgresql-client \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create logs directory
RUN mkdir -p /app/logs

# Set environment variable to ensure Python output is sent straight to terminal (logs)
ENV PYTHONUNBUFFERED=1

# Command will be specified in docker-compose.yml
CMD ["python", "-m", "cli", "--query-scan", "--sources", "funda,pararius"]