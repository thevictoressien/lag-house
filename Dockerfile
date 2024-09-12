
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt
RUN pip install dbt-core dbt-bigquery

# Copy project files
COPY . .

# Set environment variables
ENV PYTHONPATH=/app

# Run tests by default
CMD ["python", "-m", "pytest", "tests/"]
