FROM python:3.11-slim

WORKDIR /app

# Update system packages and install essential build tools
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser -d /home/appuser -s /sbin/nologin -c "App User" appuser && \
    mkdir -p /home/appuser && \
    chown -R appuser:appuser /home/appuser

# Install Poetry
RUN pip install poetry==2.1.1

# Copy only dependency files first (to leverage Docker caching)
COPY pyproject.toml poetry.lock* /app/

# Configure Poetry to not use a virtual environment
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-interaction --no-ansi --no-root

# Copy project files
COPY . /app/

# Change ownership of the application files and directories
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port for FastAPI
EXPOSE 8000

# Command will be overridden by docker-compose
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
