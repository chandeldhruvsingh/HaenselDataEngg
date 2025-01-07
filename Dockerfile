# Use Python 3.9 slim image as base
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Create directories
RUN mkdir -p /app/pipeline /app/data /app/output /app/venv /app/config

# Copy requirements.txt into the container
COPY requirements.txt /app/

# Install venv and create a virtual environment
RUN python -m venv /app/venv

# Activate the virtual environment and install dependencies
RUN /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install -r /app/requirements.txt

# Copy the config directory
COPY config /app/config/
COPY pipeline/ /app/pipeline/
COPY data/ /app/data/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PATH="/app/venv/bin:$PATH"

# Create volumes for persistent storage
VOLUME /app/data
VOLUME /app/output

# Run the attribution processor
CMD ["python", "/app/pipeline/attribution_processor.py"]
