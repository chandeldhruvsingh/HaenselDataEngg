# Use Python 3.9 slim image as base
FROM python:3.9-slim

# Set working directory
WORKDIR /app

RUN mkdir -p /app/pipeline /app/data /app/output /app/venv /app/config

COPY requirements.txt /app/
RUN python -m venv /app/venv
RUN /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install -r /app/requirements.txt

COPY config /app/config/
COPY pipeline/ /app/pipeline/
COPY data/ /app/data/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PATH="/app/venv/bin:$PATH"

VOLUME /app/data
VOLUME /app/output

ENTRYPOINT ["python", "/app/pipeline/attribution_processor.py"]
