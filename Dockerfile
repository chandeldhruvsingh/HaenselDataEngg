# Use Python 3.9 slim image as base
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Create directories
RUN mkdir -p /app/pipeline /app/data /app/output

# Install required packages
RUN pip install pandas requests

# Copy Python scripts from pipeline directory
COPY pipeline/setup_db.py /app/pipeline/
COPY pipeline/build_customer_journey.py /app/pipeline/
COPY pipeline/send_to_api.py /app/pipeline/
COPY pipeline/attribution_processor.py /app/pipeline/

# Copy data files
COPY data/challenge_db_create.sql /app/data/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV API_KEY=""
ENV CONV_TYPE_ID="data_engineering_challenge"
ENV OUTPUT_PATH="/app/output/channel_reporting.csv"
ENV BATCH_SIZE=200
ENV PYTHONPATH=/app

# Create volumes for persistent storage
VOLUME /app/data
VOLUME /app/output

# Run the attribution processor
CMD ["python", "/app/pipeline/attribution_processor.py"]