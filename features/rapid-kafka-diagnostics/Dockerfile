FROM python:3.12-slim

WORKDIR /app

# Install confluent-kafka
RUN pip install confluent-kafka

# Copy scripts
COPY producer.py .
COPY consumer.py .
