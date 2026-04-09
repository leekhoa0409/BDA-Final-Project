#!/bin/bash
set -e

echo "Creating Kafka topics..."

kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic weather-raw \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000

echo "Topic 'weather-raw' created (7-day retention, 3 partitions)"

kafka-topics --bootstrap-server kafka:9092 --list
echo "Kafka topic initialization complete!"
