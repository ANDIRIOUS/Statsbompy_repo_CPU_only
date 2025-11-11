#!/bin/bash
# Start Kafka producer script

set -e

echo "=========================================="
echo "Starting Kafka Producer"
echo "=========================================="
echo ""

# Check if container is running
if ! docker ps | grep -q pyspark-app; then
    echo "ERROR: pyspark-app container is not running"
    echo "Please run 'docker-compose up -d' first"
    exit 1
fi

# Run producer
docker exec -it pyspark-app python src/productor.py
