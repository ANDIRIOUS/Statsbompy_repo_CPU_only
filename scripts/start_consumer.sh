#!/bin/bash
# Start Spark Streaming consumer script

set -e

echo "=========================================="
echo "Starting Spark Streaming Consumer"
echo "=========================================="
echo ""
echo "Spark UI will be available at: http://localhost:4040"
echo ""

# Check if container is running
if ! docker ps | grep -q pyspark-app; then
    echo "ERROR: pyspark-app container is not running"
    echo "Please run 'docker-compose up -d' first"
    exit 1
fi

# Run consumer with Spark
docker exec -it pyspark-app spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.sql.streaming.checkpointLocation=/app/data/checkpoints \
    src/procesador_streaming.py
