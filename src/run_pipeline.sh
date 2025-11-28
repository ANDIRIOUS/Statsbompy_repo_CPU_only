#!/bin/bash

# Stop any existing python processes (Producer/Spark)
pkill -f python3
pkill -f spark-submit

# Clean checkpoint to avoid conflicts
rm -rf /app/data/checkpoint

# Start Producer in background
echo "Starting Producer..."
nohup python3 src/productor.py > /app/producer.log 2>&1 &

# Wait a bit for producer to warm up
sleep 5

# Start Streaming Job
echo "Starting Streaming Processor..."
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 src/procesador_streaming.py
