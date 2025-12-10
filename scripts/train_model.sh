#!/bin/bash
# Train ML model script

set -e

echo "=========================================="
echo "Training Machine Learning Model"
echo "=========================================="
echo ""

# Check if container is running
if ! docker ps | grep -q spark-app; then
    echo "ERROR: spark-app container is not running"
    echo "Please run 'docker-compose up -d' first"
    exit 1
fi

# Run training
echo "Submitting job to Spark Cluster..."
docker exec -it spark-app /usr/local/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1G \
  --executor-memory 1G \
  --executor-cores 1 \
  src/entrenamiento_ml.py

echo ""
echo "=========================================="
echo "Model training completed!"
echo "Model saved to: /app/data/model (Spark ML format)"
echo "=========================================="
