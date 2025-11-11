#!/bin/bash
# Train ML model script

set -e

echo "=========================================="
echo "Training Machine Learning Model"
echo "=========================================="
echo ""

# Check if container is running
if ! docker ps | grep -q pyspark-app; then
    echo "ERROR: pyspark-app container is not running"
    echo "Please run 'docker-compose up -d' first"
    exit 1
fi

# Run training
docker exec -it pyspark-app python src/entrenamiento_ml.py

echo ""
echo "=========================================="
echo "Model training completed!"
echo "Model saved to: /app/data/models/modelo.joblib"
echo "=========================================="
