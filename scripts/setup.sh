#!/bin/bash
# Setup script for the project

set -e

echo "=========================================="
echo "Setting up Spark Kafka Statsbomb Project"
echo "=========================================="
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "ERROR: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "[1/5] Checking Docker installation..."
docker --version
docker-compose --version
echo ""

echo "[2/5] Creating necessary directories..."
mkdir -p data/raw data/processed data/models config/spark config/kafka
echo "✓ Directories created"
echo ""

echo "[3/5] Setting up environment variables..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "✓ .env file created from .env.example"
else
    echo "✓ .env file already exists"
fi
echo ""

echo "[4/5] Building Docker images..."
docker-compose build
echo "✓ Docker images built"
echo ""

echo "[5/5] Starting services..."
docker-compose up -d
echo "✓ Services started"
echo ""

echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Services running:"
docker-compose ps
echo ""
echo "Next steps:"
echo "1. Wait for services to fully start (30-60 seconds)"
echo "2. Train the ML model: docker exec -it pyspark-app python src/entrenamiento_ml.py"
echo "3. Start streaming consumer: docker exec -it pyspark-app spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 src/procesador_streaming.py"
echo "4. Start producer: docker exec -it pyspark-app python src/productor.py"
echo ""
echo "Access Spark UI at: http://localhost:4040"
echo "Access Spark Master UI at: http://localhost:8080"
echo ""
