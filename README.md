# Spark + Kafka + Statsbomb Project

This project simulates a real-time Big Data architecture using Spark Structured Streaming and Kafka to process Statsbomb football event data.

## Architecture

- **Producer**: Python script fetching data from Statsbomb API and pushing to Kafka.
- **Broker**: Kafka (single node) + Zookeeper.
- **Processing**: Spark Structured Streaming (PySpark) reading from Kafka.
- **ML**: Random Forest Classifier trained on processed data.

## Prerequisites

- Docker
- Docker Compose

## Setup & Execution

### Quick Start (Recommended)
Use the provided scripts in `scripts/` for easy execution:

1.  **Start Services**:
    ```bash
    docker-compose up -d --build
    ```
    Wait for `spark-worker-1` and `spark-worker-2` to be ready.

2.  **Run Training (Distributed)**:
    This script submits the job to the Spark Cluster (2 Workers):
    ```bash
    bash scripts/train_model.sh
    ```

### Manual Execution

1.  **Build and Start Services**
    ```bash
    docker-compose up --build
    ```
    Verify that `spark-worker-1` and `spark-worker-2` are running.

2.  **Access the App Container**
    ```bash
    docker exec -it spark-app bash
    ```

3.  **Run the Producer** (in the app container)
    ```bash
    python3 src/productor.py
    ```

4.  **Run the Streaming Processor** (in the app container)
    ```bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 src/procesador_streaming.py
    ```

5.  **Train the ML Model**
    To train using the cluster (ensure data is collected properly first):
    ```bash
    /usr/local/bin/spark-submit \
      --master spark://spark-master:7077 \
      src/entrenamiento_ml.py
    ```
    This will save the model to `data/model` (Spark ML format).

6.  **Run Inference**
    Start the streaming processor again. It will now detect the model and output predictions.
    ```bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 src/procesador_streaming.py
    ```

## Performance Comparison

To compare architectures (e.g., Aorus i7 vs MacBook Air i3), monitor the **Spark UI** at [http://localhost:4040](http://localhost:4040).

**Metrics to Record:**
1.  **Processing Time**: Average time to process a batch (visible in Streaming tab).
2.  **Input Rate**: Events/sec.
3.  **Shuffle Write/Read**: Data moved between executors.
4.  **Executor CPU Time**: CPU usage.

Take screenshots of the "Streaming" tab and "Stages" tab for comparison.
