from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when, avg, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
import os
import shutil

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
KAFKA_TOPIC = 'statsbomb-eventos'
CHECKPOINT_DIR = "/app/data/checkpoint"
OUTPUT_PATH = "/app/data/raw_events"
MODEL_PATH = "/app/data/model"

def get_spark_session():
    return SparkSession.builder \
        .appName("StatsbombStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

# Define Schema (Simplified based on common Statsbomb fields)
# We treat most complex fields as Strings or ignore them for simplicity unless needed
schema = StructType([
    StructField("id", StringType(), True),
    StructField("index", IntegerType(), True),
    StructField("period", IntegerType(), True),
    StructField("timestamp", StringType(), True), # HH:MM:SS.mmm
    StructField("minute", IntegerType(), True),
    StructField("second", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("possession", IntegerType(), True),
    StructField("possession_team", StringType(), True),
    StructField("play_pattern", StringType(), True),
    StructField("team", StringType(), True),
    StructField("player", StringType(), True),
    StructField("position", StringType(), True),
    StructField("location", StringType(), True), # Usually a list/array in JSON, might need parsing
    StructField("duration", DoubleType(), True),
    StructField("under_pressure", BooleanType(), True),
    StructField("match_id", IntegerType(), True),
    # Specific event type fields (simplified)
    StructField("pass_outcome", StringType(), True),
    StructField("shot_statsbomb_xg", DoubleType(), True),
    StructField("shot_outcome", StringType(), True)
])

def process_batch(df, epoch_id):
    print(f"Processing batch {epoch_id}...")
    
    # 1. Calculate Statistics (Batch Level)
    stats = df.groupBy("team").agg(
        count("*").alias("event_count"),
        avg("shot_statsbomb_xg").alias("avg_xg"),
        (count(when(col("pass_outcome").isNull(), 1)) / count(when(col("type") == "Pass", 1))).alias("pass_completion_rate")
    )
    
    print("=== Batch Statistics ===")
    stats.show()
    
    # 2. Save Raw Data for ML Training
    # We append the new events to the historical data
    df.write.mode("append").parquet(OUTPUT_PATH)
    
    # 3. ML Inference (if model exists)
    if os.path.exists(MODEL_PATH):
        try:
            model = RandomForestClassificationModel.load(MODEL_PATH)
            
            # Identify matches in this batch to update predictions for
            current_match_ids = [row.match_id for row in df.select("match_id").distinct().collect()]
            
            if current_match_ids:
                # Read cumulative data from Parquet to get full match context
                # We use the spark session from the dataframe
                full_df = df.sparkSession.read.parquet(OUTPUT_PATH)
                
                # Filter for the current matches
                match_stats = full_df.filter(col("match_id").isin(current_match_ids)) \
                    .groupBy("match_id", "team").agg(
                        avg("shot_statsbomb_xg").alias("avg_xg"),
                        count("*").alias("event_count")
                    )
                
                print("=== Cumulative Match Stats ===")
                match_stats.show()
                
                # Feature Assembler (must match training)
                assembler = VectorAssembler(inputCols=["avg_xg", "event_count"], outputCol="features")
                data_with_features = assembler.transform(match_stats.fillna(0))
                
                predictions = model.transform(data_with_features)
                
                print("=== Match Predictions ===")
                predictions.select("match_id", "team", "prediction", "probability").show()
            
        except Exception as e:
            print(f"Inference failed: {e}")

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # Add timestamp for windowing (optional, but good for streaming)
    # We'll just use processing time for simplicity in this batch-based approach
    
    # Start Query
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
