from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.functions import col, when
import os

# Configuration
INPUT_PATH = "/app/data/raw_events"
MODEL_PATH = "/app/data/model"

def get_spark_session():
    return SparkSession.builder \
        .appName("StatsbombMLTraining") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Loading data...")
    if not os.path.exists(INPUT_PATH):
        print("No data found. Run the streaming job first to collect data.")
        return

    df = spark.read.parquet(INPUT_PATH)
    
    # Feature Engineering
    # We want to predict Match Outcome (Win/Draw/Loss)
    # But we only have event data. We need labels.
    # In a real scenario, we'd join with match results.
    # For this simulation, we will CREATE a synthetic label or try to infer it.
    # Or better, we assume the 'winner' is the team with more xG (just for demo purposes)
    # OR we can't really train a valid model without labels.
    
    # Let's create a synthetic label for demonstration:
    # 0: Draw, 1: Win
    # Rule: If avg_xg > 0.5 -> Win (1), else Draw (0)
    
    print("Preparing features...")
    training_data = df.groupBy("match_id", "team").agg(
        {"shot_statsbomb_xg": "avg", "*": "count"}
    ).withColumnRenamed("avg(shot_statsbomb_xg)", "avg_xg") \
     .withColumnRenamed("count(1)", "event_count")
    
    # Create synthetic label
    training_data = training_data.withColumn("label", when(col("avg_xg") > 0.1, 1.0).otherwise(0.0))
    
    # Assemble features
    assembler = VectorAssembler(inputCols=["avg_xg", "event_count"], outputCol="features")
    final_data = assembler.transform(training_data.fillna(0))
    
    # Train Model
    print("Training Random Forest Model...")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
    model = rf.fit(final_data)
    
    # Save Model
    print(f"Saving model to {MODEL_PATH}...")
    model.write().overwrite().save(MODEL_PATH)
    print("Model saved successfully!")

if __name__ == "__main__":
    main()
