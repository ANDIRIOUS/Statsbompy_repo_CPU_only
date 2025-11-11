"""
Spark Structured Streaming Consumer for Statsbomb Events
Processes events in real-time and calculates match statistics
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as _sum, avg,
    when, lit, expr, current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, ArrayType, MapType, TimestampType
)
import joblib


class StatsbombStreamProcessor:
    """Processes Statsbomb events from Kafka using Spark Structured Streaming"""

    def __init__(
        self,
        app_name: str = "StatsbombStreaming",
        kafka_servers: str = "kafka:29092",
        kafka_topic: str = "statsbomb-eventos",
        checkpoint_location: str = "/app/data/checkpoints",
        output_path: str = "/app/data/processed",
        model_path: str = None
    ):
        """
        Initialize Spark Streaming Processor

        Args:
            app_name: Spark application name
            kafka_servers: Kafka bootstrap servers
            kafka_topic: Kafka topic to consume
            checkpoint_location: Directory for streaming checkpoints
            output_path: Directory for Parquet output
            model_path: Path to trained ML model (optional)
        """
        self.app_name = app_name
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.checkpoint_location = checkpoint_location
        self.output_path = output_path
        self.model_path = model_path
        self.spark = None
        self.model = None

    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        print("[INFO] Creating Spark session...")

        spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        print(f"[SUCCESS] Spark session created: {spark.sparkContext.applicationId}")
        print(f"[INFO] Spark UI available at: http://localhost:4040")

        self.spark = spark
        return spark

    def get_event_schema(self) -> StructType:
        """
        Define schema for Statsbomb events

        Returns:
            StructType schema for events
        """
        return StructType([
            StructField("id", StringType(), True),
            StructField("index", IntegerType(), True),
            StructField("period", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("minute", IntegerType(), True),
            StructField("second", IntegerType(), True),
            StructField("possession", IntegerType(), True),
            StructField("duration", FloatType(), True),
            StructField("type", MapType(StringType(), StringType()), True),
            StructField("possession_team", MapType(StringType(), StringType()), True),
            StructField("play_pattern", MapType(StringType(), StringType()), True),
            StructField("team", MapType(StringType(), StringType()), True),
            StructField("player", MapType(StringType(), StringType()), True),
            StructField("position", MapType(StringType(), StringType()), True),
            StructField("location", ArrayType(FloatType()), True),
            StructField("under_pressure", BooleanType(), True),
            StructField("pass", MapType(StringType(), StringType()), True),
            StructField("shot", MapType(StringType(), StringType()), True),
            StructField("match_id", IntegerType(), True),
            StructField("_producer_timestamp", StringType(), True)
        ])

    def read_kafka_stream(self):
        """
        Read streaming data from Kafka

        Returns:
            Streaming DataFrame
        """
        print(f"[INFO] Reading from Kafka topic: {self.kafka_topic}")

        stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", "100") \
            .load()

        print("[SUCCESS] Kafka stream connected")
        return stream_df

    def parse_events(self, stream_df):
        """
        Parse JSON events from Kafka

        Args:
            stream_df: Raw Kafka DataFrame

        Returns:
            Parsed DataFrame with event schema
        """
        schema = self.get_event_schema()

        parsed_df = stream_df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("event_timestamp", to_timestamp(col("_producer_timestamp")))

        return parsed_df

    def calculate_possession(self, events_df):
        """
        Calculate possession percentage by team using windowing

        Args:
            events_df: Parsed events DataFrame

        Returns:
            DataFrame with possession statistics
        """
        print("[INFO] Calculating possession statistics...")

        # Extract team name from map
        possession_df = events_df \
            .filter(col("possession_team").isNotNull()) \
            .withColumn("team_name", col("possession_team.name")) \
            .withColumn("window_time", window(col("event_timestamp"), "5 minutes"))

        # Count possessions per team per window
        possession_stats = possession_df \
            .groupBy("window_time", "team_name") \
            .agg(count("*").alias("possession_count"))

        # Calculate total possessions per window
        total_possessions = possession_df \
            .groupBy("window_time") \
            .agg(count("*").alias("total_possessions"))

        # Calculate possession percentage
        possession_pct = possession_stats \
            .join(total_possessions, on="window_time") \
            .withColumn(
                "possession_percentage",
                (col("possession_count") / col("total_possessions") * 100)
            ) \
            .select(
                col("window_time.start").alias("window_start"),
                col("window_time.end").alias("window_end"),
                "team_name",
                "possession_percentage"
            )

        return possession_pct

    def calculate_xg(self, events_df):
        """
        Calculate average Expected Goals (xG) by team

        Args:
            events_df: Parsed events DataFrame

        Returns:
            DataFrame with xG statistics
        """
        print("[INFO] Calculating xG statistics...")

        # Extract shot data
        xg_df = events_df \
            .filter(col("shot").isNotNull()) \
            .withColumn("team_name", col("team.name")) \
            .withColumn("xg_value", col("shot.statsbomb_xg").cast(FloatType())) \
            .withColumn("window_time", window(col("event_timestamp"), "5 minutes"))

        # Calculate average xG per team per window
        xg_stats = xg_df \
            .groupBy("window_time", "team_name") \
            .agg(
                avg("xg_value").alias("avg_xg"),
                _sum("xg_value").alias("total_xg"),
                count("*").alias("shots_count")
            ) \
            .select(
                col("window_time.start").alias("window_start"),
                col("window_time.end").alias("window_end"),
                "team_name",
                "avg_xg",
                "total_xg",
                "shots_count"
            )

        return xg_stats

    def calculate_pass_completion(self, events_df):
        """
        Calculate pass completion percentage by team

        Args:
            events_df: Parsed events DataFrame

        Returns:
            DataFrame with pass completion statistics
        """
        print("[INFO] Calculating pass completion statistics...")

        # Extract pass data
        pass_df = events_df \
            .filter(col("pass").isNotNull()) \
            .withColumn("team_name", col("team.name")) \
            .withColumn("pass_outcome", col("pass.outcome.name")) \
            .withColumn(
                "is_complete",
                when(col("pass_outcome").isNull(), lit(1)).otherwise(lit(0))
            ) \
            .withColumn("window_time", window(col("event_timestamp"), "5 minutes"))

        # Calculate pass completion stats
        pass_stats = pass_df \
            .groupBy("window_time", "team_name") \
            .agg(
                _sum("is_complete").alias("completed_passes"),
                count("*").alias("total_passes")
            ) \
            .withColumn(
                "pass_completion_pct",
                (col("completed_passes") / col("total_passes") * 100)
            ) \
            .select(
                col("window_time.start").alias("window_start"),
                col("window_time.end").alias("window_end"),
                "team_name",
                "pass_completion_pct",
                "completed_passes",
                "total_passes"
            )

        return pass_stats

    def save_to_parquet(self, df, output_name: str):
        """
        Save DataFrame to Parquet format

        Args:
            df: DataFrame to save
            output_name: Name for the output directory
        """
        output_location = f"{self.output_path}/{output_name}"

        query = df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_location) \
            .option("checkpointLocation", f"{self.checkpoint_location}/{output_name}") \
            .start()

        print(f"[INFO] Saving {output_name} to {output_location}")
        return query

    def display_statistics(self, df, stat_name: str):
        """
        Display statistics to console

        Args:
            df: DataFrame with statistics
            stat_name: Name of the statistic
        """
        query = df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .start()

        print(f"[INFO] Displaying {stat_name} statistics")
        return query

    def load_ml_model(self):
        """Load trained ML model if available"""
        if self.model_path and os.path.exists(self.model_path):
            print(f"[INFO] Loading ML model from {self.model_path}")
            try:
                self.model = joblib.load(self.model_path)
                print("[SUCCESS] ML model loaded successfully")
            except Exception as e:
                print(f"[WARNING] Failed to load model: {e}")
                self.model = None
        else:
            print("[INFO] No ML model specified or found")

    def run(self):
        """Main execution method"""
        try:
            # Create Spark session
            self.create_spark_session()

            # Load ML model if available
            self.load_ml_model()

            # Read from Kafka
            raw_stream = self.read_kafka_stream()

            # Parse events
            events_df = self.parse_events(raw_stream)

            # Save raw events to Parquet
            parquet_query = self.save_to_parquet(events_df, "events_raw")

            # Calculate statistics
            possession_stats = self.calculate_possession(events_df)
            xg_stats = self.calculate_xg(events_df)
            pass_stats = self.calculate_pass_completion(events_df)

            # Display statistics
            possession_query = self.display_statistics(possession_stats, "Possession")
            xg_query = self.display_statistics(xg_stats, "xG")
            pass_query = self.display_statistics(pass_stats, "Pass Completion")

            # Save statistics to Parquet
            poss_parquet = self.save_to_parquet(possession_stats, "possession_stats")
            xg_parquet = self.save_to_parquet(xg_stats, "xg_stats")
            pass_parquet = self.save_to_parquet(pass_stats, "pass_stats")

            print("\n" + "="*60)
            print("[SUCCESS] All streaming queries started")
            print("="*60 + "\n")

            # Wait for termination
            self.spark.streams.awaitAnyTermination()

        except KeyboardInterrupt:
            print("\n[INFO] Streaming stopped by user")
        except Exception as e:
            print(f"[ERROR] Streaming error: {e}")
            raise
        finally:
            if self.spark:
                print("[INFO] Stopping Spark session...")
                self.spark.stop()


def main():
    """Main execution function"""
    # Configuration from environment or defaults
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'statsbomb-eventos')
    checkpoint_location = os.getenv('CHECKPOINT_LOCATION', '/app/data/checkpoints')
    output_path = os.getenv('DATA_PROCESSED_PATH', '/app/data/processed')
    model_path = os.getenv('MODEL_PATH', '/app/data/models/modelo.joblib')

    # Create processor
    processor = StatsbombStreamProcessor(
        kafka_servers=kafka_servers,
        kafka_topic=kafka_topic,
        checkpoint_location=checkpoint_location,
        output_path=output_path,
        model_path=model_path if os.path.exists(model_path) else None
    )

    # Run streaming
    processor.run()


if __name__ == "__main__":
    main()
