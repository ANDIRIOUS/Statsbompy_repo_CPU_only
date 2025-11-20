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
import pandas as pd
from utils.inference import create_predictor


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
        self.predictor = None

    def ensure_storage_paths(self):
        """
        Make sure checkpoint/output directories exist and are writable
        so Spark executors (running as a different user) can update them.
        """
        for path in (self.checkpoint_location, self.output_path):
            if not path:
                continue
            try:
                os.makedirs(path, exist_ok=True)
                os.chmod(path, 0o777)
            except OSError as exc:
                print(f"[WARNING] Could not prepare storage path {path}: {exc}")

    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        print("[INFO] Creating Spark session...")

        spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
            .config("spark.hadoop.fs.permissions.umask-mode", "000") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        print(f"[SUCCESS] Spark session created: {spark.sparkContext.applicationId}")
        print(f"[INFO] Spark UI available at: http://localhost:4040")

        self.spark = spark
        return spark

    def get_event_schema(self) -> StructType:
        """
        Define schema for Statsbomb events
        Updated to match the new flat structure from Statsbomb API

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
            # Changed from MapType to StringType for flat columns
            StructField("type", StringType(), True),
            StructField("possession_team", StringType(), True),
            StructField("play_pattern", StringType(), True),
            StructField("team", StringType(), True),
            StructField("player", StringType(), True),
            StructField("position", StringType(), True),
            StructField("location", ArrayType(FloatType()), True),
            StructField("under_pressure", BooleanType(), True),
            # Shot and pass related fields are now flat columns
            StructField("pass_outcome", StringType(), True),
            StructField("pass_recipient", StringType(), True),
            StructField("pass_length", FloatType(), True),
            StructField("shot_statsbomb_xg", FloatType(), True),
            StructField("shot_outcome", StringType(), True),
            StructField("shot_type", StringType(), True),
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
        Calculate possession count by team using windowing
        Note: Percentage calculation removed to avoid stream-stream join limitation

        Args:
            events_df: Parsed events DataFrame

        Returns:
            DataFrame with possession statistics
        """
        print("[INFO] Calculating possession statistics...")

        # Extract team name (new API: possession_team is a string, not a struct)
        possession_df = events_df \
            .filter(col("possession_team").isNotNull()) \
            .withColumn("team_name", col("possession_team")) \
            .withColumn("window_time", window(col("event_timestamp"), "5 minutes"))

        # Count possessions per team per window
        # Return counts instead of percentages to avoid stream-stream join
        possession_stats = possession_df \
            .groupBy("window_time", "team_name") \
            .agg(count("*").alias("possession_count")) \
            .select(
                col("window_time.start").alias("window_start"),
                col("window_time.end").alias("window_end"),
                "team_name",
                "possession_count"
            )

        return possession_stats

    def calculate_xg(self, events_df):
        """
        Calculate average Expected Goals (xG) by team

        Args:
            events_df: Parsed events DataFrame

        Returns:
            DataFrame with xG statistics
        """
        print("[INFO] Calculating xG statistics...")

        # Extract shot data (new API: team and shot columns are flat, not nested)
        xg_df = events_df \
            .filter(col("type") == "Shot") \
            .filter(col("shot_statsbomb_xg").isNotNull()) \
            .withColumn("team_name", col("team")) \
            .withColumn("xg_value", col("shot_statsbomb_xg").cast(FloatType())) \
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

        # Extract pass data (new API: flat structure)
        pass_df = events_df \
            .filter(col("type") == "Pass") \
            .withColumn("team_name", col("team")) \
            .withColumn(
                "is_complete",
                # In new API, pass_outcome is null/NaN when pass is successful
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
        Uses 'append' mode (Parquet only supports append, not update)

        Args:
            df: DataFrame to save (should be non-aggregated raw events)
            output_name: Name for the output directory
        """
        output_location = f"{self.output_path}/{output_name}"

        # Use 'append' mode - Parquet format only supports append
        # Note: This should only be used for raw events, not aggregated statistics
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
                scaler_path = self.model_path.replace("modelo.joblib", "scaler.joblib")
                feature_names_path = self.model_path.replace("modelo.joblib", "feature_names.joblib")
                self.predictor = create_predictor(
                    model_path=self.model_path,
                    scaler_path=scaler_path,
                    feature_names_path=feature_names_path
                )
                if self.predictor:
                    print("[SUCCESS] ML predictor loaded successfully")
                else:
                    print("[WARNING] Failed to create predictor")
            except Exception as e:
                print(f"[WARNING] Failed to load model: {e}")
                self.predictor = None
        else:
            print("[INFO] No ML model specified or found")

    def make_predictions_batch(self, batch_df, batch_id):
        """
        Process a micro-batch and make predictions
        This is called by foreachBatch for each batch of aggregated statistics

        Args:
            batch_df: Spark DataFrame with aggregated statistics
            batch_id: Batch ID
        """
        if self.predictor is None:
            return

        try:
            # Convert Spark DataFrame to Pandas for processing
            pandas_df = batch_df.toPandas()

            if pandas_df.empty:
                return

            # Extract features and make prediction
            prediction, probabilities = self.predictor.predict(pandas_df)

            # Get team names if available
            teams = pandas_df.get('team_name', pd.Series(['Home', 'Away'])).unique()
            home_team = teams[0] if len(teams) > 0 else "Home"
            away_team = teams[1] if len(teams) > 1 else "Away"

            # Format and display prediction
            prediction_str = self.predictor.format_prediction(
                prediction,
                probabilities,
                home_team=home_team,
                away_team=away_team
            )

            print(prediction_str)

        except Exception as e:
            print(f"[WARNING] Prediction failed for batch {batch_id}: {e}")

    def display_statistics_with_predictions(self, df, stat_name: str):
        """
        Display statistics to console and trigger predictions

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

    def combine_stats_for_prediction(self, batch_df, batch_id):
        """
        Combine statistics from all sources and make a prediction
        This is called by foreachBatch for combined statistics

        Args:
            batch_df: Combined Spark DataFrame with all statistics
            batch_id: Batch ID
        """
        if self.predictor is None:
            return

        try:
            # Convert to pandas
            pandas_df = batch_df.toPandas()

            if pandas_df.empty or len(pandas_df) < 2:
                return

            # Extract features using the predictor's method
            # We need to reshape the data - currently it's one row per team
            # but predictor expects features in specific format

            # Get unique teams
            teams = pandas_df['team_name'].unique()
            if len(teams) < 2:
                return

            home_team = teams[0]
            away_team = teams[1]

            # Build features dictionary
            features = {}

            for team, prefix in [(home_team, 'home'), (away_team, 'away')]:
                team_data = pandas_df[pandas_df['team_name'] == team].iloc[0]

                # Possession
                features[f'{prefix}_possession_count'] = team_data.get('possession_count', 0)

                # xG and shots
                features[f'{prefix}_xg'] = team_data.get('total_xg', 0)
                features[f'{prefix}_shots'] = team_data.get('shots_count', 0)

                # Passes
                features[f'{prefix}_passes'] = team_data.get('total_passes', 0)
                features[f'{prefix}_completed_passes'] = team_data.get('completed_passes', 0)
                features[f'{prefix}_pass_completion'] = team_data.get('pass_completion_pct', 0) / 100

                # Defaults for missing features
                features[f'{prefix}_duels'] = 0
                features[f'{prefix}_tackles'] = 0

            # Create features DataFrame
            features_df = pd.DataFrame([features])

            # Make prediction
            prediction, probabilities = self.predictor.predict(features_df)

            # Format and display prediction
            prediction_str = self.predictor.format_prediction(
                prediction,
                probabilities,
                home_team=home_team,
                away_team=away_team
            )

            print(prediction_str)

        except Exception as e:
            import traceback
            print(f"[WARNING] Prediction failed for batch {batch_id}: {e}")
            print(f"[DEBUG] Traceback: {traceback.format_exc()}")

    def create_combined_stats_stream(self, events_df):
        """
        Create a single stream that combines all statistics for predictions

        Args:
            events_df: Parsed events DataFrame

        Returns:
            Combined statistics DataFrame
        """
        print("[INFO] Creating combined statistics stream for predictions...")

        # Create a windowed aggregation that combines all stats
        windowed_events = events_df \
            .withColumn("window_time", window(col("event_timestamp"), "5 minutes"))

        # Combine all stats in one aggregation
        combined_stats = windowed_events \
            .withColumn("team_name", col("team")) \
            .groupBy("window_time", "team_name") \
            .agg(
                # Possession
                count("*").alias("possession_count"),
                # Shots and xG
                _sum(when(col("type") == "Shot", 1).otherwise(0)).alias("shots_count"),
                _sum(when(col("type") == "Shot", col("shot_statsbomb_xg")).otherwise(0)).alias("total_xg"),
                # Passes
                _sum(when(col("type") == "Pass", 1).otherwise(0)).alias("total_passes"),
                _sum(when((col("type") == "Pass") & col("pass_outcome").isNull(), 1).otherwise(0)).alias("completed_passes")
            ) \
            .withColumn(
                "pass_completion_pct",
                when(col("total_passes") > 0, (col("completed_passes") / col("total_passes") * 100)).otherwise(0)
            ) \
            .select(
                col("window_time.start").alias("window_start"),
                col("window_time.end").alias("window_end"),
                "team_name",
                "possession_count",
                "shots_count",
                "total_xg",
                "total_passes",
                "completed_passes",
                "pass_completion_pct"
            )

        return combined_stats

    def run(self):
        """Main execution method"""
        try:
            # Ensure filesystem locations exist before Spark touches them
            self.ensure_storage_paths()

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

            # Display statistics to console
            # Note: Aggregated statistics are only displayed, not saved to Parquet
            # because Parquet only supports 'append' mode, not the 'complete' mode
            # required for aggregations
            possession_query = self.display_statistics(possession_stats, "Possession")
            xg_query = self.display_statistics(xg_stats, "xG")
            pass_query = self.display_statistics(pass_stats, "Pass Completion")

            # Create combined statistics stream for predictions
            if self.predictor is not None:
                combined_stats = self.create_combined_stats_stream(events_df)

                # Use foreachBatch to make predictions on each batch
                prediction_query = combined_stats.writeStream \
                    .outputMode("complete") \
                    .foreachBatch(self.combine_stats_for_prediction) \
                    .start()

                print("[INFO] Real-time predictions enabled")

            print("\n" + "="*60)
            print("[SUCCESS] All streaming queries started")
            print("[INFO] Raw events are saved to Parquet")
            print("[INFO] Statistics are displayed to console only")
            if self.predictor is not None:
                print("[INFO] ML predictions will be shown every 5 minutes")
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
