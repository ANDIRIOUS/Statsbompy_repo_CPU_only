from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspectData").getOrCreate()
df = spark.read.parquet("/app/data/raw_events")
print(f"Total rows: {df.count()}")
df.printSchema()
df.show(5, truncate=False)

# Check for nulls in critical columns
df.select("shot_statsbomb_xg", "match_id", "team").show(5)
