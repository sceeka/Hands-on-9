# task2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, avg, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

HOST, PORT = "localhost", 9999

# 1) Create a Spark session
spark = (SparkSession.builder
         .appName("Task2_Aggregations_By_Driver")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 2) Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id",     StringType(),  True),   # UUID
    StructField("driver_id",   IntegerType(), True),   # matches your generator
    StructField("distance_km", DoubleType(),  True),
    StructField("fare_amount", DoubleType(),  True),
    StructField("timestamp",   StringType(),  True)    # "YYYY-MM-DD HH:MM:SS"
])

# 3) Read streaming data from socket
lines = (spark.readStream
         .format("socket")
         .option("host", HOST)
         .option("port", PORT)
         .load())

# 4) Parse JSON data into columns using the defined schema
parsed = lines.select(from_json(col("value"), schema).alias("j")).select("j.*")

# 5) Convert timestamp column to TimestampType and add a watermark
with_ts = parsed.withColumn(
    "event_time",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)
# Note: Watermark has effect with time-based ops; it's included here per instructions
watermarked = with_ts.withWatermark("event_time", "1 minute")

# 6) Compute aggregations: total fare and average distance grouped by driver_id
aggregated = (watermarked
              .groupBy("driver_id")
              .agg(
                  _sum("fare_amount").alias("total_fare"),
                  avg("distance_km").alias("avg_distance")
              ))

# 7) Define a function to write each batch to a CSV file
def write_batch(df, batch_id: int):
    # Save the batch DataFrame as a single CSV file with the batch ID in the path
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", "true")
       .csv(f"outputs/task2/batch_{batch_id}"))

# 8) Use foreachBatch to apply the function to each micro-batch
query = (aggregated.writeStream
         .outputMode("complete")  # required for non-windowed aggregations
         .foreachBatch(write_batch)
         .option("checkpointLocation", "checkpoints/task2")
         .trigger(processingTime="10 seconds")
         .start())

# 9) Keep the stream alive
query.awaitTermination()