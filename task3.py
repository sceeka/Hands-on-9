# task3.py

# import the necessary libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, sum as _sum
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

HOST, PORT = "localhost", 9999

# Create a Spark session
spark = (SparkSession.builder
         .appName("Task3_WindowedAggregation")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id",     StringType(),  True),
    StructField("driver_id",   IntegerType(), True),
    StructField("distance_km", DoubleType(),  True),
    StructField("fare_amount", DoubleType(),  True),
    StructField("timestamp",   StringType(),  True)  # "YYYY-MM-DD HH:MM:SS"
])

# Read streaming data from socket
lines = (spark.readStream
         .format("socket")
         .option("host", HOST)
         .option("port", PORT)
         .load())

# Parse JSON data into columns using the defined schema
parsed = lines.select(from_json(col("value"), schema).alias("j")).select("j.*")

# Convert timestamp column to TimestampType and add a watermark
with_ts = parsed.withColumn(
    "event_time",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
).withWatermark("event_time", "1 minute")

# Task 3 # Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed = (with_ts
            .groupBy(window(col("event_time"), "5 minutes", "1 minute"))
            .agg(_sum("fare_amount").alias("sum_fare")))

# Extract window start and end times as separate columns
final_df = (windowed
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("sum_fare")
            ))

# Define a function to write each batch to a CSV file with column names
def write_batch(df, batch_id: int):
    # Save the batch DataFrame as a CSV file with headers included
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", "true")
       .csv(f"outputs/task3/batch_{batch_id}"))

# Use foreachBatch to apply the function to each micro-batch
query = (final_df.writeStream
         .outputMode("append")  # append is standard for windowed + watermark
         .foreachBatch(write_batch)
         .option("checkpointLocation", "checkpoints/task3")
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()