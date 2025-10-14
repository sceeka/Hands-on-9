# task1.py
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

HOST, PORT = "localhost", 9999

# 1) Spark session
spark = (SparkSession.builder
         .appName("Task1_EachRowToCSV")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 2) Schema matching data_generator.py
schema = StructType([
    StructField("trip_id",     StringType(),  True),
    StructField("driver_id",   IntegerType(), True),
    StructField("distance_km", DoubleType(),  True),
    StructField("fare_amount", DoubleType(),  True),
    StructField("timestamp",   StringType(),  True)
])

# 3) Read streaming JSON lines from socket
lines = (spark.readStream
         .format("socket")
         .option("host", HOST)
         .option("port", PORT)
         .load())

# 4) Parse JSON to columns
parsed = lines.select(from_json(col("value"), schema).alias("j")).select("j.*")

# 5) Write each micro-batch so every row is saved separately
def write_each_row(df, batch_id):
    rows = df.collect()
    for i, row in enumerate(rows):
        # create a tiny DataFrame with one row
        single_row_df = spark.createDataFrame([row.asDict()])
        single_row_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"outputs/task1/row_{batch_id}_{i}_{uuid.uuid4()}")

# 6) Stream to foreachBatch (no console output)
query = (parsed.writeStream
         .foreachBatch(write_each_row)
         .trigger(processingTime="5 seconds")
         .start())

# 7) Keep stream alive
query.awaitTermination()