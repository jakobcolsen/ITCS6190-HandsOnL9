# import the necessary libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
init = spark.readStream .format("socket") .option("host", "localhost").option("port", 9999).load()

# Convert timestamp column to TimestampType and add a watermark
result = init.select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())).withWatermark("timestamp", "1 minute")


# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
aggregated_query = result.groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("driver_id")).agg(sum("fare_amount").alias("total_fare"))

# Extract window start and end times as separate columns
window_query = aggregated_query.withColumn("window_start", col("window.start")).withColumn("window_end", col("window.end")).drop("window")

# Define a function to write each batch to a CSV file with column names
def write_batch_to_csv(batch_df, batch_id):
    # Save the batch DataFrame as a CSV file with headers included 
    batch_df.write.csv(f"output/task3/task3_{batch_id}", header=True)
    
# Use foreachBatch to apply the function to each micro-batch
query = window_query.writeStream \
    .foreachBatch(write_batch_to_csv) \
    .outputMode("update") \
    .start()

query.awaitTermination()
