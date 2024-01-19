from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, split

conf = SparkConf() \
    .setAppName("speed-streamer") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.cores.max", "4") \
    .set("spark.executor.cores", "2")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


# Define Kafka parameters
kafka_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "run_stats"
}

# Read from Kafka using the Kafka source for Spark Structured Streaming
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_input_options) \
    .option("failOnDataLoss", "false") \
    .load()

csv_df = df.select(col("value").cast("string"), col("timestamp"))

# Extract altitude and keep timestamp
speed_df = csv_df.select(
    col("timestamp"),
    split(col("value"), ',')[3].alias("value")
)

# Round altitude to 4 decimals
speed_df = speed_df.withColumn("value", col("value").cast("float"))
speed_df = speed_df.withColumn("value", round(col("value"), 4))
speed_df = speed_df.withColumn("value", col("value").cast("string"))



# Define Kafka parameters for writing to the "speed" topic
write_kafka_params = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "topic": "total_altitude"
}


# Write the speed data to the "speed" topic
query = speed_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .options(**write_kafka_params) \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/altitude_streamer/") \
    .start()


# Wait for the streaming query to finish
query.awaitTermination()
