from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

conf = SparkConf() \
    .setAppName("avg-speed-streamer") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.cores.max", "4") \
    .set("spark.executor.cores", "2") \

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


# Define Kafka parameters
kafka_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "speed"
}

# Read from Kafka using the Kafka source for Spark Structured Streaming
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_input_options) \
    .option("failOnDataLoss", "false") \
    .load()

raw_df = df.select(col("value").cast("string"))

speed_df = raw_df.withColumn("speed", col("value").cast("float"))




# Define Kafka parameters for writing to the "speed" topic
write_kafka_params = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "topic": "avg_speed"
}


# Write the speed data to the "speed" topic
#     query = speed_df.writeStream \
query = raw_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .options(**write_kafka_params) \
    .option("checkpointLocation", "/tmp/avg_speed_streamer/") \
    .start()


# Wait for the streaming query to finish
query.awaitTermination()
