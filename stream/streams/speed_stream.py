from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, split, to_timestamp, date_format, concat, lit

# Cluster config
conf = SparkConf() \
    .setAppName("speed-streamer") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.cores.max", "4") \
    .set("spark.executor.cores", "2") \

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


kafka_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "run_stats"
}

df = spark.readStream \
    .format("kafka") \
    .options(**kafka_input_options) \
    .option("failOnDataLoss", "false") \
    .load()

csv_df = df.select(col("value").cast("string"))

# Extract speed and time
speed_df = csv_df.select(
    split(col("value"), ',')[0].alias("time_str"),
    split(col("value"), ',')[6].alias("value")
)

# Handling time
speed_df = speed_df.withColumn("time", to_timestamp(col("time_str")))

# Round speed to 4 decimals
speed_df = speed_df.withColumn("value", col("value").cast("float"))
speed_df = speed_df.withColumn("value", round(col("value"), 4))
speed_df = speed_df.withColumn("value", col("value").cast("string"))

speed_df = speed_df.withColumn("time_str", date_format("time", "yyyy-MM-dd HH:mm:ss"))
speed_df = speed_df.withColumn("value", concat(col("time_str"), lit(","), col("value")))

# Define Kafka parameters for writing to the "speed" topic
write_kafka_params = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "topic": "speed"
}


# Write the speed data to the "speed" topic
query = speed_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .options(**write_kafka_params) \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/speed_streamer/") \
    .start()


# Wait for the streaming query to finish
query.awaitTermination()
