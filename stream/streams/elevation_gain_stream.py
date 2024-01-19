from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, split, to_timestamp, sum as spark_sum, window

# Cluster config
conf = SparkConf() \
    .setAppName("elevation-gain-streamer") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.cores.max", "2") \
    .set("spark.executor.cores", "2") \

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


kafka_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "altitude_gains"
}

df = spark.readStream \
    .format("kafka") \
    .options(**kafka_input_options) \
    .option("failOnDataLoss", "false") \
    .load()

csv_df = df.select(col("value").cast("string"))

# Extract elevation gain and time
elevation_df = csv_df.select(
    split(col("value"), ',')[0].alias("time_str"),
    split(col("value"), ',')[1].alias("elevation_gain_str")
)

# Handling time
elevation_df = elevation_df.withColumn("time", to_timestamp(col("time_str")))

# Round gain to 4 decimals
elevation_df = elevation_df.withColumn("elevation_gain", col("elevation_gain_str").cast("float"))
elevation_df = elevation_df.withColumn("elevation_gain", round(col("elevation_gain"), 4))

# Tumbling 5 sec window
elevation_df = elevation_df \
    .withWatermark("time", "5 seconds") \
    .groupBy(window(col("time"), "5 seconds")) \
    .agg(spark_sum("elevation_gain").alias("elevation_gain_cum"))

elevation_df = elevation_df.withColumn("elevation_gain_cum", round(col("elevation_gain_cum"), 4))



def write_to_postgres(micro_batch_df, batch_id):
    # JDBC connection properties
    jdbc_url = "jdbc:postgresql://citus-coordinator:5432/running-analytics"
    pg_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Write the micro-batch DataFrame to PostgreSQL
    micro_batch_df.select(col("window.end").alias("time"), col("elevation_gain_cum").alias("value")).write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "altitude_gain") \
        .option("user", pg_properties["user"]) \
        .option("password", pg_properties["password"]) \
        .option("driver", pg_properties["driver"]) \
        .mode("append") \
        .save()

# Start the streaming query with foreachBatch
query = elevation_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()