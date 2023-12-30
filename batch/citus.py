from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


conf = SparkConf() \
    .setAppName("batch-preprocessing-citus") \
    .setMaster("spark://spark-master:7077")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/user/aleksandar/data/batch_sample.csv", header=True, inferSchema=True)
df = df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])
reduced_df = df.select("Event_name", "Athlete_ID")

jdbc_url = "jdbc:postgresql://citus-coordinator:5432/postgres"
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
reduced_df.write.jdbc(jdbc_url, "public.runners_reduced", mode="overwrite", properties=properties)
