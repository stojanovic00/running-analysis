from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


conf = SparkConf() \
    .setAppName("batch-preprocessing") \
    .setMaster("spark://spark-master:7077")  # Executes job on whole cluster

# this connects to hive clusters metastore
# without it spark makes his own hive metastore, and by that we get 2 separate hive db instances
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

# Initialize Spark session with hive support
spark = SparkSession.builder \
    .enableHiveSupport() \
    .config(conf=conf)\
    .getOrCreate()

# Connecting to "default" db
spark.sql("USE default")

# Read CSV file from HDFS, infer schema, and modify column names
df = spark.read.csv("hdfs://namenode:9000/user/aleksandar/data/batch_sample.csv", header=True, inferSchema=True)
df = df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])


# Write the DataFrame to Hive
df.write.mode("overwrite").saveAsTable("runners")

spark.stop()
