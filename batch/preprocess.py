from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("running-analysis").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)


df = spark.read.csv("hdfs://namenode:9000/user/aleksandar/data/batch_sample.csv", header=True, inferSchema=True)
# df = spark.read.csv("../datasets/batch_sample.csv", header=True, inferSchema=True)

# Split the DataFrame based on the type of race
df_time_based = df.filter(col("Event distance/length").contains('d') | col("Event distance/length").contains('h'))
df_distance_based = df.subtract(df_time_based)

# Write the DataFrame to a CSV file
df_time_based.write.csv("hdfs://namenode:9000/user/aleksandar/data/time_based", header=True, mode="overwrite")
df_time_based.write.csv("hdfs://namenode:9000/user/aleksandar/data/distance_based", header=True, mode="overwrite")
# df_time_based.write.csv("../datasets/time_based", header=True, mode="overwrite")
# df_distance_based.write.csv("../datasets/distance_based", header=True, mode="overwrite")
