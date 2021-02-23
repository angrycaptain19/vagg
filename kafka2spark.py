from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]")\
    .config("spark.mongodb.input.uri", "mongodb://localhost/demo.kafka_mqtt")\
    .config("spark.mongodb.output.uri", "mongodb://localhost/demo.kafka_mqtt")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
#    .option("subscribe", "test1")
#
#df1 = df.selectExpr("CAST(value AS STRING) as kafka_output")
#
#df1.writeStream.format("console").outputMode("Append").start().awaitTermination()
#
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "test1") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
