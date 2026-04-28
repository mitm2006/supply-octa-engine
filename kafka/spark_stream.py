from pyspark.sql import SparkSession
from pyspark.sql.functions import col   

spark = SparkSession.builder \
    .appName("KafkaSpark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .load()

words = df.selectExpr("CAST(value AS STRING)")

filtered = words.filter(
    col("value").contains("ERROR") | col("value").contains("WARNING")
)

query = filtered.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "C:/kafka-project/checkpoint") \
    .start()

query.awaitTermination()