import time
from pyspark.sql import *
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *  
from pyspark.streaming.kafka import KafkaUtils
import json



spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .config("spark.sql.streaming.schemaInference","true") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "get-it-done-instance.c.get-it-done-project.internal:6667") \
  .option("subscribe", "twitterstream") \
  .option("startingOffsets", "earliest") \
  .option("failOnDataLoss","false") \
  .load()

spark.conf.set("spark.sql.shuffle.partitions", "4")

df.printSchema()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

schema = StructType() \
    .add("created_at", StringType()) \
    .add("id", DecimalType(38,0)) \
    .add("id_str", StringType()) \
    .add("text", StringType()) \
    .add("source", StringType()) \
    .add("truncated", StringType()) \
    .add("in_reply_to_status_id", StringType()) \
    .add("in_reply_to_status_id_str", StringType()) \
    .add("in_reply_to_user_id", StringType()) \
    .add("in_reply_to_screen_name", StringType()) \
    .add("user", StructType()) \
    .add("geo", StringType()) \
    .add("coordinates", StringType()) \
    .add("place", StringType()) \
    .add("contributors", StringType()) \
    .add("retweeted_status", StructType()) \
    .add("is_quote_status", StringType()) \
    .add("quote_count", StringType()) \
    .add("reply_count", StringType()) \
    .add("retweet_count", StringType()) \
    .add("favorite_count", StringType()) \
    .add("entities", StructType()) \
    .add("favorited", StringType()) \
    .add("retweeted", StringType()) \
    .add("lang", StringType()) \
    .add("favorite_count", StringType()) \
    .add("timestamp_ms", StringType())


parsed= df.selectExpr("CAST(value AS STRING)") \
  .select(from_json("value", schema).alias("value")) \
  .select(from_utc_timestamp(from_unixtime(col("value.timestamp_ms").cast("long")/1000),'MST').alias("timestamp_ms")) \
  .withWatermark("timestamp_ms", "1 minute") \
  .groupBy(window("timestamp_ms", "1 minute")) \
  .count() \
  .select(to_json( struct("window.start","window.end","count")).cast("string").alias("value"))

parsed.printSchema()

query = parsed \
  .writeStream \
  .outputMode("append") \
  .trigger(processingTime='1 minute') \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "get-it-done-instance.c.get-it-done-project.internal:6667") \
  .option("topic", "test") \
  .option("checkpointLocation", "/projects/twitterAnalysis/spark/checkpoint") \
  .start()

query.awaitTermination()


'''

parsed= df.selectExpr("CAST(value AS STRING)") \
  .select(from_json("value", schema).alias("value")) \
  .select(from_utc_timestamp(from_unixtime(col("value.timestamp_ms").cast("long")/1000),'MST').alias("timestamp_ms")) \
  .withWatermark("timestamp_ms", "10 minutes") \
  .groupBy(window("timestamp_ms", "5 minutes")) \
  .count() \
  .select(to_json(struct( "window.start","window.end","count")).cast("string").alias("value"))



  .selectExpr("CAST(userId AS STRING) AS key", col("id").cast("string").alias("value")) \
## To Write to standard output ##
query = parsed \
        .writeStream\
        .trigger(processingTime='1 minute') \
        .outputMode('update')\
        .option("truncate","false") \
        .format('console')\
        .start()


query = parsed \
        .writeStream\
        .trigger(processingTime='1 minute') \
        .outputMode('append')\
        .format('json')\
        .option("path","/tmp/sparkStreaming/twitter/count/data") \
        .option("checkpointLocation", "/tmp/sparkStreaming/twitter/count/check") \
        .start()
'''

'''
## To write to kafka ##
query = parsed \
  .writeStream \
  .outputMode("complete") \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "get-it-done-instance.c.get-it-done-project.internal:6667") \
  .option("topic", "test") \
  .option("checkpointLocation", "/projects/twitterAnalysis/spark/checkpoint") \
  .start()


'''

'''
	>>> cd /usr/hdp/current/spark2-client/
	>>> sudo -u hdfs ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --master yarn-client --driver-cores 1 --executor-cores 1 /projects/twitterAnalysis/spark/structuredStreamingTest.py
'''
