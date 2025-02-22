from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master('local').appName('Streaming').getOrCreate()

KAFKA_TOPIC_NAME = "first_kafka_topic"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

sampleDataframe = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

sampleDataframe.printSchema()