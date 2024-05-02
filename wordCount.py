# import findspark
# findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .master('yarn')\
    .appName("Python Spark DataFrames basic example") \
    .getOrCreate()

text = spark.sparkContext.textFile('hdfs://127.0.0.1:9000/data/varshini.txt')
text_fm = text.flatMap(lambda a:a.split(' '))
text_map = text.map(lambda a:(a,1)).reduceByKey(lambda a,b:a+1)
text_map.saveAsTextFile('hdfs://127.0.0.1:9000/data/word_count_result')