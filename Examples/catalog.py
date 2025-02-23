from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import sys

spark = SparkSession.builder \
    .appName('Beeline to spark SQL') \
    .enableHiveSupport() \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("hive.exec.dynamic.partition","true")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
spark.conf.set("spark.sql.shuffle.partitions","600")
spark.conf.set("spark.default.parallelism","600")

file = open("C:/Users/varsh/Sample_Data/tablename_sqp_query_mapping.json")
config = json.load(file)

tablename = sys.argv[1]

print("tablename is ",tablename)

if not tablename:
    print("No tablenae given, exiting the application")
    sys.exit(1)
    
query = config.get(tablename)
print(query)

if not query:
    print("tablename not found in the config....exiting the application")
    sys.exit(1)
    
spark.sql(query)

spark.stop()