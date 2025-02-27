from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as t

# Initialize Spark session
spark = SparkSession.builder.appName("Top5BestSellingProducts") \
    .config("spark.executor.memory",'4g') \
    .config("spark.driver.memory",'6g') \
    .config("spark.sql.sources.partitionOverwriteMode","dynamic").getOrCreate()
    # static will delete all the partitions whereas dynamic will overwite only the partitions that have new data

# Sample DataFrame: Replace this with your actual sales data
# Assuming sales_data contains product_id, product_name, category, and sales_amount
sales_data = [
    ("p1", "Product A", "Electronics", 150),
    ("p2", "Product B", "Electronics", 200),
    ("p3", "Product C", "Furniture", 100),
    ("p4", "Product D", "Furniture", 250),
    ("p5", "Product E", "Electronics", 300),
    ("p6", "Product F", "Electronics", 50),
    ("p7", "Product G", "Furniture", 150),
    ("p8", "Product H", "Furniture", 350),
    ("p9", "Product I", "Furniture", 100),
    ("p10", "Product J", "Electronics", 400),
]

sales_data_schema = t.StructType(
    [t.StructField('id',t.StringType()),
     t.StructField('name',t.StringType()),
     t.StructField('category',t.StringType()),
     t.StructField('amount',t.IntegerType())
     ])

sales_data_df = spark.createDataFrame(sales_data,sales_data_schema)

result = sales_data_df.groupBy('category','id','name').agg(F.sum('amount').alias('total_sales'))

result.show(truncate=False)

window_spec = Window.partitionBy("category").orderBy(F.desc("total_sales"))

ranked_df = result.withColumn("rank", F.row_number().over(window_spec))

ranked_df.show(truncate=False)

spark.stop()