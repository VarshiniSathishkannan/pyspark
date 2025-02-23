from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

transaction_schema = StructType([StructField('customer_id',IntegerType(),False),StructField('transaction_type',StringType(),False),StructField('transaction_amount',FloatType(),False)])

transaction_data = [(1,'credit',30.0),(1,'debit',90.0),(2,'credit',50.0),(3,'debit',57.0),(2,'debit',90.0)]

transaction_df = spark.createDataFrame(transaction_data,schema=transaction_schema)

transaction_df.show()

amount_schema = StructType([StructField('customer_id',IntegerType(),False),StructField('amount',FloatType(),False)])

amount_data = [(1,1000.0),(2,2000.0),(3,3000.0),(4,4000.0)]

amount_df = spark.createDataFrame(amount_data,schema=amount_schema)

amount_df.show()

credit_df = transaction_df.groupBy('customer_id').agg(sum(when(transaction_df.transaction_type == 'credit',transaction_df.transaction_amount).otherwise(0)).alias('total credit'))

credit_df.show()

debit_df = transaction_df.groupBy('customer_id').agg(sum(when(transaction_df.transaction_type == 'debit',transaction_df.transaction_amount).otherwise(0)).alias('total dedit'))

debit_df.show()

credit_debit_df = credit_df.join(debit_df)