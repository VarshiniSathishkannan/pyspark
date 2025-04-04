spark-shell --master yarn --num-executors 4 --executor-cores 4 --executor-memory 4G --driver-memory 2G --conf 'spark.sql.storeAssignmentPolicy=LEGACY' --conf 'spark.sql.analyzer.failAmbiguousSelfJoin=false'

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val df = spark.sql("select * from db.table where id not in ('a','b)")

val col = spark.table("db.table").columns

df.select(col.head,col.tail:_*).coalesce(1).write.option("compression","none").option("sep","^").option("emptyValue","").mode("overwrite").csv("gs://")

