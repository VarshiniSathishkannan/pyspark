https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

# col:
Purpose: col is a function used to refer to a column in a DataFrame. It helps in accessing columns by name and performing operations on them.

Use Case: You use col when you want to refer to a column within a DataFrame and apply transformations like arithmetic, filtering, or aggregation.

Syntax: col("column_name")

Example:


import org.apache.spark.sql.functions.col

val df = spark.createDataFrame(Seq(
  (1, "A"), 
  (2, "B"), 
  (3, "C")
)).toDF("id", "name")

// Using `col` to access the "id" column
df.select(col("id")).show()
Key Points:

col("column_name") returns a Column object.

You can apply operations on the column, such as col("id") + 1 or use it in filtering like df.filter(col("id") > 1).

# lit:
Purpose: lit is used to create a literal (constant) column in a DataFrame. This is useful when you want to add a constant value as a column or in operations.

Use Case: You use lit when you need to use a constant value within a DataFrame transformation. For example, adding a constant column to a DataFrame or using it in expressions.

Syntax: lit(value)

Example:


import org.apache.spark.sql.functions.lit

val df = spark.createDataFrame(Seq(
  (1, "A"), 
  (2, "B"), 
  (3, "C")
)).toDF("id", "name")

// Adding a constant column with value 100
df.select(col("id"), lit(100).alias("constant_column")).show()
Key Points:

lit(value) creates a column with a constant value for every row.

This is particularly useful for creating new columns or comparisons (e.g., col("id") > lit(10)).

# max:
Purpose: max is an aggregate function used to find the maximum value in a column.

Use Case: You use max when you want to calculate the maximum value from a specific column in a DataFrame.

Syntax: max(column)

Example:

import org.apache.spark.sql.functions.max

val df = spark.createDataFrame(Seq(
  (1, "A"), 
  (2, "B"), 
  (3, "C")
)).toDF("id", "name")

// Getting the maximum value from the "id" column
df.agg(max("id")).show()
Key Points:

max("column_name") returns the maximum value from the specified column.

It’s an aggregate function, so it’s often used in conjunction with groupBy for grouped data or agg for global aggregations.

# when:
Purpose: when is used for conditional logic. It is similar to a CASE or IF statement in SQL. It allows you to define conditions and return different values based on those conditions.

Use Case: You use when for creating new columns based on conditional logic, such as categorizing data or performing transformations based on a condition.

Syntax: when(condition, value)

Example:


import org.apache.spark.sql.functions.{when, col}

val df = spark.createDataFrame(Seq(
  (1, "A"), 
  (2, "B"), 
  (3, "C")
)).toDF("id", "name")

// Create a new column "category" based on a condition
val result = df.withColumn("category", 
  when(col("id") > 1, "GreaterThan1").otherwise("LessThanOrEqualTo1")
)
result.show()
Key Points:

when(condition, value) creates a conditional column, where condition is a boolean expression, and value is the value to return when the condition is true.

.otherwise(value) is used to define the value when the condition is false.

You can chain multiple when conditions to build more complex logic (like when(condition1, value1).otherwise(when(condition2, value2).otherwise(default))).