# Practice: Data Quality

df_issues = spark.read.csv("/FileStore/tables/orders_with_issues.csv", header=True, inferSchema=True)
df_issues.filter("quantity IS NULL OR quantity <= 0").show()

from pyspark.sql.functions import count, when, col
df_issues.select([count(when(col(c).isNull(), c)).alias(c) for c in df_issues.columns]).show()