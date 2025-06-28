# Databricks Comprehensive Example: PySpark + SQL
# Dataset: sales_data.csv

# Load the CSV file
df = spark.read.csv("/FileStore/tables/sales_data.csv", header=True, inferSchema=True)
df.show(5)

# Inspect the schema
df.printSchema()

# Add a computed column (PySpark)
from pyspark.sql.functions import col, when

df_transformed = df.withColumn("high_quantity_flag", when(col("quantity") > 2, "Yes").otherwise("No"))

# Filter records with quantity > 1
df_filtered = df_transformed.filter(col("quantity") > 1)

# Group by region and get total quantity
df_grouped = df_filtered.groupBy("region").sum("quantity")
df_grouped.show()

# Save as temporary view for SQL queries
df_transformed.createOrReplaceTempView("sales_view")

# SQL: Total quantity by region
spark.sql("""
SELECT region, SUM(quantity) AS total_quantity
FROM sales_view
GROUP BY region
ORDER BY total_quantity DESC
""").show()

# SQL: Top selling products (assuming product_id exists)
spark.sql("""
SELECT product_id, SUM(quantity) AS total_sold
FROM sales_view
GROUP BY product_id
ORDER BY total_sold DESC
LIMIT 5
""").show()

# Write to Delta Lake
df_transformed.write.format("delta").mode("overwrite").saveAsTable("sales_data_transformed")

# Read back as a Delta table
df_delta = spark.read.table("sales_data_transformed")
df_delta.show(5)

# Optional: Cache the table
spark.sql("CACHE TABLE sales_data_transformed")

# End of notebook
print("All PySpark and SQL operations executed successfully.")