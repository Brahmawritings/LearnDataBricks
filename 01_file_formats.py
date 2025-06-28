# Practice: File Formats (CSV, JSON, Parquet)

# Load CSV
df_csv = spark.read.csv("/FileStore/tables/employees.csv", header=True, inferSchema=True)
df_csv.show()

# Save as Parquet
df_csv.write.mode("overwrite").parquet("/FileStore/output/employees_parquet")

# Read Parquet
df_parquet = spark.read.parquet("/FileStore/output/employees_parquet")
df_parquet.show()