# Dataset: transactions.csv

df = spark.read.csv("/FileStore/tables/transactions.csv", header=True, inferSchema=True)

# Filter suspicious transactions
df.filter("amount > 2000 OR is_fraud = 1").show()

# Aggregate by location
df.groupBy("location").agg({"amount": "sum", "is_fraud": "sum"}).show()

# SQL
df.createOrReplaceTempView("tx_view")
spark.sql("""
SELECT location, COUNT(*) AS total_tx, SUM(is_fraud) AS fraud_count
FROM tx_view
GROUP BY location
""").show()