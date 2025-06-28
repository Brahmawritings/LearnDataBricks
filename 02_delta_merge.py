# Practice: Delta Merge and Upsert

from delta.tables import DeltaTable

# Load data
df_orders = spark.read.csv("/FileStore/tables/orders.csv", header=True, inferSchema=True)
df_orders.write.format("delta").mode("overwrite").saveAsTable("orders_delta")

df_updates = spark.read.csv("/FileStore/tables/orders_updates.csv", header=True, inferSchema=True)

# Merge
orders_delta = DeltaTable.forName(spark, "orders_delta")
orders_delta.alias("target").merge(
    df_updates.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()