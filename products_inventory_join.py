# Datasets: products.csv, inventory.csv

products = spark.read.csv("/FileStore/tables/products.csv", header=True, inferSchema=True)
inventory = spark.read.csv("/FileStore/tables/inventory.csv", header=True, inferSchema=True)

# Join and calculate total stock value
df = products.join(inventory, "product_id")
df = df.withColumn("stock_value", df.price * df.stock)
df.show()

# SQL version
df.createOrReplaceTempView("product_inventory")
spark.sql("""
SELECT product_name, stock, price, stock * price AS stock_value
FROM product_inventory
ORDER BY stock_value DESC
""").show()