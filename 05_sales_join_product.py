# Practice: Sales and Product Join

df_sales = spark.read.csv("/FileStore/tables/sales_data.csv", header=True, inferSchema=True)
df_products = spark.read.csv("/FileStore/tables/products.csv", header=True, inferSchema=True)

df_joined = df_sales.join(df_products, "product_id", "inner")
df_joined.createOrReplaceTempView("sales_view")

spark.sql("""SELECT product_name, SUM(quantity * price) AS total_revenue
              FROM sales_view GROUP BY product_name ORDER BY total_revenue DESC""").show()