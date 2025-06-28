# Dataset: flights.csv

from pyspark.sql.functions import unix_timestamp, col

df = spark.read.csv("/FileStore/tables/flights.csv", header=True, inferSchema=True)

# Calculate delay
df = df.withColumn("delay_minutes",
    (unix_timestamp("actual_departure") - unix_timestamp("scheduled_departure")) / 60)

# Filter delayed flights
df.filter(col("delay_minutes") > 15).show()

# SQL View
df.createOrReplaceTempView("flights_view")
spark.sql("""
SELECT origin, destination, AVG(delay_minutes) AS avg_delay
FROM flights_view
GROUP BY origin, destination
ORDER BY avg_delay DESC
""").show()