# Dataset: sensor_readings.csv

df = spark.read.csv("/FileStore/tables/sensor_readings.csv", header=True, inferSchema=True)

# Find average temp, vibration per machine
df.groupBy("machine_id").avg("temperature", "vibration").show()

# Filter readings with high pressure
df.filter(df.pressure > 30).show()

# SQL example
df.createOrReplaceTempView("sensor_view")
spark.sql("""
SELECT machine_id, COUNT(*) AS total, AVG(temperature) AS avg_temp
FROM sensor_view
GROUP BY machine_id
""").show()