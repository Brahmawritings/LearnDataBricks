# Dataset: employees.csv

df = spark.read.csv("/FileStore/tables/employees.csv", header=True, inferSchema=True)
df.show()

# Filter by department
df.filter(df.department == 'Engineering').show()

# Average salary by department
df.groupBy("department").avg("salary").show()

# Create view for SQL
df.createOrReplaceTempView("employees_view")

spark.sql("""
SELECT department, COUNT(*) AS count, AVG(salary) AS avg_salary
FROM employees_view
GROUP BY department
""").show()