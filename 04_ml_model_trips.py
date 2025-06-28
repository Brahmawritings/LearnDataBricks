# Practice: ML Regression on Trips Dataset

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

df = spark.read.csv("/FileStore/tables/trips.csv", header=True, inferSchema=True).dropna()
assembler = VectorAssembler(inputCols=["distance_km"], outputCol="features")
df_transformed = assembler.transform(df)

train, test = df_transformed.randomSplit([0.8, 0.2], seed=1)
lr = LinearRegression(featuresCol="features", labelCol="duration_min")
model = lr.fit(train)

predictions = model.transform(test)
predictions.select("duration_min", "prediction").show()