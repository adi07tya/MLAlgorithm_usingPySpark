from pyspark.ml.feature import StringIndexer 
df = spark.createDataFrame([(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],
    ["user_id", "category"])

indexer = StringIndexer(inputCol='category', outputCol='categoryIndex')
indexed = indexer.fit(df).transform(df)
indexed.show()

from pyspark.ml.linalg import Vectors 
from pyspark.ml.linalg import VectorAssembler 

df = spark.createDataFrame(
    [(0, 18, 1.0, Vectors.dense([0.0, 10.0, 0.5]), 1.0)],
    ["id", "hour", "mobile", "userFeatures", "clicked"])
df.show()

assembler = VectorAssembler(
    inputCols=["hour", "mobile", "userFeatures"],
    outputCol="features")

output = assembler.transform(df)
print("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
output.select("features", "clicked").show()
