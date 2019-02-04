# Loading Data 
from pyspark.ml import SparkSession 

spark = SparkSession.builder.appName('lr').getOrCreate()
data = spark.read.csv("Ecommerce_Customers.csv",inferSchema=True,header=True)
data.printSchema()
data.show()
data.head()
for item in data.head():
    print(item)

# Data preparation
from pyspark.ml.linalg import Vectors 
from pyspark.ml.feature import VectorAssembler 

data.columns 
assembler = VectorAssembler(
    inputCols=["Avg Session Length", "Time on App", 
               "Time on Website",'Length of Membership'],
    outputCol="features")
output = assembler.transform(data)
output.select('features').show()
output.show()

final_data = output.select(['features', 'Yearly Amount Spent'])
train_data, test_data = final_data.randomSplit([0.7, 0.3])
train_data.describe().show()
test_data.describe().show()

# Model Building 
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(labelCol='Yearly Amount Spent')
lrModel = lr.fit(train_data)
print("Coefficients: {} Intercept: {}".format(lrModel.coefficients,lrModel.intercept))

# Prediction
unlabeled_data = test_data.select('features')
predictions = lrModel.transform(unlabeled_data)
predictions.show()

# Model Evaluation 
test_results = lrModel.evaluate(test_data)
test_results.residuals.show()
print("RMSE: {}".format(test_results.rootMeanSquaredError))
print("MSE: {}".format(test_results.meanSquaredError))


