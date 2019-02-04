from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('groupbyagg').getOrCreate()
df = spark.read.csv('sales_info.csv', inferSchema=True, header=True)
df.printSchema()
df.show()

#groupby
df.groupBy('Company')
df.groupBy('Company').mean().show()
df.groupBy('Company').count().show()
df.groupBy('Company').max().show()
df.groupBy('Company').min().show()
df.groupBy('Company').sum().show()

#orderby
df.orderBy('Sales').show()
df.orderBy(df['Sales'].desc).show()

# Max accross all rows of particular column
df.agg({'Sales':'max'}).show() # Sales is column and max is an operation

#Combining groupby and agg 
grouped = df.groupBy('Company')
grouped.agg({'Sales': 'max'}).show()

#using builtin functions 
from pyspark.sql.functions import countDistinct, avg, stddev 
df.select(countDistinct('Sales')).show()
df.select(countDistinct('Sales').alias('Distinct Sales')).show()
df.select(avg('Sales')).show()
df.select(stddev('Sales')).show()

from pyspark.sql.functions import format_number 
sales_std = df.select(stddev('Sales').alias('std'))
sales_std.show()
sales_std.select(format_number('std', 2)).show()
