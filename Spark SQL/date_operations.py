from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('date').getOrCreate()
df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)

df.show()

from pyspark.sql.functions import format_number, ,dayofmonth,hour,dayofyear,month,year,weekofyear,date_format
df.select(dayofmonth(df['Date'])).show()
df.select(hour(df['Date'])).show()
df.select(dayofyear(df['Date'])).show()
df.select(month(df['Date'])).show()
df.select(year(df['Date'])).show()
df.withColumn('Year', year(df['Date'])).show()

newdf = df.withColumn('Year', year(df['Date']))
newdf.groupby('Year').mean()[['avg(Year)', 'avg(Close)']].show()

#making it presentable 
result = newdf.groupBy("Year").mean()[['avg(Year)','avg(Close)']]
result = result.withColumnRenamed("avg(Year)","Year")
result = result.select('Year',format_number('avg(Close)',2).alias("Mean Close")).show()
