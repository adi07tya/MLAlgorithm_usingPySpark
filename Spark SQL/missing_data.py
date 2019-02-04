from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('missing').getOrCreate()
df = spark.read.csv('ContainsNull.csv', inferSchema=True, header=True)
df.printSchema()
df.show()

# Drop any row that contains missing data
df.na.drop().show()

# Has to have at least 2 NON-null values
df.na.drop(thresh=2).show()

df.na.drop(subset=["Sales"]).show() #only considers specified columns 
df.na.drop(how='any').show()
df.na.drop(how='all').show()

# how : row level operation 
# thresh : row level operation 
# subset : column level operation

# filling the null values 
df.na.fill('NEW VALUE').show()
df.na.fill(0).show()
df.na.fill('No Name',subset=['Name']).show()

from pyspark.sql.functions import mean
mean_val = df.select(mean(df['Sales'])).collect()
# Weird nested formatting of Row object!
mean_sales = mean_val[0][0]
df.na.fill(mean_sales,["Sales"]).show()

# one liner for above operations 
df.na.fill(df.select(mean(df['Sales'])).collect()[0][0],['Sales']).show()
