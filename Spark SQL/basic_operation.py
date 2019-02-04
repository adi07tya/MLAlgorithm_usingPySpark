from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('people.json')

#Basic Operations
df.show()
df.printSchema()
df.columns
df.describe()
df['age']
type(df['age'])
df.select('age')
type(df.select('age'))
df.select('age').show()
df.head(2)
df.select(['age', 'name'])
df.select(['age', 'name']).show()

#Defining the schema manually
data_schema = [StructField("age", IntegerType(), True),StructField("name", StringType(), True)]
final_struc = StructType(fields=data_schema)
df = spark.read.json('people.json', schema=final_struc)
df.printSchema()

#Creating New Columns 
df.withColumn('newAge',df['age']).show()
df.show()
df.withColumnRenamed('age', 'supernewage').show()
df.withColumn('doubleage',df['age']*2).show()
df.withColumn('add_one_age',df[age]+1).show()
df.withColumn('half_age',df['age']/2).show()
df.withColumn('half_age',df['age']/2)

#Dataframes to sql
df.createOrReplaceTempView("people")
sql_results = spark.sql('SELECT * FROM people')
sql_results
sql_results.show()
spark.sql("SELECT * FROM people WHERE age=30").show()

