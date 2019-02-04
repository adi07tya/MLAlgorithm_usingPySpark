from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('filter').getOrCreate()
df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)
df.printSchema()

#filtering (Sql way)
df.filter('Close<500').show()
df.filter('Close<500').select('Open').show()
df.filter('Close<500').select(['Open', 'Close']).show()

#filtering (Dataframe way)
df.filter(df['Close'] < 200).show()
df.filter(df['Close'] < 200 and df['Open'] > 200).show() #error
df.filter(df['Close'] < 200 & df['Open'] > 200).show() #error
df.filter((df['Close'] < 200) & (df['Open'] > 200)).show()
df.filter( (df['Close'] < 200) | (df['Open'] > 200) ).show()
df.filter( (df['Close'] < 200) & ~(df['Open'] < 200) ).show()
df.filter(df['Low'] == 197.16).show()
df.filter(df["Low"] == 197.16).collect() # Returns python object ([Row(Date=datetime.datetime(2010, 1, 22, 0, 0), Open=206.78000600000001, High=207.499996, Low=197.16, Close=197.75, Volume=220441900, Adj Close=25.620401)])
result = df.filter(df["Low"] == 197.16).collect()
type(result[0])
row = result[0]
# Row to Dict
row.asDict()
for item in result[0]:
    print(item)
