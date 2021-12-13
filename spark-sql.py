from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(',')  # splits on commas since it is a CSV file
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))  # this is the info in the csv file


lines = spark.sparkContext.textFile("fakefriends.csv")  # this creates an RDD
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()  # cache keeps the Dataframe in memory
schemaPeople.createOrReplaceTempView("people")  # creates a DB view called people

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
print('\nThis is the output from a SQL query')
print('--------------------------------------------')
for teen in teenagers.collect():
    print(teen)

# We can also use functions instead of SQL queries:
print('\nThis is the output without the SQL query')
print('--------------------------------------------')
schemaPeople.groupBy("age").count().orderBy("age").show(n=1000)

spark.stop()
