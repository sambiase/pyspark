# this script counts how many friends a certain age group has

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# reads the CSV file and convert is automatically into a DataFrame
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///home/sambiase/courses/SparkCourse/fakefriends-header.csv")


print("Print the Table's Schema\n")
people.printSchema()

print("Print all names \n")
people.select("name").show()

print('Average friends per age group and sort by age')
people.groupBy("age").avg("friends").sort("age").show()

spark.stop()
