'''
    How much in total a customer spent
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# creates a SparkSession
spark = SparkSession.builder.appName("Customer Orders Dataframe").getOrCreate()

# we are determining the Schema of the Table
schema = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("itemID", IntegerType(), True),
    StructField("totalSpent", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///home/sambiase/courses/SparkCourse/customer-orders.csv")
df.printSchema()

# Filter out all but TMIN entries
totalSpentByCustomer = df.groupBy("customerID").agg(func.round(func.sum("totalSpent"), 2).alias("total_spent"))
totalSpentByCustomerSorted = totalSpentByCustomer.sort("total_spent")
totalSpentByCustomerSorted.show(totalSpentByCustomerSorted.count())

spark.stop()
