from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema when reading u.data
schema = StructType([StructField("userID", IntegerType(), True),
                    StructField("movieID", IntegerType(), True),
                    StructField("rating", IntegerType(), True),
                    StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
# \t means that it is a tab separated file and not a CSV file
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///home/sambiase/courses/SparkCourse/ml-100k/u.data")

# Some SQL-style magic to sort all movies by popularity in one line!
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
topMovieIDs.show(10)

# Stop the session
spark.stop()
