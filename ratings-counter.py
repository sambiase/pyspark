from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# lines is an RDD object that contains the dataset u.data
lines = sc.textFile("file:///home/sambiase/courses/SparkCourse/ml-100k/u.data")

# ratings is a new RDD with all ratings values
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
