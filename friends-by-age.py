# this script counts how many friends a certain age group has

from pyspark import SparkConf, SparkContext

# local = running on local computer
# setAppName = apps name
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")

# creates a SparkContext object
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    # returns key/value pairs
    return (age, numFriends)


# input data
lines = sc.textFile("file:///home/sambiase/courses/SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)

# rdd.mapValues(lambda x: (x, 1)) --> x =
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()

for result in results:
    print(result)
