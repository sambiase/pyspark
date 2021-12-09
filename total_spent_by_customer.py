# find total amount spent by customer ID

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Total amount spent by customer")
sc = SparkContext(conf=conf)


def splitLine(line):
    fields = line.split(',')     # splits the lines by , from 1800.csv
    customerID = int(fields[0])      # gets id from field pos 0
    amountSpent = float(fields[2])        # gets entryType TMIN or TMAX from field pos 2
    return (customerID, amountSpent)


input = sc.textFile("file:///home/sambiase/courses/SparkCourse/customer-orders.csv")
splitLines = input.map(splitLine)
totalByCustomer = splitLines.reduceByKey(lambda x,y:x+y)

results = totalByCustomer.collect()

for i in results:
    print (i)
