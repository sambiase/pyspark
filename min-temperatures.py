from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')        # splits the lines by , from 1800.csv
    stationID = fields[0]           # gets id from field pos 0
    entryType = fields[2]           # gets entryType from field pos 2
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0       # takes de temp and converts to Fahrenheit
    return (stationID, entryType, temperature)


lines = sc.textFile("file:///home/sambiase/courses/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)  # calls parseLine function
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1]) # just TMIN will be stored in minTemps
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f} Fahrenheit".format(result[1]))
