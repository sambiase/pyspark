from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')  # splits the lines by , from 1800.csv
    stationID = fields[0]  # gets id from field pos 0
    entryType = fields[2]  # gets entryType TMIN or TMAX from field pos 2
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0  # takes de temp and converts to Fahrenheit
    return (stationID, entryType, temperature)


lines = sc.textFile("file:///home/sambiase/courses/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)  # calls parseLine function
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])  # just TMAX will be stored in minTemps
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()

for result in results:
    # print(result[0] + "\t{:.2f} Fahrenheit".format(result[1]))
    print(f'Statio ID: {result[0]} \t {result[1]} Fahrenheit ')
