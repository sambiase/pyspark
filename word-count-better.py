import re
from pyspark import SparkConf, SparkContext

# re = regular expression
# r'\W+' --. breaks out the text in words
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///home/sambiase/courses/SparkCourse/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
