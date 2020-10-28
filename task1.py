import sys

from pyspark import SparkContext, SparkConf
import json

if len(sys.argv) == 1:
    # hdfs://localhost:9000/full/Electronics_5.json
    # hdfs://localhost:9000/full/output
    INPUT_FILE = 'hdfs://localhost:9000/part/samples_100.json'
    OUTPUT_DIR = 'hdfs://localhost:9000/part/output'
elif len(sys.argv) == 2:
    INPUT_FILE = sys.argv[1]
    OUTPUT_DIR = 'hdfs://localhost:9000/part/output'
else:
    INPUT_FILE = sys.argv[1]
    OUTPUT_DIR = sys.argv[2]


# построчная обработка json файла
def getReview(jsonLine):
    decodeJsonLine = json.loads(jsonLine)
    return (decodeJsonLine["asin"], decodeJsonLine["overall"])


# convert to csv
def toCSVFile(data):
    return ','.join([str(value) for value in data])


conf = SparkConf().setAppName("task1")

spark = SparkContext(conf=conf)
rddReview = spark.textFile(INPUT_FILE)

rddProdIdRating = rddReview.map(lambda row: getReview(row))
task1 = rddProdIdRating.aggregateByKey((0, 0), lambda x, value: (x[0] + value, x[1] + 1),
                                       lambda x, y: (x[0] + y[0], x[1] + y[1]), numPartitions=1). \
    mapValues(lambda y: y[0] / y[1]).map(toCSVFile)

task1.saveAsTextFile(OUTPUT_DIR)
