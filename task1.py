from pyspark import SparkContext, SparkConf
import json

INPUT_FILE_LOCAL = '/home/dimatroickij/datasets/samples_100.json'
OUTPUT_FILE_LOCAL = '/home/dimatroickij/datasets/output'


def getReview(jsonLine):
    decodeJsonLine = json.loads(jsonLine)
    return (decodeJsonLine["asin"], decodeJsonLine["overall"])


def toCSVFile(data):
    return ','.join([str(value) for value in data])


conf = SparkConf().setAppName("task1")

spark = SparkContext(conf=conf)
rddReview = spark.textFile(INPUT_FILE_LOCAL)

rddProdIdRating = rddReview.map(lambda row: getReview(row))
task1 = rddProdIdRating.aggregateByKey((0, 0), lambda x, value: (x[0] + value, x[1] + 1),
                                       lambda x, y: (x[0] + y[0], x[1] + y[1]), numPartitions=1). \
    mapValues(lambda y: y[0] / y[1]).map(toCSVFile)

task1.saveAsTextFile(OUTPUT_FILE_LOCAL)
