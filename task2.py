import csv
import json

from pyspark import SparkConf, SparkContext

INPUT_META_FILE_LOCAL = '/home/dimatroickij/datasets/sample_100_meta.json'
INPUT_AVG_RATING_FILE_LOCAL = '/home/dimatroickij/datasets/avg_rating.csv'
OUTPUT_FILE_LOCAL = '/home/dimatroickij/datasets/output'


def getReviewMeta(jsonLine):
    decodeJsonLine = json.loads(jsonLine)
    return (decodeJsonLine["asin"], decodeJsonLine["title"])


def getAvgRating(csvLine):
    part = list()
    for record in csvLine:
        part.append(tuple(record.split(',')))
    print(part)
    return part
#    decodeCsvLine = csvLine.split(',')
    #decodeCsvLine = csv.reader(csvLine)
#    response = []
#    for value in decodeCsvLine:
#        response.append(value)
#    return response


def toCSVFile(data):
    return ','.join([str(value) for value in data])


conf = SparkConf().setAppName("task2")
spark = SparkContext(conf=conf)
rddReviewMeta = spark.textFile(INPUT_META_FILE_LOCAL)
rddAvgRating = spark.textFile(INPUT_AVG_RATING_FILE_LOCAL)

rddProdIdTitle = rddReviewMeta.map(lambda row: getReviewMeta(row))
rddProdIdRating = rddAvgRating.mapPartitions(lambda row: getAvgRating(row)).collect()

result = rddProdIdTitle.join(rddProdIdRating, numPartitions=1)

result.mapValues(toCSVFile).saveAsTextFile(OUTPUT_FILE_LOCAL)
