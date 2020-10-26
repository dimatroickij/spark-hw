import csv
import json

from pyspark import SparkConf, SparkContext

INPUT_META_FILE_LOCAL = '/home/dimatroickij/datasets/sample_100_meta.json'
INPUT_AVG_RATING_FILE_LOCAL = '/home/dimatroickij/datasets/avg_rating.csv'
OUTPUT_FILE_LOCAL = '/home/dimatroickij/datasets/output'


def getReviewMeta(jsonLine):
    decodeJsonLine = json.loads(jsonLine)
    return decodeJsonLine["asin"], decodeJsonLine["title"]


def getAvgRating(csvLine):
    prod_id, price = csvLine.split(',')
    return prod_id, price


def toCSVFile(data):
    return data[0] + ',' + ','.join([str(value) for value in data[1]])
    # print(type(data.split(',')[1]))
    #    return data.split(',')[0] + '+' + data.split(',')[1]
    # return ','.join([str(value) for value in data])


conf = SparkConf().setAppName("task2")
spark = SparkContext(conf=conf)
rddReviewMeta = spark.textFile(INPUT_META_FILE_LOCAL)
rddAvgRating = spark.textFile(INPUT_AVG_RATING_FILE_LOCAL)

rddProdIdTitle = rddReviewMeta.map(lambda row: getReviewMeta(row))
rddProdIdRating = rddAvgRating.map(lambda row: getAvgRating(row))

result = rddProdIdTitle.join(rddProdIdRating, numPartitions=1)

result.map(toCSVFile).saveAsTextFile(OUTPUT_FILE_LOCAL)
