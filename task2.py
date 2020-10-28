import ast
import json
import sys

from pyspark import SparkConf, SparkContext

if len(sys.argv) == 1:
    # hdfs://localhost:9000/full/meta_Electronics.json
    # hdfs://localhost:9000/full/avg_rating.csv
    # hdfs://localhost:9000/full/output
    INPUT_META_FILE = 'hdfs://localhost:9000/part/sample_100_meta.json'
    INPUT_AVG_RATING_FILE = 'hdfs://localhost:9000/part/avg_rating.csv'
    OUTPUT_DIR = 'hdfs://localhost:9000/part/output'
elif len(sys.argv) == 2:
    INPUT_META_FILE = sys.argv[1]
    INPUT_AVG_RATING_FILE = 'hdfs://localhost:9000/part/avg_rating.csv'
    OUTPUT_DIR = 'hdfs://localhost:9000/part/output'
elif len(sys.argv) == 3:
    INPUT_META_FILE = sys.argv[1]
    INPUT_AVG_RATING_FILE = sys.argv[2]
    OUTPUT_DIR = 'hdfs://localhost:9000/part/output'
else:
    INPUT_META_FILE = sys.argv[1]
    INPUT_AVG_RATING_FILE = sys.argv[2]
    OUTPUT_DIR = sys.argv[3]


def getReviewMeta(jsonLine):
    decodeJsonLine = ast.literal_eval(jsonLine)
    try:
        asin = decodeJsonLine["asin"]
    except KeyError:
        asin = ''
    try:
        title = decodeJsonLine['title']
    except KeyError:
        title = ''
    return asin, title


def getAvgRating(csvLine):
    prod_id, price = csvLine.split(',')
    return prod_id, price


def toCSVFile(data):
    return data[0] + ',' + ','.join([str(value) for value in data[1]])


conf = SparkConf().setAppName("task2")
spark = SparkContext(conf=conf)
rddReviewMeta = spark.textFile(INPUT_META_FILE)
rddAvgRating = spark.textFile(INPUT_AVG_RATING_FILE)

rddProdIdTitle = rddReviewMeta.map(lambda row: getReviewMeta(row))
rddProdIdRating = rddAvgRating.map(lambda row: getAvgRating(row))

result = rddProdIdTitle.join(rddProdIdRating, numPartitions=1)

result.map(toCSVFile).saveAsTextFile(OUTPUT_DIR)
