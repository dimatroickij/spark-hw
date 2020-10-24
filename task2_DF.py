import csv
import json

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, Row

INPUT_META_FILE_LOCAL = '/home/dimatroickij/datasets/sample_100_meta.json'
INPUT_AVG_RATING_FILE_LOCAL = '/home/dimatroickij/datasets/avg_rating.csv'
OUTPUT_FILE_LOCAL = '/home/dimatroickij/datasets/output'


def getReviewMeta(jsonLine):
    decodeJsonLine = json.loads(jsonLine)
    return (decodeJsonLine["asin"], decodeJsonLine["title"])


def getAvgRating(csvLine):
    decodeCsvLine = csv.reader(csvLine)
    response = []
    for value in decodeCsvLine:
        response.append(value)

    print(response)
    return response


def toCSVFile(data):
    return ','.join([str(value) for value in data])


conf = SparkConf().setAppName("task2")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()
rddReviewMeta = sc.textFile(INPUT_META_FILE_LOCAL)

rddReviewMetaDf = rddReviewMeta.map(lambda row: getReviewMeta(row)).toDF(['ProdId', 'Name'])

schema = StructType([StructField('ProdId', StringType(), False),
                     StructField('Rating', DoubleType(), False)])

rddAvgRating = spark.read.load(path=INPUT_AVG_RATING_FILE_LOCAL, format="csv", schema=schema, header="false",
                               inferSchema="false",
                               sep=",", nullValue="null", mode="DROPMALFORMED")

#numPartitions = 1
joinDf = rddReviewMetaDf.join(rddAvgRating, ['ProdId'], how='full')
joinDf.write.format('csv').option('header', 'false').save(OUTPUT_FILE_LOCAL)
