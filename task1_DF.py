import sys

from pyspark import SparkContext, SparkConf
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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


def getReview(review):
    decodeReview = json.loads(review)
    return (decodeReview["asin"], decodeReview["overall"])


conf = SparkConf().setAppName("task1")
sc = SparkContext(conf=conf)

rddReview = sc.textFile(INPUT_FILE)

rddProdIdRating = rddReview.map(lambda row: getReview(row))
task1 = rddProdIdRating.aggregateByKey((0, 0), lambda x, value: (x[0] + value, x[1] + 1),
                                       lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda y: y[0] / y[1])

schema = StructType([StructField("ProdId", StringType(), nullable=False),
                     StructField("Rating", DoubleType(), False)])

result = SparkSession.builder.getOrCreate().createDataFrame(task1, schema)
result.coalesce(1).write.format('csv').option('header', 'false').save(OUTPUT_DIR)
