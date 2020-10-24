from pyspark import SparkContext, SparkConf
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

INPUT_FILE = '/home/dimatroickij/datasets/samples_100.json'
OUTPUT_FILE = '/home/dimatroickij/datasets/output'


def getReview(jsonLine):
    decodeJsonLine = json.loads(jsonLine)
    return (decodeJsonLine["asin"], decodeJsonLine["overall"])


conf = SparkConf().setAppName("task1")
spark = SparkContext(conf=conf)

rddReview = spark.textFile(INPUT_FILE)

rddProdIdRating = rddReview.map(lambda row: getReview(row))
task1 = rddProdIdRating.aggregateByKey((0, 0), lambda x, value: (x[0] + value, x[1] + 1),
                                       lambda x, y: (x[0] + y[0], x[1] + y[1]), numPartitions=1). \
    mapValues(lambda y: y[0] / y[1])

schema = StructType([StructField("ProdId", StringType(), nullable=False),
                     StructField("Rating", DoubleType(), False)])

result = SparkSession.builder.getOrCreate().createDataFrame(task1, schema)
result.write.format('csv').option('header', 'false').save(OUTPUT_FILE)
