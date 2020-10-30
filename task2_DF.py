import sys
import ast

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, Row

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


def getReviewMeta(reviewMeta):
    decodeReviewMeta = ast.literal_eval(reviewMeta)
    try:
        asin = decodeReviewMeta["asin"]
    except KeyError:
        asin = None
    try:
        title = decodeReviewMeta["title"]
    except:
        title = None
    return (asin, title)


conf = SparkConf().setAppName("task2")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()
rddReviewMeta = sc.textFile(INPUT_META_FILE)

rddReviewMetaDf = rddReviewMeta.map(lambda row: getReviewMeta(row)).toDF(['ProdId', 'Name'])

schema = StructType([StructField('ProdId', StringType(), False),
                     StructField('Rating', DoubleType(), False)])

rddAvgRating = spark.read.load(path=INPUT_AVG_RATING_FILE, format="csv", schema=schema, header="false",
                               inferSchema="false", sep=",", nullValue="null", mode="DROPMALFORMED")

joinDf = rddReviewMetaDf.join(rddAvgRating, ['ProdId'], how='full')
joinDf.na.drop().coalesce(1).write.format('csv').option('header', 'false').save(OUTPUT_DIR)
