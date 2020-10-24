import sys

from pyspark import SparkConf

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

INPUT_FILE = '/home/dimatroickij/datasets/prodname_avg_rating.csv'
request = sys.argv[1]

conf = SparkConf().setAppName("task3")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

schema = StructType([StructField('ProdId', StringType(), False),
                     StructField('Name', StringType(), False),
                     StructField('Rating', DoubleType(), False)])

fileDF = spark.read.load(path=INPUT_FILE, format="csv", schema=schema, header="false", inferSchema="false",
                         sep=",", nullValue="null", mode="DROPMALFORMED")

search = fileDF.filter(fileDF.Name.like('%' + request + '%'))
print(search.show())
