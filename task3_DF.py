import sys
import pyspark.sql.functions as F
from pyspark import SparkConf

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

if len(sys.argv) == 2:
    request = sys.argv[1]
    # 'hdfs://localhost:9000/full/prodname_avg_rating.csv'
    INPUT_FILE = 'hdfs://localhost:9000/part/prodname_avg_rating.csv'
elif len(sys.argv) == 3:
    request = sys.argv[1]
    INPUT_FILE = sys.argv[2]
else:
    print('Введите все значения!!!')
    sys.exit()

conf = SparkConf().setAppName("task3")
sc = SparkSession.builder.config(conf=conf).getOrCreate()

schema = StructType([StructField('ProdId', StringType(), False),
                     StructField('Name', StringType(), False),
                     StructField('Rating', DoubleType(), False)])

fileDF = sc.read.load(path=INPUT_FILE, format="csv", schema=schema, header="false", inferSchema="false",
                      sep=",", nullValue="null", mode="DROPMALFORMED")

print('Введенное слово для поиска: %s' % request)
search = fileDF.filter(F.lower(fileDF.Name).like('%' + request.lower() + '%')).show()
