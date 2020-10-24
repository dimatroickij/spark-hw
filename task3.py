import re
import sys

from pyspark import SparkConf, SparkContext

INPUT_FILE = '/home/dimatroickij/datasets/prodname_avg_rating.csv'
request = sys.argv[1]

def getRecord(data):
    row = data.split(',')
    return (row[0], row[1], row[2])

conf = SparkConf().setAppName("task3")

spark = SparkContext(conf=conf)
products = spark.textFile(INPUT_FILE).map(getRecord)

response = products.filter(lambda x: re.search(request, x[1])).collect()

print('По введённому запросу "%s" найдены следующие записи:' % request)

for row in response:
    print("ProdId: %s, Name: %s, Rating: %s" % (row[0], row[1], row[2]))
