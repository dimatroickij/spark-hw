import re
import sys

from pyspark import SparkConf, SparkContext

if len(sys.argv) == 2:
    request = sys.argv[1]
    INPUT_FILE = 'hdfs://localhost:9000/part/prodname_avg_rating.csv'
elif len(sys.argv) == 3:
    request = sys.argv[1]
    INPUT_FILE = sys.argv[2]
else:
    print('Введите все значения!!!')
    sys.exit()


def getRecord(data):
    row = data.split(',')
    try:
        prodId = row[0]
    except IndexError:
        prodId = ''
    try:
        title = row[1]
    except IndexError:
        title = ''
    try:
        rating = row[2]
    except IndexError:
        rating = ''

    return (prodId, title, rating)


conf = SparkConf().setAppName("task3")

spark = SparkContext(conf=conf)
products = spark.textFile(INPUT_FILE).map(getRecord)

response = products.filter(lambda x: re.search(request.lower(), x[1].lower())).collect()

print('По введённому запросу "%s" найдены следующие записи:' % request)

for row in response:
    print("ProdId: %s, Name: %s, Rating: %s" % (row[0], row[1], row[2]))
