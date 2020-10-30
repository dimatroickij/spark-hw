import ast

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

SPARK_APP_NAME = "task5"
SPARK_CHECKPOINT_TMP_DIR = "tmpTask5"
SPARK_BATCH_INTERVAL = 60
SPARK_LOG_LEVEL = "OFF"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "task5"


def updateCountRetweets(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount, countState)


def toJson(data):
    tweet = ast.literal_eval(data[0])
    tweet['countRetweets'] = data[1]
    return tweet


schema = StructType([StructField('screenName', StringType(), False), StructField('idTweet', StringType(), False),
                     StructField('textTweet', StringType(), False)])

sc = SparkContext(appName=SPARK_APP_NAME)

# Set log level
sc.setLogLevel(SPARK_LOG_LEVEL)

# Create Streaming Context
ssc = StreamingContext(sc, SPARK_BATCH_INTERVAL)

# Sets the context to periodically checkpoint the DStream operations for master
# fault-tolerance. The graph will be checkpointed every batch interval.
# It is used to update results of stateful transformations as well
ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)

spark = SparkSession.builder.getOrCreate()

# Create subscriber (consumer) to the Kafka topic
kafkaStream = KafkaUtils.createDirectStream(ssc, topics=[KAFKA_TOPIC],
                                            kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

countRetweetsPerMinutes = kafkaStream.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y). \
    updateStateByKey(updateCountRetweets).transform(lambda x: x.sortBy(lambda y: -y[1]))

countRetweetsPerMinutes.map(lambda x: (ast.literal_eval(x[0])['idTweet'], x[1])).pprint()

countRetweetsPerMinutes.foreachRDD(lambda x: x.map(toJson).toDF(schema).write.option("header", "true").
                                   format("json").mode("overwrite").save('/output'))

# Start Spark Streaming
ssc.start()

# Waiting for termination
ssc.awaitTermination(1800)

print("5 самых популярных твитов:")
popularTweets = spark.read.load(path="/output/par*", format="json", schema=schema, header="false", inferSchema="false",
                                nullValue="null", mode="DROPMALFORMED")
popularTweets.show(5)
