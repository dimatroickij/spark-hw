import ast

import tweepy
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

SPARK_APP_NAME = "task6"
SPARK_CHECKPOINT_TMP_DIR = "tmpTask6"
SPARK_BATCH_INTERVAL = 10
SPARK_LOG_LEVEL = "OFF"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "task6"

consumer_token = "Slfqv88hNbuYlc35kWLcuqjXW"
consumer_secret = "pTnNG2blaFzgcXDB5lXZHOJZjgB9yMYfsr5Z7LZp9hchMjplq9"
access_token = "4196894355-pEokz8B36pgZogaPEHokkPOaY0AtRVkdSsP643d"
access_secret = "aUJ5e89fYY7t2Jh2bciC6ZnzGdeeM8pWdou86OffKRChH"


def updateCountReply(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount, countState)


def toJson(data):
    return ast.literal_eval(data[0])

schema = StructType([StructField('screenName', StringType(), False), StructField('idTweet', StringType(), False)])

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

countReplyPerMinutes = kafkaStream.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y). \
    updateStateByKey(updateCountReply).transform(lambda x: x.sortBy(lambda y: -y[1]))

countReplyPerMinutes.map(lambda x: (ast.literal_eval(x[0])['idTweet'], x[1])).pprint()

countReplyPerMinutes.foreachRDD(lambda x: x.map(toJson).toDF(schema).write.option("header", "true").format("json").mode("overwrite").save('/output2'))

# Start Spark Streaming
ssc.start()

# Waiting for termination
ssc.awaitTermination(20)


def addTextTweet(tweets):
    textTweet = api.statuses_lookup([tweets])
    print(tweets)
    return lit('1')

#
auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

popularTweets = spark.read.load(path="/output2/par*", format="json", schema=schema, header="false", inferSchema="false",
                                nullValue="null", mode="DROPMALFORMED")

popularTweets.limit(5).withColumn("textTweet", addTextTweet(popularTweets['idTweet'])).show()