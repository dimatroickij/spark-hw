import ast
import json

from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

SPARK_APP_NAME = "task6"
SPARK_CHECKPOINT_TMP_DIR = "tmpTask6"
SPARK_BATCH_INTERVAL = 60
SPARK_LOG_LEVEL = "OFF"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "task6"

def updateCountReply(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount, countState)

schema = StructType([StructField('idTweet', StringType(), False),
                     StructField('screen_name', StringType(), False)])

sc = SparkContext(appName=SPARK_APP_NAME)

# Set log level
sc.setLogLevel(SPARK_LOG_LEVEL)

# Create Streaming Context
ssc = StreamingContext(sc, SPARK_BATCH_INTERVAL)

# Sets the context to periodically checkpoint the DStream operations for master
# fault-tolerance. The graph will be checkpointed every batch interval.
# It is used to update results of stateful transformations as well
ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)

# Create subscriber (consumer) to the Kafka topic
kafkaStream = KafkaUtils.createDirectStream(ssc, topics=[KAFKA_TOPIC],
                                            kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

replyPerMinutes = kafkaStream.map(lambda x: x[1])

countReplyPerMinutes = replyPerMinutes.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).\
    updateStateByKey(updateCountReply)

sortedCountRetweetsPerMinutes = countReplyPerMinutes.transform(lambda x: x.sortBy(lambda y: -y[1]))

def toJson(data):
    tweet = ast.literal_eval(data[0])
    tweet['countRetweets'] = data[1]
    return tweet

sortedCountRetweetsPerMinutes.map(lambda x: (ast.literal_eval(x[0])['idTweet'], x[1])).pprint()
#sortedCountRetweetsPerMinutes.foreachRDD(lambda x: x.map(toJson).coalesce(1, shuffle=True).saveAsTextFile('/output/res.json'))#load(schema).write.option("header", "true").format("csv").mode("overwrite").save('/output'))
#sortedCountRetweetsPerMinutes.coalesce(1).saveAsTextFiles("/output")
#sortedCountRetweetsPerMinutes.forEachRDD(lambda tweet: tweet.toDF(schema).write.option('header', 'true').format('csv').mode('overwrite').save('/output'))

#    .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y,
#                                                         windowDuration=60, slideDuration=30)

# countTweetsPer10Minutes = kafkaStream.map(lambda x: (x[1], 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y,
#                                                           windowDuration=600, slideDuration=30)

# Sort by counts
# sortedCountTweetsPerMinutes = countTweetsPerMinutes.transform(
#    lambda x_rdd: x_rdd.sortBy(lambda x: x[1], ascending=False))

# sortedCountTweetsPer10Minutes = countTweetsPer10Minutes.transform(
#    lambda x_rdd: x_rdd.sortBy(lambda x: x[1], ascending=False))

# Print result
# sortedCountTweetsPerMinutes.pprint()
# sortedCountTweetsPer10Minutes.pprint()

# Start Spark Streaming
ssc.start()

# Waiting for termination
ssc.awaitTermination(300)

def getTweet(tweet):
    decodeTweet = ast.literal_eval(tweet)
    return (decodeTweet["screenName"], decodeTweet['idTweet'], decodeTweet['textTweet'])


tweets = sc.textFile('/home/dimatroickij/result.json').map(getTweet).take(5)

for i, tweet in enumerate(tweets):
    print("%i) screenName: %s, idTweet: %s, textTweet: %s" % (i + 1, tweet[0], tweet[1], tweet[2]))