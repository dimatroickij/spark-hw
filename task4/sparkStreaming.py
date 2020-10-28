from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

SPARK_APP_NAME = "task4"
SPARK_CHECKPOINT_TMP_DIR = "tmpTask4"
SPARK_BATCH_INTERVAL = 30
SPARK_LOG_LEVEL = "OFF"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "task4"

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

countTweetsPerMinutes = kafkaStream.map(lambda x: (x[1], 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y,
                                                         windowDuration=60, slideDuration=30)

countTweetsPer10Minutes = kafkaStream.map(lambda x: (x[1], 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y,
                                                           windowDuration=600, slideDuration=30)

# Sort by counts
sortedCountTweetsPerMinutes = countTweetsPerMinutes.transform(
    lambda x_rdd: x_rdd.sortBy(lambda x: x[1], ascending=False))

sortedCountTweetsPer10Minutes = countTweetsPer10Minutes.transform(
    lambda x_rdd: x_rdd.sortBy(lambda x: x[1], ascending=False))

# Print result
sortedCountTweetsPerMinutes.pprint()
sortedCountTweetsPer10Minutes.pprint()

# Start Spark Streaming
ssc.start()

# Waiting for termination
ssc.awaitTermination()
