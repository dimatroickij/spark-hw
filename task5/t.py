import ast

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark import sql
import json

SPARK_APP_NAME_COUNT = "task5"
SPARK_CHECKPOINT_TMP_DIR = "tmpTask5"
SPARK_BATCH_INTERVAL = 10
SPARK_LOG_LEVEL = "OFF"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "task5"
schema = StructType([StructField('idTweet', StringType(), False),
                     StructField('screen_name', StringType(), False),
                     StructField('textTweet', StringType(), False)
                     ],)
sc = SparkContext(appName=SPARK_APP_NAME_COUNT)
sc.setLogLevel(SPARK_LOG_LEVEL)
sqlContext = sql.SQLContext(sc)

ssc = StreamingContext(sc, SPARK_BATCH_INTERVAL)
ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)
kafka_stream_listener = KafkaUtils.createDirectStream(ssc, topics=[KAFKA_TOPIC],
                                                      kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def update_total_count(current_count, count_state):
    if count_state is None:
        count_state = 0
    return sum(current_count, count_state)


def prepareJson(x):
    obj = ast.literal_eval(x[0])
    obj['count'] = x[1]
    print(obj)
    return obj


lines = kafka_stream_listener.map(lambda x: x[1])
# lines.pprint()
counts = lines.map(lambda line: (line, 1)).reduceByKey(lambda x1, x2: x1 + x2)
# counts.pprint()
total_counts = counts.updateStateByKey(update_total_count)
total_counts_sorted = total_counts.transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
total_counts_sorted.map(lambda x: (ast.literal_eval(x[0])['idTweet'], x[1])).pprint()
# writing result
total_counts_sorted.foreachRDD(lambda x: x.map(prepareJson).toDF(schema) \
                               .write.option("header", "true").format("csv").mode("overwrite").save('/test'))
ssc.start()
ssc.awaitTermination(40)  # Выгружаем файл, в которой сохраняли данные
total_counts_loaded = sqlContext.read.load(path="hdfs://localhost:9000/test", format="csv", header="false", sep=',',
                                           shema=schema)
total_counts_loaded.show(5)
