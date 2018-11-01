from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import functions


sc = SparkContext(appName="PythonStreamingFlumeIpsCount")
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
ssc = StreamingContext(sc, 10)

kvs = FlumeUtils.createStream(ssc,"localhost", 9092)
lines = kvs.map(lambda x: x[1])
counts = lines.map(lambda line: (line.split()[0], 1)).reduceByKey(lambda a, b: a+b)

counts.pprint()
counts.saveAsTextFiles("hdfs:///user/maria_dev/streaming/")

ssc.start()
ssc.awaitTermination()