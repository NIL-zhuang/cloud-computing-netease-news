from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import time


# TODO 时间戳

class Cluster:
    def __init__(self):
        self.spark = SparkSession.builder.appName("WordCount").master("local[*]").getOrCreate()
        self.sc = self.spark.sparkContext
        self.ssc = StreamingContext(self.sc, 61)
        self.out_stream = self.ssc.textFileStream("hdfs://mark-pc:9000/data")
        # cur_time = time.strftime("%Y-%m-%d %H-%M", time.localtime())
        self.out_stream.flatMap(lambda line: line.split(' ')).map(
            lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).repartition(1).saveAsTextFiles("./wct/word")


if __name__ == "__main__":
    cluster = Cluster()
    cluster.ssc.start()
    cluster.ssc.awaitTermination()
