from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


class Cluster:
    def __init__(self):
        self.spark = SparkSession.builder.appName("WordCount").getOrCreate()
        self.sc = self.spark.sparkContext
        self.ssc = StreamingContext(self.sc, 20)
        self.out_stream = self.ssc.textFileStream("hdfs://mark-pc:9000/data")
        word_count = self.out_stream.flatMap(lambda line: line.split(' ')).map(
            lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).repartition(1).saveAsTextFiles("./word")


if __name__ == "__main__":
    cluster = Cluster()
    cluster.ssc.start()
    cluster.ssc.awaitTermination()
