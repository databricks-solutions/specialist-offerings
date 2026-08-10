"""Hard-tier PySpark: RDD API."""

from pyspark import SparkContext, SparkConf


def main():
    sc = SparkContext(conf=SparkConf().setAppName("WordCount"))
    lines = sc.textFile("/data/raw/words.txt")
    counts = lines.flatMap(lambda line: line.split()).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile("/data/output/wordcount")
    sc.stop()
