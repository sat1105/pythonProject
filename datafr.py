import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession.builder.appName("parallelizedf").master("local").getOrCreate()









spark.createDataFrame()