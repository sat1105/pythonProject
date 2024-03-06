import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,aggregate,when

sc = SparkContext()
spark = SparkSession.builder.appName("parallelizedf").master("local").getOrCreate()




:wq!







spark.createDataFrame()