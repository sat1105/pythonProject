from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import column
import pyspark
import sys
from pyspark.sql.types import *
import datetime

if __name__ == "__main__":
    sc = SparkContext("local","dataframe_read")
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.appName("dataFrame").master("local").getOrCreate()
    df = spark\
        .read\
        .format("csv")\
        .options(inferSchema='True',delimiter =',')\
        .options(header = 'True')\
        .load("file:///D://DATASETS//txs.csv")\
        .toDF("id","date","order_id","price","product","category","state","item","type")
    df = df.withColumn('date', column('date').cast(DateType()))
    df.show()
    df.printSchema()