from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import column
import pyspark
import sys

from pyspark.sql.types import Row
from pyspark.sql.types import *
import datetime

if __name__ == "__main__":
    sc = SparkContext("local", "dataframe_read")
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.appName("dataFrame").master("local").getOrCreate()
    rdd = sc.textFile("file:///D:/DATASETS/txs1.csv")
    rdd2 = rdd.map(lambda x: x.split(","))
    rdd3_array_objects = rdd2.map(lambda x:Row(int(x[0]), int(x[1]), float(x[2]), x[3], x[4], x[5], x[6]))
    id = StructField("id", IntegerType(), True)
    orderid = StructField("orderid", IntegerType(), True)
    price = StructField("price", FloatType(), True)
    product = StructField("product", StringType(), True)
    category = StructField("category", StringType(), True)
    state = StructField("state", StringType(), True)
    type1 = StructField("type1", StringType(), True)
    struct_schma = StructType([id, orderid, price, product, category, state, type1])
    df = spark.createDataFrame(rdd3_array_objects,struct_schma)
    df.show()
    df.printSchema()
    #sk - JhD92bq1rTtGbLVnvkgCT3BlbkFJcIYIHQEAOt9MTET90K1X