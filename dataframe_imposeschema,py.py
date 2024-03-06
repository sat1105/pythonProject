from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when , aggregate
import pyspark
import sys
from pyspark.sql.types import Row
from pyspark.sql.types import *
import datetime

if __name__ == "__main__":
    sc = SparkContext("local", "dataframe_read")
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.appName("dataFrame").master("local").getOrCreate()

    id = StructField("id", IntegerType(), True)
    orderid = StructField("orderid", IntegerType(), True)
    price = StructField("price", FloatType(), True)
    product = StructField("product", StringType(), True)
    category = StructField("category", StringType(), True)
    state = StructField("state", StringType(), True)
    type1 = StructField("type1", StringType(), True)
    struct_schma = StructType([id, orderid, price, product, category, state, type1])


    df = spark\
        .read\
        .format("csv")\
        .schema(struct_schma)\
        .options(InferSchema=True)\
        .load("file:///D:/DATASETS/txs1.csv")


    df.show()
    df.printSchema()