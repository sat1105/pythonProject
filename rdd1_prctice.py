from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
import sys
import pyspark
from pyspark.sql.functions import column,col,when,window
from pyspark.sql.functions import *
from pyspark.sql.types import Row,StringType,StructType,IntegerType,StructField,FloatType



#creating a data frame from rdd a data

if __name__ == "__main__":
    sc = SparkContext("local","rdd practice")
    spark =SparkSession\
         .builder\
         .appName("rddPractice")\
         .getOrCreate()
    sc.setLogLevel("ERROR")
    data = [("satish",29,"Infosys"),("Kavya",27,"Tcs"),("veerendra",27,"Deloitte")]
    rdd = sc.parallelize(data)
    schema = (["name","age","Company"])
    df = rdd.toDF(schema)
    df.show()

#creating a rdd and dataframe from a file and imposing a schema

    """ rdd1 = sc.textFile("file:///D:/DATASETS/txs1.csv")
    result = rdd1.take(3)
    print(result)
    header = rdd1.take(2) #selecting the first row
    rdd2 = rdd1.filter(lambda x:x not in header)#filterring the first row which contain header
    rdd3 = rdd2.map(lambda x:x.split(","))#Splitting the rdd

    rdd4 = rdd3.map(lambda x: Row(
        int(x[0]),
        int(x[1]),
        float(x[2]),
        x[3],
        x[4],
        x[5],
        x[6],
        x[7]
    ))
    ids = StructField("ids", IntegerType(),True)
    order_id = StructField("order_id", IntegerType(), True)
    price = StructField("price", FloatType(), True)
    product = StructField("product", StringType(), True)
    category = StructField("category", StringType(), True)
    state = StructField("state", StringType(), True)
    item = StructField("item", StringType(), True)
    types = StructField("types", StringType(), True)

    schema = StructType([ids, order_id, price, product, category, state, item, types])

    df_rdd3 = spark.createDataFrame(rdd4, schema)
    df_rdd3.show()"""


    print("=====imposing the schema while reading the data frame=====")
    ids = StructField("ids", IntegerType(), True)
    order_id = StructField("order_id", IntegerType(), True)
    price = StructField("price", FloatType(), True)
    product = StructField("product", StringType(), True)
    category = StructField("category", StringType(), True)
    state = StructField("state", StringType(), True)
    item = StructField("item", StringType(), True)
    types = StructField("types", StringType(), True)

    schema = StructType([ids, order_id, price, product, category, state, item, types])
    df = spark.read.format("csv").schema(schema).options(inferSchema=True).load("file:///D:/DATASETS/txs1.csv")
    df.show()


