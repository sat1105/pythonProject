from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import pyspark

if __name__ == "__main__":
    sc = SparkContext("local","parelleize rdd")
    spark = SparkSession.builder.appName("Parallelizedf").master("local").getOrCreate()
    sc.setLogLevel("ERROR")
    data=([("satish",29,"infosys"),("kavya",27,"tcs"),("maidhun",35,"walmart")])
    rdd = sc.parallelize(data)
    df = rdd.toDF(["name","age","company"])
    df.show()
    df.printSchema()