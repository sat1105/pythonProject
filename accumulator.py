from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
if __name__ =="__main__":
    sc = SparkContext("local","accumulator")
    spark = SparkSession.builder.appName("todf").getOrCreate()
    sc.setLogLevel("Error")
    #data = ["satish",23,"madh"]
    myRDD = sc.parallelize(["satish",26,"madh"])
    schema = (["name","age","location"])
    df_rdd = spark.createDataFrame()
    df_rdd.show()
