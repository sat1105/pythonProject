"""from pyspark import SparkConf,SparkContext
import logging as lg



if __name__ == "__main__":
    sc = SparkContext("local","TrendyTechRddPractice")
    sc.setLogLevel("ERROR")

    rdd1 = sc.textFile("file:///D:/DATASETS/bigdata_campaign_data.csv")
    print("======The type of RDD is======: \n",type(rdd1))
    #print(rdd1.take(10))
    #rdd2 = rdd1.map(lambda x:x.split(","))
    rdd3 = rdd1.map(lambda x : (float(x.split(",")[10]),x.split(",")[0]))
    #"(24.06,big data contents)"
    print(rdd3.take(10))
    rdd4 = rdd3.flatMapValues(lambda x : x.split(" "))
    #"(24.06,big)","(24.06,data)"
    print(rdd4.take(10))
    rdd5 = rdd4.map(lambda x:(x[1].lower(),x[0]))
    #"(big,24.06)"
    print(rdd5.take(10))
    rdd6 = rdd5.reduceByKey(lambda x,y: x+y)
    print(rdd6.take(10))
    rdd5_sort = rdd6.sortBy(lambda x: x[1],False)

    result = rdd5_sort.take(20)
    print(result)
    print(type(result))
    for x in result:
        print(x)
    print("================Practice==================")
    print("================Practice==================")
    print("================Practice==================")"""


from pyspark import SparkConf,SparkContext
import sys
import __main__
def boringwords():

        boring_words = set(line.strip() for line in open("C:/Users/veeru/trendyTech/boringwords.txt"))
        return boring_words

if __name__ =="__main__":

    sc = SparkContext("local","RDD_Practice")
    sc.setLogLevel("Error")
    broadcast_set = sc.broadcast(boringwords())
    rdd = sc.textFile("file:///D:/DATASETS/bigdata_campaign_data.csv")
    #rdd = sc.textFile("file:///D:/DATASETS/bigdata_campaign_data.csv")
    rdd1 = rdd.map(lambda x :(float(x.split(",")[10]),x.split(",")[0]))
    print(rdd1.take(10))
    #(300,big data)

    rdd2 = rdd1.flatMapValues(lambda x:x.split(" "))
    print(rdd2.take(10))
    #(300,big),(300,data)

    rdd3 = rdd2.map(lambda x : (x[1],x[0]))
    print(rdd3.take(10))
    #(big,300) (data,300)

    fianlfilter = rdd3.filter(lambda x: x[0] not in broadcast_set.value)
    rdd4 = fianlfilter.reduceByKey(lambda x,y : x+y)
    print(type(rdd4))
    print(rdd4.take(10))

    rdd5 = rdd4.sortBy(lambda x:x[1],False)
    result = rdd5.take(20)
    print(type(rdd5))

    print("Printing type of RDD:\n", type(result))
    for x in result:
        print(x)
