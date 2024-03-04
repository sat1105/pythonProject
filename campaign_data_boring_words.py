from pyspark import SparkContext,SparkConf

def broadfunction():
    broadset = set(line.strip() for line in open("C:/Users/veeru/trendyTech/boringwords.txt"))
    return broadset

if __name__ == "__main__":

    sc = SparkContext()
    print(" ")
    print("=====defining RDD=========")
    rdd =sc.textFile("file:///D:/DATASETS/bigdata_campaign_data.csv")
    broadvar = sc.broadcast(broadfunction)
    rdd1 = rdd.map(lambda x : (float(x.split(",")[10]),x.split(",")[0]))
    rdd2 = rdd1.flatMapValues(lambda x : x.split(" "))
    rdd3 = rdd2.map(lambda x:(x[1],x[0]))
    rdd_filter = rdd3.map(lambda x: x[0] not in broadvar)
    rdd4= rdd3.reduceByKey(lambda x,y : x+y )

    rdd5 = rdd4.sortBy(lambda x: x[1],False)


    print("=======FINAL RESULT========")

    result = rdd5.take(10)


    for x in result:
        print(x)

