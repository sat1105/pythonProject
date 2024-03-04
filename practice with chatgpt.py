from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import Row, StringType, StructType, IntegerType, StructField, FloatType

if __name__ == "__main__":
    sc = SparkContext("local", "rdd practice")
    spark = SparkSession.builder.appName("rddPractice").getOrCreate()
    sc.setLogLevel("ERROR")

    # Creating a DataFrame from an RDD with a specified schema
    data = [("satish", 29, "Infosys"), ("Kavya", 27, "Tcs"), ("veerendra", 27, "Deloitte")]
    rdd = sc.parallelize(data)
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("Company", StringType(), True)
    ])
    df = spark.createDataFrame(rdd, schema)
    df.show()

    # Creating a DataFrame from a file and imposing a schema
    rdd1 = sc.textFile("file:///D:/DATASETS/txs1.csv")
    print(type(rdd1))
    header = rdd1.first()
    print(type(header))
    print(header)
    rdd2 = rdd1.filter(lambda x:x != header)#line removes the header
    rdd3 = rdd2.map(lambda x: x.split(","))
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

    ids = StructField("ids", IntegerType(), True)
    order_id = StructField("order_id", IntegerType(), True)
    price = StructField("price", FloatType(), True)
    product = StructField("product", StringType(), True)
    category = StructField("category", StringType(), True)
    state = StructField("state", StringType(), True)
    item = StructField("item", StringType(), True)
    types = StructField("types", StringType(), True)

    schema = StructType([ids, order_id, price, product, category, state, item, types])

    df_rdd3 = spark.createDataFrame(rdd4, schema)
    df_rdd3.show()
