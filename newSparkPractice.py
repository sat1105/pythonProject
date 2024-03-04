"""Find out how many customers in California
paid through cash and
using credit card.
And total Sales happened"""

from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Sales in California").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    filePath = "file:///D://Pyspark//DATASETS//txs.csv"
    file = sc.textFile(filePath)

    df = spark.read.csv(filePath, header=False, inferSchema=True)
    df.printSchema()
    df.show(10)

    fileList = file.map(lambda line: line.split(","))
    caliList = fileList.filter(lambda line: "california" in line[7].lower())

    creditCusLst = caliList.filter(lambda line: "credit" in line[8].lower())
    cashCusLst = caliList.filter(lambda line: "cash" in line[8].lower())

    print(f"Total customers in California is {caliList.count()} :: "
          f"In that customers paid through Credit card is {creditCusLst.count()} :: "
          f"And Customers paid through cash is {cashCusLst.count()}")

    salesInCal = caliList.map(lambda line: float(line[3]))
    print(f"total sales in california is {salesInCal.sum()}")

    amtwithCrdCard = creditCusLst.map(lambda line: float(line[3]))
    print(f"total sales amount in california paid through credit card is {amtwithCrdCard.sum()}")

    amtwithCash = cashCusLst.map(lambda line: float(line[3]))
    print(f"total sales amount in california paid through cash is {amtwithCash.sum()}")