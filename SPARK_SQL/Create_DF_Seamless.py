import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
os.environ["PYSPARK_PYTHON"] = "C:/Users/cvnsa/Documents/Python/Python37/python.exe"

if __name__ == "__main__":
    print("hello spark")

    spark = SparkSession.builder.appName("List_DF_Seamless").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    custid = StructField("custid",IntegerType(),True)
    fname = StructField("fname",StringType(),True)
    lname = StructField("lname",StringType(),True)
    age = StructField("age", IntegerType(), True)
    city = StructField("city", StringType(), True)
    state = StructField("state", StringType(), True)

    custschema = StructType([custid,fname,lname,age,city,state])

    df = spark.read.format("csv").schema(custschema).load("file:///D:/DataSets/GMDE17/customer_records_UPD.csv")

    df.show()
    df.printSchema()
