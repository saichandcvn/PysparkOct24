import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
os.environ["PYSPARK_PYTHON"] = "C:/Users/cvnsa/Documents/Python/Python37/python.exe"

if __name__ == "__main__":
    print("hello spark")

    spark = SparkSession.builder.appName("DF_Processing").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    txs_Df = spark.read.format('csv') \
                        .options(inferSchema='True',delimiter=',') \
                        .load("file:///D:/DataSets/GMDE17/txs.csv") \
                        .toDF("txnid","txndate","custid","amount","category","product","city","state","payment_type")

#    txs_Df.show()

#    txs_Df.select("*").show()

#    txs_Df.select("txnid","txndate","custid","amount","category").show(5)

#    txs_Df.select("txnid",col("txndate"),txs_Df.custid,column("amount"),"category").show(5)

#    txs_Df.select("txnid","txndate",expr("amount * 2 as amount_updated")).show(5)

#    california_count = txs_Df.filter(txs_Df["state"] == "California").count()
#    print(f"total count in california : {california_count}")

#    txs_Df.filter("state = 'Texas' or state = 'California'").show(10)

#    txs_Df.filter("payment_type='cash' and (state = 'Texas' or state = 'California')").show(10)

#    txs_Df.where("payment_type='cash' and (state = 'Texas' or state = 'California')").show(10)

#    txs_Df.select("state").distinct().show()

# For california state, display distinct cities

    txs_Df.filter("state = 'California'") \
          .select(txs_Df["city"].alias("distinct_cities"),txs_Df["state"].alias("state")) \
          .distinct().show()

# Get the highest value of amount for each state

    txs_Df.groupby("state").max("amount").show()

# Get the highest value of amount for each city and state

    txs_Df.groupby("state","city").max("amount").show()

# Get the total sum of category - Team Sports

    txs_Df.filter(col("category") == "Team Sports").agg(sum("amount").alias("total_sum_teamSports")).show()
