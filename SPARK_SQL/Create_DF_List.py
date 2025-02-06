import os
from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"] = "C:/Users/cvnsa/Documents/Python/Python37/python.exe"

if __name__ == "__main__":
    print("hello spark")
    # Initialize the Spark session
    spark = SparkSession.builder.appName("List_DF").master("local").getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("ERROR")

    # List of tuples
    list_df = [("python", 1), ("scala", 2), ("java", 3)]

    # Parallelize the list to create an RDD
    rdd1 = spark.sparkContext.parallelize(list_df)

    # Convert RDD to DataFrame using .toDF() and the required column names
    df = rdd1.toDF(["language","priority"])

    # Show DataFrame contents and schema
    df.show()
    df.printSchema()
