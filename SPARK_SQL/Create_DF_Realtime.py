import os
from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"] = "C:/Users/cvnsa/Documents/Python/Python37/python.exe"


def create_session():
    spark_ses = SparkSession.builder \
                .master("local") \
                .appName("GlobalSparkSessionObject") \
                .getOrCreate()
    return spark_ses


def create_df(spark, data, schema):
    df1 = spark.createDataFrame(data, schema)
    return df1


if __name__ == "__main__":
    spark = create_session()
    input_data = [(1,"Python"),
                  (2,"Scala"),
                  (3,"Java"),
                  (4,"R")]
    schema = ["Priority_No","Language"]
    df = create_df(spark, input_data, schema)
    df.show()
