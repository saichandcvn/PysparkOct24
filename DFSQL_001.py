import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "C:/Users/cvnsa/Documents/Python/Python37/python.exe"
spark = SparkSession.builder.appName("PySparkProgram").master("local[*]").getOrCreate()

schema="id int,Name string,Age int"

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .schema(schema) \
    .option("path", "C:/Users/cvnsa/Downloads/Datasets/info.csv") \
    .load()

df.createOrReplaceTempView("info")
spark.sql("""
select * from info
""").show()
