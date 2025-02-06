from pyspark import SparkContext
import os

os.environ["PYSPARK_PYTHON"] = "C:/Users/cvnsa/Documents/Python/Python37/python.exe"

sc = SparkContext("local[4]","sparkrdd")

rdd1 = sc.textFile("C:/Users/cvnsa/Documents/SparkInput/IpAddress.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x+y)
rdd5 = rdd4.sortBy(lambda x: x[1], False)

for i in rdd5.take(2):
    print(i)
