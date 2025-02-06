import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "C:/Users/cvnsa/Documents/Python/Python37/python.exe"
spark = SparkSession.builder.appName("PySparkProgram").master("local[*]").getOrCreate()

employees = [
("karthik", "2024-11-01"),
("neha", "2024-10-20"),
("priya", "2024-10-28"),
("mohan", "2024-11-02"),
("ajay", "2024-09-15"),
("vijay", "2024-10-30"),
("veer", "2024-10-25"),
("aatish", "2024-10-10"),
("animesh", "2024-10-15"),
("nishad", "2024-11-01"),
("varun", "2024-10-05"),
("aadil", "2024-09-30")
]
employees_df = spark.createDataFrame(employees, ["name", "last_checkin"])

employees_df.createOrReplaceTempView("employees")
spark.sql("""
select initcap(name) as name, 
datediff(current_date(), last_checkin) as datadiff,
(case when datediff(current_date(), last_checkin) < 7 then "Active" else "Inactive" end) as workstatus
from employees
""").show()
