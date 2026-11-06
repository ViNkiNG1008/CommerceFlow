
import os
import sys

# Tell Spark to use the active virtual environment's Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceFlow")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

print("Spark Version:", spark.version)
print("Python Executable:", sys.executable)
print("Spark Master:", spark.sparkContext.master)

data = [
    (1, "Orders"),
    (2, "Customers"),
    (3, "Products"),
]

df = spark.createDataFrame(data, ["id", "dataset"])

df.show()

spark.stop()
