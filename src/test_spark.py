from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EntityResolution") \
    .master("local[*]") \
    .getOrCreate()

print("Name: R. Harshith Subramanyam")
print("Roll No: 2023BCS0116")

csv_df = spark.read.option("header", True).option(
    "inferSchema", True
).csv("data/customers.csv")

json_df = spark.read.option("multiLine", True).json(
    "data/customers.json"
)

print("CSV records:", csv_df.count())
print("JSON records:", json_df.count())

csv_df.show(truncate=False)
json_df.show(truncate=False)

spark.stop()
