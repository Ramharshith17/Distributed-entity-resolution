from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("EntityResolutionEvaluation") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Name: R. Harshith Subramanyam")
print("Roll No: 2023BCS0116")
print("ENTITY RESOLUTION - EVALUATION")

# Load both input datasets
a = spark.read.option("header", True).csv("data/customers.csv")
b = spark.read.option("multiLine", True).json("data/customers.json")

# All possible cross-dataset pairs for evaluation
all_pairs = a.select(F.col("id").alias("id_a")).crossJoin(
    b.select(F.col("id").alias("id_b"))
)

# Known correct matches (ground truth)
truth_data = [
    ("1", "101"), ("2", "102"), ("3", "103"),
    ("4", "104"), ("5", "105")
]

schema = StructType([
    StructField("id_a", StringType(), False),
    StructField("id_b", StringType(), False)
])

truth = spark.createDataFrame(truth_data, schema)

all_pairs = all_pairs.withColumn(
    "id_a", F.col("id_a").cast("string")
).withColumn(
    "id_b", F.col("id_b").cast("string")
)

all_pairs = all_pairs.join(
    truth.withColumn("actual_match", F.lit(1)),
    ["id_a", "id_b"],
    "left"
).fillna(0, subset=["actual_match"])

# Read predictions produced by the matching module
predictions = spark.read.option(
    "header", True
).csv("output/match_results").select(
    "id_a", "id_b", "match_status"
)

all_pairs = all_pairs.join(
    predictions, ["id_a", "id_b"], "left"
).withColumn(
    "predicted_match",
    F.when(F.col("match_status") == "MATCH", 1).otherwise(0)
)

tp = all_pairs.filter(
    (F.col("actual_match") == 1) &
    (F.col("predicted_match") == 1)
).count()

fp = all_pairs.filter(
    (F.col("actual_match") == 0) &
    (F.col("predicted_match") == 1)
).count()

fn = all_pairs.filter(
    (F.col("actual_match") == 1) &
    (F.col("predicted_match") == 0)
).count()

tn = all_pairs.filter(
    (F.col("actual_match") == 0) &
    (F.col("predicted_match") == 0)
).count()

precision = tp / (tp + fp) if tp + fp else 0
recall = tp / (tp + fn) if tp + fn else 0
f1 = (
    2 * precision * recall / (precision + recall)
    if precision + recall else 0
)

print("\nCONFUSION MATRIX COUNTS")
print("True Positives:", tp)
print("False Positives:", fp)
print("False Negatives:", fn)
print("True Negatives:", tn)

print("\nEVALUATION METRICS")
print("Precision:", round(precision, 4))
print("Recall:", round(recall, 4))
print("F1-score:", round(f1, 4))

print("\nTOTAL POSSIBLE CROSS-DATASET PAIRS:", all_pairs.count())
print("GROUND-TRUTH MATCHES:", truth.count())

spark.stop()
