from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder \
    .appName("EntityResolutionPerformance") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Name: R. Harshith Subramanyam")
print("Roll No: 2023BCS0116")
print("DISTRIBUTED ENTITY RESOLUTION - PERFORMANCE TEST")

# Read both heterogeneous datasets
a = spark.read.option("header", True).csv("data/customers.csv")
b = spark.read.option("multiLine", True).json("data/customers.json")

a = a.select(
    F.col("id").alias("id_a"),
    F.lower(F.trim("name")).alias("name_a"),
    F.lower(F.trim("email")).alias("email_a")
)

b = b.select(
    F.col("id").alias("id_b"),
    F.lower(F.trim("name")).alias("name_b"),
    F.lower(F.trim("email")).alias("email_b")
)

a = a.cache()
b = b.cache()

n_a = a.count()
n_b = b.count()
total_pairs = n_a * n_b

print("\nCSV records:", n_a)
print("JSON records:", n_b)
print("All possible cross-dataset pairs:", total_pairs)

# Measure the naive all-pairs approach
start = time.time()
naive_count = a.crossJoin(b).count()
naive_time = time.time() - start

print("\nNAIVE COMPARISON")
print("Pairs generated:", naive_count)
print("Execution time (seconds):", round(naive_time, 4))

# Measure blocking: same email OR same first three name characters
start = time.time()

candidates = a.alias("a").join(
    b.alias("b"),
    (
        (F.col("a.email_a") == F.col("b.email_b")) &
        (F.col("a.email_a") != "")
    ) |
    (
        (F.substring(F.col("a.name_a"), 1, 3) ==
         F.substring(F.col("b.name_b"), 1, 3)) &
        (F.col("a.name_a") != "") &
        (F.col("b.name_b") != "")
    ),
    "inner"
).select("id_a", "id_b").dropDuplicates()

candidate_count = candidates.count()
blocked_time = time.time() - start

reduction = (
    100 * (1 - candidate_count / total_pairs)
    if total_pairs else 0
)

print("\nBLOCKING APPROACH")
print("Candidate pairs generated:", candidate_count)
print("Execution time (seconds):", round(blocked_time, 4))
print("Comparison reduction (%):", round(reduction, 2))

# Save performance summary
summary = spark.createDataFrame([
    ("Naive all-pairs", int(naive_count), round(naive_time, 4)),
    ("Blocking", int(candidate_count), round(blocked_time, 4))
], ["Method", "PairsCompared", "ExecutionTimeSeconds"])

summary.write.mode("overwrite").option(
    "header", True
).csv("results/performance")

summary.show(truncate=False)

print("Performance results saved in results/performance")

spark.stop()
