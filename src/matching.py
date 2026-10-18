from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from difflib import SequenceMatcher
import re
import time

spark = SparkSession.builder \
    .appName("DistributedEntityResolution") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
start = time.time()

print("=" * 60)
print("DISTRIBUTED ENTITY RESOLUTION")
print("Name: R. Harshith Subramanyam")
print("Roll No: 2023BCS0116")
print("=" * 60)

# 1. Load heterogeneous datasets
csv = spark.read.option("header", True).option(
    "inferSchema", True
).csv("data/customers.csv")

js = spark.read.option("multiLine", True).json(
    "data/customers.json"
)

# 2. Standardize columns and identify data sources
a = csv.select(
    F.col("id").cast("string").alias("record_id"),
    "name", "email", "phone", "city"
).withColumn("source", F.lit("CSV"))

b = js.select(
    F.col("id").cast("string").alias("record_id"),
    "name", "email", "phone", "city"
).withColumn("source", F.lit("JSON"))

def clean(df):
    for c in ["name", "email", "phone", "city"]:
        df = df.withColumn(
            c, F.lower(F.trim(F.col(c).cast("string")))
        )
    return df.fillna("", subset=["name", "email", "phone", "city"])

a = clean(a)
b = clean(b)

print("\nSTEP 1: INPUT COUNTS")
print("CSV records:", a.count())
print("JSON records:", b.count())

# 3. Blocking: generate candidate pairs sharing email OR phone
left = a.alias("a")
right = b.alias("b")

pairs = left.join(
    right,
    (
        (F.col("a.email") == F.col("b.email")) &
        (F.col("a.email") != "")
    ) |
    (
        (F.col("a.phone") == F.col("b.phone")) &
        (F.col("a.phone") != "")
    ) |
    (
        (F.substring(F.col("a.name"), 1, 3) ==
         F.substring(F.col("b.name"), 1, 3)) &
        (F.col("a.name") != "") &
        (F.col("b.name") != "")
    ),
    "inner"
).select(
    F.col("a.record_id").alias("id_a"),
    F.col("b.record_id").alias("id_b"),
    F.col("a.name").alias("name_a"),
    F.col("b.name").alias("name_b"),
    F.col("a.email").alias("email_a"),
    F.col("b.email").alias("email_b"),
    F.col("a.phone").alias("phone_a"),
    F.col("b.phone").alias("phone_b"),
    F.col("a.city").alias("city_a"),
    F.col("b.city").alias("city_b")
).dropDuplicates(["id_a", "id_b"])

print("\nSTEP 2: CANDIDATE PAIRS")
print("Candidate pairs:", pairs.count())

# 4. Calculate name similarity
def similarity(x, y):
    if not x or not y:
        return 0.0
    return float(SequenceMatcher(None, x, y).ratio())

similarity_udf = udf(similarity, DoubleType())

scored = pairs.withColumn(
    "name_similarity",
    similarity_udf("name_a", "name_b")
).withColumn(
    "email_match",
    F.when(
        (F.col("email_a") != "") &
        (F.col("email_a") == F.col("email_b")), 1.0
    ).otherwise(0.0)
).withColumn(
    "phone_match",
    F.when(
        (F.col("phone_a") != "") &
        (F.col("phone_a") == F.col("phone_b")), 1.0
    ).otherwise(0.0)
).withColumn(
    "confidence",
    F.round(
        0.40 * F.col("email_match") +
        0.40 * F.col("phone_match") +
        0.20 * F.col("name_similarity"), 3
    )
)

scored = scored.withColumn(
    "match_status",
    F.when(F.col("confidence") >= 0.60, "MATCH")
     .when(F.col("confidence") >= 0.45, "REVIEW")
     .otherwise("NON_MATCH")
)

print("\nSTEP 3: MATCH RESULTS")
scored.select(
    "id_a", "name_a", "id_b", "name_b",
    "name_similarity", "confidence", "match_status"
).show(50, truncate=False)

# 5. Save results
scored.write.mode("overwrite").option(
    "header", True
).csv("output/match_results")

# Save the unified entity mapping for high-confidence matches.
# Unmatched records receive their own unique entity IDs.
mapping = a.select(
    F.concat(F.lit("CSV_"), F.col("record_id")).alias("record_key")
).unionByName(
    b.select(F.concat(F.lit("JSON_"), F.col("record_id")).alias("record_key"))
).withColumn(
    "entity_id", F.col("record_key")
)

matched = scored.filter(F.col("match_status") == "MATCH")

edges = matched.select(
    F.concat(F.lit("CSV_"), F.col("id_a")).alias("left_key"),
    F.concat(F.lit("JSON_"), F.col("id_b")).alias("right_key")
)

# For this prototype, map each high-confidence pair to a shared entity ID.
# A production system would also resolve transitive links.
entity_map = mapping
for row in edges.collect():
    shared = min(row["left_key"], row["right_key"])
    entity_map = entity_map.withColumn(
        "entity_id",
        F.when(
            F.col("record_key").isin(row["left_key"], row["right_key"]),
            shared
        ).otherwise(F.col("entity_id"))
    )

entity_map.write.mode("overwrite").option(
    "header", True
).csv("output/entity_mapping")

print("\nSTEP 4: ENTITY MAPPING")
entity_map.orderBy("entity_id").show(50, truncate=False)

print("Candidate pairs:", scored.count())
print("High-confidence matches:", matched.count())
print("Execution time (seconds):", round(time.time() - start, 2))
print("Saved results to output/match_results")
print("Saved entity mapping to output/entity_mapping")

spark.stop()
