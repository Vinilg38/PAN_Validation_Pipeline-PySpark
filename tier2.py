import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, regexp_replace, when, count, length,
    substring,
)

# Initialize SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("Optimized PAN Validation") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define file paths
csv_path = "/Users/vinilg7/PycharmProjects/PAN_Validation/PAN Number Validation Dataset.csv"
parquet_path = "/tmp/pan_data_parquet"
final_results_path = "/tmp/pan_final_results"

# --- Data Ingestion and Optimization: CSV to Parquet ---
# This step ensures the expensive CSV read happens only once
if not os.path.exists(parquet_path):
    print("Converting CSV to Parquet for performance...")
    raw_csv_df = spark.read.csv(csv_path, header=True, schema="pan_no STRING")
    raw_csv_df.write.mode("overwrite").parquet(parquet_path)

raw_df = spark.read.parquet(parquet_path)

# --- Core Data Pipeline (Single, Optimized Pass) ---
# All cleaning, validation, and status assignment in one pass
# Repartitioning for even workload distribution across cores/executors
final_results_df = raw_df.repartition(200).withColumn(
    "cleaned_pan",
    trim(upper(regexp_replace(col("pan_no"), "[^a-zA-Z0-9]", "")))
).withColumn(
    "status",
    when(
        # Rule 1: Correct Length and Regex Pattern
        (length(col("cleaned_pan")) == 10) &
        (col("cleaned_pan").rlike("^[A-Z]{5}[0-9]{4}[A-Z]$")) &

        # Rule 2: No Adjacent Same Alphabetic Characters
        (substring(col("cleaned_pan"), 1, 1) != substring(col("cleaned_pan"), 2, 1)) &
        (substring(col("cleaned_pan"), 2, 1) != substring(col("cleaned_pan"), 3, 1)) &
        (substring(col("cleaned_pan"), 3, 1) != substring(col("cleaned_pan"), 4, 1)) &
        (substring(col("cleaned_pan"), 4, 1) != substring(col("cleaned_pan"), 5, 1)) &

        # Rule 3: No Adjacent Same Digits
        (substring(col("cleaned_pan"), 6, 1) != substring(col("cleaned_pan"), 7, 1)) &
        (substring(col("cleaned_pan"), 7, 1) != substring(col("cleaned_pan"), 8, 1)) &
        (substring(col("cleaned_pan"), 8, 1) != substring(col("cleaned_pan"), 9, 1)) &

        # Rule 4: No Sequential Alphabetic Characters
        ~substring(col("cleaned_pan"), 1, 5).rlike(
            "ABCDE|BCDEF|CDEFG|DEFGH|EFGHI|FGHIJ|GHIJK|HIJKL|IJKLM|JKLMN|KLMNO|LMNOP|MNOPQ|NOPQR|OPQRS|PQRST|QRSTU|RSTUV|STUVW|TUVWX|UVWXYZ") &

        # Rule 5: No Sequential Digits
        ~substring(col("cleaned_pan"), 6, 4).rlike("0123|1234|2345|3456|4567|5678|6789"),

        "Valid"
    ).otherwise("Invalid")
).withColumn(
    "is_unprocessed",
    when((col("pan_no").isNull()) | (trim(col("pan_no")) == ""), True).otherwise(False)
).cache()  # Cache for multiple actions

# --- Final Reporting and Persistence ---
# Calculate all summary metrics in a single aggregation
summary_df = final_results_df.agg(
    count(col("pan_no")).alias("total_records"),
    count(when(col("status") == "Valid", True)).alias("total_valid_pans"),
    count(when(col("status") == "Invalid", True)).alias("total_invalid_pans"),
    count(when(col("is_unprocessed"), True)).alias("total_unprocessed_records")
)

# Identify duplicates
duplicates_df = final_results_df.groupBy("cleaned_pan").agg(
    count("*").alias("count")
).filter(col("count") > 1)

# Write all final results to Parquet for future use
final_results_df.write.mode("overwrite").parquet(final_results_path)
summary_df.write.mode("overwrite").parquet("/tmp/pan_summary")
duplicates_df.write.mode("overwrite").parquet("/tmp/pan_duplicates")

print("Final Summary Report:")
summary_df.show()
print("Duplicate PANs and their counts:")
duplicates_df.show(truncate=False)

spark.stop()
