from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace, substring, udf, when
from pyspark.sql.types import BooleanType


def check_adj(a):
    n = len(a)
    for i in range(0, n - 1):
        if a[i] == a[i + 1]:
            return True
    return False


def check_seq(a):
    n = len(a)
    for i in range(n - 1):
        if ord(a[i + 1]) - ord(a[i]) != 1:
            return False
    return True


spark = SparkSession.builder \
    .appName("PAN Card Validation") \
    .master("local[*]") \
    .getOrCreate()
df = spark.read.csv(
    "/Users/vinilg7/PycharmProjects/PAN_Validation/PAN Number Validation Dataset.csv",
    header=True,
    inferSchema=True)

# Step 1: Clean data (remove nulls, empty strings,extra symbols, trim)
clean_df = df.filter(
    (col("pan_no").isNotNull()) &
    (trim(col("pan_no")) != "")).withColumn("pan_no",
                                            trim(upper(regexp_replace(col("pan_no"), "[^a-zA-Z0-9]", "")))).distinct().cache()

# Step 2: Creating UDFs using Python functions for PAN Validation and using them for filtering valid PANs along with
check_adj_udf = udf(check_adj, BooleanType())
check_seq_udf = udf(check_seq, BooleanType())

valid_pan = clean_df.filter(
    col("pan_no").rlike("^[A-Z]{5}[0-9]{4}[A-Z]{1}$") &
    check_adj_udf(col("pan_no")) == False &
    (check_seq_udf(substring(col("pan_no"), 1, 5)) == False) &
    (check_seq_udf(substring(col("pan_no"), 6, 4)) == False)
)

# Final Report To Classify Valid and Invalid PANs
final_pan = clean_df.alias("cln").join(
    valid_pan.alias("v"),
    on="pan_no",
    how="left"
).select(
    col("cln.pan_no"),
    when(col("v.pan_no").isNotNull(), "Valid").otherwise("Invalid").alias("status")
)

# Summary Report
total_processed_records = df.count()
total_valid_pans = final_pan.where(col("status") == "Valid").count()
total_invalid_pans = final_pan.where(col("status") == "Invalid").count()
total_unprocessed_records = total_processed_records - clean_df.count()

print("Summary Report:")
print(f"Total Processed Records: {total_processed_records}")
print(f"Total Valid PANs: {total_valid_pans}")
print(f"Total Invalid PANs: {total_invalid_pans}")
print(f"Total Unprocessed Records (Duplicates/Nulls): {total_unprocessed_records}")
spark.stop()
