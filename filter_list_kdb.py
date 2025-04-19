from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, concat_ws, broadcast

# ----------------------------------------
# Step 1: Initialize Spark session
# ----------------------------------------
spark = SparkSession.builder \
    .appName("FilterQueryOutputByInterestList") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# ----------------------------------------
# Step 2: Load interest list and parse timestamps
# ----------------------------------------
interest_df = spark.read.option("header", True).csv("interest_list.csv") \
    .withColumn("start", to_timestamp("Execution_Start_Time", "yyyy-MM-dd'T'HH:mm:ss.SSSX")) \
    .withColumn("end", to_timestamp("Execution_End_Time", "yyyy-MM-dd'T'HH:mm:ss.SSSX")) \
    .select("RIC", "start", "end") \
    .cache()

# ----------------------------------------
# Step 3: Load and prepare query output
# Replace 'date_column' and 'time_column' with actual column names
# ----------------------------------------
query_df_raw = spark.read.option("header", True).csv("query_output.csv")

# Combine date and time into timestamp column 'ts'
query_df = query_df_raw.withColumn("ts", to_timestamp(
    concat_ws(" ", col("date_column"), col("time_column")),
    "yyyy-MM-dd HH:mm:ss.SSS"
))

# Drop duplicates early (based on original columns excluding ts)
original_columns = [c for c in query_df.columns if c not in ["ts"]]
query_df = query_df.dropDuplicates(original_columns)

# ----------------------------------------
# Step 4: Join with broadcasted interest list and apply time filtering
# ----------------------------------------
filtered = query_df.join(broadcast(interest_df), query_df["sym"] == interest_df["RIC"]) \
                   .filter((col("ts") >= col("start")) & (col("ts") <= col("end")))

# ----------------------------------------
# Step 5: Deduplicate again (in case overlaps in intervals matched same row)
# ----------------------------------------
filtered = filtered.dropDuplicates(original_columns)

# Drop extra join columns
filtered = filtered.drop("RIC", "start", "end")

# ----------------------------------------
# Step 6: Write filtered output to CSV
# ----------------------------------------
filtered.repartition(100).write.option("header", True).csv("filtered_output.csv")

print("✅ Filtered and deduplicated file written to filtered_output.csv")
