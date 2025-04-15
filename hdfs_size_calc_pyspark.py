from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace, expr, sum as spark_sum

# Create SparkSession (adjust config as needed).
spark = (SparkSession.builder
         .appName("HDFS Directory Size Calculation")
         .getOrCreate())

# -----------------------------------------------------------------------------
# 1. Load all files (recursively) under the base path.
#    Spark’s binaryFile format fetches metadata (including "path" and "length").
#    The option "recursiveFileLookup"="true" ensures we go through all subdirs.
# -----------------------------------------------------------------------------
base_path = "/project/abcd"
df = (spark.read.format("binaryFile")
      .option("recursiveFileLookup", "true")
      .load(base_path)
      # We only need "path" and "length" columns
      .select("path", "length"))

# -----------------------------------------------------------------------------
# 2. Clean up the path string (remove 'file:' or 'hdfs://...' prefixes, if any).
#    This makes splitting by '/' more consistent.
# -----------------------------------------------------------------------------
# Example path patterns you might see:
#   file:/project/abcd/TeamAlpha/subdir1/file1
#   hdfs://namenode:8020/project/abcd/TeamBeta/subdir2/file2
# We'll remove up to the first occurrence of '/project' from the start of the string.
df_clean = df.withColumn(
    "clean_path",
    # remove leading scheme (like file: or hdfs://...), then everything up to the base path
    regexp_replace(col("path"), r"^.*(/project/abcd)", "/project/abcd")
)

# -----------------------------------------------------------------------------
# 3. Split the cleaned path into parts by '/'.
#    For example: '/project/abcd/TeamAlpha/subdir1/file1' -> ["", "project", "abcd", "TeamAlpha", "subdir1", "file1"]
# -----------------------------------------------------------------------------
df_parts = df_clean.withColumn("path_parts", split(col("clean_path"), "/"))

# -----------------------------------------------------------------------------
# 4. Identify top-level directory and second-level directory.
#    In a path /project/abcd/<topLevel>/<secondLevel>/..., 
#    - topLevel dir is path_parts[3] if 0-based indexing includes the empty element at [0].
#    - secondLevel dir is path_parts[4], if it exists.
#
#    Indexing details depend on how your path splits. Adjust if your environment differs.
# -----------------------------------------------------------------------------
df_with_levels = (
    df_parts
    # For safety, check array lengths. If the array isn't that long, fill with null.
    .withColumn("topLevelDir", expr("CASE WHEN size(path_parts) > 3 THEN path_parts[3] ELSE null END"))
    .withColumn("secondLevelDir", expr("CASE WHEN size(path_parts) > 4 THEN path_parts[4] ELSE null END"))
)

# -----------------------------------------------------------------------------
# 5. Compute total size by top-level directory.
# -----------------------------------------------------------------------------
df_top_level_sizes = (
    df_with_levels
    .groupBy("topLevelDir")
    .agg(spark_sum("length").alias("total_size_bytes"))
    .orderBy(col("total_size_bytes").desc())
)

# -----------------------------------------------------------------------------
# 6. Compute total size by (top-level, second-level) to go one more level down.
# -----------------------------------------------------------------------------
df_second_level_sizes = (
    df_with_levels
    .groupBy("topLevelDir", "secondLevelDir")
    .agg(spark_sum("length").alias("total_size_bytes"))
    .orderBy(col("total_size_bytes").desc())
)

# -----------------------------------------------------------------------------
# 7. Show results.
#    If the data set is large, consider writing to a table or saving as CSV/Parquet.
# -----------------------------------------------------------------------------
print("=== Top-Level Directories ===")
df_top_level_sizes.show(truncate=False)

print("=== Second-Level Directories ===")
df_second_level_sizes.show(truncate=False)

spark.stop()
