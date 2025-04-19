from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, concat_ws, broadcast

spark = SparkSession.builder.appName("FilterByInterval").getOrCreate()

# Load interest list and parse ISO format to timestamp (UTC)
interest_df = spark.read.option("header", True).csv("interest_list.csv") \
    .withColumn("start", to_timestamp(col("Execution_Start_Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")) \
    .withColumn("end", to_timestamp(col("Execution_End_Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")) \
    .select("RIC", "start", "end") \
    .cache()

# Load query output and combine date + time into timestamp
query_df = spark.read.option("header", True).csv("query_output.csv") \
    .withColumn("ts", to_timestamp(
        concat_ws(" ", col("date_column"), col("time_column")),
        "yyyy-MM-dd HH:mm:ss.SSS"
    )) \
    .select("RIC", "ts", *[c for c in spark.read.option("header", True).csv("query_output.csv").columns if c not in ["RIC", "date_column", "time_column"]])

# Broadcast join and filter
filtered = query_df.join(broadcast(interest_df), query_df["sym"] == interest_df["RIC"]) \
                   .filter((col("ts") >= col("start")) & (col("ts") <= col("end")))

# Save result
filtered.write.option("header", True).csv("filtered_output.csv")
