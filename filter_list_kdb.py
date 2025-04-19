from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, broadcast

spark = SparkSession.builder \
    .appName("FilteredOutput") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

interest_df = spark.read.option("header", True).csv("interest_list.csv") \
    .withColumn("start", to_timestamp("Execution_Start_Time")) \
    .withColumn("end", to_timestamp("Execution_End_Time")) \
    .select("RIC", "start", "end") \
    .cache()

query_df = spark.read.option("header", True).csv("query_output.csv") \
    .withColumn("ts", to_timestamp("timestamp_column")) \
    .repartition("RIC") \
    .select("RIC", "ts", *[col for col in spark.read.csv("query_output.csv", header=True).columns if col not in ["RIC", "timestamp_column"]])

filtered = query_df.join(broadcast(interest_df), on="RIC") \
                   .filter((col("ts") >= col("start")) & (col("ts") <= col("end")))

filtered.write.option("header", True).csv("filtered_output.csv")
