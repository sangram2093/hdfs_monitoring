//
// hdfs_list.scala
//
// Usage: spark-shell -i hdfs_list.scala
//
// Demonstrates listing HDFS files on the driver using the Hadoop FileSystem API,
// then building a Spark DataFrame from that local metadata without serializing
// the FileSystem object. This avoids NotSerializableException.
//

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}
import org.apache.spark.sql.functions._
import java.time.Instant
import java.time.temporal.ChronoUnit

//
// 1) Configuration
//
val basePath = "/project/abcd"

// Only these top-level directories matter (just under /project/abcd)
val directoriesToCheck = Seq("model", "alert", "feed", "process")

// Team keywords for subdirectory names
val teamKeywords = Seq("bm", "ib", "fx", "fi", "wm", "pcc")

// 5 years in milliseconds (approx 5 * 365 days)
val cutoffMillis = Instant.now().minus(5 * 365, ChronoUnit.DAYS).toEpochMilli

//
// 2) Initialize Spark (driver code)
//
val spark = SparkSession.builder()
  .appName("HdfsFileSystemDriverListing")
  .getOrCreate()

import spark.implicits._

//
// 3) Create Hadoop FileSystem handle (on the driver)
//
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

//
// 4) A driver-only function to recursively list files under a path.
//    Returns a local Scala collection (Seq of tuples).
//    This function never runs on executors, so we won't get serialization errors.
//
def listFilesRecursively(baseDir: String): Seq[(String, Long, Long)] = {
  // (path, size, modTime)
  val pathObj = new Path(baseDir)
  if (!fs.exists(pathObj)) {
    Seq.empty
  } else {
    val it = fs.listFiles(pathObj, true) // 'true' => recursive
    val buffer = scala.collection.mutable.ArrayBuffer.empty[(String, Long, Long)]
    while (it.hasNext) {
      val status = it.next()
      val filePath = status.getPath.toString
      val length   = status.getLen
      val mtime    = status.getModificationTime
      buffer += ((filePath, length, mtime))
    }
    buffer.toSeq
  }
}

//
// 5) Use the driver function to list all files in /project/abcd
//    Then we'll create a Spark DataFrame out of that local data.
//
println(s"Listing files under $basePath. This may take a while if there are many files...")

val allFiles: Seq[(String, Long, Long)] = listFilesRecursively(basePath)
println(s"Found a total of ${allFiles.size} files in $basePath.\n")

// Convert to DataFrame
val schema = StructType(Seq(
  StructField("path", StringType, nullable=false),
  StructField("length", LongType, nullable=false),
  StructField("modificationTime", LongType, nullable=false)
))

val rows = allFiles.map { case (p, sz, mtime) => Row(p, sz, mtime) }
val rdd  = spark.sparkContext.parallelize(rows)
val df   = spark.createDataFrame(rdd, schema)

//
// 6) Parse each file path to identify:
//    - topLevelDir  (e.g. "model", "alert", "feed", "process")
//    - teamGroup    (based on subdirectory name containing "bm", "ib", etc.)
//
def stripScheme(p: String): String = {
  val idx = p.indexOf(":/")
  if (idx >= 0) {
    val slashIdx = p.indexOf('/', idx + 2)
    if (slashIdx >= 0) p.substring(slashIdx) else p
  } else {
    p
  }
}

def findTeam(subdir: String): String = {
  if (subdir == null) {
    "other"
  } else {
    val lower = subdir.toLowerCase
    teamKeywords.find(kw => lower.contains(kw)).getOrElse("other")
  }
}

val parsePathUdf = udf { (fullPath: String) =>
  // Example path after scheme removal:
  //   "/project/abcd/model/cd-alert-bm-lay"
  // splitted => ["project","abcd","model","cd-alert-bm-lay",...]
  val cleaned = stripScheme(fullPath)
  val parts = cleaned.split("/").filterNot(_.isEmpty)
  if (parts.length < 3) {
    // Not in expected structure
    ("unknown", "other")
  } else {
    val topDir  = parts(2)  // e.g. "model", "alert", ...
    val subdir  = if (parts.length > 3) parts(3).toLowerCase else ""
    val teamGrp = findTeam(subdir)
    (topDir, teamGrp)
  }
}

val dfParsed = df
  .withColumn("parsed", parsePathUdf($"path"))
  .withColumn("topLevelDir", $"parsed._1")
  .withColumn("teamGroup",   $"parsed._2")
  .drop("parsed")

// Filter only rows whose topLevelDir is in our directoriesToCheck
val dfFiltered = dfParsed.filter($"topLevelDir".isin(directoriesToCheck:_*))

//
// 7) Summation of total sizes by (topLevelDir, teamGroup)
//
println("=== Overall Occupied Size by (TopLevelDir, TeamGroup) ===")
val dfSizeByGroup = dfFiltered
  .groupBy("topLevelDir", "teamGroup")
  .agg(sum("length").alias("total_size_bytes"))
  .orderBy(desc("total_size_bytes"))

dfSizeByGroup.show(50, truncate=false)

// Optional: convert bytes to human-readable strings
def formatSize(bytes: Long): String = {
  val TB = 1024L*1024L*1024L*1024L
  val GB = 1024L*1024L*1024L
  val MB = 1024L*1024L
  val KB = 1024L
  if (bytes >= TB)      f"${bytes.toDouble / TB}%.2f TB"
  else if (bytes >= GB) f"${bytes.toDouble / GB}%.2f GB"
  else if (bytes >= MB) f"${bytes.toDouble / MB}%.2f MB"
  else if (bytes >= KB) f"${bytes.toDouble / KB}%.2f KB"
  else                  s"$bytes B"
}

val formatSizeUdf = udf((b: Long) => formatSize(b))

val dfSizeByGroupFormatted = dfSizeByGroup
  .withColumn("size_human", formatSizeUdf($"total_size_bytes"))

dfSizeByGroupFormatted.show(50, truncate=false)

//
// 8) Find files older than 5 years, group them
//
println("\n=== Old Files (>5 years) by (TopLevelDir, TeamGroup) ===")
val dfOld = dfFiltered.filter($"modificationTime" < cutoffMillis)

val dfOldSizes = dfOld
  .groupBy("topLevelDir", "teamGroup")
  .agg(sum("length").alias("old_size_bytes"))
  .orderBy(desc("old_size_bytes"))

val dfOldSizesFormatted = dfOldSizes
  .withColumn("old_size_human", formatSizeUdf($"old_size_bytes"))

dfOldSizesFormatted.show(50, truncate=false)

//
// 9) (Optional) Collect old-file paths for each group
//    Be careful if you have many old files => large driver memory usage.
//
println("\n=== Listing Old Files (first 20 groups) ===")
val dfOldGrouped = dfOld
  .groupBy("topLevelDir", "teamGroup")
  .agg(
    sum("length").alias("old_size_bytes"),
    collect_list("path").alias("old_file_paths")
  )
  .orderBy(desc("old_size_bytes"))

val dfOldGroupedFormatted = dfOldGrouped
  .withColumn("old_size_human", formatSizeUdf($"old_size_bytes"))

dfOldGroupedFormatted.show(20, truncate=false)

//
// 10) Done - Stop Spark & Exit
//
println("\nAll processing complete. Stopping Spark session and exiting.")
spark.stop()
System.exit(0)
