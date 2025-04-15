//
// hdfs_list.scala
//
// Usage: spark-shell -i hdfs_list.scala
//
// This script:
// 1) Lists all files recursively under /project/abcd
// 2) Filters to top-level dirs "model", "alert", "feed", "process"
// 3) Matches subdirectories by team keywords ("bm", "ib", etc.)
// 4) Aggregates total sizes, plus old-file sizes (>5 years)
//

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

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

// 5 years in milliseconds, approx (5 * 365 days)
val cutoffMillis = Instant.now().minus(5 * 365, ChronoUnit.DAYS).toEpochMilli

//
// 2) Spark Session
//
val spark = SparkSession.builder()
  .appName("HdfsFileSystemListing")
  .getOrCreate()

import spark.implicits._

// Hadoop FileSystem handle
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

//
// 3) Utility: List files recursively under baseDir
//    Returns a Seq of (path, size, modificationTime)
//
def listFilesRecursively(baseDir: String): Seq[(String, Long, Long)] = {
  val pathObj = new Path(baseDir)
  if (!fs.exists(pathObj)) {
    Seq.empty
  } else {
    val it = fs.listFiles(pathObj, true) // true => recursive
    val buffer = scala.collection.mutable.ArrayBuffer.empty[(String, Long, Long)]
    while (it.hasNext) {
      val status = it.next()
      val filePath = status.getPath.toString
      val length = status.getLen
      val mtime = status.getModificationTime
      buffer += ((filePath, length, mtime))
    }
    buffer.toSeq
  }
}

//
// 4) Get all files under /project/abcd in one pass.
//    Then we'll filter to the top-level directories of interest.
//
println(s"Listing files under $basePath (recursively). This may take a while...")

val allFiles: Seq[(String, Long, Long)] = listFilesRecursively(basePath)
println(s"Found a total of ${allFiles.size} files.\n")

//
// 5) Build a Spark DataFrame for grouping & analysis
//
val schema = StructType(Seq(
  StructField("path", StringType, nullable=false),
  StructField("length", LongType, nullable=false),
  StructField("modificationTime", LongType, nullable=false)
))

val rows = allFiles.map { case (p, size, mtime) => Row(p, size, mtime) }
val rdd = spark.sparkContext.parallelize(rows)
val df = spark.createDataFrame(rdd, schema)

//
// 6) Parse each file path to identify:
//    - topLevelDir (e.g. model/alert/feed/process)
//    - subdirectory (for team matching)
//    - teamGroup
//
def stripScheme(p: String): String = {
  val idx = p.indexOf(":/")
  if (idx >= 0) {
    val slashIdx = p.indexOf('/', idx+2)
    if (slashIdx >= 0) p.substring(slashIdx) else p
  } else {
    p
  }
}

def findTeamKeyword(subdir: String): String = {
  if (subdir == null) "other"
  else {
    val lower = subdir.toLowerCase
    teamKeywords.find(kw => lower.contains(kw)).getOrElse("other")
  }
}

// We'll parse path in a single UDF returning (topDir, teamGroup)
val parsePath = udf { (fullPath: String) =>
  val cleaned = stripScheme(fullPath)
  // e.g. /project/abcd/model/cd-alert-bm-lay => split => ["project","abcd","model","cd-alert-bm-lay",...]
  val parts = cleaned.split("/").filterNot(_.isEmpty)
  if (parts.length < 3) {
    // Not in the structure we expect
    ("unknown", "other")
  } else {
    val top = parts(2)           // e.g. "model", "alert", etc.
    val subdir = if (parts.length > 3) parts(3).toLowerCase else ""
    val team = findTeamKeyword(subdir)
    (top, team)
  }
}

val dfParsed = df.withColumn("parsed", parsePath($"path"))
  .withColumn("topLevelDir", $"parsed._1")
  .withColumn("teamGroup",   $"parsed._2")
  .drop("parsed")

// Filter out any files not in directoriesToCheck
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

// Optionally format bytes
def formatSize(bytes: Long): String = {
  val TB = 1024L*1024L*1024L*1024L
  val GB = 1024L*1024L*1024L
  val MB = 1024L*1024L
  val KB = 1024L
  if (bytes >= TB) f"${bytes.toDouble / TB}%.2f TB"
  else if (bytes >= GB) f"${bytes.toDouble / GB}%.2f GB"
  else if (bytes >= MB) f"${bytes.toDouble / MB}%.2f MB"
  else if (bytes >= KB) f"${bytes.toDouble / KB}%.2f KB"
  else s"$bytes B"
}

val formatSizeUdf = udf((b: Long) => formatSize(b))

val dfSizeByGroupFormatted = dfSizeByGroup
  .withColumn("size_human", formatSizeUdf($"total_size_bytes"))

dfSizeByGroupFormatted.show(50, truncate=false)

//
// 8) Find files older than 5 years
//
println("\n=== Old Files (> 5 years) by (TopLevelDir, TeamGroup) ===")
val dfOld = dfFiltered.filter($"modificationTime" < cutoffMillis)

val dfOldSizes = dfOld
  .groupBy("topLevelDir", "teamGroup")
  .agg(sum("length").alias("old_size_bytes"))
  .orderBy(desc("old_size_bytes"))

val dfOldSizesFormatted = dfOldSizes
  .withColumn("old_size_human", formatSizeUdf($"old_size_bytes"))

dfOldSizesFormatted.show(50, truncate=false)

//
// 9) (Optional) Collect old-file paths. Be careful with large data sets!
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
// 10) Done - Stop Spark and exit
//
println("\nAll processing complete. Stopping Spark session.\n")
spark.stop()
// This will end the shell session as well:
System.exit(0)
