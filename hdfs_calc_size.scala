//
// my_spark_script.scala
//
// Run with: spark-shell -i my_spark_script.scala
//

// ======================
// 1) Configuration
// ======================
val basePath = "/project/abcd"

// Which top-level directories to scan under basePath?
// e.g. /project/abcd/model, /project/abcd/alert, etc.
val directoriesToCheck = Seq("model", "alert", "feed", "process")

// Team name keywords to map subdirectory names to a team/group
val teamKeywords = Seq("bm", "ib", "fx", "fi", "wm", "pcc")

// For 5-year-old files (approx. 365 days * 5 = 1825 days)
val days5Years = 5 * 365
val cutoffMillis = System.currentTimeMillis() - (days5Years.toLong * 24 * 60 * 60 * 1000)

// ======================
// 2) Spark Setup and Reading All Files
// ======================
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

// Build a single glob pattern:
// e.g. "/project/abcd/{model,alert,feed,process}"
val globPattern = directoriesToCheck.mkString("{", ",", "}")
val fullPath = s"$basePath/$globPattern"

// Use Spark's "binaryFile" to read file metadata in a distributed manner.
// We'll only select path, length, and modificationTime, ignoring file contents.
val dfAll = spark.read
  .format("binaryFile")
  .option("recursiveFileLookup", "true")
  .load(fullPath)
  .select("path", "length", "modificationTime")

println(s"Scanning the following top-level directories under $basePath:")
directoriesToCheck.foreach(dir => println(s"  - $dir"))
println()

// ======================
// 3) Path Parsing UDFs
// ======================

// Remove "hdfs://..." or "file:/..." from the path for easier splitting.
val stripSchemeUdf = udf { (fullPath: String) =>
  val idx = fullPath.indexOf(":/")
  if (idx >= 0) {
    val slashIdx = fullPath.indexOf('/', idx + 2)
    if (slashIdx >= 0) fullPath.substring(slashIdx) else fullPath
  } else {
    fullPath
  }
}

// Extract the top-level directory name (e.g., "model", "alert", etc.)
val topDirUdf = udf { (pathParts: Seq[String]) =>
  // Typically:
  // 0: ""
  // 1: "project"
  // 2: "abcd"
  // 3: <top-level dir>
  // 4: <subdirectory>
  if (pathParts.size > 3) pathParts(3) else null
}

// Extract the next subdirectory under that top-level folder.
val teamSubdirUdf = udf { (pathParts: Seq[String]) =>
  if (pathParts.size > 4) pathParts(4) else null
}

// Identify which team keyword is present in the subdirectory name
val findTeamKeywordUdf = udf { (subdir: String) =>
  if (subdir == null) {
    "other"
  } else {
    val lower = subdir.toLowerCase
    teamKeywords.find(kw => lower.contains(kw)).getOrElse("other")
  }
}

// Create a new DataFrame with additional columns that parse out path info.
val dfParsed = dfAll
  .withColumn("cleanPath", stripSchemeUdf($"path"))
  .withColumn("pathParts", split($"cleanPath", "/"))
  .withColumn("topLevelDir", topDirUdf($"pathParts"))
  .withColumn("teamSubdir", teamSubdirUdf($"pathParts"))
  .withColumn("teamGroup", findTeamKeywordUdf($"teamSubdir"))

// ======================
// 4) Summation of Sizes by (topLevelDir, teamGroup)
// ======================
println("=== Overall Occupied Size by Group (All Files) ===")

val dfSizeByGroup = dfParsed
  .groupBy("topLevelDir", "teamGroup")
  .agg(sum("length").alias("total_size_bytes"))
  .orderBy(desc("total_size_bytes"))

// Show the raw bytes
dfSizeByGroup.show(50, truncate = false)

// Convert bytes to human-readable format
def formatSize(bytes: Long): String = {
  val TB = 1024L * 1024L * 1024L * 1024L
  val GB = 1024L * 1024L * 1024L
  val MB = 1024L * 1024L
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

println("\n=== Occupied Size by Group (Human-Readable) ===")
dfSizeByGroupFormatted.show(50, truncate = false)

// ======================
// 5) Find Files Older Than 5 Years
// ======================
println("\n=== Files Older Than 5 Years ===")

val dfOldFiles = dfParsed
  // Convert modificationTime to long (epoch ms)
  .withColumn("modTimeMillis", $"modificationTime".cast(LongType))
  .filter($"modTimeMillis" < cutoffMillis)

// Summarize old-file size by (topLevelDir, teamGroup)
val dfOldSizesByGroup = dfOldFiles
  .groupBy("topLevelDir", "teamGroup")
  .agg(sum("length").alias("old_size_bytes"))
  .orderBy(desc("old_size_bytes"))

val dfOldSizesByGroupFormatted = dfOldSizesByGroup
  .withColumn("old_size_human", formatSizeUdf($"old_size_bytes"))

dfOldSizesByGroupFormatted.show(50, truncate = false)

// ======================
// 6) (Optional) List All Old File Paths for Each Group
//     NOTE: If you have millions of files, this can be very large.
// ======================
import org.apache.spark.sql.functions.collect_list

val dfOldFilesGrouped = dfOldFiles
  .groupBy("topLevelDir", "teamGroup")
  .agg(
    sum("length").alias("old_size_bytes"),
    collect_list("path").alias("old_file_paths")
  )
  .orderBy(desc("old_size_bytes"))

val dfOldFilesGroupedFormatted = dfOldFilesGrouped
  .withColumn("old_size_human", formatSizeUdf($"old_size_bytes"))

println("\n=== Listing Old Files (showing first 20 groups) ===")
dfOldFilesGroupedFormatted.show(20, truncate = false)

// ======================
// 7) Stop Spark and Exit
// ======================
println("\nAll processing complete. Stopping Spark session and exiting.\n")
spark.stop()
System.exit(0)
