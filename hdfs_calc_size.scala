// ============================================
// 1) Configuration
// ============================================
val basePath = "/project/abcd"

// Which top-level directories to scan under basePath?
// e.g. /project/abcd/model, /project/abcd/alert, etc.
val directoriesToCheck = Seq("model", "alert", "feed", "process")

// Team name keywords to map subdirectory names to a team/group
val teamKeywords = Seq("bm", "ib", "fx", "fi", "wm", "pcc")

// For 5-year-old files:
val days5Years = 5 * 365
val cutoffMillis = System.currentTimeMillis() - (days5Years.toLong * 24 * 60 * 60 * 1000)

// ============================================
// 2) Spark Setup and Reading All Files
// ============================================
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

// Build a single glob path, e.g. "/project/abcd/{model,alert,feed,process}"
val globPattern = directoriesToCheck.mkString("{", ",", "}")
val fullPath = s"$basePath/$globPattern"

// Use Spark's "binaryFile" to read file metadata in a distributed manner.
// We'll only select path, length, modificationTime.
val dfAll = spark.read
  .format("binaryFile")
  .option("recursiveFileLookup", "true")
  .load(fullPath)
  .select("path", "length", "modificationTime")

// ============================================
// 3) Path Parsing UDFs
// ============================================

// Strip "hdfs://..." or "file:/..." from the path so splitting by "/" is easier.
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
// after "/project/abcd". Typically, pathParts(3) if the path starts with "/project/abcd".
val topDirUdf = udf { (pathParts: Seq[String]) =>
  if (pathParts.size > 3) pathParts(3) else null
}

// Extract the next subdirectory under the top-level (e.g. "cd-alert-bm-lay").
val teamSubdirUdf = udf { (pathParts: Seq[String]) =>
  if (pathParts.size > 4) pathParts(4) else null
}

// Identify which team keyword is present in the subdirectory name.
val findTeamKeywordUdf = udf { (subdir: String) =>
  if (subdir == null) {
    "other"
  } else {
    val lower = subdir.toLowerCase
    teamKeywords.find(kw => lower.contains(kw)).getOrElse("other")
  }
}

// Create a new DataFrame with parsed path info.
val dfParsed = dfAll
  .withColumn("cleanPath", stripSchemeUdf($"path"))
  .withColumn("pathParts", split($"cleanPath", "/"))
  .withColumn("topLevelDir", topDirUdf($"pathParts"))
  .withColumn("teamSubdir", teamSubdirUdf($"pathParts"))
  .withColumn("teamGroup", findTeamKeywordUdf($"teamSubdir"))

// ============================================
// 4) Summation of Sizes by (topLevelDir, teamGroup)
// ============================================
println("=== Overall Occupied Size by Group (All Files) ===")

val dfSizeByGroup = dfParsed
  .groupBy("topLevelDir", "teamGroup")
  .agg(sum("length").alias("total_size_bytes"))
  .orderBy(desc("total_size_bytes"))

// Quick preview in bytes:
dfSizeByGroup.show(50, truncate = false)

// Optionally, convert bytes to human-readable format:
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

// ============================================
// 5) Find Files Older Than 5 Years
// ============================================
println("\n=== Files Older Than 5 Years ===")

val dfOldFiles = dfParsed
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

// ============================================
// 6) (Optional) If you want to list the actual old file paths per group,
//    note that it can be large. This example uses collect_list, which may blow up
//    driver memory if you have many, many files. Use cautiously.
// ============================================
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

// Show only the top 20 groups or so, to avoid massive output:
dfOldFilesGroupedFormatted.show(20, truncate = false)

// Done! You can continue exploring the DataFrames as needed.
