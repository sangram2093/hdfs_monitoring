package example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object HdfsDirectorySize extends App {

  // ----------------------------------------------------------------------------
  // Configuration
  // ----------------------------------------------------------------------------
  val basePath = "/project/abcd"

  // We want to check these directories under /project/abcd.
  // e.g. /project/abcd/dir1, /project/abcd/dir2, etc.
  val directoriesToCheck = Seq("dir1", "dir2", "dir3", "dir4")

  // Team name keywords to look for in subdirectory names
  val teamKeywords = Seq("ab", "bc", "cd", "de", "ef", "fgh")

  // 5 years in milliseconds (approx. 365 days * 5)
  val days5Years = 5 * 365
  val cutoffMillis = System.currentTimeMillis() - (days5Years.toLong * 24 * 60 * 60 * 1000)

  // ----------------------------------------------------------------------------
  // Create Spark session
  // ----------------------------------------------------------------------------
  val spark = SparkSession.builder()
    .appName("HdfsDirectorySizeScala")
    .getOrCreate()

  import spark.implicits._

  // ----------------------------------------------------------------------------
  // 1) Read all files under the relevant top-level directories with one pass.
  //    We build a glob path like: /project/abcd/{dir1,dir2,dir3,dir4}
  //    so Spark will find them all recursively.
  // ----------------------------------------------------------------------------
  val globPattern = directoriesToCheck.mkString("{", ",", "}")
  val fullPath = s"$basePath/$globPattern"

  // "binaryFile" returns DataFrame with columns:
  //   - path (string)
  //   - length (long, file size in bytes)
  //   - modificationTime (timestamp)
  //   - content (binary) [we won't use it, but Spark still references it]
  //   - ... (depending on Spark version)
  //
  // We enable recursive lookup to find all subdirectories.
  //
  val dfAll = spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load(fullPath)
    // We only need path, length, and modificationTime
    .select("path", "length", "modificationTime")

  // ----------------------------------------------------------------------------
  // 2) Clean / parse the path to identify:
  //    - Which top-level directory (dir1, dir2, dir3, dir4) this file belongs to
  //    - Which subdirectory name matches a team keyword
  //
  //    We'll define UDFs or column expressions to do it.
  // ----------------------------------------------------------------------------

  // A small utility function to remove "hdfs://..." or "file:/" from the path
  // so that splitting by "/" is easier.
  val stripSchemeUdf = udf { (fullPath: String) =>
    // You can adapt this logic. For example:
    //   "hdfs://namenode:8020/project/abcd/dir1/..." -> "/project/abcd/dir1/..."
    //   "file:/project/abcd/dir1/..." -> "/project/abcd/dir1/..."
    val idx = fullPath.indexOf(":/")
    if (idx >= 0) {
      // The substring after '://...' or ':/...'
      val slashIdx = fullPath.indexOf('/', idx + 2)
      if (slashIdx >= 0) fullPath.substring(slashIdx)
      else fullPath
    } else {
      fullPath
    }
  }

  // Identify which top-level directory (dir1, dir2, dir3, dir4)
  // by taking the 3rd or 4th slash segment from the path
  // E.g. "/project/abcd/dir1/someSubdir/... => "dir1"
  //
  // We'll do something like:
  //   0: ""
  //   1: "project"
  //   2: "abcd"
  //   3: "dir1"
  // if the path starts with "/project/abcd"
  //
  // This is obviously dependent on the exact path structure in HDFS.
  // Adjust as needed if your environment differs.
  val topDirUdf = udf { (pathParts: Seq[String]) =>
    if (pathParts.size > 3) pathParts(3) else null
  }

  // Identify the subdirectory name after the top-level directory:
  // E.g. "/project/abcd/dir1/cd-dir2-bm-lay/..." => "cd-dir2-bm-lay"
  // We'll call that the "teamSubdir" or something.
  val teamSubdirUdf = udf { (pathParts: Seq[String]) =>
    if (pathParts.size > 4) pathParts(4) else null
  }

  // Determine which team keyword is contained in the subdirectory name
  // or "other" if none.
  val findTeamKeywordUdf = udf { (subdir: String) =>
    if (subdir == null) {
      "other"
    } else {
      val lower = subdir.toLowerCase
      val foundKw = teamKeywords.find(kw => lower.contains(kw))
      foundKw.getOrElse("other")
    }
  }

  val dfParsed = dfAll
    .withColumn("cleanPath", stripSchemeUdf($"path"))
    .withColumn("pathParts", split($"cleanPath", "/"))
    // top-level directory
    .withColumn("topLevelDir", topDirUdf($"pathParts"))
    // subdir under the top-level
    .withColumn("teamSubdir", teamSubdirUdf($"pathParts"))
    // which keyword does the subdir name contain?
    .withColumn("teamGroup", findTeamKeywordUdf($"teamSubdir"))

  // ----------------------------------------------------------------------------
  // 3) Sum total size by (topLevelDir, teamGroup).
  // ----------------------------------------------------------------------------
  println("=== Overall Occupied Size by Group (All Files) ===")
  val dfSizeByGroup = dfParsed
    .groupBy("topLevelDir", "teamGroup")
    .agg(sum("length").alias("total_size_bytes"))
    .orderBy(desc("total_size_bytes"))

  // We'll collect or show these results:
  dfSizeByGroup.show(100, truncate = false)

  // If you want to convert to human-readable strings in Scala,
  // you can define a small Scala function and turn it into a UDF.
  def formatSize(bytes: Long): String = {
    val TB = 1024L * 1024L * 1024L * 1024L
    val GB = 1024L * 1024L * 1024L
    val MB = 1024L * 1024L
    val KB = 1024L

    if (bytes >= TB) f"${bytes.toDouble / TB}%.2f TB"
    else if (bytes >= GB) f"${bytes.toDouble / GB}%.2f GB"
    else if (bytes >= MB) f"${bytes.toDouble / MB}%.2f MB"
    else if (bytes >= KB) f"${bytes.toDouble / KB}%.2f KB"
    else s"$bytes B"
  }

  val formatSizeUdf = udf((b: Long) => formatSize(b))

  // We can show a new column with formatted sizes:
  val dfSizeByGroupFormatted = dfSizeByGroup
    .withColumn("size_human", formatSizeUdf($"total_size_bytes"))

  dfSizeByGroupFormatted.show(100, truncate = false)

  // ----------------------------------------------------------------------------
  // 4) Find files older than 5 years, group by (topLevelDir, teamGroup).
  //    "modificationTime" is a timestamp column. We'll compare it to cutoffMillis.
  // ----------------------------------------------------------------------------
  println("=== Files Older Than 5 Years ===")

  val dfOldFiles = dfParsed
    // convert modificationTime to a long (milliseconds)
    .withColumn("modTimeMillis", $"modificationTime".cast(LongType))
    .filter($"modTimeMillis" < cutoffMillis)

  // Now group by topLevelDir, teamGroup for these old files
  val dfOldSizesByGroup = dfOldFiles
    .groupBy("topLevelDir", "teamGroup")
    .agg(sum("length").alias("old_size_bytes"))
    .orderBy(desc("old_size_bytes"))

  val dfOldSizesByGroupFormatted = dfOldSizesByGroup
    .withColumn("old_size_human", formatSizeUdf($"old_size_bytes"))

  dfOldSizesByGroupFormatted.show(100, truncate = false)

  // ----------------------------------------------------------------------------
  // 5) (Optional) If you want the *list of old file paths* for each group,
  //    you can collect them. But be careful – if you have millions of files,
  //    collecting them in the driver can be huge.
  //
  //    For demonstration, we can do a groupBy on (topLevelDir, teamGroup)
  //    and use collect_list(path). But that might be very large. 
  // ----------------------------------------------------------------------------
  import org.apache.spark.sql.functions.collect_list

  val dfOldFilesGrouped = dfOldFiles
    .groupBy("topLevelDir", "teamGroup")
    .agg(
      sum("length").alias("old_size_bytes"),
      collect_list("path").alias("old_file_paths")
    )

  val dfOldFilesGroupedFormatted = dfOldFilesGrouped
    .withColumn("old_size_human", formatSizeUdf($"old_size_bytes"))

  // Show only a small number of results or handle carefully if huge
  dfOldFilesGroupedFormatted.show(20, truncate = false)

  // ----------------------------------------------------------------------------
  // Done
  // ----------------------------------------------------------------------------
  spark.stop()
}
