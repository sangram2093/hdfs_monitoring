package example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}
import org.apache.spark.sql.functions._
import java.time.Instant
import java.time.temporal.ChronoUnit

object HdfsFileSystemApp extends App {

  // ===========================================================================
  // 1) Configuration
  // ===========================================================================
  val basePath = "/project/abcd"

  // We only care about these top-level directories (just under /project/abcd).
  // e.g. /project/abcd/model, /project/abcd/alert, etc.
  val directoriesToCheck = Seq("model", "alert", "feed", "process")

  // Team keywords for subdirectory names
  val teamKeywords = Seq("bm", "ib", "fx", "fi", "wm", "pcc")

  // Age cutoff for "old files": 5 years in milliseconds
  // Approximately 5 * 365 days (no leap-year nuance).
  val cutoffMillis = Instant.now().minus(5 * 365, ChronoUnit.DAYS).toEpochMilli

  // ===========================================================================
  // 2) Initialize Spark
  // ===========================================================================
  val spark = SparkSession.builder()
    .appName("HdfsFileSystemUsingAPI")
    .getOrCreate()

  import spark.implicits._

  // Hadoop FileSystem handle (uses the cluster config set by Spark)
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  // ===========================================================================
  // 3) Recursively list all files under basePath, capturing:
  //     - path
  //     - size (length)
  //     - modificationTime (epoch millis)
  // ===========================================================================
  def listFilesRecursively(baseDir: String): Seq[(String, Long, Long)] = {
    // Returns sequence of (fullPath, sizeInBytes, modificationTimeMillis)
    val pathObj = new Path(baseDir)
    if (!fs.exists(pathObj)) {
      Seq.empty
    } else {
      val it = fs.listFiles(pathObj, true) // 'true' => recursive
      val buffer = scala.collection.mutable.ArrayBuffer.empty[(String, Long, Long)]
      while (it.hasNext) {
        val status = it.next()
        val p = status.getPath.toString
        val sz = status.getLen
        val modTime = status.getModificationTime
        buffer += ((p, sz, modTime))
      }
      buffer.toSeq
    }
  }

  // We'll do a single pass for the entire /project/abcd path,
  // then filter to only keep top-level directories we care about.
  // Alternatively, you could do multiple passes: one for each directory in directoriesToCheck.
  // For large file systems, either approach enumerates everything in those subdirs.

  val allFiles: Seq[(String, Long, Long)] = listFilesRecursively(basePath)

  // ===========================================================================
  // 4) Convert these results to a Spark DataFrame for grouping & analysis
  // ===========================================================================
  val schema = StructType(Seq(
    StructField("path", StringType, nullable = false),
    StructField("length", LongType, nullable = false),
    StructField("modificationTime", LongType, nullable = false)
  ))

  val rows = allFiles.map { case (p, l, m) => Row(p, l, m) }
  val rdd = spark.sparkContext.parallelize(rows)
  val df = spark.createDataFrame(rdd, schema)

  // ===========================================================================
  // 5) Parse each path to identify:
  //    - topLevelDir (e.g. "model", "alert", etc.)
  //    - subdirectory after topLevelDir
  //    - teamGroup (based on subdirectory name)
  // ===========================================================================
  // A helper function to strip "hdfs://..." or "file:/..." from the path
  def stripScheme(p: String): String = {
    val idx = p.indexOf(":/")
    if (idx >= 0) {
      val slashIdx = p.indexOf('/', idx + 2)
      if (slashIdx >= 0) p.substring(slashIdx) else p
    } else {
      p
    }
  }

  // We'll do the logic in a single UDF:
  val parsePathUdf = udf { (fullPath: String) =>
    // 1) Remove the scheme
    val cleaned = stripScheme(fullPath)

    // 2) Split by '/'
    val parts = cleaned.split("/").filterNot(_.isEmpty) // remove empty segments

    // We expect something like:
    //   parts(0) = "project"
    //   parts(1) = "abcd"
    //   parts(2) = "model" (the top-level directory we want)
    //   parts(3) = "cd-alert-bm-lay" (a subdirectory name)
    //
    // We'll find:
    // topLevelDir = parts(2)   (if it exists)
    // teamSubdir  = parts(3)   (if it exists)
    //
    // Then identify the team by checking if any keyword is in the subdir name.
    // We'll store: (topLevelDir, teamGroup)

    if (parts.length < 3) {
      // Not under /project/abcd in the structure we expect
      ("unknown", "other")
    } else {
      val topLevel = parts(2) // e.g. "model", "alert", "feed", "process", ...
      val maybeSubdir = if (parts.length > 3) parts(3).toLowerCase else ""

      val foundKw = teamKeywords.find(kw => maybeSubdir.contains(kw))
      val team = foundKw.getOrElse("other")

      (topLevel, team)
    }
  }

  val dfParsed = df.withColumn("parsed", parsePathUdf($"path"))
    .withColumn("topLevelDir", $"parsed._1")
    .withColumn("teamGroup",   $"parsed._2")
    .drop("parsed")

  // Filter out any files not in the top-level directories we actually care about.
  val dfFiltered = dfParsed.filter($"topLevelDir".isin(directoriesToCheck: _*))

  // ===========================================================================
  // 6) Summation of total sizes by (topLevelDir, teamGroup)
  // ===========================================================================
  println("\n=== Overall Occupied Size by (TopLevelDir, TeamGroup) ===")
  val dfSizeByGroup = dfFiltered
    .groupBy("topLevelDir", "teamGroup")
    .agg(sum("length").alias("total_size_bytes"))
    .orderBy(desc("total_size_bytes"))

  dfSizeByGroup.show(50, truncate = false)

  // (Optional) Convert bytes to human-readable format
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

  dfSizeByGroupFormatted.show(50, truncate = false)

  // ===========================================================================
  // 7) Find files older than 5 years (compare modTime with cutoffMillis),
  //    then sum by (topLevelDir, teamGroup)
  // ===========================================================================
  println("\n=== Old Files (> 5 years) by (TopLevelDir, TeamGroup) ===")

  val dfOldFiles = dfFiltered.filter($"modificationTime" < cutoffMillis)

  val dfOldSizesByGroup = dfOldFiles
    .groupBy("topLevelDir", "teamGroup")
    .agg(sum("length").alias("old_size_bytes"))
    .orderBy(desc("old_size_bytes"))

  val dfOldSizesByGroupFormatted = dfOldSizesByGroup
    .withColumn("old_size_human", formatSizeUdf($"old_size_bytes"))

  dfOldSizesByGroupFormatted.show(50, truncate = false)

  // ===========================================================================
  // 8) (Optional) Collect the actual old-file paths for each group
  //    Caution: This can become huge if many files are old.
  // ===========================================================================
  println("\n=== Listing Old Files (First 20 Groups) ===")
  val dfOldFilesGrouped = dfOldFiles
    .groupBy("topLevelDir", "teamGroup")
    .agg(
      sum("length").alias("old_size_bytes"),
      collect_list("path").alias("old_file_paths")
    )
    .orderBy(desc("old_size_bytes"))

  val dfOldFilesGroupedFormatted = dfOldFilesGrouped
    .withColumn("old_size_human", formatSizeUdf($"old_size_bytes"))

  dfOldFilesGroupedFormatted.show(20, truncate = false)

  // ===========================================================================
  // Done - stop Spark
  // ===========================================================================
  println("\nAll processing complete. Stopping Spark session.")
  spark.stop()
  // If running via spark-shell -i, you can also do System.exit(0) if you want to auto-exit:
  // System.exit(0)
}
