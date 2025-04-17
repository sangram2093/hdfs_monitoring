import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SaveMode

val inputFilePath = spark.conf.get("spark.cleanup.inputPath")
val outputDir = spark.conf.get("spark.cleanup.outputPath") // Directory where final output will be placed
val finalFileName = "directory_sizes.csv" // Customize this
val tempOutputDir = outputDir + "_tmp"

println(s"Input file list path: $inputFilePath")

def humanReadableByteCountSI(bytes: Long): String = {
  if (bytes < 1000) s"$bytes B"
  else {
    val exp = (Math.log(bytes) / Math.log(1000)).toInt
    val pre = "KMGTPE".charAt(exp - 1) + "B"
    f"${bytes / Math.pow(1000, exp)}%.1f $pre"
  }
}

val fileList = spark.sparkContext.textFile(inputFilePath)

import spark.implicits._

val fileSizes = fileList.mapPartitions { paths =>
  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  paths.map { dirPathStr =>
    try {
      val dirPath = new Path(dirPathStr)
      if (fs.exists(dirPath) && fs.getFileStatus(dirPath).isDirectory) {
        val files: Array[FileStatus] = fs.listStatus(dirPath)
        val totalSize = files.map(_.getLen).sum
        val readableSize = humanReadableByteCountSI(totalSize)
        (dirPathStr, readableSize)
      } else {
        (dirPathStr, "INVALID_PATH")
      }
    } catch {
      case e: Exception =>
        (dirPathStr, "ERROR")
    }
}.toDF("path", "size")

// Write to temp directory
fileSizes.coalesce(1)
  .write
  .mode(SaveMode.Overwrite)
  .option("header", "true")
  .csv(tempOutputDir)

// Rename part file to custom name and move to final output path
val conf = new Configuration()
val fs = FileSystem.get(conf)

val tempPath = new Path(tempOutputDir)
val finalPath = new Path(outputDir)
val finalFilePath = new Path(finalPath, finalFileName)

// Delete existing final file if exists
if (fs.exists(finalFilePath)) {
  fs.delete(finalFilePath, false)
}

// Create output directory if it doesn't exist
if (!fs.exists(finalPath)) {
  fs.mkdirs(finalPath)
}

// Find the part file
val partFile = fs.listStatus(tempPath).find(_.getPath.getName.startsWith("part-")).get.getPath

// Move and rename
fs.rename(partFile, finalFilePath)

// Clean up temp directory
fs.delete(tempPath, true)

println(s"Output written to: $finalFilePath")
