import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SaveMode

val inputFilePath = spark.conf.get("spark.cleanup.inputPath")
val outputDir = spark.conf.get("spark.cleanup.outputPath")
val finalFileName = "directory_sizes.csv"
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
      case _: Exception => (dirPathStr, "ERROR")
    }
  }
}.toDF("path", "size")

fileSizes.coalesce(1)
  .write
  .mode(SaveMode.Overwrite)
  .option("header", "true")
  .csv(tempOutputDir)

// Rename part file and move to final output
val conf = new Configuration()
val fs = FileSystem.get(conf)

val tempPath = new Path(tempOutputDir)
val finalPath = new Path(outputDir)
val finalFilePath = new Path(finalPath, finalFileName)

// Delete existing final file if it exists
if (fs.exists(finalFilePath)) {
  fs.delete(finalFilePath, false)
}

// Create output directory if not exists
if (!fs.exists(finalPath)) {
  fs.mkdirs(finalPath)
}

// Rename part file to final name
val partFile = fs.listStatus(tempPath).find(_.getPath.getName.startsWith("part-")).get.getPath
fs.rename(partFile, finalFilePath)

// Delete temp dir
fs.delete(tempPath, true)

println(s"Output written to: $finalFilePath")

spark.stop()
System.exit(0)
