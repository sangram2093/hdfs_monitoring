import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

val spark = SparkSession.builder().getOrCreate()
val sc = spark.sparkContext

val conf = spark.sparkContext.hadoopConfiguration
val fs = FileSystem.get(conf)

// Parameters
val basePath = spark.conf.get("spark.cleanup.basePath", "/project/abcd")
val maxLevel = spark.conf.get("spark.cleanup.recursiveLevel", "1").toInt
val outputPath = spark.conf.get("spark.cleanup.outputPath", "/tmp/hdfs_dir_sizes")

println(s"Base Path: $basePath")
println(s"Recursive Level: $maxLevel")
println(s"Output Path: $outputPath")

// Convert bytes to human-readable format
def humanReadableByteCountSI(bytes: Long): String = {
  if (bytes < 1000) s"$bytes B"
  else {
    val exp = (Math.log(bytes) / Math.log(1000)).toInt
    val pre = "kMGTPE".charAt(exp - 1) + ""
    f"${bytes / Math.pow(1000, exp)}%.1f ${pre}B"
  }
}

// Recursively collect directories up to a specific level
def listDirs(path: Path, level: Int): List[Path] = {
  if (level == 0 || !fs.exists(path) || !fs.isDirectory(path)) return List.empty
  val status = fs.listStatus(path)
  val dirs = status.filter(_.isDirectory).map(_.getPath).toList
  dirs ++ dirs.flatMap(p => listDirs(p, level - 1))
}

// Get size of a directory recursively
def getDirSize(path: Path): Long = {
  if (!fs.exists(path)) return 0L
  val status = fs.listStatus(path)
  status.map { s =>
    if (s.isDirectory) getDirSize(s.getPath)
    else s.getLen
  }.sum
}

// Main logic
val dirs = listDirs(new Path(basePath), maxLevel)
println(s"Found ${dirs.length} directories.")

val results = dirs.map { dir =>
  val size = getDirSize(dir)
  val sizeHR = humanReadableByteCountSI(size)
  Row(dir.toString, size, sizeHR)
}

// Define schema
val schema = StructType(List(
  StructField("path", StringType, nullable = false),
  StructField("size_bytes", LongType, nullable = false),
  StructField("size_human_readable", StringType, nullable = false)
))

// Create DataFrame and display/save
val df = spark.createDataFrame(sc.parallelize(results), schema)
df.show(truncate = false)
df.write.mode("overwrite").option("header", "true").csv(outputPath)
