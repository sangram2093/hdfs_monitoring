import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration

// ✅ Read input path from Spark conf
val inputFilePath = spark.conf.get("spark.cleanup.inputPath", "hdfs:///default/path/to/files.txt")

println(s"Input file list path: $inputFilePath")

// Helper function to check if the path is safe
def isSafePath(path: String): Boolean = {
  val cleaned = path.trim
  if (cleaned.isEmpty || cleaned == "/" || cleaned == "." || cleaned.matches("^\\s*$")) {
    false
  } else if (cleaned == ("/usr/sangram") || cleaned == ("usr/sangram/ct/private")) { 
    false
  } else {
    true
  }
}

// Read list and clean + filter the paths
val fileList = spark.sparkContext.textFile(inputFilePath)
  .map(_.trim)
  .filter(isSafePath)

val deleteResults = fileList.mapPartitions { paths =>
  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  paths.map { pathStr =>
    try {
      val path = new Path(pathStr)
      if (fs.exists(path)) {
        val isDirectory = fs.getFileStatus(path).isDirectory

        // ✅ Use positional arguments, not named
        val deleted = fs.delete(path, true)

        if (deleted) {
          (pathStr, if (isDirectory) "Directory Deleted (Skip Trash)" else "File Deleted (Skip Trash)")
        } else {
          (pathStr, "Failed to Delete")
        }
      } else {
        (pathStr, "File Not Found")
      }
    } catch {
      case e: Exception =>
        (pathStr, s"Error: ${e.getMessage}")
    }
  }
}

// Print results
deleteResults.collect().foreach { case (path, status) =>
  println(s"$path\t$status")
}

// ✅ Clean exit: stop Spark and exit shell
spark.stop()
System.exit(0)
