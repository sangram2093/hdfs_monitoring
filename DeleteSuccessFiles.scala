import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

// Initialize Spark session
val spark = SparkSession.builder()
  .appName("Delete _SUCCESS Files")
  .getOrCreate()

// Set the base directory path (pass via spark-submit conf or default path)
val basePath = spark.conf.get("spark.cleanup.basePath", "/project/abcd")

println(s"Starting cleanup in base path: $basePath")

// Get Hadoop FileSystem
val fs = FileSystem.get(new Configuration())

// Recursive function to find and delete _SUCCESS files
def deleteSuccessFiles(path: Path): Unit = {
  val filesIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, true)

  var deletedCount = 0
  while (filesIterator.hasNext) {
    val fileStatus = filesIterator.next()
    val filePath = fileStatus.getPath
    if (filePath.getName == "_SUCCESS" && fileStatus.isFile) {
      val deleted = fs.delete(filePath, false)
      if (deleted) {
        println(s"Deleted: ${filePath}")
        deletedCount += 1
      } else {
        println(s"Failed to delete: ${filePath}")
      }
    }
  }

  println(s"Cleanup completed. Total _SUCCESS files deleted: $deletedCount")
}

// Begin cleanup
deleteSuccessFiles(new Path(basePath))

// Stop Spark
spark.stop()
