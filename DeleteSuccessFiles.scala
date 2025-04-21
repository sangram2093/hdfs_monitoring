import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import java.time.{Instant}
import java.time.temporal.ChronoUnit

// Initialize Spark session
val spark = SparkSession.builder()
  .appName("Delete _SUCCESS Files Older Than N Days")
  .getOrCreate()

// Get base directory and age threshold from spark conf or defaults
val basePath = spark.conf.get("spark.cleanup.basePath", "/project/abcd")
val thresholdDays = spark.conf.get("spark.cleanup.daysThreshold", "3").toInt

println(s"Deleting _SUCCESS files older than $thresholdDays days under: $basePath")

val fs = FileSystem.get(new Configuration())

def deleteOldSuccessFiles(path: Path, daysThreshold: Int): Unit = {
  val now = Instant.now()
  val filesIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, true)

  var deletedCount = 0
  var skippedCount = 0

  while (filesIterator.hasNext) {
    val fileStatus = filesIterator.next()
    val filePath = fileStatus.getPath
    val fileName = filePath.getName

    if (fileStatus.isFile && fileName == "_SUCCESS") {
      val modificationTime = Instant.ofEpochMilli(fileStatus.getModificationTime)
      val ageInDays = ChronoUnit.DAYS.between(modificationTime, now)

      if (ageInDays >= daysThreshold) {
        try {
          val deleted = fs.delete(filePath, false)
          if (deleted) {
            println(s"Deleted: $filePath (Age: $ageInDays days)")
            deletedCount += 1
          } else {
            println(s"Failed to delete: $filePath (Age: $ageInDays days)")
            skippedCount += 1
          }
        } catch {
          case ex: Exception =>
            println(s"Error deleting $filePath: ${ex.getMessage}")
            skippedCount += 1
        }
      }
    }
  }

  println(s"Cleanup complete. Total deleted: $deletedCount, Skipped due to error: $skippedCount")
}

// Run deletion logic
deleteOldSuccessFiles(new Path(basePath), thresholdDays)

// Stop Spark session
spark.stop()
