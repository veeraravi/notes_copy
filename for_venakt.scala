import org.apache.spark.sql.SparkSession
import scala.io.Source

// Create a Spark session
val spark = SparkSession.builder()
  .appName("Spark SQL Execution with Validation")
  .getOrCreate()

// Sample DataFrame (Replace with actual DataFrame)
val data = Seq(
  ("IRAQ387", "11-12-2024", 11, "DuplicateCustomerCreation.sql", "U_D_DSV_001_RIS_1.Dup_CIF_pilot", "D", "REPORTING_DATE"),
  ("IRAQ388", "11-12-2024", 0, "DuplicateCustomerCreation.sql", null, "D", "REPORTING_DATE"),
  ("IRAQ389", "11-12-2024", 1, "DuplicateCustomerCreation.sql", "U_D_DSV_001_RIS_1.Dup_CIF_pilot", "D", null),
  ("IRAQ390", "11-12-2024", 13, "DuplicateCustomerCreation.sql", "U_D_DSV_001_RIS_1.Dup_CIF_pilot", "D", "REPORTING_DATE"),
  ("IRAQ391", "22-12-2024", 11, "DuplicateCustomerCreation.sql", "U_D_DSV_001_RIS_1.Dup_CIF_pilot", "D", "REPORTING_DATE")
)

val df = spark.createDataFrame(data).toDF("alert_code", "date_to_load", "dt_count", "bteq_location", "source_table_name", "frequency", "filter_column")

// Iterate through each record
df.collect().foreach(row => {
  val alertCode = row.getAs[String]("alert_code")
  val dtCount = row.getAs[Int]("dt_count")
  val dateToLoad = row.getAs[String]("date_to_load")
  val bteqLocation = row.getAs[String]("bteq_location")
  val sourceTableName = row.getAs[String]("source_table_name")
  val frequency = row.getAs[String]("frequency")
  val filterColumn = row.getAs[String]("filter_column")

  try {
    if (dtCount > 0) {
      // Check for nulls in required columns
      if (sourceTableName == null || frequency == null || filterColumn == null) {
        throw new Exception(s"One or more required columns are null for alertCode: $alertCode")
      }

      // Run the query to count rows in the source table with the filter
      // Read data from Teradata using JDBC
      val jdbcQuery = s"(SELECT COUNT(*) AS cnt FROM $sourceTableName WHERE $filterColumn = '$dateToLoad') AS subquery"
      val sourceTableCountDF = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", jdbcQuery)
        .option("user", jdbcUser)
        .option("password", jdbcPassword)
        .option("driver", "com.teradata.jdbc.TeraDriver")
        .load()

      // Extract the count
      val sourceTableCount = sourceTableCountDF.collect()(0).getAs[Long]("cnt")
      

      // Compare counts
      if (sourceTableCount == dtCount) {
        // Execute shell script for email notification
        val shellScriptPath = "/path/to/email_notification.sh"
        val process = new ProcessBuilder("bash", shellScriptPath).start()
        process.waitFor()
        println(s"Email notification sent for alertCode: $alertCode")
      } else {
        println(s"Source table count ($sourceTableCount) does not match DT_COUNT ($dtCount) for alertCode: $alertCode")
      }
    } else {
      throw new Exception(s"DT_COUNT is less than or equal to 0 for alertCode: $alertCode")
    }
  } catch {
    case ex: Exception =>
      println(s"Error processing alertCode: $alertCode - ${ex.getMessage}")
  }
})

// Stop Spark session
spark.stop()
