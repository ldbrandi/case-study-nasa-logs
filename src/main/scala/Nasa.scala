
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.alvinalexander.accesslogparser._

object Nasa {

  def main(args: Array[String]) {
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("nasa")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("NASA")
      .getOrCreate()

    val parser = new AccessLogParser
    val jul95 = sc.textFile("src/main/resources/NASA_access_log_Jul95")
    val aug95 = sc.textFile("src/main/resources/NASA_access_log_Aug95")

    val logs = jul95.union(aug95)

    val accessLogRecord = logs.map(parser.parseRecordReturningNullObjectOnFailure).filter(_ != null)

    val dfLogs = spark.createDataFrame(accessLogRecord)

//    dfLogs.show()

    dfLogs.createOrReplaceTempView("logs")

    val dfHosts = spark.sql("SELECT COUNT(DISTINCT clientIpAddress) AS HOSTS_UNICOS FROM logs")
//    dfHosts.show()

    val df404 = spark.sql("SELECT COUNT(*) AS ERROS_404 FROM logs WHERE httpStatusCode = '404'")
//    df404.show()

    val dfHTTPs404 = spark.sql(
      """SELECT ltrim(request) AS HTTP, COUNT(*) AS ERROS_404 FROM logs WHERE httpStatusCode = '404'
        |GROUP BY 1 ORDER BY 2 DESC LIMIT 5""".stripMargin)
//    dfHTTPs404.show(truncate = false)

    val df404Day = spark.sql("SELECT SUBSTR(dateTime, 2, 11) AS DATA, COUNT(*) AS QTD FROM logs " +
      "WHERE httpStatusCode = '404' GROUP BY 1 ORDER BY 1")
//    df404Day.show(truncate = false)
//    df404Day.coalesce(1).write.format("com.databricks.spark.csv").save("Output")

    df404Day.createOrReplaceTempView("df404Day")

    val df404DayAvg = spark.sql("SELECT AVG(QTD) FROM df404Day")
//    df404DayAvg.show()

    val dfBytes = spark.sql("SELECT SUM(BIGINT(bytesSent)) AS TOTAL_BYTES FROM logs")
//    dfBytes.show(truncate = false)

  }

}
