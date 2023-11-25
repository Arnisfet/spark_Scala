import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import java.net.{URL, URLDecoder}
import scala.util.Try

object Main {
  private val CASSANDRA_PORT = "9042"
  private val CASSANDRA_HOST = "10.0.0.31"
  /** * Decode function. Actually dont understand how does it decode пддонлайн...** */
  def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost
    }.getOrElse("")
  })

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local")
      .config("spark.cassandra.connection.host", "CASSANDRA_HOST")
      .config("spark.cassandra.connection.port", "CASSANDRA_PORT")
      .appName("lab03")
      .getOrCreate()

    val hdfs_logs = spark.read
      .option("header", true)
      .json("extra/weblogs.json").toDF
      .select(col("uid"), explode(col("visits")))
      .select(col("uid"), col("col.*")).toDF

    hdfs_logs.show()

//    val clients: DataFrame = spark.read
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> "таблица_Cassandra", "keyspace" -> "кейспейс_Cassandra"))
//      .load()
  }
}