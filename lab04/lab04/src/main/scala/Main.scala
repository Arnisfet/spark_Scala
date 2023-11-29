import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateView.conf
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import sys.process._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Lab02")
      .getOrCreate()


    val initDF = (spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      )

//    val sdf = spark.readStream.format("rate").load()
//    sdf.isStreaming
  }
}