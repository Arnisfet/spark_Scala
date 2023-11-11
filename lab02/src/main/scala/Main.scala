import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("First app")
      .getOrCreate()
    println("Hello world!")

    val df = spark.range(start = 1, end = 10, step = 1, numPartitions = 1).toDF()
    val retDf = addColumn("test", lit("test1"), df)
    retDf.show(10)
  }

  def addColumn(name: String, c: Column, df: DataFrame): DataFrame ={
    val returnDF = df.withColumn(name, c)
    returnDF
  }
}