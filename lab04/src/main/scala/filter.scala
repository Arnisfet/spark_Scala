import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sys.process._

object filter {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val dir = spark.conf.get("spark.filter.output_dir_prefix")
    val topic = spark.conf.get("spark.filter.topic_name")
    var offset = spark.conf.get("spark.filter.offset")
    if (offset != "earliest") {
      offset = s"""{"$topic":{"0":$offset}}"""

      val kafka_params = Map(
        "kafka.bootstrap.servers" -> "spark-master-1:6667"
        ,"subscribe" -> "lab04_input_data"
        ,"startingOffsets" -> offset
      )

      val kafka_df = spark
        .read
        .format("kafka")
        .options(kafka_params)
        .load

      val json_string = kafka_df.select(col("value").cast("string")).as[String]

      val parsed_string = spark.read.json(json_string)
        .withColumn("date", to_date(from_unixtime(col("timestamp") / 1000)))
        .withColumn("date", from_unixtime(unix_timestamp(col("date"), "yyyy-MM-dd"), "yyyyMMdd"))
        .withColumn("p_date", col("date"))

      val view_df = parsed_string.filter(col("event_type") === "view")
      val buy_df = parsed_string.filter(col("event_type") === "buy")

      s"hdfs dfs -rm -r -f ${dir}/view/*".!!
      s"hdfs dfs -rm -r -f ${dir}/buy/*".!!

      buy_df.write
        .format("json")
        .mode("overwrite")
        .partitionBy("p_date")
        .save(dir + "/buy")

      view_df.write
        .format("json")
        .mode("overwrite")
        .partitionBy("p_date")
        .save(dir + "/view")

      s"hdfs dfs -rm -r -f ${dir}/view/_SUCCESS".!!
      s"hdfs dfs -rm -r -f ${dir}/buy/_SUCCESS".!!
    }
  }
}