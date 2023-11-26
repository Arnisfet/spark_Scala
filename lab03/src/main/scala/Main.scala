import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

import scala.util.Try
import java.net.URL
import java.net.URLDecoder.decode



object data_mart extends App {
  val spark = SparkSession.builder.master("local[*]")
    .config("spark.cassandra.connection.host", "CASSANDRA_HOST")
    .config("spark.cassandra.connection.port", "CASSANDRA_PORT")
    .appName("lab03")
    .getOrCreate()

  val hdfs_logs = spark.read
    .option("header", true)
    .json("/labs/laba03/weblogs.json").toDF()
    .select(col("uid"), explode(col("visits")))
    .select(col("uid"), col("col.*")).toDF()

  val CASSANDRA_HOST = "10.0.0.31"
  val CASSANDRA_PORT = "9042"

  val cassandra_clients = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "clients", "keyspace" -> "labdata"))
    .load()

  import org.elasticsearch.spark.sql
  import org.apache.spark.sql.DataFrame

  val ELASTIC_PORT = "9200"
  val ELASTIC_ADRESS = "10.0.0.31"
  val ELASTIC_INDEX = "visits"

  val elastic_visits: DataFrame = spark.read
    .format("org.elasticsearch.spark.sql")
    .options(Map("es.read.metadata" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.port" -> ELASTIC_PORT,
      "es.nodes" -> ELASTIC_ADRESS,
      "es.net.ssl" -> "false"))
    .load(ELASTIC_INDEX)

  val POSTGRE_URL = "jdbc:postgresql://10.0.0.31:5432/labdata"
  val POSTGRE_TABLE = "domain_cats"

  val postgre_cats: DataFrame = spark.read
    .format("jdbc")
    .option("url", POSTGRE_URL)
    .option("dbtable", POSTGRE_TABLE)
    .option("user", "***")
    .option("password", "***")
    .option("driver", "org.postgresql.Driver")
    .load()

  val col1 = when(cassandra_clients("age").between(18, 24), "18-24")
    .when(cassandra_clients("age").between(25, 34), "25-34")
    .when(cassandra_clients("age").between(35, 44), "35-44")
    .when(cassandra_clients("age").between(45, 54), "45-54")
    .when(cassandra_clients("age") >= 55, ">=55")
  val clients_tab_age = cassandra_clients.withColumn("age_cat", col1)
  val clients_tab = clients_tab_age.drop("age")

  import org.apache.spark.sql.functions.udf
  import scala.util.Try
  import java.net.URL
  import java.net.URLDecoder.decode

  val decoded_url = udf { (url: String) => Try(new URL(decode(url, "UTF-8")).getHost).toOption}

  val filtered_logs = hdfs_logs.filter(col("url").startsWith("http"))
    .withColumn("url", decoded_url(col("url")))
    .withColumn("url", regexp_replace(col("url"), "^www.", ""))
    .dropDuplicates()

  val web_domains = filtered_logs.join(postgre_cats, filtered_logs("url") === postgre_cats("domain"))
    .groupBy("uid", "category").count()
    .withColumn("category", concat(lit("web_"), col("category")))

  val websites_with_cat = web_domains.groupBy("uid")
    .pivot("category")
    .sum("count")
    .na.fill(0)

  val shops_cat = elastic_visits.select("uid", "category")
    .withColumn("category", lower(col("category")))
    .withColumn("category", regexp_replace(col("category"), "[ -]", "_"))
    .groupBy("uid", "category").count()
    .withColumn("category", concat(lit("shop_"), col("category")))

  val pivot_shops_cat = shops_cat.groupBy("uid")
    .pivot("category")
    .sum("count")
    .na.fill(0)

  val firstjoin = pivot_shops_cat.withColumnRenamed("uid", "right_uid")

  val tmp = clients_tab
    .join(firstjoin, firstjoin("right_uid") === clients_tab("uid"), "left")
    .drop("right_uid")
    .na.fill(0)

  val second_join = websites_with_cat.withColumnRenamed("uid", "right_uid")

  val res = tmp
    .join(second_join, second_join("right_uid") === tmp("uid"), "left")
    .drop("right_uid")
    .na.fill(0)

  val writeOptions = Map(
    "url" -> "jdbc:postgresql://10.0.0.31:5432/***",
    "dbtable" -> "clients",
    "user" -> "***",
    "password" -> "***",
    "driver" -> "org.postgresql.Driver")

  res
    .write
    .format("jdbc")
    .options(writeOptions)
    .mode("overwrite")
    .save

}