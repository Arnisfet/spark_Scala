import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, count, desc, explode, from_json, lit, regexp_extract, split, sum, udf, when}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.net.{URL, URLDecoder}
import scala.util.Try
import java.net.URLDecoder
import scala.util.{Failure, Success, Try}


object Main {
  private val local_path = "extra\\"
  private val cluster_path = ""
  private val autousers = "autousers.json"
  private val logs = "logs"

  def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost
    }.getOrElse("")
  })

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Lab02")
      .getOrCreate()

    val usersSchema =
      StructType(
        List(
          StructField("autousers", ArrayType(StringType, containsNull = false))
        )
      )
    val users = spark.read.schema(usersSchema).json(local_path + autousers)
    /*** Explode way to split data in array[string] ***/
    val users_split = users.select(explode(col("autousers")).as("autousers"))
    /*** Split way to split data in array[string]  - does not work((, maybe split works only with String type***/
    //    val dfSplit = users.select(split(col("autousers"), ",").as("autousers")).show()

    val logsSchema =
    StructType(
      List(
        StructField("uid", StringType),
        StructField("ts", StringType),
        StructField("url", StringType)
      )
    )
    val domains = spark.read.schema(logsSchema).option("delimiter", "\t").csv(local_path + logs)
    val filtered_domains = domains.na.drop()
      .filter(col("url")
        .contains("http") || col("url").contains("https"))
    val decoded_domain = filtered_domains.select(col("uid"), decodeUrlAndGetDomain(col("url")).alias("url"))
//    decoded_domain.show(false)
    val regexp_domains = decoded_domain
      .withColumn("domain", regexp_extract(col("url"), "(?:www\\.|)([\\w.-]+).*", 1))
//    regexp_domains.show(false)
    val users_domains : DataFrame = regexp_domains.select("uid", "domain")
    /*** 6571047 domains number ***/
    val union_table = users_domains.join(users_split, users_domains("uid") === users_split("autousers"), "left").distinct()
      .select("*")
    val matched_table = union_table
      .withColumn("nums", when(col("autousers").isNull or col("autousers") === "", 0)
        .otherwise(1))
      .select("domain", "nums")
//    matched_table.show(200)
    val grouped = matched_table
      .groupBy(col("domain"))
      .agg(sum(col("nums")).as("sum"),
        count(col("domain")).as("count"))
    grouped.orderBy("domain").show(100)

    /*** Start counting of the users ***/
//    val domen_counter = union_table.groupBy("domain").count()
//    domen_counter.show()
    }
}
