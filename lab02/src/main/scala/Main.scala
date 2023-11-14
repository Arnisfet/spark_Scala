import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, explode, from_json, lit, split}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}


object Main {
  private val local_path = "extra\\"
  private val cluster_path = ""
  private val autousers = "autousers.json"
  private val logs = "logs"

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
    val dfSplit = users.select(explode(col("autousers")).as("autousers")).show()
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
    val decoded_domain = filtered_domains
      .selectExpr("*", "reflect('java.net.URLDecoder','decode', url, 'utf-8') as newcol")
//    val count = decoded_domain.count()
//    println(count)
    }
}
