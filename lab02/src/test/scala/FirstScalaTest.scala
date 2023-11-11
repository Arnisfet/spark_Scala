import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class FirstScalaTest extends AnyFlatSpec with should.Matchers {
  val sc: SparkSession = SparkSession.builder().master("local").getOrCreate()

  "Run test #1" should "add column" in {
    val df = sc.range(10).toDF()
    val retDF = Main.addColumn("col1",  lit("col1"), df)
    val elem = retDF.select("col1").limit(1).collect()
    val result = elem(0).getString(0)

    result shouldBe "col1"
  }

  "Run test #2" should "add column" in {
    val df = sc.range(10).toDF()
    val retDF = Main.addColumn("col2", lit("col2"), df)
    retDF.show()
  }
}
