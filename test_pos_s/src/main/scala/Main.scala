import java.io.BufferedReader
import scala.io.{BufferedSource, Source}
import scala.io.Source.fromFile

object Main {
  /*Shitty win's path*/
  private val local_path = "C:\\Users\\Пользователь\\Desktop\\spark_Scala\\test_pos_s\\extra\\lab01\\u.data"
  private val general_path = "u.data"
  private val exclusive_number = "328"

  def main(args: Array[String]): Unit = {
    var sequence = Array[String]()
    val source = Source.fromFile(local_path)
    /*Seems like it is not a good way to realloc array every time. Greedy solution*/
    for (line <- source.getLines())
        sequence = sequence :+ line
    val listed_source = sequence.map(string => string.split("\t")).toList
    source.close()
  }


}