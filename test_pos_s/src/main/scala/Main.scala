import java.io.BufferedReader
import scala.io.{BufferedSource, Source}
import scala.io.Source.fromFile

object Main {
  /*Shitty win's path*/
  private val local_path = "C:\\Users\\Пользователь\\Desktop\\spark_Scala\\test_pos_s\\extra\\lab01\\u.data"
  private val general_path = "u.data"
  private val exclusive_number = "328"

/*** The counter function for the films' raitings ***/
  def counter(source: Array[Int]) : Array[Int] = {
    var result1: Array[Int] = Array()
    for (i <- 1 to  5)
      result1 = result1 :+ source.count(_ == i)
    result1
  }: Array[Int]

  def main(args: Array[String]): Unit = {
    var sequence = Array[String]()
    val source = Source.fromFile(local_path)
    var result1: Array[Int] = Array()
    var result2: Array[Int] = Array()

    /*Seems like it is not a good way to realloc array every time. Greedy solution*/
    for (line <- source.getLines())
        sequence = sequence :+ line
    source.close()
    /* Init array which requires for the whole calculations */
    val init_source = sequence.map(string => string.split("\t").slice(1,3))
    /* First part with all the films */
    val all_rait_int = init_source.flatMap(string => string.slice(1, 2)).map(_.toInt)
    result1 = counter(all_rait_int)
    /* Second part with all the exclusive films */
    val exclusive_rait = init_source.filter(_.apply(0) == exclusive_number)
      .flatMap(string => string.slice(1, 2)).map(_.toInt)
    result2 = counter(exclusive_rait)

  }


}