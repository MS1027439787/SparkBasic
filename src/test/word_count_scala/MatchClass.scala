package word_count_scala

object MatchClass extends App {

  import scala.util.Random

  var arr = Array("hadoop", "spark", "hive", "flink", "storm")
  var name = arr(Random.nextInt(5))
  println("start")
  println(name)
  "hive" match {
    case "hadoop" => println("大数据分布式计算框架")
    case "hive" => println("sql解析引擎")
    case "spark" => println("实时框架")
    case "flink" => println("实时框架2")
    case "storm" => println("实时框架3")
    case _ => println("我不认识你")
  }

  def sum(args: Int*) = {
    var result = 0
    for (arg <- args) result += arg
    result
  }

  val s = sum(1 to 5: _*)
  println(s)
}
