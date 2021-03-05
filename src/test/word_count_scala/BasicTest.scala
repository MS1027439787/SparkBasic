package word_count_scala

class BasicTest {
  var a: Int = 10;
  val b: Int = 20;
  var c = 100;
  lazy val d = 1000;
  var name: String = _

  def add(a: Int, b: Int): Int = a + b
  def sayHello(msg:String) = {
    println(msg)
  }
  var add1 = (a: Int, b: Int) => a + b
}

object Main {
  def main(args: Array[String]): Unit = {
    var a = 10;
    var map1 = Map("a" -> "马赛", "b" -> "许晓旭", "c" -> "李金铎")
    var basicTest = new BasicTest()
    basicTest.name = "hello"
    println(basicTest.name)
    //println(basicTest.add(basicTest.a, basicTest.b))
    //println(basicTest.add1(100, 1))
    for (k <- map1.keys) println(k + " -> " + map1(k))
    println(map1("a"))
    for ((k, v) <- map1) println(k + " -> " + v)


    var list = List("masai", "mali", "maqiqi")
    //list.foreach((x:String) => println(x))
    //list.foreach(x => println(x))
    //list.foreach(println(_))
    list.foreach(println)
    basicTest.sayHello("hello ,I want play")

  }
}

