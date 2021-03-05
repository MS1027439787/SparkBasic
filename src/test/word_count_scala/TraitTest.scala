package word_count_scala

trait TraitTest {
  def fuck(msg: String)
}
trait TraitTest2{
  def love(msg: String) = println(msg)
  def hate(msg: String) = println(msg)
}
class Logger extends TraitTest with TraitTest2 {
  override def fuck(msg: String) = println(msg)
  override def love(msg: String): Unit = println(msg)
}

class Logger2  {
}
object Logger{
  def main(args: Array[String]): Unit = {
    val logger = new Logger
    logger.fuck("dear")
    logger.love("love")
    val logger2 = new Logger2 with TraitTest2

  }
}