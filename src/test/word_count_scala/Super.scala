package word_count_scala

class Super

class Sub extends Super

//非变
class Temp1[A](title: String)

//协变
class Temp2[+A](title: String)

//逆变
class Temp3[-A](title: String)

object Covariance_demo {
  def main(args: Array[String]): Unit = {
    val a = new Sub()
    // 没有问题，Sub是Super的子类
    val b: Super = a
    // 非变
    val t1: Temp1[Sub] = new Temp1[Sub]("测试")
    // 报错！默认不允许转换
    // val t2:Temp1[Super] = t1
    // 协变
    val t3: Temp2[Sub] = new Temp2[Sub]("测试")
    val t4: Temp2[Super] = t3
    // 逆变
    val t5: Temp3[Super] = new Temp3[Super]("测试")
    val t6: Temp3[Sub] = t5
  }
}

