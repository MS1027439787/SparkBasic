

//主构造方法
class Person(var name: String, var age: Int) {
  override def toString = s"Person($name, $age)"
}

object Person {
  // 实现apply方法
  // 返回的是伴生类的对象
  def apply(name: String, age: Int): Person = new Person(name, age)

  // apply方法支持重载
  def apply(name: String): Person = new Person(name, 20)

  def apply(age: Int): Person = new Person("某某某", age)

  def apply(): Person = new Person("某某某", 20)

  def unapply(arg: Person): Option[(String, Int)] = Some(arg.name, arg.age)
}

object Main2 {
  def main(args: Array[String]): Unit = {
    val p1 = Person("张三", 20)
    val p2 = Person("李四")
    val p3 = Person(100)
    val p4 = Person()
    println(p1)
    println(p2)
    println(p3)
    println(p4)
    println(s"对象1$p1,对象2${p2}")
    var masai = Person("masai", 10)
    masai match {
      case Person(name,age) => println(s"姓名：$name，年龄：$age")
      case _ => println("我不认识你")
    }
  }
}