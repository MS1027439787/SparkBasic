package word_count_scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test {

  import java.io.PrintWriter

  def main(args: Array[String]): Unit = {
    //本地设置
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.5")
    //1、构建sparkConf对象 设置application名称和master地址
    //本地
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //2、构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
    // 它内部会构建 DAGScheduler和 TaskScheduler 对象
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    //每个元素乘10，_指集合每个元素
    val list2 = rdd1.map(_ * 10).collect
    //匿名函数，省略参数类型
    val list1 = rdd1.filter(x => x > 5).collect
    for (i <- list1)
      println(i)
    sc.stop()
  }

}
