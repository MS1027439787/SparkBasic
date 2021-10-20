package basicPractice

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //本地设置
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    //1、构建sparkConf对象 设置application名称和master地址
    //本地
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    //集群
    //val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")
    //2、构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
    // 它内部会构建 DAGScheduler和 TaskScheduler 对象
    val sc = new SparkContext(sparkConf)
    //设置日志输出级别
    sc.setLogLevel("warn")
    //3、读取数据文件Starry Starry Night
    //val data: RDD[String] = sc.textFile("/tmp/daoshu/tmp_wcl_dunj_sevn_zx.csv")
    val data: RDD[String] = sc.textFile("./src/main/resources/words")

    val dataResult:Array[String] = data.collect()
    for (i <- dataResult) {
      println(i)
    }
    //4、 切分每一行，获取所有单词，空格也会被算作一个字符
    val words: RDD[String] = data.flatMap(x => x.split(" "))
    //等价写法
    //val words: RDD[String] = data.flatMap(_.split(" "))
    val wordsResult:Array[String] = words.collect()
    for (i <- wordsResult) {
      println(i)
    }
//    //测试方法
//    val wordsmap: RDD[Array[String]] = data.map(x => x.split(" "))
//    val wordsmapResult:Array[Array[String]] = wordsmap.collect()
//    for (i <- wordsmapResult) {
//      println(i)
//    }
    //5、每个单词计为1
    val wordAndOne: RDD[(String, Int)] = words.map(x =>(x, 1))
    //等价写法
//    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //测试方法ª
//    “flatMap “函数的一半功能和map函数一样，不过有个要求，传入的函数在处理完后返回值必须是List(应该是Seq)，如果结果不是List(Seq)，
//    那么将出错。也就是说，传入的函数是有要求的——返回值是Seq才行。这样，每个元素处理后返回一个List，
//    我们得到一个包含List元素的List，flatMap自动将所有的内部list的元素取出来构成一个List返回。
//    val wordAndOneflatmap: RDD[Array[Tuple2[String, Int]]] = words.flatMap((_, 1))
    val wordAndOneResult: Array[(String, Int)] = wordAndOne.collect()

    for (i <- wordAndOneResult) {
      println(i)
    }
    //6、相同单词出现的1累加,后面参数用于提高并行度
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey((x,y) => x+y,500)
    //等价写法
//    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)


    //按照单词出现的次数降序排列 第二个参数默认是true表示升序，设置为false表示降序
    val sortedRDD: RDD[(String, Int)] = result.sortBy(x => x._2, false)
    //等价写法
//    val sortedRDD: RDD[(String, Int)] = result.sortBy(_._2, false)
    dirDel(new File("result"))
    sortedRDD.saveAsTextFile("result")
    //7、收集数据打印
    val finalResult: Array[(String, Int)] = sortedRDD.collect()
    for (i <- finalResult) {
      println(i)
    }
    //8、关闭sc
    sc.stop()
  }
  def dirDel(path: File) {
    if (!path.exists())
      return
    else if (path.isFile()) {
      path.delete()
      println(path + ":  文件被删除")
      return
    }

    val file: Array[File] = path.listFiles()
    for (d <- file) {
      dirDel(d)
    }

    path.delete()
    println(path + ":  目录被删除")

  }
}
