import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

object WordCount {
  def main(args: Array[String]): Unit = {
    //本地设置
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    //1、构建sparkConf对象 设置application名称和master地址
    //本地
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local").set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //集群
    //val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")
    //2、构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
    // 它内部会构建 DAGScheduler和 TaskScheduler 对象
    val sc = new SparkContext(sparkConf)
    //设置日志输出级别
    sc.setLogLevel("warn")
    //3、读取数据文件Starry Starry Night
    //val data: RDD[String] = sc.textFile("/tmp/daoshu/tmp_wcl_dunj_sevn_zx.csv")
    val data: RDD[String] = sc.textFile("E:\\Program\\SparkBasic\\src\\main\\resources\\words.txt")
    //4、 切分每一行，获取所有单词
    val words: RDD[String] = data.flatMap(x => x.split(" "))
    //5、每个单词计为1
    val wordAndOne: RDD[(String, Int)] = words.map(x => (x, 1))
    //6、相同单词出现的1累加
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey((x, y) => x + y)
    //按照单词出现的次数降序排列 第二个参数默认是true表示升序，设置为false表示降序
    val sortedRDD: RDD[(String, Int)] = result.sortBy(x => x._2, false)
    //7、收集数据打印
    val finalResult: Array[(String, Int)] = sortedRDD.collect()
    for(i <- finalResult){
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
