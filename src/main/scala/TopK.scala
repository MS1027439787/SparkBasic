import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeMap
import scala.collection.mutable

object TopK {
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
    val data: RDD[String] = sc.textFile("E:\\Program\\SparkBasic\\src\\main\\resources\\words.txt")

    //执行topK
    dotopK1(data)
    dotopK2(data)
    sc.stop()
  }

  /**
   * 第一种topK的实现
   */
  def dotopK1(words :RDD[String] ): Unit ={
    //计算每一个单次的词频,"\\s+"正则表达式匹配多个空白字符
    val wordCountRDD = words.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)
    //排序,交换k,v顺序
    val sortedRDD = wordCountRDD.map{case (key,value) => (value, key)}.sortByKey(true,3)
    //val sortedRDD1 = wordCountRDD.map{case (key,value) => (value, key)}.sortBy(_,true,3)
    //得到词频最高的四个单词
    sortedRDD.top(4).foreach(println)
  }

  /**
   * 第二种topK实现，典型的大数据分布式思想
   */
  def dotopK2(words :RDD[String] ): Unit ={
    //计算每一个单次的词频,"\\s+"正则表达式匹配多个空白字符
    val wordCountRDD = words.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)
    //在每一个分区内进行topk查询
    val topk = wordCountRDD.mapPartitions(iter => {
      val partionHeap = new Heap()
      while(iter.hasNext){
        partionHeap.putToHeap(iter.next())
      }
      partionHeap.getHeap().iterator
    })
    val driverHeap = new Heap()
    //将每个分区中统计出来的top k合并成一个新的集合，再统计新集合中的top k。
    topk.collect().foreach(driverHeap.putToHeap(_))
    driverHeap.getHeap().foreach(println)
  }
}

/**
 * 一个能够根据treeMap的value大小降序排序的堆。
 * @param k 保留前面k个数
 */
class Heap(k:Int = 4){
  /**
   * 辅助数据结构，加快查找速度
   */
  private val hashMap:mutable.Map[String,Int] = new mutable.HashMap[String, Int]()

  implicit val valueOrdering = new Ordering[String]{
    override def compare(x: String, y: String): Int = {
      val xValue:Int = if(hashMap.contains(x)) hashMap.get(x).get else 0
      val yValue:Int = if(hashMap.contains(y)) hashMap.get(y).get else 0
      if(xValue > yValue) -1 else 1
    }
  }

  /**
   *存储有序数据
   */
  private var treeMap = new TreeMap[String, Int]()

  /**
   * 把数据存入堆中
   * 自动截取，只保留前面k个数据
   * @param word
   */
  def putToHeap(word:(String,Int)):Unit = {
    //数据加入
    hashMap += (word._1 -> word._2)
    treeMap = treeMap + (word._1 -> word._2)
    //treeMap.drop(k)返回除前k值之外的多余值
    val dropItem = treeMap.drop(k)
    //这句的含义不太清楚，debug尝试注释掉也没啥影响
    dropItem.foreach(treeMap -= _._1)
    //取前k个值
    treeMap = treeMap.take(this.k)
    treeMap.foreach(println)
  }

  /**
   * 取出堆中的数据
   * @return
   */
  def getHeap():Array[(String,Int)] = {
    val result = new Array[(String, Int)](treeMap.size)
    var i = 0
    this.treeMap.foreach(item => {
      result(i) = (item._1, item._2)
      i += 1
    })
    result
  }
}
