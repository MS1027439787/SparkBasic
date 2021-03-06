import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.StorageLevel

object Chapters3 {
  def main(args: Array[String]): Unit = {
    //本地
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1\\")
    val sparkConf: SparkConf = new SparkConf().setAppName("Chapters1").setMaster("local")
    //2、构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
    // 它内部会构建 DAGScheduler和 TaskScheduler 对象
    val sc = new SparkContext(sparkConf)
    val rdd_list = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd_array = sc.parallelize(Array("hadoop", "hive", "spark"))

    //map自定义函数
    val rdd_list_map1 = rdd_list.map((x: Int) => x * 10)
    //等价函数
    val rdd_list_map2 = rdd_list.map(x => x * 10) //省略参数值
    val list:Array[Int] = rdd_list_map1.collect()
    for(i <- list) {
      println(i)
    }
    val rdd_list_map3 = rdd_list.map(_ * 10) //最简写

    //flatMap 调用元素必须可迭代
    val rdd_array_flat_map1 = rdd_array.flatMap(x => x.tail)
    val array1:Array[Char] = rdd_array_flat_map1.collect()
    for(i <- array1) {
      println(i)
    }
    val rdd_array_flat_map2 = rdd_array.flatMap(x => x.toUpperCase)
    val array2:Array[Char] = rdd_array_flat_map2.collect()
    for(i <- array2) {
      println(i)
    }
    //mapPartitions
    val rdd_array_flat_par = rdd_array.mapPartitions(x => x.filter(_== "hadoop"))
    val partition = rdd_array_flat_par.collect()
    for(i <- partition) {
      println(i)
    }
    //glom
    val rdd_array_glom = rdd_list.glom()
    //union
    val union = rdd_array.union(rdd_array)
    dirDel(new File("result"))
    union.saveAsTextFile("result")
    val union1 = union.collect()
    for(i <- union1) {
      println(i)
    }
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


