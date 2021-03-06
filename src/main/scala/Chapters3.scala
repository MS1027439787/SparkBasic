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
    val rdd_array = sc.parallelize(Array("hadoop", "hadoop", "hive", "spark"))

    //map自定义函数
    val rdd_list_map1 = rdd_list.map((x: Int) => x * 10)
    //等价函数
    val rdd_list_map2 = rdd_list.map(x => x * 10) //省略参数值
    rdd_list_map1.collect().foreach(println)
    val rdd_list_map3 = rdd_list.map(_ * 10) //最简写

    //flatMap 调用元素必须可迭代
    val rdd_array_flat_map1 = rdd_array.flatMap(x => x.tail)
    rdd_array_flat_map1.collect().foreach(println)
    val rdd_array_flat_map2 = rdd_array.flatMap(x => x.toUpperCase)
    rdd_array_flat_map2.collect().foreach(println)
    //mapPartitions
    val rdd_array_flat_par = rdd_array.mapPartitions(x => x.filter(_== "hadoop"))
    rdd_array_flat_par.collect().foreach(println)
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
    //reduceByKey
    val pairRdd = rdd_array.map((_,1))//rdd_array.map(x => (x,1))的简写，将x转换为键值对的形式
    val pairRdd1 = rdd_array.map(x => (x,x.length))
    pairRdd.collect().foreach(println)
    pairRdd.reduceByKey(_+_).collect.foreach(println)
    //等价于下面写法(reduceByKey会寻找相同key的数据，当找到这样的两条记录时会对其value(分别记为x,y)做(x,y) => x+y的处理，即只保留求和之后的数据作为value)
    pairRdd1.reduceByKey((x,y) => x + y).collect.foreach(println)
    //groupByKey
    pairRdd.groupByKey().foreach(println)
    pairRdd.groupByKey().map(t => (t._1,t._2.sum)).foreach(println)
    //combineByKey
    pairRdd.combineByKey((v : Int) => List(v), (c : List[Int], v : Int) => v :: c, (c1 : List[Int], c2 : List[Int]) => c1 ::: c2).collect.foreach(println)
    //val wordcount = rdd.flatMap(_.split(' ')).map((_,1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).saveAsTextFile("/data/project/result2")
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


