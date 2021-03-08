import org.apache.spark.{SparkConf, SparkContext}

object Test{

  import java.io.PrintWriter

  def main(args:Array[String]):Unit={
    import org.apache.spark.rdd.RDD


    //本地设置
    System.setProperty("hadoop.home.dir","E:\\hadoop-3.0.1")
    //1、构建sparkConf对象 设置application名称和master地址
    //本地
    val sparkConf:SparkConf=new SparkConf().setAppName("WordCount").setMaster("local")
    //2、构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
    // 它内部会构建 DAGScheduler和 TaskScheduler 对象
    val sc=new SparkContext(sparkConf)
    //sc.textFiles(path) 能将path 里的所有文件内容读出，以文件中的每一行作为一条记录的方式
    val data: RDD[String] = sc.textFile("./src/main/resources/words")
    val x = sc.parallelize(List("spark", "rdd", "example",  "sample", "example"), 3)
    val y1 = x.map(x => (x, 1))
    y1.collect
    // res0: Array[(String, Int)] =
    //    Array((spark,1), (rdd,1), (example,1), (sample,1), (example,1))
    // rdd y can be re writen with shorter syntax in scala as
    val y2 = x.map((_, 1))
    y2.collect
    // res1: Array[(String, Int)] =
    //    Array((spark,1), (rdd,1), (example,1), (sample,1), (example,1))
    // Another example of making tuple with string and it's length
    val y3 = x.map(x => (x, x.length))
    y3.collect
    // res3: Array[(String, Int)] =
    //    Array((spark,5), (rdd,3), (example,7), (sample,6), (example,7))

    //reduceByKey
    //reduceByKey会寻找相同key的数据，当找到这样的两条记录时会对其value(分别记为x,y)做(x,y) => x+y的处理，即只保留求和之后的数据作为value。反复执行这个操作直至每个key只留下一条记录。
    val word = y1.reduceByKey((x,y) => x+y)
    val word1 = y1.reduceByKey(_+_)

    /**
     * 通过groupByKey()后调用map遍历每个分组，然后通过t => (t._1,t._2.sum)对每个分组的值进行累加。
     * 因为groupByKey()操作是把具有相同类型的key收集到一起聚合成一个集合，集合中有个sum方法，对所有元素进行求和。
     * 注意，（k，v）形式的数据，我们可以通过 ._1，._2 来访问键和值，
     * 用占位符表示就是 _._1，_._2，这里前面的两个下划线的含义是不同的，前边下划线是占位符，后边的是访问方式。
     * 我们记不记得 ._1，._2，._3 是元组的访问方式。我们可以把键值看成二维的元组。
     */
    y1.reduceByKey(_+_).collect.foreach(println)
    //等价于
    y1.groupByKey().map(t => (t._1,t._2.sum)).collect.foreach(println)

    val rdd1=sc.parallelize(List(1,2,3,4,5,6,7,8,9))
    //每个元素乘10，_指集合每个元素
    val list2=rdd1.map(_*10).collect
    //匿名函数，省略参数类型
    val list1=rdd1.filter(x=>x>5).collect
    for(i<-list1)
      println(i)
    sc.stop()
  }

}
