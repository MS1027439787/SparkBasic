import org.apache.spark.SparkContext
class Demo1{

  import org.apache.spark.SparkConf

  //本地
  //val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
  //集群
  val conf: SparkConf = new SparkConf().setAppName("helloWorld")
  //2、构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
  // 它内部会构建 DAGScheduler和 TaskScheduler 对象
  val sc = new SparkContext(conf)
  val file = sc.textFile("")
}


