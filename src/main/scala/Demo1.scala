import org.apache.spark.SparkContext
class Demo1{

  import org.apache.spark.{Partitioner, SparkConf}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.serializer.Serializer
  import org.apache.spark.storage.StorageLevel
  //本地
  //val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
  //集群
  val conf: SparkConf = new SparkConf().setAppName("helloWorld")
  //2、构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
  // 它内部会构建 DAGScheduler和 TaskScheduler 对象
  val sc = new SparkContext(conf)
  val file = sc.textFile("")
  //errors也是一个RDD
  val errors = file.filter(line=>line.contains("ERROR"))
  val count = errors.count()

  val f = (file:String) => file.filter(_>=3)

  //val cleanF = sc.clean(f)

//  def reduceByKey(partitioner: Partitioner, func: (String, String) => String ) : RDD[(Int,Int)] = {
//    combineByKey[String]( ( v: String) => v, func, func, partitioner)
//  }

  //缓存操作
  //def persist(newLevel: StorageLevel)
}


