package streaming

object SparkStreaming {

  import org.apache.spark.SparkConf
  import org.apache.spark.streaming._

  def main(args: Array[String]): Unit = {
    //本地设置
    //System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    //    通过创建输入DStream定义输入源。
    //    通过将转换和输出操作应用于DStream来定义流计算。
    //    开始接收数据并使用进行处理streamingContext.start()。
    //    等待使用停止处理（手动或由于任何错误）streamingContext.awaitTermination()。
    //    可以使用手动停止处理streamingContext.stop()。

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("./tmp")
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    print("该窗口数据打印：")
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    //状态更新函数
    print("历史状态数据打印：")
    val runningcounts = pairs.updateStateByKey(updateFunction)
    //runningcounts.print()


    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

    //    一旦启动上下文，就无法设置新的流计算或将其添加到该流计算中。
    //    上下文一旦停止，就无法重新启动。
    //    JVM中只能同时激活一个StreamingContext。
    //    StreamingContext上的stop（）也会停止SparkContext。要仅停止的StreamingContext，设置可选的参数stop()叫做stopSparkContext假。
    //    只要在创建下一个StreamingContext之前停止（而不停止SparkContext）上一个StreamingContext，即可将SparkContext重新用于创建多个StreamingContext。
    //var seq:Seq[Int] = Seq(1,2,3,4)
  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    var newCount = newValues.sum // add the new values with the previous running count to get the new count
    //getOrElse()主要就是防范措施，如果有值，
    //那就可以得到这个值，主要是安全性
    newCount += runningCount.getOrElse(0)
    Some(newCount)
  }

  def print(data: String) = {
    println(data)
  }


}
