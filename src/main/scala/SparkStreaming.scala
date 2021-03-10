object SparkStreaming {

  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark._
  import org.apache.spark.streaming._
  def main(args: Array[String]): Unit = {
    import org.apache.spark.streaming.dstream.DStream
    //本地设置
    //System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    val wordsOne = lines.map(word =>(word, word +"one"))
    val wordsTwo = lines.map(word =>(word, word +"one"))
    //ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
    val words_join = wordsOne.join(wordsTwo)
    val windowedStream1 = wordsOne.window(Seconds(20))
    val windowedStream2 = wordsTwo.window(Minutes(1))
    val joinedStream = windowedStream1.join(windowedStream2)

  }
}
