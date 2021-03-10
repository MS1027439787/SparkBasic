object StockTrend {

  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.streaming._
  import SparkStreamingStockPrediction.SinaStock
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[1]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("./tmp")
    /* 创建股票的输入流，该输入流是自定义的 */
    val lines = ssc.receiverStream(new SinaStockReceiver())
    /** 将数据的每一行映射成一个SinaStock对象。注意此处的每一行数据都是SinaStockReceiver对象调用store传过来的 **/
    val words = lines.map(SinaStock(_))
    words.print()
    import scala.util.Random
    /* reduce从左到右进行折叠。其实就是先处理t-6，t-5的RDD，将结果与t-4的RDD再次调用reduceFunc，依次类推直到当前RDD */
    def reduceFunc(left: (Float, Int), right: (Float, Int)): (Float, Int) = {
      println("left " + left + "right " + right)
      (right._1, left._2 + right._2)
    }

    /* 3点之后股票价格不在变化，故为了测试，此处使用随机数修改股票当前价格 */
    /* 根据上一次股票价格更新股票的变化方向 */
    /** 由于股票信息只有当前价格，如果要判断股票上涨与否就要记录上一次的股票价格，所以此处使用updateStateByKey更新当前股票价格是否上涨。　若上涨则记为1，不变记为0，否则记为1    **/
    val stockState = words.map(sinaStock =>
      (sinaStock.name, (sinaStock.curPrice + Random.nextFloat, -1))).filter(stock => stock._1.isEmpty == false)
      .updateStateByKey(updatePriceTrend)
    /* 每3秒，处理过去6秒的数据，对数据进行变化的累加 */
    val stockTrend = stockState.reduceByKeyAndWindow(reduceFunc(_, _), Seconds(6), Seconds(3))
    /* 每3秒，处理过去6秒的数据，对数据进行正向变化的累加 */
    //val stockPosTrend=stockState.filter(x=>x._2._2>=0).reduceByKeyAndWindow(reduceFunc(_,_),Seconds(6),Seconds(3))
    stockState.print()
    stockTrend.print()
    //stockPosTrend.print()
    ssc.start()
    ssc.awaitTermination()
    println("StockTrend")
  }

  def updatePriceTrend(newValue: Seq[(Float, Int)], preValue: Option[(Float, Int)]): Option[(Float, Int)] = {
    if (newValue.length > 0) {
      val priceDiff = newValue(0)._1 - preValue.getOrElse((newValue(0)._1, 0))._1
      // ("update state: new Value "+newValue(0) + ",pre Value " + preValue.getOrElse((newValue(0)._1 ,0)))
      Some((newValue(0)._1, priceDiff.compareTo(0.0f)))
    } else preValue
  }
}
