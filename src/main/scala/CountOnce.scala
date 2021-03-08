/**
  * 假设HDFS只存储一个标号为ID的Block， 每份数据保存2个备份， 这样就有2个机器存储
  * 了相同的数据。 其中ID是小于10亿的整数。 若有一个数据块丢失， 则需要找到哪个是丢失的
  * 数据块。
  * 已知一个数组， 数组中只有一个数据是出现一遍的， 其他数据都是出现两
  * 遍， 将出现一次的数据找出。
  */
object CountOnce {

  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    //本地设置
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    val sparkConf: SparkConf = new SparkConf().setAppName("CountOnce").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //读取数据
    val dataRDD = sc.textFile("./src/main/resources/blockId")
      .flatMap(_.split(",")).map(_.toInt)
    //每个分区分别对数据进行异或运算， 最后在reduceByKey阶段， 将
    //各分区异或运算的结果再做异或运算合并。 偶数次出现的数字， 异或运算
    //之后为0， 奇数次出现的数字， 异或后为数字本身
    val result = dataRDD.reduce(_^_)
    println(result)

  }
}
