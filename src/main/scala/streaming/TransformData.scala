package streaming
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
/**
  * @author masai
  * @date 2021/3/22 15:24
  */
object TransformData {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    val blacklist = ArrayBuffer[(String, Boolean)](
      ("zhangsan", true),
      ("lisi", true))
    //parallelize从已有集合上创建RDD
    val blacklistRDD = ssc.sparkContext.parallelize(blacklist)

    val adsClickLogDStream = ssc.socketTextStream("localhost", 9999)

    // (username, date username)映射处理
    val userAdsClickLogDStream = adsClickLogDStream.map(adsClickLog => (adsClickLog.split(" ")(1), adsClickLog))


    val validAdsClickLogDStream = userAdsClickLogDStream.transform {
      userAdsClickLogRDD => {
        //需要数据实际操作看看
        val joinedRDD = userAdsClickLogRDD.leftOuterJoin(blacklistRDD)
        val filteredRDD = joinedRDD.filter(!_._2._2.getOrElse(false))
        val validAdsClickLogRDD = filteredRDD.map(_._2._1)
        validAdsClickLogRDD
      }
    }

    validAdsClickLogDStream.print()
  }
}
