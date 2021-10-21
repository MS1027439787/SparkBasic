package basicPractice

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author masai
 * @date 2021/10/21
 */
object MapJoin {
  def main(args: Array[String]): Unit = {
    //本地设置
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    //1、构建sparkConf对象 设置application名称和master地址
    //本地
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(sparkConf)

    // Fact table
    val flights = sc.parallelize(List(
      ("SEA", "JFK", "DL", "418",  "7:00"),
      ("SFO", "LAX", "AA", "1250", "7:05"),
      ("SFO", "JFK", "VX", "12",   "7:05"),
      ("JFK", "LAX", "DL", "424",  "7:10"),
      ("LAX", "SEA", "DL", "5737", "7:10")))

    // Dimension table
    val airports = sc.parallelize(List(
      ("JFK", "John F. Kennedy International Airport", "New York", "NY"),
      ("LAX", "Los Angeles International Airport", "Los Angeles", "CA"),
      ("SEA", "Seattle-Tacoma International Airport", "Seattle", "WA"),
      ("SFO", "San Francisco International Airport", "San Francisco", "CA")))

    // Dimension table
    val airlines = sc.parallelize(List(
      ("AA", "American Airlines"),
      ("DL", "Delta Airlines"),
      ("VX", "Virgin America")))

    //广播只能从driver端，因此必须先将数据收集至driver端，collectAsmap方法收集，broadcast广播至每个Executor，然后进行join操作
//    The fact table be very large, while dimension tables are often quite small. Let’s download the dimension tables to the Spark driver, create maps and broadcast them to each worker node
    val airportsMap = sc.broadcast(airports.map{case(a, b, c, d) => (a, c)}.collectAsMap)
    val airlinesMap = sc.broadcast(airlines.collectAsMap)

    val res = flights.map{case(a, b, c, d, e) =>
      (airportsMap.value.get(a).get,
        airportsMap.value.get(b).get,
        airlinesMap.value.get(c).get, d, e)}.collect
    res.iterator.foreach(i =>println(i))
  }

}
