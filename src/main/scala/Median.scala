
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks.{break, breakable}

object Median {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //本地设置
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //读取数据
    val dataRDD = sc.textFile("./src/main/resources/data")
      .flatMap(_.split(",")).map(_.toInt)
    dataRDD.foreach(println)
    //数据分区
    val mappedDataRDD = dataRDD.map(num => (num / 1000, num)).sortByKey()
    mappedDataRDD.foreach(println)
    //统计每个桶数据量(等价于reduceByKey((a,b) =>{a + b}),这里必须将RDD转换为map，否则count(1)访问元素会报错
    val count = dataRDD.map(num => (num / 1000, 1)).reduceByKey(_+_).sortByKey()
    val countMap = count.collectAsMap()
    //根据总的数据量，逐次根据桶序号要低到高依次累加，判断中位数落在哪个桶中，并获取到在桶中的偏移量
    //总数据量个数 = 每个桶的数据量个数累加
    val sum_count = count.map(data => {
      data._2
    }).sum.toInt
    var tmp = 0 //中值累加的个数(包括所在区间)
    var tmp2 = 0 //中值累加的个数(包括所在区间)
    var index = 0
    var mid = sum_count / 2
    if(sum_count%2!=0){
      mid =sum_count/2+1//中值在整个数据的偏移量
    }
    else{
      mid =sum_count/2
    }
    breakable{
      for (i <-0 to count.count().toInt - 1){
        tmp = tmp + countMap(i)
        tmp2 = tmp - countMap(i)
        if(tmp >= mid){
          index = i//保存所在分桶数
          break
        }
      }
    }
    //中位数在桶中的偏移量
    val offset = mid - tmp2
    //获取中位数,takeOrdered它默认可以将key从小到大排序后，获取rdd中的前n个元素
    val result = mappedDataRDD.filter(num => num._1 == index).takeOrdered(offset)
    println("Median is " + result(offset-1)._2)
    sc.stop()
  }
}
