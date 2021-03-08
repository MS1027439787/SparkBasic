import org.apache.spark.{SparkConf, SparkContext}

/**
  * 倒排索引
  */

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    //本地设置
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    val sparkConf: SparkConf = new SparkConf().setAppName("InvertedIndex").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //读取数据(数据格式：“\n”分割不同的文件，“:”分割id和属性)
    //按“:”分割映射，sc.textFile读取文件是按行读取的
    val dataRDD = sc.textFile("./src/main/resources/invertedindexdata")
        .flatMap{lines =>{
          //对每一行数据按：切分，line(0)为索引值，然后再对数据line(1)进行分割，每一个分割值都映射索引值
          val line = lines.split(":")
          line(1).split(",").map((_, line(0)))
        }}
    dataRDD.foreach(println)
    //分组
    val groupRDD = dataRDD.groupByKey()
    groupRDD.foreach(println)
    //字典序排序
    val sortRDD = groupRDD.sortByKey()
    sortRDD.foreach(x => println(s"${x._1}:${x._2.mkString(",")}"))
  }
}
