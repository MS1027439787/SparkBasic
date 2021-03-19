package basicPractice

/**
 * 数据倾斜连接
 */
object SkewJoin {

  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    //本地设置
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.0.1")
    val sparkConf: SparkConf = new SparkConf().setAppName("SkewJoin").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //读取数据
    val skewTable = sc.textFile("./src/main/resources/skewTable")
    val table = sc.textFile("./src/main/resources/table")
    //对倾斜数据进行采样，获取最大倾斜key
    //sample算子时用来抽样用的，其有3个参数
    //withReplacement：表示抽出样本后是否在放回去，true表示会放回去，这也就意味着抽出的样本可能有重复
    //fraction ：抽出多少，这是一个double类型的参数,0-1之间，eg:0.3表示抽出30%
    //seed：表示一个种子，根据这个seed随机抽取，一般情况下只用前两个参数就可以，那么这个参数是干嘛的呢，这个参数一般用于调试，有时候不知道是程序出问题还是数据出了问题，就可以将这个参数设置为定值
    val sample = skewTable.sample(false, 1, 9)
  }
}
