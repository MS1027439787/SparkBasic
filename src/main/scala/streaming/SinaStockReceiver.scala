package streaming

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class SinaStockReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  def onStart() {
    /* 创建一个线程用来查询新浪股票数据，并将数据发送给Spark Streaming */
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  private def receive(): Unit = {
    try {
      var i = 0
      while (i <= 100) {
        var stockIndex = 1
        while (stockIndex != 0) {
          import scala.io.Source
          val stockCode = 601000 + stockIndex
          val url = "http://hq.sinajs.cn/list=sh%d".format(stockCode)
          println(url)
          val sinaStockStream = Source.fromURL(url, "gbk")
          val sinaLines = sinaStockStream.getLines
          for (line <- sinaLines) {
            store(line)
          }
          sinaStockStream.close()
          stockIndex = (stockIndex + 1) % 1
        }
        i += 1
      }
      println("Stopped receiving")
    } catch {
      case e: java.net.ConnectException =>
        println("Error connecting to", e)
      case t: Throwable =>
        println("Error receiving data", t)
    }
  }
}

