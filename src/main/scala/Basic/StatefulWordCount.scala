package Basic

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulWordCount {

  val microBatchDuration = 10
  val host = "localhost"
  val port = 46666
  val dirPath = "src/main/resources"

  /**
   * Create Spark Session
   */
  val spark:SparkSession = SparkSession.builder()
    .appName("Stateful Word Count")
    .master("local[2]")
    .getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  ssc.checkpoint("src/main/checkpointing")

  /**
   * Read Stream from Socket
   *
   * @return DStream[String]
   */
  def readSocketStream(): DStream[String] = {
    ssc.socketTextStream(this.host, this.port)
  }

  /**
   * Scala Main Function
   * @param args Array[String]
   */
  def main(args: Array[String]): Unit = {


    def updateFunc(values:Seq[Int], currentCount:Option[Int]) = {
      val newCount = values.sum + currentCount.getOrElse(0)
      new Some(newCount)
    }

    val lines: DStream[String] = this.readSocketStream()
    val words = lines.flatMap(x => x.split(" ")).filter(x => x.nonEmpty).map(y => (y, 1)).reduceByKey(_+_)

    val totalWC = words.updateStateByKey(updateFunc _)
    totalWC.print()


    /**
     * Start Streaming and Await for Termination
     */
    ssc.start()
    ssc.awaitTermination()
  }


}