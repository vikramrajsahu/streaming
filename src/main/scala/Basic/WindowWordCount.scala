package Basic

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowWordCount {

  val microBatchDuration = 10
  val host = "localhost"
  val port = 46666
  val dirPath = "src/main/resources"

  /**
   * Create Spark Session
   */
  val spark:SparkSession = SparkSession.builder()
    .appName("Window Word Count")
    .master("local[2]")
    .getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  /**
   * Read Stream from Socket
   *
   * @return DStream[String]
   */
  def readSocketStream(): DStream[String] = {
    ssc.socketTextStream(this.host, this.port)
  }

  /**
   *  Read Stream from File
   * @return DStream[String]
   */
  def readFileStream(): DStream[String] = {
    ssc.textFileStream(this.dirPath)
  }

  /**
   * Scala Main Function
   * @param args Array[String]
   */
  def main(args: Array[String]): Unit = {


    val lines: DStream[String] = this.readSocketStream()
    val words = lines.flatMap(x => x.split(" ")).filter(x => x.nonEmpty).map(y => (y, 1)).reduceByKeyAndWindow((a:Int,b:Int) => a + b, Seconds(30), Seconds(10))
    words.print()


    /**
     * Start Streaming and Await for Termination
     */
    ssc.start()
    ssc.awaitTermination()
  }
}