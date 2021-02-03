package Basic

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession

object WordCount {

  val microBatchDuration = 10
  val host = "localhost"
  val port = 46666
  val dirPath = "src/main/resources"

  /**
   * Create Spark Session
   */
  val spark = SparkSession.builder()
    .appName("Word Count")
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
   * Transformations
   */
  def performTransformation(lines: DStream[String]) : Unit = {
    val words = lines.flatMap(x => x.split(" ")).map(y => (y, 1)).reduceByKey(_ + _)
    words.print()
  }

  def main(args: Array[String]): Unit = {

    if(args(0) == "socket") {
      val lines: DStream[String] = this.readSocketStream()
      this.performTransformation(lines)
    } else if(args(0) == "file") {
      val lines: DStream[String] = this.readFileStream()
      this.performTransformation(lines)
    } else {
      println("Invalid Option.")
      return
    }

    /**
     * Start Streaming and Await for Termination
     */
    ssc.start()
    ssc.awaitTermination()
  }
}