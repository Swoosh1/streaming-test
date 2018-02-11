
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON._
import MyUtils._

import scala.collection.JavaConversions._

object Processor {
  def main(args: Array[String]): Unit = {
    println("starting streaming process")
    val streamingLocation = {
      if(args.length > 0){
        args(0)
      }else{
        throw new IllegalArgumentException("Please provide the directory to the logs to be streamed")
      }
    }

    if(streamingLocation == null || streamingLocation.isEmpty()){
      throw new IllegalArgumentException("Please provide a valid directory which is not empty")
    }

    println("will stream from " + streamingLocation)
    val awsAccessKey =  {
      if(args.length > 1){
        args(1)
      }else{
        ""
      }
    }

    val awsSecretKey =  {
      if(args.length > 2){
        args(2)
      }else{
        ""
      }
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("EventStreamer")
        .setMaster("local[*]")
    val sc = new SparkContext(conf)

    if(awsAccessKey != null && !awsAccessKey.isEmpty()){
      println("using access key: " + awsAccessKey)
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId",awsAccessKey)
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",awsAccessKey)
    }

    if(awsSecretKey != null && !awsSecretKey.isEmpty()){
      println("using secret key: " + awsSecretKey)
      sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey",awsSecretKey)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",awsSecretKey)
    }

    val ssc = new StreamingContext(
      sc,Seconds(5))

    val events = ssc.textFileStream(streamingLocation)

    val typeCount = events
      .filter(line => line != None && line != None && line != Some(None)) // take care of empty lines
      .map(line => parseFull(line).get.asInstanceOf[Map[String, Any]].get("Type")) // get event type
      .map(word => (word.get, 1)).reduceByKey(_+_) // get count

    typeCount.foreachRDD((wordCount, time) => {

      wordCount.foreach(x => {
        val eventType = x._1.asInstanceOf[String]

        if(countList.containsKey(eventType)){
          countList.put(eventType, countList.get(eventType) + x._2)
        }else{
          countList.put(eventType, x._2)
        }
      })

      for ((k, v) <- countList){
        println("\"" + k + "\": " + v)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
