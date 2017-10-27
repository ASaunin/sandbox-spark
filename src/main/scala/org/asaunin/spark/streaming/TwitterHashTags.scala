package org.asaunin.spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.asaunin.spark.core.SessionProvider

object TwitterHashTags extends TwitterAuthorization {

  private val log = Logger.getLogger("org.asaunin")
  private val spark = SessionProvider.getSession(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val context = spark.sparkContext
    context.setLogLevel("ERROR")

    val poolingInterval = Seconds(5)
    val duration = Seconds(30000)

    val streamingContext = new StreamingContext(context, poolingInterval)

    val hashTags = TwitterUtils.createStream(streamingContext, None)
      .map(_.getText())
      .flatMap(_.split(" "))
      .filter(_.startsWith("#"))

    val hashTagKeyValues = hashTags.map(hashtag => (hashtag, 1))

    val hashtagCounts = hashTagKeyValues.reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, duration)
    //val hashtagCounts = hashTagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))

    val sortedResults = hashtagCounts.transform(_.sortBy(_._2, ascending = false))

    sortedResults.foreachRDD(rdd => {
      println(f"\nPopular topics in last $duration seconds (${rdd.count()} total):")
      rdd.take(10).foreach {
        case (count, tag) => println(f"$tag ($count tweets)")
      }
    })

    // Set a checkpoint directory, and kick it all off
    streamingContext.checkpoint("C:/tmp/twitter")
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
