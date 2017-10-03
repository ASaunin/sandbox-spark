package org.asaunin.spark.rdd.examples

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.asaunin.spark.core.SessionProvider

object WordCounter {

  private val log = Logger.getLogger("org.asaunin")
  private val path = "src/main/resources/data/book.txt"

  def getAllWords(path: String): RDD[String] = {
    val spark = SessionProvider.getContext(this.getClass.getName)
    val rdd = spark.textFile(path)
    rdd.flatMap(row => row.split("\\W+"))
      .map(word => word.toLowerCase)

  }

  def main(args: Array[String]) {
    val rows = getAllWords(path)

    val wordCounts = rows
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)

    val topCount = 10
    val mostPopularWords = wordCounts.top(topCount)(Ordering[Int].on(_._2))

    mostPopularWords
        .foreach(wordCount => {
          val word = wordCount._1
          val count = wordCount._2
          println(f"Word '$word' repeats $count times")
        })
  }

}
