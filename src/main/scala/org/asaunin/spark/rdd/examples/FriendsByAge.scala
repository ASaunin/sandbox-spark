package org.asaunin.spark.rdd.examples

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.asaunin.spark.core.SessionProvider

object FriendsByAge {

  private val log = Logger.getLogger("org.asaunin")
  private val path = "src/main/resources/data/friends_by_age.csv"

  def getFriendsByAge(path: String): RDD[(Int, Int)] = {
    val spark = SessionProvider.getContext(this.getClass.getName)
    val rdd = spark.textFile(path)
    val header = rdd.first()

    rdd.filter(row => row != header)
      .map { row =>
        val fields = row.split(",")
        val age = fields(2).toInt
        val friends = fields(3).toInt
        (age, friends)
      }
  }

  def main(args: Array[String]): Unit = {
    val rdd = getFriendsByAge(path)

    val totalsByFriends = rdd
      .mapValues(row => (row, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val averageByAge = totalsByFriends
      .mapValues(x => x._1 / x._2)
      .collect()

    averageByAge
      .sorted
      .foreach(tuple => {
        val age = tuple._1
        val friendsCount = tuple._2
        println(f"For age: $age average friends: $friendsCount")
      })
  }

}
