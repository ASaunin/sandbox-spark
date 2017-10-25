package org.asaunin.spark.ds.examples

import org.apache.spark.sql.{Dataset, Encoder}
import org.asaunin.spark.core.SessionProvider

object BasicDataSetExample {

  private val spark = SessionProvider.getSession(this.getClass.getName)

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def getFriendsByAge(fileName: String): Dataset[Person] = {
    import spark.implicits._

    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv("data/" + fileName)
      .as[Person]
  }

  def main(args: Array[String]): Unit = {
    val people = getFriendsByAge("friends_by_age.csv").cache()

    println("Inferred schema:")
    people.printSchema()

    println("Top 20 ordered names:")
    people.select("name").distinct().orderBy("name").show()

    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).orderBy("age", "name").show()

    println("Group by age:")
    people.groupBy("age").count().orderBy("age").show()

    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()

    spark.stop()
  }

}
