package org.asaunin.spark.df.examples

import org.apache.spark.sql.DataFrame
import org.asaunin.spark.core.SessionProvider

object BasicSparkSqlExample {

  private val spark = SessionProvider.getSession(this.getClass.getName)

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def getFriendsByAge(fileName: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv("data/" + fileName)
  }

  def main(args: Array[String]): Unit = {
    val df = getFriendsByAge("friends_by_age.csv")

    println("Inferred schema:")
    df.printSchema()

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val teenagers = spark.sql(
      "SELECT people.age, people.name, people.friends " +
        "FROM people " +
        "WHERE age >= 13 AND age <= 19 " +
        "ORDER BY age, name")

    println("Filtered teenagers:")
    teenagers.show()

    spark.stop()
  }

}
