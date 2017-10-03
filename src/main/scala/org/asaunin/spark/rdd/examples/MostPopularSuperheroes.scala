package org.asaunin.spark.rdd.examples

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.asaunin.spark.core.SessionProvider

object MostPopularSuperheroes {

  private val log = Logger.getLogger("org.asaunin")
  private val folder = "src/main/resources/data/"
  private val spark = SessionProvider.getContext(this.getClass.getName)

  def getHeroNames(path: String): collection.Map[Int, String] = {
    val rdd = spark.textFile(path)
    rdd.map(row => {
      val fields = row.split('\"')
      val id = fields(0).trim.toInt
      val name = if (fields.length > 1) fields(1) else "No name hero"
      (id, name)
    }).collectAsMap()
  }

  def getHeroRelations(path: String): RDD[(Int, Set[Int])] = {
    val rdd = spark.textFile(path)
    rdd.map(row => {
      val fields = row.split("\\s+")
      val heroId = fields(0).toInt
      var relations: Set[Int] = Set()
      for (i <- 1 until fields.length) {
        relations += fields(i).toInt
      }
      (heroId, relations)
    })
  }

  def main(args: Array[String]) {
    val heroesMap = getHeroNames(folder + "marvel_names.txt")
    val heroNames = spark.broadcast(heroesMap)

    val heroRelations = getHeroRelations(folder + "marvel_graph.txt")

    val totalHeroRelations = heroRelations.map(hero => {
      val id = hero._1
      val relationsCount = hero._2.size
      (id, relationsCount)
    })

    val topCount = 10
    totalHeroRelations
      .reduceByKey((x, y) => x + y)
      .map(hero => {
        val value = hero._2
        val head = heroNames.value(hero._1)
        (value, head)
      })
      .sortByKey(ascending = false)
      .top(topCount)
      .foreach(hero => {
        val name = hero._2
        val relationsCount = hero._1
        println(f"'$name' is related with $relationsCount other Marvel heroes")
      })
  }

}
