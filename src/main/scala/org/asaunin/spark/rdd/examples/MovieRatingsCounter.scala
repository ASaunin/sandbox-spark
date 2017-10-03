package org.asaunin.spark.rdd.examples

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.asaunin.spark.core.SessionProvider

import scala.io.{Codec, Source}

object MovieRatingsCounter {

  private val log = Logger.getLogger("org.asaunin")
  private val folder = "src/main/resources/data/"
  private val spark = SessionProvider.getContext(this.getClass.getName)

  def getMovieRatings(path: String): RDD[(Int, Int, Int)] = {
    val rdd = spark.textFile(path)
    val header = rdd.first()

    rdd.filter(row => row != header)
      .map(row => {
        val fields = row.split("\t")
        (fields(0).toInt, fields(1).toInt, fields(2).toInt)
      })
  }

  def getMovieNames(path: String): Map[Int, String] = {
    val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movies: Map[Int, String] = Map()
    val lines = Source.fromFile(path)(codec).getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        val movieId = fields(0).toInt
        val movieName = fields(1)
        movies += (movieId -> movieName)
      }
    }
    movies
  }

  def main(args: Array[String]) {
    val movieNames = getMovieNames(folder + "movies.item")
    val movies = spark.broadcast(movieNames)

    val rows = getMovieRatings(folder + "movie_ratings.data")

    println("Movies count by rating:")
    val ratings = rows.map(row => row._3)

    val ratingCounts = ratings.countByValue().toSeq.sortBy(_._1)

    ratingCounts.foreach(ratingCount => {
      val rating = ratingCount._1
      val count = ratingCount._2
      println(f"Rating: $rating has $count movies")
    })

    val topCount = 25
    println(s"\nTop $topCount most popular movies:")
    val movieAverageRatings = rows.map(row => (row._2, (row._3, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(tuple => (tuple._2._1.toFloat / tuple._2._2.toFloat, tuple._1))

    movieAverageRatings
      .sortByKey(ascending = false)
      .top(topCount)
      .foreach(movieRating => {
        val rating = movieRating._1
        val movieId = movieRating._2
        val movieName = movies.value(movieId)
        println(f"Movie '$movieName' has rating: $rating%.2f")
      })
  }

}
