package org.asaunin.spark.rdd.examples

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.asaunin.spark.core.SessionProvider

import scala.io.{Codec, Source}

object MovieRatingsCounter {

  private val log = Logger.getLogger("org.asaunin")
  private val spark = SessionProvider.getContext(this.getClass.getName)

  type MovieRating = (Int, Double)

  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def getMovieRatings(fileName: String): RDD[(Int, MovieRating)] = {
    val rdd = spark.textFile("data/" + fileName)
    val header = rdd.first()

    rdd.filter(row => row != header)
      .map(row => {
        val fields = row.split("\t")
        val userId = fields(0).toInt
        val movieId = fields(1).toInt
        val rating = fields(2).toDouble
        (userId, (movieId, rating))
      })
  }

  def getMovieNames(fileName: String): Map[Int, String] = {
    val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movies: Map[Int, String] = Map()
    val lines = Source.fromFile("data/" + fileName)(codec).getLines()
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
    val movieNames = getMovieNames("movie_names.data")
    val movies = spark.broadcast(movieNames)

    val rows = getMovieRatings("movie_ratings.data")

    log.info("Movies count by rating:")
    val ratings = rows.map(_._2._2)

    val ratingCounts = ratings.countByValue().toSeq.sortBy(_._1)

    ratingCounts.foreach(ratingCount => {
      val rating = ratingCount._1
      val count = ratingCount._2
      log.info(f"Rating: $rating has $count movies")
    })

    val topCount = 25
    log.info(s"\nTop $topCount most popular movies:")
    val movieAverageRatings = rows.map(row => (row._2._1, (row._2._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(tuple => (tuple._2._1 / tuple._2._2, tuple._1))

    movieAverageRatings
      .sortByKey(ascending = false)
      .top(topCount)
      .foreach(movieRating => {
        val rating = movieRating._1
        val movieId = movieRating._2
        val movieName = movies.value(movieId)
        log.info(f"Movie '$movieName' has rating: $rating%.2f")
      })
  }

}
