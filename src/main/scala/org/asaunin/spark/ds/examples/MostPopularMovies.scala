package org.asaunin.spark.ds.examples

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.desc
import org.asaunin.spark.core.SessionProvider
import org.asaunin.spark.ds.examples.BasicDataSetExample.{Person, spark}
import org.asaunin.spark.rdd.examples.MovieRatingsCounter.{getMovieNames, spark}

import scala.io.{Codec, Source}

object MostPopularMovies {

  private val log = Logger.getLogger("org.asaunin")
  private val spark = SessionProvider.getSession(this.getClass.getName)

  final case class Movie(id: Int)

  def getMovieRatings(fileName: String): Dataset[Movie] = {
    import spark.implicits._
    val ds = spark.read.textFile("data/" + fileName)
    val header = ds.first()

    ds.filter(!_.equals(header))
      .map(_.split("\t")(1).toInt)
      .map(Movie)
  }

  def main(args: Array[String]) {
    val movies = getMovieRatings("movie_ratings.data")
      .groupBy("id")
      .count()
      .orderBy(desc("count"))
      .cache()

    movies.show()

    val movieNames = getMovieNames("movie_names.data")

    val topCount = 25
    val topMovies = movies.take(topCount)

    for (movie <- topMovies) {
      val movieName = movieNames(movie(0).asInstanceOf[Int]) + ": "
      val count = movie(1)
      log.info(f"$movieName was rated $count times")
    }

    spark.stop()
  }

}
