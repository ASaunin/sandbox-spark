package org.asaunin.spark.rdd.examples

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.asaunin.spark.core.SessionProvider
import org.asaunin.spark.rdd.examples.MovieRatingsCounter._

import scala.math.sqrt

object ItemBasedCollaborativeFiltering {

  private val log = Logger.getLogger("org.asaunin")
  private val spark = SessionProvider.getContext(this.getClass.getName)

  type MoviePair = (Int, Int)

  type RatingPair = (Double, Double)

  type Similarity = (Double, Int)

  def filterDuplicates(userRatingPair: UserRatingPair): Boolean = {
    val userRating = userRatingPair._2
    val movieRating1 = userRating._1
    val movieRating2 = userRating._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    movie1 < movie2
  }

  def makePairs(userRatingPair: UserRatingPair): (MoviePair, RatingPair) = {
    val userRating = userRatingPair._2
    val movieRating1 = userRating._1
    val movieRating2 = userRating._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    val rating1 = movieRating1._2
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  def computeCosineSimilarity(ratingPairs: Iterable[RatingPair]): Similarity = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    val numerator: Double = sum_xy
    val denominator: Double = sqrt(sum_xx) * sqrt(sum_yy)

    var score: Double = if (denominator != 0) numerator / denominator else 0.0

    (score, numPairs)
  }

  def main(args: Array[String]): Unit = {
    // Reading movie names data...
    val movieNames: Map[Int, String] = getMovieNames("movie_names.data")

    // Reading movie rates data...
    val movieRatings: RDD[(Int, MovieRating)] = getMovieRatings("movie_ratings.data")

    // Joining rates...
    val joinedRatings: RDD[UserRatingPair] = movieRatings.join(movieRatings)

    // Filtering unique rates...
    val uniqueJoinedRatings: RDD[UserRatingPair] = joinedRatings.filter(filterDuplicates)

    // Creating movie => rate pairs...
    val moviePairs: RDD[(MoviePair, RatingPair)] = uniqueJoinedRatings.map(makePairs)

    // Grouping ratу pairs to list...
    val moviePairRatings: RDD[(MoviePair, Iterable[RatingPair])] = moviePairs.groupByKey()

    // Calculating movie pairs similarity...
    val moviePairSimilarities: RDD[(MoviePair, Similarity)] = moviePairRatings.mapValues(computeCosineSimilarity)

    // Caching similarity results...
    moviePairSimilarities.cache()

    // Selector variables
    val movieId = if (args.length == 0) 127 else args(0).toInt //Godfather by default
    val movieName = movieNames(movieId)
    val similarCoeff = 0.97
    val totalUsersRated = 50

    log.info(f"Calculating movies similar to '$movieName' with coefficient=$similarCoeff%.2f rated by at least $totalUsersRated users")

    val filteredResults: RDD[(MoviePair, Similarity)] = moviePairSimilarities.filter(x => {
      val moviePair = x._1
      val similarity = x._2
      ((moviePair._1 == movieId || moviePair._2 == movieId)
        && similarity._1 > similarCoeff
        && similarity._2 > totalUsersRated)
    })

    log.info("Top 10 similar movies for " + movieName)

    filteredResults.map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .top(10)
      .foreach(result => {
        val similarity = result._1
        val moviePair = result._2
        var similarMovieId = if (moviePair._1 == movieId) moviePair._2 else moviePair._1
        val similarMovieName = movieNames(similarMovieId)
        log.info(f"$similarMovieName is similar with coefficient=${similarity._1}%.2f rated by ${similarity._2} users")
      })
  }

}