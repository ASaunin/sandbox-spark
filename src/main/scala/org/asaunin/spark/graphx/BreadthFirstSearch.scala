package org.asaunin.spark.graphx

import org.apache.log4j._
import org.apache.spark.graphx.{EdgeTriplet, PartitionID, _}
import org.apache.spark.rdd.RDD
import org.asaunin.spark.core.SessionProvider

import scala.collection.mutable.ArrayBuffer

object BreadthFirstSearch {

  private val log = Logger.getLogger("org.asaunin")
  private val spark = SessionProvider.getContext(this.getClass.getName)

  private val fromHeroId: VertexId = 5249 //SNOW
  private val toHeroId: VertexId = 14 //ADAM

  def getHeroNames(fileName: String): RDD[(VertexId, String)] = {
    val rdd = spark.textFile("data/" + fileName)
    rdd.map(row => {
      val fields = row.split('\"')
      val id: VertexId = fields(0).trim.toInt
      val name = if (fields.length > 1) fields(1) else "No name hero"
      (id, name)
    }).filter(tuple => tuple._1 < 6487) // ID's above 6486 aren't real characters
  }

  def getHeroRelations(fileName: String): RDD[Edge[Int]] = {
    val rdd = spark.textFile("data/" + fileName)

    val value = rdd.map(row => {
      val fields = row.split("\\s+")
      val heroId = fields(0).toInt

      var relations: ArrayBuffer[Edge[PartitionID]] = ArrayBuffer()
      for (relation <- 1 until fields.length) {
        relations += Edge(heroId, fields(relation).toInt, 0)
      }

      relations
    })

    value.flatMap(x => x)
  }

  def sendMsg(triplet: EdgeTriplet[Double, PartitionID]): Iterator[(VertexId, Double)] = {
    if (triplet.srcAttr != Double.PositiveInfinity) {
      Iterator((triplet.dstId, triplet.srcAttr + 1))
    } else {
      Iterator.empty
    }
  }

  def main(args: Array[String]) {
    val vertexes = getHeroNames("marvel_names.txt")

    val fromHeroName = vertexes.lookup(fromHeroId).head
    val toHeroName = vertexes.lookup(toHeroId).head

    val edges = getHeroRelations("marvel_graph.txt")

    val graph = Graph(vertexes, edges, "No name hero").cache()

    val topCount1 = 10
    log.info(f"\nTop $topCount1 most-related superheroes:")
    graph.degrees
      .join(vertexes)
      .sortBy(_._2._1, ascending = false)
      .take(topCount1)
      .foreach(tuple => {
        val name = tuple._2._2
        val relationsCount = tuple._2._1
        log.info(f"'$name' is related with $relationsCount other Marvel heroes")
      })

    val initialGraph = graph
      .mapVertices((id, _) => if (id == fromHeroId) 0.0 else Double.PositiveInfinity)

    val maxIterations: PartitionID = 10
    val bfs = initialGraph
      .pregel(Double.PositiveInfinity, maxIterations)(
        (_, attr, msg) => math.min(attr, msg),
        triplet => sendMsg(triplet),
        (a, b) => math.min(a, b))
      .cache()

    val topCount2 = 15
    log.info(f"\nTop $topCount2 superheroes most-related with '$fromHeroName'")
    bfs.vertices
      .join(vertexes)
      .filter(_._2._1 != 0.0)
      .filter(_._2._1 != Double.PositiveInfinity)
      .sortBy(_._2._1, ascending = true)
      .take(topCount2)
      .foreach(tuple => {
        val name = tuple._2._2
        val degree = tuple._2._1
        log.info(f"'$name' is separated from '$fromHeroName' by $degree degrees")
      })

    bfs.vertices
      .filter(_._1 == toHeroId)
      .collect
      .foreach(tuple => {
        val degree = tuple._2
        log.info(f"\nHeroes '$fromHeroName' & '$toHeroName' are separated from each other by $degree degrees")
      })
  }

}
