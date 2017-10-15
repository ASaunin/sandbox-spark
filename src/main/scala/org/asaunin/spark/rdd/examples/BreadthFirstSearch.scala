package org.asaunin.spark.rdd.examples

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.asaunin.spark.core.SessionProvider
import org.asaunin.spark.rdd.examples.BreadthFirstSearch.Color.Color

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object BreadthFirstSearch {

  private val log = Logger.getLogger("org.asaunin")
  private val spark = SessionProvider.getContext(this.getClass.getName)

  object Color extends Enumeration {
    type Color = Value
    val WHITE, GRAY, BLACK = Value
  }

  private val defaultColor = Color.WHITE
  private val defaultDistance = Int.MaxValue

  private val fromHeroId = 5249 //SNOW
  private val toHeroId = 14 //ADAM

  private var counter: Option[LongAccumulator] = None

  private type BfsData = (Array[Int], Int, Color)
  private type BfsNode = (Int, BfsData)

  def getHeroRelationsGraph(fileName: String): RDD[BfsNode] = {
    val rdd = spark.textFile("data/" + fileName)
    rdd.map(row => {
      val fields = row.split("\\s+")
      val heroId = fields(0).toInt

      var connections: ArrayBuffer[Int] = ArrayBuffer()
      for (connection <- 1 until fields.length) {
        connections += fields(connection).toInt
      }

      var color = defaultColor
      var distance = defaultDistance
      if (heroId == fromHeroId) {
        color = Color.GRAY
        distance = 0
      }

      (heroId, (connections.toArray, distance, color))
    })
  }

  def getHeroNames(fileName: String): RDD[(Int, String)] = {
    val rdd = spark.textFile("data/" + fileName)
    rdd.map(row => {
      val fields = row.split('\"')
      val id = fields(0).trim.toInt
      val name = if (fields.length > 1) fields(1) else "No name hero"
      (id, name)
    })
  }

  def mapBfs(node: BfsNode): Array[BfsNode] = {
    val characterID: Int = node._1
    val data: BfsData = node._2
    val connections: Array[Int] = data._1
    val distance: Int = data._2
    var color: Color = data._3

    var results: ArrayBuffer[BfsNode] = ArrayBuffer()
    if (color == Color.GRAY) {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = Color.GRAY

        if (toHeroId == connection) {
          if (counter.isDefined) {
            counter.get.add(1)
          }
        }

        val newEntry: BfsNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }

      color = Color.BLACK
    }

    results += node
    results.toArray
  }

  def reduceBfs(data1: BfsData, data2: BfsData): BfsData = {
    val edges1: Array[Int] = data1._1
    val edges2: Array[Int] = data2._1
    val distance1: Int = data1._2
    val distance2: Int = data2._2
    val color1: Color = data1._3
    val color2: Color = data2._3

    var color: Color = defaultColor
    var distance: Int = defaultDistance
    var edges: ArrayBuffer[Int] = ArrayBuffer()

    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }

    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }

    if (color1 == Color.WHITE && (color2 == Color.GRAY || color2 == Color.BLACK)) {
      color = color2
    }
    if (color1 == Color.GRAY && color2 == Color.BLACK) {
      color = color2
    }
    if (color2 == Color.WHITE && (color1 == Color.GRAY || color1 == Color.BLACK)) {
      color = color1
    }
    if (color2 == Color.GRAY && color1 == Color.BLACK) {
      color = color1
    }

    (edges.toArray, distance, color)
  }

  def main(args: Array[String]) {
    var namesById = getHeroNames("marvel_names.txt")
    val fromHeroName = namesById.lookup(fromHeroId).head
    val toHeroName = namesById.lookup(toHeroId).head

    var bfsNodes = getHeroRelationsGraph("marvel_graph.txt")

    counter = Some(spark.longAccumulator("Counter"))

    val maxIterations: Int = 6
    breakable {
      for (i <- 1 to maxIterations) {
        log.info(f"$i iteration is started...")

        val mapped = bfsNodes.flatMap(mapBfs)

        log.info("Succeed to process " + mapped.count() + " values.")

        if (counter.isDefined) {
          val count = counter.get.value
          if (count > 0) {
            log.info(f"Heroes '$fromHeroName' & '$toHeroName' are separated from each other by $i degrees from $count different direction(s)")
            break
          }
        }

        bfsNodes = mapped.reduceByKey(reduceBfs)
      }
    }

    if (counter.isEmpty || counter.get.isZero) {
      log.warn(f"Heroes $fromHeroName & $toHeroName are separated too much, that none relations were found after $maxIterations iterations")
    }
  }

}
