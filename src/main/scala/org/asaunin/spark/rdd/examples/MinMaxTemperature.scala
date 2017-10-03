package org.asaunin.spark.rdd.examples

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.asaunin.spark.core.SessionProvider


object MinMaxTemperature {

  private val log = Logger.getLogger("org.asaunin")
  private val path = "src/main/resources/data/temperature.csv"

  object Type extends Enumeration {
    type Type = Value
    val Max: Value = Value("TMAX")
    val Min: Value = Value("TMIN")
    val Perceptron: Value = Value("PRCP")
  }

  def getStationData(path: String): RDD[(String, Type.Type, Float)] = {
    val spark = SessionProvider.getContext(this.getClass.getName)
    val rdd = spark.textFile(path)
    val header = rdd.first()
    rdd.filter(row => row != header)
      .map { row =>
        val fields = row.split(",")
        val stationId = fields(0)
        val entityType = Type.withName(fields(2))
        val value = fields(3).toFloat * 0.1f
        (stationId, entityType, value)
      }
  }

  def main(args: Array[String]): Unit = {
    val stationsData = getStationData(path)

    val minTemperature = stationsData
      .filter(row => row._2 == Type.Min)
      .map(row => (row._1, row._3))
      .reduceByKey((x, y) => Math.min(x, y))

    val maxTemperature = stationsData
      .filter(row => row._2 == Type.Max)
      .map(row => (row._1, row._3))
      .reduceByKey((x, y) => Math.max(x, y))

    val minMaxTemperature = minTemperature.join(maxTemperature)

    minMaxTemperature.foreach(row => {
      val station = row._1
      val minTemp = row._2._1
      val maxTemp = row._2._2
      println(f"For station: $station temperature values are: min=$minTemp%.2f, max=$maxTemp%.2f (°C)")
    })
  }

}
