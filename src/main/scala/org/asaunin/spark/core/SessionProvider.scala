package org.asaunin.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SessionProvider {

  def getContext(appName: String): SparkContext = {
    getSession(appName).sparkContext
  }

  def getSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

}
