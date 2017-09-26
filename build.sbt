name := "sandbox-spark"

organization := "com.asaunin"

version := "0.1"

scalaVersion := "2.11.8"

val versionLog4j2 = "2.9.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",

  "com.typesafe.scala-logging" %% "scala-logging_2.11" % "3.7.2",

  "org.apache.spark" %% "spark-core_2.11" % "2.2.0",

  "org.apache.spark" %% "spark-sql" % "2.2.0"
)