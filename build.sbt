name := "sandbox-spark"

organization := "com.asaunin"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"
val typesafeVersion = "1.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "com.typesafe" % "config" % typesafeVersion
)

mainClass in assembly := Some("org.asaunin.spark.rdd.examples.ItemBasedCollaborativeFiltering")

assemblyJarName in assembly := "movies-similarity.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
