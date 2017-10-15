name := "sandbox-spark"

organization := "com.asaunin"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

mainClass in assembly := Some("org.asaunin.spark.rdd.examples.ItemBasedCollaborativeFiltering")

assemblyJarName in assembly := "movies-similarity.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
