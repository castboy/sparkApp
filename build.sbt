name := "simple-app"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.1"
