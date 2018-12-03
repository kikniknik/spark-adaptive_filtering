organization := "gr.auth.csd.datalab"
name := "spark-adaptive_filtering"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
