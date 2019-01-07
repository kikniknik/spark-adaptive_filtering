organization := "gr.auth.csd.datalab"
name := "spark-adaptive_filtering"

version := "0.4-SNAPSHOT"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
