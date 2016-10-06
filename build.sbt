name := "dyna2"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.1"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.1"