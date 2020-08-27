
scalaVersion := "2.12.12"
libraryDependencies += ("org.apache.spark" %% "spark-core" % "2.4.6")
libraryDependencies += "joda-time" % "joda-time" % "2.10.6"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.0" % "test"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.1"
libraryDependencies ++= Seq(
    "com.typesafe" % "config" % "1.4.0"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3"


assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}