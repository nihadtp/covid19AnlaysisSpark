libraryDependencies += ("org.apache.spark" %% "spark-core" % "2.4.6")
libraryDependencies ++= Seq(
 "com.typesafe.akka" %% "akka-http"   % "10.1.12", 
"com.typesafe.akka" %% "akka-stream" % "2.5.26" )
libraryDependencies += "joda-time" % "joda-time" % "2.10.6"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.0" % "test"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.1"


