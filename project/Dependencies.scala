import sbt._

object Dependencies {
  lazy val akkaStreams = "com.typesafe.akka" %% "akka-stream" % "2.5.3"
  lazy val cassandraDriver = "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0"

  lazy val akka = Seq(akkaStreams)

  lazy val alpakkaCassandra = "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % "0.11"

  lazy val akkaStreamsTest = "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"

  lazy val akkaTest = Seq(scalaTest, akkaStreamsTest).map(_ % "it,test")
}


