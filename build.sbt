import Dependencies._

lazy val root = (project in file("."))
  .aggregate(connector, examples, benchmarks)
  .settings(
    inThisBuild(List(
      organization := "info.batey",
      scalaVersion := "2.12.3",
      version := "0.1.0-SNAPSHOT"
    )),
    name := "akka-streams-cassandra"
  )

lazy val connector = project.in(file("connector"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    libraryDependencies ++= akka,
    libraryDependencies ++= akkaTest,
    libraryDependencies += cassandraDriver
  )

lazy val examples = project.in(file("examples"))
  .settings(
    libraryDependencies += scalaTest,
    libraryDependencies += alpakkaCassandra
  )
  .dependsOn(connector)

lazy val benchmarks = project.in(file("benchmarks"))
  .settings()
  .dependsOn(connector)
  .enablePlugins(JmhPlugin)
