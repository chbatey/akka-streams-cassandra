import Dependencies._

lazy val root = (project in file("."))
  .aggregate(connector, examples)
  .settings(
    inThisBuild(List(
      organization := "info.batey",
      scalaVersion := "2.12.2",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "cassandra-connector"
  )

lazy val connector = project.in(file("connector"))

lazy val examples = project.in(file("examples"))
