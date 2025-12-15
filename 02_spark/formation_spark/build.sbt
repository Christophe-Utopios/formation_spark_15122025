ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.21"

lazy val root = (project in file("."))
  .settings(
    name := "formation_spark"
  )
