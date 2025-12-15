ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.21"

lazy val root = (project in file("."))
  .settings(
    name := "formation_spark"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.7"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.7"