ThisBuild / name := "httpds-spark"
ThisBuild / organization := "com.amadeus.spark"
ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val sparkVersion      = "3.5.0"
val sttpClientVersion = "4.0.13"
val jacksonVersion    = "2.15.3"
val slf4jVersion      = "2.0.9"
val scalaLoggingVersion = "3.9.5"
val scalatestVersion  = "3.2.17"
val wiremockVersion   = "3.13.2"

// RELEASE SETUP
import sbt.Keys.libraryDependencies

libraryDependencies ++= Seq(
  // Apache Spark (provided)
  "org.apache.spark" %% "spark-core"     % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"      % sparkVersion % Provided,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
  // HTTP Client
  "com.softwaremill.sttp.client4" %% "core" % sttpClientVersion,
  // Logging
  "org.slf4j"                   % "slf4j-api"     % slf4jVersion % Provided,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  // Test Dependencies
  "org.scalatest"     %% "scalatest"      % scalatestVersion     % Test,
  "org.scalatestplus" %% "mockito-5-8"    % s"${scalatestVersion}.0"   % Test,
  "org.apache.spark"  %% "spark-sql"      % sparkVersion % Test classifier "tests",
  "org.apache.spark"  %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.wiremock"       % "wiremock"       % wiremockVersion     % Test
)

ThisBuild / dependencyOverrides ++= Seq(
  // Required as spark relies on Jackson 2.15.3 and wiremock relies on newer versions, we need to enforce spark's version.
  "com.fasterxml.jackson.core"    % "jackson-databind"     % jacksonVersion % Test,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion % Test
)

// Compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-unused:imports"
)

// JVM options for tests (Java 17+ compatibility with Spark)
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)
