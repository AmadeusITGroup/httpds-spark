name := "httpds-spark"
ThisBuild / organization := "com.amadeus.spark"
ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"

val sparkVersion        = "3.5.0"
val sttpClientVersion   = "4.0.13"
val jacksonVersion      = "2.15.3"
val slf4jVersion        = "2.0.9"
val scalaLoggingVersion = "3.9.5"
val scalatestVersion    = "3.2.17"
val wiremockVersion     = "3.13.2"

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
  "org.scalatest"     %% "scalatest"      % scalatestVersion         % Test,
  "org.scalatestplus" %% "mockito-5-8"    % s"$scalatestVersion.0" % Test,
  "org.apache.spark"  %% "spark-sql"      % sparkVersion             % Test classifier "tests",
  "org.apache.spark"  %% "spark-catalyst" % sparkVersion             % Test classifier "tests",
  "org.wiremock"       % "wiremock"       % wiremockVersion          % Test
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
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:params",
  "-Ywarn-unused:privates",
  "-Ywarn-value-discard",
  "-Xfatal-warnings"
)

// Scaladoc emits non-fatal "could not find any member to link" warnings for some
// [[...]] references; under -Xfatal-warnings these would abort `doc` (and thus
// `publish`). Keep fatal warnings for compilation, but not for doc generation.
Compile / doc / scalacOptions -= "-Xfatal-warnings"

// JVM options for tests (Java 17+ compatibility with Spark)
Test / fork := true

// Command aliases for linting
addCommandAlias("lint", "scalafmtCheck; scalafixAll --check")
addCommandAlias("lintFix", "scalafmtAll; scalafixAll")
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

// Coverage settings
coverageMinimumStmtTotal := 90
coverageFailOnMinimum := true

// RELEASE SETUP
import sbtrelease.ReleaseStateTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

releaseCommitMessage := s"chore(release): set version to ${(ThisBuild / version).value} [skip ci]"
releaseNextCommitMessage := s"chore(release): bump version to ${(ThisBuild / version).value} [skip ci]"

// Publishing is environment-driven so the same build serves the public
// GitHub Packages release and any internal mirror, without hardcoding internal
// infrastructure here. To target a different repository at publish time, export:
//   PUBLISH_REPO_URL   - target Maven repo URL (e.g. a snapshot/release repo)
//   PUBLISH_REPO_NAME  - optional display name for that repo
//   PUBLISH_REALM / PUBLISH_HOST / PUBLISH_USER / PUBLISH_PASSWORD - credentials
// When unset, publishing defaults to GitHub Packages.
ThisBuild / credentials ++= {
  val envCreds = for {
    realm <- sys.env.get("PUBLISH_REALM")
    host  <- sys.env.get("PUBLISH_HOST")
    user  <- sys.env.get("PUBLISH_USER")
    pass  <- sys.env.get("PUBLISH_PASSWORD")
  } yield Credentials(realm, host, user, pass)

  envCreds match {
    case Some(c) => Seq(c)
    case None    => Seq(Credentials("GitHub Package Registry", "maven.pkg.github.com", "", sys.env.getOrElse("GITHUB_REGISTRY_TOKEN", "")))
  }
}

// PUBLISH SETUP (Maven style)
ThisBuild / publishTo := {
  sys.env.get("PUBLISH_REPO_URL") match {
    case Some(url) => Some(sys.env.getOrElse("PUBLISH_REPO_NAME", "Internal Repository") at url)
    case None      => Some("GitHub Packages" at "https://maven.pkg.github.com/AmadeusITGroup/httpds-spark")
  }
}

ThisBuild / publishMavenStyle := true
// Additional Maven metadata
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / pomExtra :=
  <url>https://github.com/AmadeusITGroup/httpds-spark</url>
    <licenses>
      <license>
        <name>Apache License 2.0</name>
        <url>https://github.com/AmadeusITGroup/httpds-spark/blob/main/LICENSE</url>
      </license>
    </licenses>

ThisBuild / developers := List(
  Developer(
    id = "AmadeusITGroup",
    name = "Amadeus IT Group",
    email = "",
    url = url("https://github.com/AmadeusITGroup")
  )
)

ThisBuild / Test / publishArtifact := false
