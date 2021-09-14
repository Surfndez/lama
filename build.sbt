import sbt.Compile
// Build shared info
ThisBuild / organization := "co.ledger"
ThisBuild / scalaVersion := "2.13.3"
ThisBuild / resolvers += Resolver.sonatypeRepo("releases")
ThisBuild / scalacOptions ++= CompilerFlags.all

// Dynver settings
ThisBuild / dynverVTagPrefix := false
ThisBuild / dynverSeparator := "-"

// Shared Plugins
enablePlugins(BuildInfoPlugin)
ThisBuild / libraryDependencies += compilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")

lazy val disableDocGeneration = Seq(
  Compile / doc / sources := Seq.empty,
  Compile / packageDoc / publishArtifact := false
)

lazy val ignoreFiles = List("application.conf.sample")

// Runtime
scalaVersion := "2.13.3"
scalacOptions ++= CompilerFlags.all
resolvers += Resolver.sonatypeRepo("releases")
addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")

lazy val buildInfoSettings = Seq(
  buildInfoKeys := Seq[BuildInfoKey](version, git.gitHeadCommit),
  buildInfoPackage := "buildinfo"
)

lazy val dockerSettings = Seq(
  dockerBaseImage := "openjdk:14.0.2",
  dockerRepository := Some("ghcr.io"),
  dockerUsername := Some("ledgerhq"),
  dockerUpdateLatest := true, //should always update latest
  dockerAliases ++= Seq(dockerAlias.value.withTag(Option("main"))),
  javaAgents += "com.datadoghq" % "dd-java-agent" % "0.78.3"
)

lazy val coverageSettings = Seq(
  coverageMinimum := 0,
  coverageFailOnMinimum := false
)

lazy val sharedSettings =
  dockerSettings ++ Defaults.itSettings ++ coverageSettings ++ disableDocGeneration

lazy val lamaScheduler = (project in file("lama-scheduler"))
  .enablePlugins(JavaAgent, JavaServerAppPackaging, DockerPlugin, BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(
    name := "lama-scheduler",
    sharedSettings,
    buildInfoSettings,
    libraryDependencies ++= (Dependencies.lamaScheduler ++ Dependencies.test)
  )
