ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "stateproc"

version := "0.1-SNAPSHOT"

organization := "it.datasoil"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.9.1"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-state-processor-api"  % flinkVersion % "provided",
   "org.apache.flink" % "flink-contrib" % "1.9.1" pomOnly(),

"org.apache.flink" %% "flink-statebackend-rocksdb" % "1.9.1" % "provided",
  "org.apache.flink" % "flink-state-backends" % "1.9.1" % "provided",
"com.typesafe.play" %% "play-json" % "2.7.2",
 "org.mongodb" % "bson" % "3.11.0-beta3"

)
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25" % "provided"
libraryDependencies += "org.apache.flink" % "flink-s3-fs-presto" % "1.9.1"
libraryDependencies += "com.tdunning" % "t-digest" % "3.2"
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % "2.9.8"
libraryDependencies += "org.mongodb" % "bson" % "3.11.0-beta3"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.2"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )


assemblyMergeStrategy in assembly := {
  case PathList("mozilla", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assembly / mainClass := Some("it.datasoil.Job")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
