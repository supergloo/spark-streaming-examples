name := "spark-structured-streaming-simple-kafka"
organization := "com.supergloo"
version := "0.2"
scalaVersion := "2.12.10"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case PathList(pl @ _*) if pl.contains("log4j.properties") => MergeStrategy.concat
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
)

// still want to be able to run in sbt
// https://github.com/sbt/sbt-assembly#-provided-configuration
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")

val sparkVersion = "2.4.5"
val confluentVersion = "5.4.1"

resolvers += "Confluent Platform" at "http://packages.confluent.io/maven/"

//  https://stackoverflow.com/questions/51515010/incompatible-jackson-version-spark-structured-streaming
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "io.confluent" % "kafka-schema-registry-client" % confluentVersion,
  "io.confluent" % "kafka-avro-serializer" % confluentVersion


)
