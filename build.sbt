
name := "spark-json-streaming"

version := "0.1"

scalaVersion := "2.11.8"

mainClass in Compile := Some("Processor")

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % "test",
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1",
  "org.apache.hadoop" % "hadoop-aws" % "2.9.0"

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}