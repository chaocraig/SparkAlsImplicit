
name := "SparkAlsImplicitPG"


version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % Provided,
  "org.apache.spark" %% "spark-mllib" % "1.5.2" % Provided,
  "org.apache.hadoop" % "hadoop-aws" % "2.7.1"
)

assemblyMergeStrategy in assembly := {
  case p if p.startsWith("org/apache/commons") => MergeStrategy.first
  case other =>
    val s = (assemblyMergeStrategy in assembly).value
    s(other)
}