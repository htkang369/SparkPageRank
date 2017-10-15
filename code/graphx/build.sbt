name := "pagerankgraphx"

version := "1.0"

scalaVersion := "2.10.5"

exportJars := true

mainClass in(Compile, run) := Some("pagerankgraphx")
mainClass in(Compile, packageBin) := Some("pagerankgraphx")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "org.apache.spark" %% "spark-graphx" % "1.6.0"
)


