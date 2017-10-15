name := "topuniversities"

version := "1.0"

scalaVersion := "2.10.5"

exportJars := true

mainClass in(Compile, run) := Some("pageuni")
mainClass in(Compile, packageBin) := Some("pageuni")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "org.apache.spark" %% "spark-graphx" % "1.6.0"
)


