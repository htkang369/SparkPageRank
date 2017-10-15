name := "pagerankspark"

version := "1.0"

scalaVersion := "2.10.5"
        
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "org.apache.spark" %% "spark-graphx" % "1.6.0"
)