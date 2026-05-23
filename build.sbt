name := "retail-platform"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.scalatest"    %% "scalatest" % "3.2.17" % Test,
  "io.delta" %% "delta-spark" % "3.1.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
)

fork := true

javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "-Dhadoop.home.dir=C:\\hadoop"   
)