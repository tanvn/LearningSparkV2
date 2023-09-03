//name of the package
name := "main/scala/chapter2"
//version of our package
version := "1.0"
//version of Scala
scalaVersion := "2.12.18"
// spark library dependencies 

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.4",
  "org.apache.spark" %% "spark-sql"  % "3.2.4"
)
