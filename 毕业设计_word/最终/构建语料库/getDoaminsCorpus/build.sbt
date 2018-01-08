name := "getDomainsCorpus"

version := "1.0"

organization := "com.buaa.first.getDomainsCorpus"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % "1.6.0",
 "org.apache.spark" %% "spark-sql" % "1.6.0",
 "org.apache.spark" %% "spark-hive" % "1.6.0",
 "org.apache.spark" %% "spark-mllib" % "1.6.0",
 "org.apache.hadoop" % "hadoop-client" % "2.5.0"
)
