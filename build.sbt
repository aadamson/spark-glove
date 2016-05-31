import AssemblyKeys._

name := "spark-glove"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies  ++= Seq(
  "commons-cli" % "commons-cli" % "1.2",
  "com.databricks" % "spark-csv_2.11" % "1.4.0",
  // Spark libs
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"
  // Breeze libs
  // "org.scalanlp" %% "breeze" % "0.12",
  // "org.scalanlp" %% "breeze-natives" % "0.12",
  // "org.scalanlp" %% "breeze-viz" % "0.12"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

assemblySettings