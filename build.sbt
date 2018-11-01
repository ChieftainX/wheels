name := "wheels"

version := "0.1"

scalaVersion := "2.11.8"

lazy val spark_version = "2.2.1"

resolvers ++= Seq(
  "aliyun" at "http://maven.aliyun.com/nexus/content/groups/public",
  "sonatype" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_version % Provided,
  "org.apache.spark" %% "spark-sql" % spark_version % Provided,
  "org.apache.spark" %% "spark-mllib" % spark_version % Provided
)

libraryDependencies += "redis.clients" % "jedis" % "2.9.0"

libraryDependencies ++= Seq(
  "org.junit.platform" % "junit-platform-launcher" % "1.3.1" % Test,
  "org.junit.jupiter" % "junit-jupiter-engine" % "5.3.1" % Test,
  "org.junit.vintage" % "junit-vintage-engine" % "5.3.1" % Test
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)