name := "wheels"

isSnapshot := false

version := "0.2.0" + {
  if (isSnapshot.value) "-SNAPSHOT"
  else ""
}

organization := "com.kjzero"
organizationName := "kjzero"
organizationHomepage := Some(url("https://github.com/ChieftainX/wheels"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/ChieftainX/wheels"),
    "scm:git@github.com:ChieftainX/wheels.git"
  )
)
developers := List(
  Developer(
    id = "com.kjzero",
    name = "ChieftainX",
    email = "chieftains@yeah.net",
    url = url("https://github.com/ChieftainX")
  )
)
licenses := Seq("BSD-style" -> url("http://www.opensource.org/licenses/bsd-license.php"))
homepage := Some(url("https://github.com/ChieftainX/wheels"))
pomIncludeRepository := { _ => false }
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
publishMavenStyle := true
publishArtifact in Test := false
publishArtifact in Provided := false

useGpg := true

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")

crossPaths := false

//publishConfiguration := publishConfiguration.value.withOverwrite(true)
//publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

scalaVersion := "2.11.8"

lazy val spark_version = "2.2.1"
lazy val hbase_version = "1.0.0"
lazy val jedis_version = "2.9.0"

resolvers ++= Seq(
  "aliyun" at "http://maven.aliyun.com/nexus/content/groups/public",
  "sonatype" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_version % Provided,
  "org.apache.spark" %% "spark-sql" % spark_version % Provided,
  "org.apache.spark" %% "spark-mllib" % spark_version % Provided,
  "org.apache.spark" %% "spark-streaming" % spark_version % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % spark_version % Provided

)

libraryDependencies ++= Seq(
  "redis.clients" % "jedis" % jedis_version % Provided,
  //调试低版本kafka兼容会用到
  //"org.apache.kafka" % "kafka-clients" % "0.8.2.0" % Provided,
  "org.apache.hbase" % "hbase-server" % hbase_version % Provided,
  "org.apache.hbase" % "hbase-common" % hbase_version % Provided,
  "org.apache.hbase" % "hbase-hadoop-compat" % hbase_version % Provided
)

libraryDependencies ++= Seq(
  "org.junit.platform" % "junit-platform-launcher" % "1.3.1" % Test,
  "org.junit.jupiter" % "junit-jupiter-engine" % "5.3.1" % Test,
  "org.junit.vintage" % "junit-vintage-engine" % "5.3.1" % Test
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

//解决spark-sql-kafka冲突
assemblyMergeStrategy in assembly := {
  case PathList(ps@_*) if ps.last endsWith "UnusedStubClass.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

//publishSigned
//hkp://keyserver.ubuntu.com
// Mac necessary
//brew install pinentry-mac
//echo "pinentry-program /usr/local/bin/pinentry-mac" >> ~/.gnupg/gpg-agent.conf
//killall gpg-agent