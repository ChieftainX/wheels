# 安装

## 添加依赖（build.sbt）
```
libraryDependencies += "com.kjzero" % "wheels" % "0.2.0-SNAPSHOT"
```

## 要求
1. spark2.2.x+，scala 2.11.x
2. 依赖如下lib(build.sbt)
```
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
```