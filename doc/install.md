# 安装
## 要求
1. spark2.2.x+，scala 2.11.x
2. 依赖如下lib
```
spark-core
spark-sql
spark-mllib
```
## 步骤
1. 将项目使用sbt package打包
2. 在自己项目根目录创建lib文件夹
3. 把打好的包放的lib文件夹并且使用开发工具将此包添加至工程class path