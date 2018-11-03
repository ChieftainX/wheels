# 使用手册

- ***[简介](#introduction)***
  - [编程模式](#programming-model)
  - [常用符号](#symbols)
- ***[基础模块](#base-model)***
  - [开启方式](#base-model-open)
  - [视图与dataframe](#view-df)
    - [创建视图](#view-create)
    - [创建dataframe](#df-create)
    - [互相转换](#dv-conversion)
  - [数据处理](#data-processing)
  - [数据存储](#data-save)
    - [全量表](#data-save-f)
    - [分区表](#data-save-p)
- ***[Database 模块](#db-model)***
  - [开启方式](#db-model-open)
  - [hbase](#hbase)
  - [redis](#redis)
- ***[ML 模块](#ml-model)***
  - [开启方式](#ml-model-open)
  - [联合加权](#union-weighing)
  
## <a name='introduction'>简介</a>

Wheels主要对大数据主流框架及常用算法库进行统一封装，优化及实现，对外提供简洁且高效的API。<br>
从而达到降低从事大数据场景下开发人员的编程技术门槛及提高整体项目质量的目的。

### <a name='programming-model'>编程模式</a>

使用Wheels编程，每行程序主要有三个区域组成：
<br>
【对象区域】 【符号区域】 【参数区域】