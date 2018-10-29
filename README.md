# Wheels
## 简介
Wheels主要对大数据主流框架及常用算法库进行统一封装，优化及实现，对外提供简洁且高效的API。从而达到降低从事大数据场景下算法开发人员的编程技术门槛及提高整体项目质量的目的。
## 10秒入门
### 创建sql对象
```
import com.zhjy.wheel.spark._

val sql = Core().support_sql
```
### 使用sql进行数据处理
```
sql ==> (
      """
        select
        country,count(1) country_count
        from emp
        group by country
      """, "tmp_country_agg")

    sql ==> (
      """
        select
        org_id,count(1) org_count
        from emp
        group by org_id
      """, "tmp_org_agg")

    sql ==> (
      """
        select
        e.*,c.country_count,o.org_count
        from emp e,tmp_country_agg c,tmp_org_agg o
        where
        e.country = c.country and
        o.org_id = e.org_id and
        e.height > 156
      """, "emp_res")
```
### 保存结果数据
```
sql <== "emp"
```
## 更多内容
+ [安装](/wikis/install)
+ [API DOC](/wikis/api-doc)
+ [配置](/wikis/conf)