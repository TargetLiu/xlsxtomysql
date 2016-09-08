# Excel(xlsx)导入MySQL数据库

## 简介

这是工作中用到的一个小工具，将Excel(xlsx)表导入MySQL表中，用Golang写的，每条记录单独一条 `goroutine` 处理，提高效率。支持随机数生成、密码生成、时间戳；支持关联查询、附表操作等。

## 使用方法

1. 使用Go编译安装或直接下载：[https://github.com/TargetLiu/xlsxtomysql/releases](https://github.com/TargetLiu/xlsxtomysql/releases)

2. 使用命令： `xlsxtomysql [DSN] [数据表名称] [*.xlsx]`

## DSN

格式：

```
[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
```

示例：

```
root:123@tcp(127.0.0.1:3306)/dbname
```

注意：
Linux、Mac下可能需要输入 `\( \)`

## Excel表导入结构说明

1. 只支持单Sheet

2. 第一行对应数据库表字段

 * 通过 `|` 分割

 * `字段名|unique` 判断重复，重复的自动跳过

 * `字段名|password|[md5|bcrypt]` 密码生成，第二个参数[md5|bcrypt]

 * `字段名|find|表名|需要获取的字段|查询字段` 根据内容从其它表查询并获取字段，格式 `SELECT 需要获取的字段 FROM 表名 WHERE 查询字段 = 内容`

 * `:other` 附表操作

3. 内容行

 * `:random` 生成随机字符串

 * `:time ` 当前unix时间戳

 * `:null`  空值，自动跳过该值，一般可用于自增id

 * 如果该列为 `password` ，内容为[明文密码]或[明文密码|盐]，盐可以通过[:字段名]获取之前的字段名。密码将自动根据字段名中填写的加密方式进行加密

 *  如果该列为 `other` , 格式[表名|字段1|字段2|字段3....] 其它表需要按顺序为每个字段添加内容。字段可以为[:null|:id(主表生成的自增ID)|:random|:time]

## Excel截图

![Excel截图](https://github.com/TargetLiu/xlsxtomysql/raw/master/screenshot.jpg)

## 关于作者

我的博客：[http://targetliu.com/](http://targetliu.com/)

GitHub：[https://github.com/TargetLiu](https://github.com/TargetLiu)