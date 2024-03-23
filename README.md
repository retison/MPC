****# FedSQL 的 SQL 隐私集合求交集模块 （MPC）

## 调试运行配置

对于 MPC 模块而言，成功运行该模块需要2个节点，每个节点均持有一部分数据，通过 MPC 操作求得双方数据ID的交集部分。

总体而言，分以下几步：

### 数据库配置

SQL Parser 的运行需要通过配合 SQL 数据库执行（记录任务信息等），请按照 `config.py` 中的参数（可根据实际需求修改），配置 MySQL数据库。

为了便捷考虑，还可以使用 docker 运行 MySQL （性能稍差，但是可以完成快速部署），用户名密码等字段可根据实际需求修改：

```
docker pull mysql:5.7;

docker run -itd --name mysql-test -p 13306:3306 -e MYSQL_ROOT_PASSWORD=123456 mysql:5.7

docker exec -it mysql /bin/bash

mysql -uroot -p123456
```

### 安装相应依赖包

请使用 `python 3.8` 版本运行本组件，可以使用 `conda create -n env_name python=3.8` 创建单独的虚拟环境。

使用 `pip install -r requirements.txt` 完成依赖包的安装；

### 运行组件

使用 `python app.py` 开启组件。

第一次运行成功之后，本组件会自动连接数据库，并创建相应的数据表。

## 组件 API 及其文档

本组件通过 Restful API 进行调用或者运行。

## 状态码

本组件的状态码定义，目前请参考 `utilities/sql_utils/status_code.py`。
