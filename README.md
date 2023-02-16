# How to Develop MySQL Storage Engine
本项目是受到[upscaledb-mysql](https://github.com/cruppstahl/upscaledb-mysql)的启发，基于 upscaleDB 开发的 MySQL 存储引擎，将原项目迁移到 MySQL8 上，并且加入了事务支持。

本项目支持 MySQL 的全部功能，包括基础增删改查、索引、多列索引、null作为索引、自增列、自动主键、事务、auto commit等。

目前来说，该项目的体量不大（纯代码不到2000行），但是麻雀虽小，五脏俱全，后续我会撰写相关教程，让本项目成为一个学习性项目，以帮助读者快速了解如何开发一个 MySQL 存储引擎，也可以帮助读者熟悉 MySQL 存储引擎接口代码，为阅读 MyRocks、InnoDB 等工程项目做好准备。

## How to Build
### 安装 [upscaleDB](https://upscaledb.com/)
以下是必须的依赖
`protobuf-compiler  libprotobuf-dev  libgoogle-perftools-dev  libboost-system-dev  libboost-thread-dev  libboost-dev`
参照官网教程进行安装
```shell
git clone https://github.com/cruppstahl/upscaledb.git
cd upscaledb
git checkout topic/2.2.1
sh bootstrap.sh
./configure
make -j 5 && sudo make install
```

### 下载 MySQL8 源码
```shell
git clone https://github.com/mysql/mysql-server.git
```

### clone 本项目
```shell
cd mysql-server/storage
git clone https://github.com/yulangz/How-to-Develop-MySQL-Storage-Engine.git
```

### 编译 MySQL
```shell
cd mysql-server
mkdir build & cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j 32
sudo make install
```

## Run
### 快速启动 MySQL
注意！！！这种启动方法只是为了测试，不能在生产环境中使用
```shell
cd /usr/local/mysql
sudo chmod 777 *

# 初始化 mysqld，该步骤中会生成随机密码
./bin/mysqld --initialize --user=mysql

# 启动 mysqld
./bin/mysqld
```
之后在另一个 terminal 中连接 mysql
```shell
mysql -h 127.0.0.1 -P 3306 -u root -p
# 输入前面生成的随机密码
```

### 运行测试
连接后首先修改 root 密码
```sql
alter user 'root'@'localhost' identified by '';
```
创建数据库与表
```sql
create database t;
create table t(id int, name varchar(10), primary key(id,name))engine=sar;
```
接下来就可以自由进行增删改查了。

### benchmark
```shell
sysbench oltp_common --create_secondary=off --db-driver=mysql --mysql-user=root --mysql-storage-engine=sar --mysql-db=t --mysql-host=127.0.0.1 --skip-trx=false prepare
sysbench oltp_read_write --create_secondary=off --db-driver=mysql --mysql-user=root --mysql-storage-engine=sar --mysql-db=t --mysql-host=127.0.0.1 --skip-trx=false run
```

## TODO
* 通过环境变量设置 Cache Size
* 在 update t set id=id+10 (其中id是int型主键)的时候，会无线循环，原因是，假设原来有id=1，在rnd_next的时候读到了它，并且通过update_row更新了它(id=11)，下一次rnd_next的时候就会又读到更新的值(id=11)，因此造成无限循环