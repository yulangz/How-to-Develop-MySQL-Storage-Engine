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

```
