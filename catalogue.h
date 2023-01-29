//
// Created by yulan on 22-6-28.
//

#ifndef MYSQL_CATALOGUE_H
#define MYSQL_CATALOGUE_H

#include <map>
#include <string>
#include <utility>
#include <vector>
#include <bitset>

#include <ups/upscaledb_int.h>
#include <ups/upscaledb_srv.h>
#include <atomic>
#include <mutex>

#include "sql/field.h"
#include "sql/key.h"
#include "utils.h"

namespace Catalogue {

// 保持Index信息，一个Index会对应一个upsdb
struct Index {
  explicit Index(ups_db_t *db_, int key_index_, bool is_primary_index_ = false,
                 uint32_t key_type_ = 0)
      : db(db_),
        key_index(key_index_),
        is_primary_index(is_primary_index_),
        key_type(key_type_) {}

  // 不能这样搞，每一次连接的mysql Table对象会变，key_info地址也会变
  // MySQL KEY info of this index
  // KEY *key_info;

  // upsdb
  ups_db_t *db;
  // 这是Table里面的第几个Key
  int key_index;
  // 是否是主键
  bool is_primary_index;
  // upscaledb key type (UPS_TYPE_UINT32 etc)
  uint32_t key_type;
};

// 保存一个MySQL的Table，一个Table下有多个Index
struct Table {
  explicit Table(std::string name_) : name(std::move(name_)) {}

  // table name, ./{db_name}/{table_name}
  std::string name;

  // 一个Mysql Table有多个Index，每个Index对应一个upsDB的db
  std::vector<Index> indices;
};


// 追踪有哪些dbname被使用的类
class DBNameTracker {
 public:
  /**
   * 创建一个至多能追踪size个dbname的tracker
   */
  explicit DBNameTracker(uint16_t size);

  // 重置tracker，将所有dbname设置为free
  void reset();

  // 将names中的dbname设置为used，names的长度为length
  void assign(uint16_t *names, uint32_t length);

  // 分配一个新的dbname，并将其设置为used，返回0代表没有更多的dbname可用了
  uint16_t get_new_dbname();

  // 将一个dbname设置为free
  void free_dbname(uint16_t dbname);

  // system table 的 dbname， system table的作用是保存每个Table的元信息，暂时未实现
  uint16_t system_db_name = 1;
 private:
  std::vector<bool> name_track;
};

// 管理整个UpsDB的Environment
class EnvManager {
 public:
  /**
   * @param env_ 被管理的env
   * @param max_database env中最多允许的database数（是upsdb的database数）
   */
  EnvManager(ups_env_t* env_, uint16_t max_database): env(env_), dbName_tracker(max_database) {}

  ~EnvManager() {
      ups_env_close(env, UPS_AUTO_CLEANUP | UPS_TXN_AUTO_ABORT);
  }

  // 根据Table名字，获取Table对象，该函数没有加锁，需要自己加锁
  std::shared_ptr<Table> get_table_from_name(const char* table_name);

  // 根据名字删除Table，会同时删除相关的upsdb，把dbname加入到dbname_tracker
  ups_status_t free_table(const char* table_name);

  typedef std::map<std::string, std::shared_ptr<Table> > TableMap;
  // table_name => Table
  TableMap table_map;
  // 避免多线程同时创建table
  std::mutex lock;

  ups_env_t *env;

  DBNameTracker dbName_tracker;

  std::atomic<ulonglong> trx_id_count{0};
};

extern EnvManager* env_manager;
}  // namespace Catalogue

#endif  // MYSQL_CATALOGUE_H
