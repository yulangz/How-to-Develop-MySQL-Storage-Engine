//
// Created by yulan on 22-6-28.
//

#ifndef MYSQL_CATALOGUE_H
#define MYSQL_CATALOGUE_H

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <boost/thread/mutex.hpp>
#include <boost/dynamic_bitset.hpp>

#include <ups/upscaledb_int.h>
#include <ups/upscaledb_srv.h>

#include "sql/field.h"
#include "sql/key.h"
#include "utils.h"

namespace Catalogue {

//
// This struct stores information about a index
//
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

  // The upscaledb database structure
  ups_db_t *db;

  // 这是Table里面的第几个Key
  int key_index;

  // |true| if this is the primary index
  bool is_primary_index;

  // the upscaledb key type (UPS_TYPE_UINT32 etc)
  uint32_t key_type;
//
//  inline bool enable_duplicate() const {
//    return !(key_info->actual_flags & HA_NOSAME);
//  }
};

//
// This struct stores information about a MySQL table, its indices and the
// associated upscaledb databases.
//
struct Table {
  explicit Table(std::string name_) : name(std::move(name_)) {}

  // table name, ./{db_name}/{table_name}
  std::string name;

  // The table's indices
  // 一个Mysql Table有多个Index，每个Index对应一个upsDB的db
  std::vector<Index> indices;
};


// 追踪有哪些dbname被使用的类
class DBNameTracker {
 public:
  // 创建一个至多能追踪size个dbname的tracker
  explicit DBNameTracker(uint16_t size);

  // 重置tracker，将所有dbname设置为free
  void reset();

  // 将names中的dbname设置为used，names的长度为length
  void assign(uint16_t *names, uint32_t length);

  // 分配一个新的dbname，并将其设置为used，返回0代表没有更多的dbname可用了
  uint16_t get_new_dbname();

  // 将一个dbname设置为free
  void free_dbname(uint16_t dbname);

  uint16_t system_db_name = 1;
 private:
  boost::dynamic_bitset<> name_track;
};

//
// 管理整个UpsDB的Environment
//
class EnvManager {
 public:
  EnvManager(ups_env_t* env_, uint16_t max_database): env(env_), dbName_tracker(max_database) {}

  ~EnvManager() {
      ups_env_close(env, UPS_AUTO_CLEANUP | UPS_TXN_AUTO_ABORT);
  }

  // 根据Table名字，获取Table对象，该函数没有加锁，需要自己加锁
  boost::shared_ptr<Table> get_table_from_name(const char* table_name);

  // 根据名字删除Table，会同时删除相关的upsdb，把dbname加入到dbname_tracker
  ups_status_t free_table(const char* table_name);

  // table_name => Table
  typedef std::map<std::string, boost::shared_ptr<Table> > TableMap;
  TableMap table_map;
  boost::mutex lock;

  // The upscaledb environment with all tables of this database
  ups_env_t *env;

  DBNameTracker dbName_tracker;
};

extern EnvManager* env_manager;
}  // namespace Catalogue

#endif  // MYSQL_CATALOGUE_H
