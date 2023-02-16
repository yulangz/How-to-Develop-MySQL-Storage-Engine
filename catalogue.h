//
// Created by yulan on 22-6-28.
//

#ifndef MYSQL_CATALOGUE_H
#define MYSQL_CATALOGUE_H

#include <bitset>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <ups/upscaledb_int.h>
#include <ups/upscaledb_srv.h>
#include <atomic>
#include <mutex>

#include "sql/field.h"
#include "sql/key.h"
#include "utils.h"

namespace Catalogue {

enum key_type {
  KEY_TYPE_PRIMARY = 0,
  KEY_TYPE_HIDDEN_PRIMARY = 1,
  KEY_TYPE_GENERAL = 2
};

// 保存 Index 的基本信息，每个 Index 就是一个 MySQL Table Key，对应一个 upsdb 存储
struct Index {
  explicit Index(ups_db_t *db_, int key_index_, enum key_type index_key_type_ = KEY_TYPE_GENERAL,
                 uint32_t key_type_ = 0)
      : db(db_),
        key_index(key_index_),
        index_key_type(index_key_type_),
        key_type(key_type_) {}

  // upsdb
  ups_db_t *db;
  // 这是 Table 里面的第几个 Key
  int key_index;
  // 键的类型
  uint8 index_key_type;
  // upscaledb key type (UPS_TYPE_UINT32 etc.，目前版本已经不需要使用这个属性了)
  uint32_t key_type;


  int keys_per_page() {
    // todo 使用 ups_db_get_parameters()
    return 40;
  }
};

// 保存一个 MySQ L的Table，一个 Table 下有多个 Index
struct Table {
  explicit Table(std::string name_) : name(std::move(name_)) {}

  // table name, ./{db_name}/{table_name}
  std::string name;

  // 一个 Mysql Table 有多个 Index，每个 Index 对应一个 upsDB 的 db
  std::vector<Index> indices;

  // next value for hidden key
  std::atomic<uint64> hidden_index_value{0};
};

class EnvManager;

// 追踪有哪些dbname被使用的类
class DBNameTracker {
 public:
  /**
   * 创建一个至多能追踪 size 个 dbname 的 tracker
   */
  explicit DBNameTracker(uint16_t size);

  // 重置 tracker，将所有 dbname 设置为 free
  void reset();

  // 分配一个新的 dbname，并将其设置为 used，返回 0 代表没有更多的 dbname 可用了
  uint16_t get_new_dbname();

  // 将一个 dbname 设置为 free
  void free_dbname(uint16_t dbname);

 private:
  friend class EnvManager;
  // 将 db_name 标记为已使用
  void set_used(uint16_t db_name);

 private:
  std::vector<bool> name_track;
};

// 管理整个 UpsDB 的 Environment，全局只有一个 UpsDB Environment
// todo 换成每个 MySQL Database 一个 UpsDB Environment
class EnvManager {
 public:
  /**
   * @param env_ 被管理的 env
   * @param max_database env 中最多允许的 database 数（是 upsDB 的 database 数）
   */
  EnvManager(ups_env_t *env_, uint16_t max_database)
      : env(env_), dbName_tracker(max_database) {
    dbName_tracker.set_used(MetaDBName);
    ups_status_t st;
    if ((st = ups_env_open_db(env, &meta_db_, MetaDBName, 0, nullptr)) !=
        UPS_SUCCESS) {
      if (st == UPS_DATABASE_NOT_FOUND) {
        // 需要创建新的 meta db
        ups_parameter_t params[] = {{UPS_PARAM_KEY_TYPE, UPS_TYPE_BINARY},
                                    {UPS_PARAM_RECORD_TYPE, UPS_TYPE_BINARY},
                                    {0, 0}};
        st = ups_env_create_db(env, &meta_db_, MetaDBName, 0, params);
        if (st != UPS_SUCCESS) {
          log_error("ups create meta db error", st);
        }
      }
    }
    read_all_tables();
  }

  ~EnvManager() {
    lock.lock();
    flush_all_tables();
    lock.unlock();
    ups_env_close(env, UPS_AUTO_CLEANUP | UPS_TXN_AUTO_ABORT);
  }

  // 根据 Table 名字，获取 Table 对象
  std::shared_ptr<Table> get_table_from_name(const char *table_name);

  // 根据名字删除 Table，会同时删除相关的 upsDB，把 dbname 加入到 dbname_tracker
  ups_status_t free_table(const char *table_name);

  inline ups_env_t *get_env() { return env; }

  // 分配一个新的 dbname
  inline uint16_t get_new_db_name() {
    std::lock_guard<std::mutex> g(lock);
    return dbName_tracker.get_new_dbname();
  }

  // 将一个新 Table 加入到 Env
  inline void add_table(const char *table_name,
                        const std::shared_ptr<Table> &table) {
    std::lock_guard<std::mutex> g(lock);
    table_map.emplace(table_name, table);
    flush_all_tables();
  }

 private:
  typedef std::map<std::string, std::shared_ptr<Table>> TableMap;
  // table_name => Table
  TableMap table_map;
  // 避免多线程同时创建table
  std::mutex lock;

  ups_env_t *env;

  DBNameTracker dbName_tracker;

 private:
  static constexpr uint16_t MetaDBName = 1;
  static constexpr int BytePerIndexRecord = 11;
  ups_db_t *meta_db_;

  // 将全部的表元数据刷新到 env 的 0 号 db 里面
  ups_status_t flush_all_tables();

  // 从 0 号 db 里面读取出表的元数据
  ups_status_t read_all_tables();
};

extern EnvManager *env_manager;
}  // namespace Catalogue

#endif  // MYSQL_CATALOGUE_H
