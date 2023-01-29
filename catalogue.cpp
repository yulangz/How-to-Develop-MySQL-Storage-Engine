//
// Created by yulan on 22-6-28.
//
#include "catalogue.h"

namespace Catalogue {

EnvManager* env_manager;

std::shared_ptr<Table> EnvManager::get_table_from_name(
    const char *table_name) {
  auto table_iter = Catalogue::env_manager->table_map.find(table_name);
  if (unlikely(table_iter == Catalogue::env_manager->table_map.end())) {
    return nullptr;
  }

  return table_iter->second;
}

ups_status_t EnvManager::free_table(const char *table_name) {
  ups_status_t st;

  auto table = get_table_from_name(table_name);
  if (unlikely(table == nullptr)) {
    sql_print_error("Table %s not found", table_name);
    return UPS_DATABASE_NOT_FOUND;
  }

  // 删除与这个table相关的全部upsdb
  for (auto &index : table->indices) {
    uint16_t dbname = ups_db_get_name(index.db);
    st = ups_db_close(index.db, UPS_AUTO_CLEANUP);
    if (unlikely(st != 0)) {
      log_error("ups_env_erase_db", st);
      return st;
    }
    st = ups_env_erase_db(env, dbname, 0);
    if (unlikely(st != 0)) {
      log_error("ups_env_erase_db", st);
      return st;
    }
    dbName_tracker.free_dbname(dbname);
  }

  table_map.erase(table_name);
  return 0;
}

DBNameTracker::DBNameTracker(uint16_t size) : name_track(size, false) {}

void DBNameTracker::reset() { name_track.clear(); }

void DBNameTracker::assign(uint16_t *names, uint32_t length) {
  for (uint32_t i = system_db_name + 1; i < length; ++i) {
    name_track[names[i]] = true;
  }
}

uint16_t DBNameTracker::get_new_dbname() {
  auto l = (int32_t)name_track.size();
  for (int32_t i = system_db_name + 1; i < l; ++i) {
    if (!name_track[i]) {
      name_track[i] = true;
      return i;
    }
  }
  return 0;
}

void DBNameTracker::free_dbname(uint16_t dbname) {
  DBUG_ASSERT(name_track[dbname]);
  name_track[dbname] = false;
}
}  // namespace Catalogue