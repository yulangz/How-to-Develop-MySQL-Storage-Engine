//
// Created by yulan on 22-6-28.
//
#include "catalogue.h"
#include "ups/upscaledb_uqi.h"
#include "ups/upscaledb.h"

namespace Catalogue {

EnvManager *env_manager;

std::shared_ptr<Table> EnvManager::get_table_from_name(const char *table_name) {
  std::lock_guard<std::mutex> g(lock);
  auto table_iter = Catalogue::env_manager->table_map.find(table_name);
  if (unlikely(table_iter == Catalogue::env_manager->table_map.end())) {
    return nullptr;
  }

  return table_iter->second;
}

ups_status_t EnvManager::free_table(const char *table_name) {
  ups_status_t st = UPS_SUCCESS;

  auto table = get_table_from_name(table_name);
  if (unlikely(table == nullptr)) {
    sql_print_error("Table %s not found", table_name);
    return UPS_DATABASE_NOT_FOUND;
  }

  std::lock_guard<std::mutex> g(lock);
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
  ups_key_t k = ups_make_key((void *)table_name, (uint16_t)strlen(table_name));
  st = ups_db_erase(meta_db_, nullptr, &k, 0);
  if (st != UPS_SUCCESS) {
    log_error("ups erase table in meta db error", st);
  }
  return st;
}

ups_status_t EnvManager::flush_all_tables() {
  ups_status_t st = UPS_SUCCESS;
  ByteVector index_arena;
  for (const auto &table_it : this->table_map) {
    const std::string &table_name = table_it.first;
    std::shared_ptr<Table> table = table_it.second;

    // 写磁盘格式为：
    //   key 为 table name，value为 Table 下全部的 Index
    //     value 编码：
    //       |         ... index 1 ...        |   ... index 2 ...  | ....
    //       |db_name|key_index|is_pk|key_type|
    //       |2byte  |4byte    |1byte|4byte   |

    size_t index_num = table->indices.size();
    index_arena.resize(BytePerIndexRecord * index_num);
    for (size_t i = 0; i < index_num; i++) {
      const auto &index = table->indices[i];
      uint16_t db_name = ups_db_get_name(index.db);
      memcpy(index_arena.data() + i * BytePerIndexRecord + 0, &db_name, 2);
      memcpy(index_arena.data() + i * BytePerIndexRecord + 2, &index.key_index,
             4);
      memcpy(index_arena.data() + i * BytePerIndexRecord + 6,
             &index.index_key_type, 1);
      memcpy(index_arena.data() + i * BytePerIndexRecord + 7, &index.key_type,
             4);
    }
    ups_key_t k =
        ups_make_key((void *)table_name.c_str(), (uint16_t)table_name.length());
    ups_record_t r =
        ups_make_record(index_arena.data(), (uint32_t)index_arena.size());

    st = ups_db_insert(meta_db_, nullptr, &k, &r, UPS_OVERWRITE);
    if (st != UPS_SUCCESS) {
      log_error("write table meta", st);
      return st;
    }
  }

  return st;
}

static uint64 get_next_key(ups_db_t *db) {
  ups_status_t st;
  uint64 v = UINT64_MAX;
  ups_key_t key = ups_make_key(&v, sizeof(v));
  key.flags |= UPS_KEY_USER_ALLOC;
  CursorProxy c(db, nullptr);
  st = ups_cursor_move(c.cursor, &key, nullptr, UPS_CURSOR_LAST);
  if (st == UPS_SUCCESS) {
    return v + 1;
  } else {
    if (st != UPS_KEY_NOT_FOUND) {
      log_error("get last key", st);
    }
    return 0;
  }
}

ups_status_t EnvManager::read_all_tables() {
  ups_status_t st = UPS_SUCCESS;
  ups_key_t k = ups_make_key(0,0);
  ups_record_t r = ups_make_record(0,0);

  CursorProxy cursorProxy(meta_db_, nullptr);
  st = ups_cursor_move(cursorProxy.cursor, &k, &r, UPS_CURSOR_FIRST);
  while (st == UPS_SUCCESS) {
    std::string table_name((char*)k.data, k.size);
    std::shared_ptr<Table> new_table(new Table(table_name));
    DBUG_ASSERT(r.size % BytePerIndexRecord == 0);

    // 读出所有的 Index
    size_t index_num = r.size / BytePerIndexRecord;
    for (size_t i = 0;i < index_num; i++) {
      uint16_t db_name;
      int key_index;
      enum key_type index_key_type;
      uint32_t key_type;
      memcpy(&db_name, (char*)r.data + i * BytePerIndexRecord + 0, 2);
      memcpy(&key_index, (char*)r.data + i * BytePerIndexRecord + 2, 4);
      memcpy(&index_key_type, (char*)r.data + i * BytePerIndexRecord + 6, 1);
      memcpy(&key_type, (char*)r.data + i * BytePerIndexRecord + 7, 4);
      ups_db_t *db;
      st = ups_env_open_db(env, &db, db_name, 0, nullptr);
      if (st != UPS_SUCCESS) {
        log_error("read_all_tables ups_env_open_db error", st);
        return st;
      }
      dbName_tracker.set_used(db_name);
      Index new_index(db, key_index, index_key_type, key_type);
      new_table->indices.push_back(new_index);

      if (index_key_type == KEY_TYPE_HIDDEN_PRIMARY) {
        new_table->hidden_index_value = get_next_key(db);
      }
    }

    // 读出表中的统计信息
//    uqi_result_t *uqi_result = nullptr;
//    ups_record_t record = ups_make_record(0,0);
//    char uqi_string_buffer[40] = "COUNT($key) FROM DATABASE ";
//    char *db_name_p = uqi_string_buffer + sizeof("COUNT($key) FROM DATABASE ") - 1;
//    auto db_name = ups_db_get_name(new_table->indices.back().db);
//    sprintf(db_name_p, "%u", (uint32_t)db_name);
//    st = uqi_select(env, uqi_string_buffer, &uqi_result);
//    if (unlikely(st != UPS_SUCCESS || !uqi_result)) {
//      log_error("uqi_select", st);
//      return st;
//    }
//    uqi_result_get_record(uqi_result, 0, &record);
//    new_table->records_num = *(uint32_t *)record.data;
//    uqi_result_close(uqi_result);

    // 插入 table map
    table_map[table_name] = new_table;
    st = ups_cursor_move(cursorProxy.cursor, &k, &r, UPS_CURSOR_NEXT);
  }
  if (st == UPS_KEY_NOT_FOUND) {
    st = UPS_SUCCESS;
  } else {
    log_error("read_all_tables ups_cursor_move error", st);
  }

  return st;
}

DBNameTracker::DBNameTracker(uint16_t size) : name_track(size, false) {}

void DBNameTracker::reset() { name_track.clear(); }

//void DBNameTracker::assign(uint16_t *names, uint32_t length) {
//  for (uint32_t i = system_db_name + 1; i < length; ++i) {
//    name_track[names[i]] = true;
//  }
//}

uint16_t DBNameTracker::get_new_dbname() {
  auto l = (int32_t)name_track.size();
  for (int32_t i = 1; i < l; ++i) {
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

void DBNameTracker::set_used(uint16_t db_name) {
  name_track[db_name] = true;
}
}  // namespace Catalogue