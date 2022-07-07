/* Copyright (c) 2004, 2019, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_sar.cc

  @brief
  The ha_sar engine is a stubbed storage engine for sar purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/sar/ha_sar.h.

  @details
  ha_sar will let you create/open/delete tables, but
  nothing further (for sar, indexes are not supported nor can data
  be stored in the table). Use this sar as a template for
  implementing the same functionality in your own storage engine. You
  can enable the sar storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-sar-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE \<table name\> (...) ENGINE=SAR;

  The sar storage engine is set up to use table locks. It
  implements an sar "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  sar handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_sar.h before reading the rest
  of this file.

  @note
  When you create an SAR table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an sar select that would do a scan of an entire
  table:

  @code
  ha_sar::store_lock
  ha_sar::external_lock
  ha_sar::info
  ha_sar::rnd_init
  ha_sar::extra
  ha_sar::rnd_next
  ha_sar::rnd_next
  ha_sar::rnd_next
  ha_sar::rnd_next
  ha_sar::rnd_next
  ha_sar::rnd_next
  ha_sar::rnd_next
  ha_sar::rnd_next
  ha_sar::rnd_next
  ha_sar::extra
  ha_sar::external_lock
  ha_sar::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the sar storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_sar::open() would also have been necessary. Calls to
  ha_sar::extra() are hints as to what will be occurring to the request.

  A Longer Example can be found called the "Skeleton Engine" which can be
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#include "storage/sar/ha_sar.h"

#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/log.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "typelib.h"

#include <boost/filesystem.hpp>

#ifdef REMOTE
// TODO 用Mysql SysVar
#define UPS_ENV_NAME "ups://localhost:8085/env.db"
#define UPS_ENV_MODE 0
#else
#define UPS_ENV_NAME "env.db"
#define UPS_ENV_MODE 0644
#endif

// #define CUSTOM_COMPARE_NAME "varlencmp"

///////////////////////////////////////////////////////////
// Globals
///////////////////////////////////////////////////////////
static handlerton *sar_hton;

static ups_txn_t *get_tx_from_thd(THD *const thd) {
  return reinterpret_cast<ups_txn_t *>(thd_get_ha_data(thd, sar_hton));
}

__attribute__((unused)) static ups_txn_t *get_or_create_tx(THD *const thd) {
  thd_set_ha_data(thd, sar_hton, nullptr);  // TODO 现在先不支持事务

  ups_txn_t *tx = get_tx_from_thd(thd);
  if (tx == nullptr) {
    // create and start tx
    thd_set_ha_data(thd, sar_hton, tx);
  } else {
    // start tx
  }

  return tx;
}

static inline KEY* get_key_info(TABLE* table, int idx) {
  return &(table->key_info[idx]);
}

static inline bool key_enable_duplicates(KEY * key_info) {
  return !(key_info->actual_flags & HA_NOSAME);
}

static handler *sar_create_handler(handlerton *hton, TABLE_SHARE *table, bool,
                                   MEM_ROOT *mem_root) {
  return new (mem_root) ha_sar(hton, table);
}

/* Interface to mysqld, to check system tables supported by SE */
static bool sar_is_supported_system_table(const char *db,
                                          const char *table_name,
                                          bool is_sql_layer_system_table);

static int sar_init_func(void *p) {
  DBUG_ENTER("sar_init_func");

  ups_env_t *env;
  ups_status_t st;
  ups_parameter_t params[] = {{0, 0}, {0, 0}, {0, 0}, {0, 0},
                              {0, 0}, {0, 0}, {0, 0}, {0, 0}};
  uint32_t flag = 0;
  // ups_register_compare(CUSTOM_COMPARE_NAME, custom_compare_func);

  flag |= UPS_ENABLE_TRANSACTIONS;
  //  flag |= UPS_DISABLE_RECOVERY;
  //  flag |= UPS_CACHE_UNLIMITED;
  //  flag |= UPS_IN_MEMORY;
  //  flag |= UPS_DISABLE_MMAP;
  // DBUG_ASSERT(params[7].name == 0 && params[7].value == 0);

#ifdef REMOTE
  st = ups_env_open(&env, UPS_ENV_NAME, flag, params);
#else
  st = ups_env_open(&env, UPS_ENV_NAME, flag, params);
  if (st == UPS_FILE_NOT_FOUND)
    st = ups_env_create(&env, UPS_ENV_NAME, flag, UPS_ENV_MODE, params);
#endif

  if (st != UPS_SUCCESS) {
    log_error("ups_env_create", st);
    DBUG_RETURN(1);
  }

  params[0] = {UPS_PARAM_MAX_DATABASES, 0};
  params[1] = {0, 0};
  ups_env_get_parameters(env, params);
  auto max_databases = (uint16_t)params[0].value;

  Catalogue::env_manager = new Catalogue::EnvManager(env, max_databases);

  // TODO connect and open all table

  sar_hton = (handlerton *)p;
  sar_hton->state = SHOW_OPTION_YES;
  sar_hton->create = sar_create_handler;
  sar_hton->flags =
      HTON_TEMPORARY_NOT_SUPPORTED |
      HTON_SUPPORTS_EXTENDED_KEYS;  // TODO HTON_CAN_RECREATE?
                                    // HTON_SUPPORTS_TABLE_ENCRYPTION?
  // TODO 干啥用的？
  sar_hton->is_supported_system_table = sar_is_supported_system_table;

  DBUG_RETURN(0);
}

static int sar_uninstall_func(void *p __attribute__ ((unused))) {
  DBUG_ENTER("sar_uninstall_func");
  Catalogue::env_manager->lock.lock();
  ups_env_t *env = Catalogue::env_manager->env;
  Catalogue::env_manager->lock.unlock();
  delete Catalogue::env_manager;
  Catalogue::env_manager = nullptr;
  ups_env_close(env, 0);
  DBUG_RETURN(0);
}

// key是否存在于db中
static inline bool key_exists(ups_db_t *db, ups_key_t *key) {
  ups_record_t record = ups_make_record(nullptr, 0);
  ups_status_t st = ups_db_find(db, nullptr, key, &record, 0);
  return st == 0;
}

// 删除一张Mysql表，删除upsdb中与这张表相关的全部内容，最后从env_manager内将其删除
static ups_status_t delete_mysql_table(const char *table_name) {
  boost::mutex::scoped_lock l(Catalogue::env_manager->lock);
  return Catalogue::env_manager->free_table(table_name);
}

// 创建一张Mysql表，创建upsdb中与这张表相关的全部内容，并把这张表添加到env_manager内
static ups_status_t create_mysql_table(const char *table_name, TABLE *table) {
  boost::mutex::scoped_lock lock(Catalogue::env_manager->lock);

  ups_db_t *db;
  ups_status_t st;
  int num_indices = table->s->keys;
  bool has_primary_index = false;
  boost::shared_ptr<Catalogue::Table> new_table(
      new Catalogue::Table(table_name));
  ups_env_t *env = Catalogue::env_manager->env;

  DBUG_ASSERT(num_indices > 0);
  std::vector<uint16_t> dbnames(num_indices);
  for (int i = 0; i < num_indices; ++i) {
    dbnames[i] = Catalogue::env_manager->dbName_tracker.get_new_dbname();
    if (unlikely(dbnames[i] == 0)) {
      sql_print_error("no more ups database could be use.");
      return UPS_LIMITS_REACHED;
    }
  }

  // 在创建db的时候可以把lock解锁
  lock.unlock();

  // key info for the primary key
  std::pair<uint32_t, uint32_t> primary_type_info;

  // 每轮一个index，创建出对应的db
  KEY *key_info = table->key_info;
  for (int i = 0; i < num_indices; i++, key_info++) {
    // Field *field = key_info->key_part->field;

    uint32_t key_type;
    uint32_t key_size;
    st = table_key_info(key_info, key_type, key_size, nullptr);
    if (unlikely(st != 0)) {
      return -1;
    }
    DBUG_ASSERT(key_type != UPS_TYPE_CUSTOM);

    bool enable_duplicates = true;
    bool is_primary_key = false;

    if (0 == ::strcmp("PRIMARY", key_info->name)) is_primary_key = true;
    // 我们不支持无主键
    if (num_indices == 1) DBUG_ASSERT(is_primary_key);

    if (is_primary_key) {
      has_primary_index = true;
      primary_type_info.first = key_type;
      primary_type_info.second = key_size;
      enable_duplicates = false;
    }
    if (key_info->flags & HA_NOSAME) enable_duplicates = false;

    uint32_t flags = 0;
    if (enable_duplicates) flags |= UPS_ENABLE_DUPLICATE_KEYS;

    int p = 1;
    ups_parameter_t params[] = {{UPS_PARAM_KEY_TYPE, key_type},
                                {0, 0},
                                {0, 0},
                                {0, 0},
                                {0, 0},
                                {0, 0},
                                {0, 0},
                                {0, 0}};

    // set a key size if the key is CHAR(n) or BINARY(n)
    if (key_size != 0 && key_type == UPS_TYPE_BINARY) {
      params[p].name = UPS_PARAM_KEY_SIZE;
      params[p].value = key_size;
      p++;
    }

    // for secondary indices: set the record type to the same type as
    // the primary key
    if (!is_primary_key) {
      params[p].name = UPS_PARAM_RECORD_TYPE;
      params[p].value = primary_type_info.first;
      p++;
      if (primary_type_info.second != 0) {
        params[p].name = UPS_PARAM_RECORD_SIZE;
        params[p].value = primary_type_info.second;
        p++;
      }
    }

    // primary key: if a record has fixed length then set a parameter
    if (is_primary_key && row_is_fixed_length(table)) {
      params[p].name = UPS_PARAM_RECORD_SIZE;
      params[p].value = table->s->stored_rec_length;
      p++;
    }

    st = ups_env_create_db(env, &db, dbnames[i], flags, params);
    if (unlikely(st != 0)) {
      log_error("ups_env_create_db", st);
      return st;
    }

    new_table->indices.emplace_back(db, i, is_primary_key, key_type);
  }

  DBUG_ASSERT(has_primary_index == true);
  // 把new_table放到env_manager里面，需要重新加锁
  lock.lock();
  Catalogue::env_manager->table_map.emplace(table_name, new_table);
  return 0;
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each sar handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/
Sar_share *ha_sar::get_share() {
  Sar_share *tmp_share;

  DBUG_ENTER("ha_sar::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<Sar_share *>(get_ha_share_ptr()))) {
    tmp_share = new Sar_share;
    if (!tmp_share) goto err;

    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_handler_tablename ha_sar_system_tables[] = {
    {(const char *)NULL, (const char *)NULL}};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @return
    @retval true   Given db.table_name is supported system table.
    @retval false  Given db.table_name is not a supported system table.
*/
static bool sar_is_supported_system_table(const char *db,
                                          const char *table_name,
                                          bool is_sql_layer_system_table) {
  st_handler_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table) return false;  // TODO 是这样吗？

  // Check if this is SE layer system tables
  systab = ha_sar_system_tables;
  while (systab && systab->db) {
    if (systab->db == db && strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}

/*
  It's just an example of THDVAR_SET() usage below.
*/

// static MYSQL_THDVAR_STR(last_create_thdvar, PLUGIN_VAR_MEMALLOC, NULL, NULL,
//                         NULL, NULL);
//
// static MYSQL_THDVAR_UINT(create_count_thdvar, 0, NULL, NULL, NULL, 0, 0,
// 1000,
//                          0);
//{
//     THD *thd = ha_thd();
//     THDVAR_SET(thd, last_create_thdvar, buf);
//     my_free(buf);
//
//     uint count = THDVAR(thd, create_count_thdvar) + 1;
//     THDVAR_SET(thd, create_count_thdvar, &count);
// }

/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_sar::create(const char *name, TABLE *table, HA_CREATE_INFO *,
                   dd::Table *) {
  DBUG_ENTER("ha_sar::create");
  ups_status_t st;

  // create a database for each index
  st = create_mysql_table(name, table);

  DBUG_RETURN(st ? 1 : 0);
}

/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/
int ha_sar::open(const char *name, int, uint, const dd::Table *) {
  DBUG_ENTER("ha_sar::open");

  // TODO 应该把其他函数修改地更通用，而不是在这assert
  DBUG_ASSERT(table_share->primary_key == 0);

  share = get_share();
  if (unlikely(!share)) DBUG_RETURN(1);

  thr_lock_data_init(&share->lock, &lock_data, nullptr);
  // ups_register_compare(CUSTOM_COMPARE_NAME, custom_compare_func);
  ups_set_committed_flush_threshold(30);
  record_arena.resize(table->s->rec_buff_length);

  Catalogue::env_manager->lock.lock();
  current_table = Catalogue::env_manager->get_table_from_name(name);
  Catalogue::env_manager->lock.unlock();

  if (!current_table) {
    sql_print_error("table %s not found", name);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_sar::close() {
  DBUG_ENTER("ha_sar::close");

  // if trx then commit or abort trx

  if (likely(current_table != nullptr)) {
    current_table = nullptr;
  }
  active_index = MAX_KEY;
  if (current_cursor) ups_cursor_close(current_cursor);

  DBUG_RETURN(0);
}

// static inline int insert_auto_index(Catalogue::Table *cattbl, TABLE *table,
//                                     uint8_t *buf, ups_txn_t *txn,
//                                     ByteVector &key_arena,
//                                     ByteVector &record_arena) {
//   ups_key_t key = ups_make_key(nullptr, 0);
//   ups_record_t record = record_from_row(table, buf, record_arena);
//
//   ups_status_t st = ups_db_insert(cattbl->autoidx.db, txn, &key, &record, 0);
//   if (unlikely(st != 0)) {
//     log_error("ups_db_insert", st);
//     return 1;
//   }
//
//   // Need to copy the key in the ByteVector - it will be required later
//   key_arena.resize(key.size);
//   ::memcpy(key_arena.data(), key.data, key.size);
//   return 0;
// }

static inline int insert_primary_key(Catalogue::Index &catidx, TABLE *table,
                                     uint8_t *buf, ups_txn_t *txn,
                                     ByteVector &key_arena,
                                     ByteVector &record_arena) {
  ups_key_t key = key_from_row(buf, get_key_info(table, catidx.key_index), key_arena);
  ups_record_t record = record_from_row(table, buf, record_arena);

  ups_status_t st = ups_db_insert(catidx.db, txn, &key, &record, 0);

  if (unlikely(st == UPS_DUPLICATE_KEY)) return HA_ERR_FOUND_DUPP_KEY;
  if (unlikely(st != 0)) {
    log_error("ups_db_insert", st);
    return 1;
  }
  return 0;
}

static inline int insert_secondary_key(TABLE* table, Catalogue::Index &catidx, uint8_t *buf,
                                       ups_txn_t *txn, ByteVector &key_arena,
                                       ByteVector &record_arena) {
  // The record of the secondary index is the primary key of the row
  ups_key_t primary_key = key_from_row(buf, get_key_info(table, 0), record_arena);
  ups_record_t record = ups_make_record(primary_key.data, primary_key.size);

  // The actual key is the column's value
  ups_key_t key = key_from_row(buf, get_key_info(table, catidx.key_index), key_arena);

  uint32_t flags = 0;
  if (key_enable_duplicates(get_key_info(table, catidx.key_index))) flags = UPS_DUPLICATE;

  ups_status_t st = ups_db_insert(catidx.db, txn, &key, &record, flags);
  if (unlikely(st == UPS_DUPLICATE_KEY)) return HA_ERR_FOUND_DUPP_KEY;
  if (unlikely(st != 0)) {
    log_error("ups_db_insert", st);
    return 1;
  }
  return 0;
}

static inline int insert_multiple_indices(Catalogue::Table *cattbl,
                                          TABLE *table, uint8_t *buf,
                                          ups_txn_t *txn, ByteVector &key_arena,
                                          ByteVector &record_arena) {
  for (auto &idx : cattbl->indices) {
    // is this the primary index?
    if (idx.is_primary_index) {
      int rc =
          insert_primary_key(idx, table, buf, txn, key_arena, record_arena);
      if (unlikely(rc)) return rc;
    }
    // is this a secondary index? the last parameter is a ByteVector with
    // the primary key.
    else {
      int rc = insert_secondary_key(table, idx, buf, txn, key_arena, record_arena);
      if (unlikely(rc)) return rc;
    }
  }

  return 0;
}

// 找到secondary_index中指向对应位置的cursor，其中record的值必须为primary_record
static inline ups_cursor_t *locate_secondary_key(ups_db_t *db, ups_txn_t *txn,
                                                 ups_key_t *key,
                                                 ups_record_t *primary_record) {
  CursorProxy cp(db, txn);
  if (unlikely(cp.cursor == nullptr)) return nullptr;

  ups_record_t record = ups_make_record(nullptr, 0);

  ups_status_t st = ups_cursor_find(cp.cursor, key, &record, 0);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_find", st);
    return nullptr;
  }

  // Locate the duplicate with the primary key
  do {
    if (likely(record.size == primary_record->size &&
               !::memcmp(record.data, primary_record->data, record.size)))
      break;

    st = ups_cursor_move(cp.cursor, nullptr, &record,
                         UPS_ONLY_DUPLICATES | UPS_CURSOR_NEXT);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_move", st);
      return nullptr;
    }
  } while (true);

  return cp.detach();
}

// 删除secondary_index中的一条记录，其中primary_key是记录的值
static inline int delete_from_secondary(ups_db_t *db, KEY *key_info,
                                        const uint8_t *buf, ups_txn_t *txn,
                                        ups_key_t *primary_key,
                                        ByteVector &key_arena) {
  ups_key_t key = key_from_row(buf, key_info, key_arena);
  ups_record_t primary_record =
      ups_make_record(primary_key->data, primary_key->size);
  CursorProxy cp(locate_secondary_key(db, txn, &key, &primary_record));
  if (unlikely(cp.cursor == nullptr)) return 1;

  // delete this duplicate
  ups_status_t st = ups_cursor_erase(cp.cursor, 0);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_erase", st);
    return 1;
  }

  return 0;
}

// 删除一个Mysql的row记录，以及与它相关的secondary_index
static int delete_multiple_indices(TABLE* table, Catalogue::Table *cattbl, const uint8_t *buf,
                                   ByteVector &key_arena) {
  ups_status_t st;

  TxnProxy txnp(Catalogue::env_manager->env);
  if (unlikely(!txnp.txn)) return 1;

  ups_key_t primary_key;

  // extract the key from |buf|
  ups_key_t key = key_from_row(buf, get_key_info(table, cattbl->indices[0].key_index), key_arena);
  primary_key = key;

  for (auto &idx : cattbl->indices) {
    // for debug
    Field *field = get_key_info(table, idx.key_index)->key_part->field;
    DBUG_ASSERT(field->m_indexed == true && field->stored_in_db != false);
    (void)field;

    // is this the primary index?
    if (idx.is_primary_index) {
      st = ups_db_erase(idx.db, txnp.txn, &primary_key, 0);
      if (unlikely(st != 0)) {
        log_error("ups_db_erase", st);
        return 1;
      }
    }
    // is this a secondary index?
    else {
      int rc = delete_from_secondary(idx.db, get_key_info(table, idx.key_index), buf, txnp.txn,
                                     &primary_key, key_arena);
      if (unlikely(rc != 0)) return rc;
    }
  }

  st = txnp.commit();
  if (unlikely(st != 0)) return 1;

  return 0;
}

static inline bool are_keys_equal(ups_key_t *lhs, ups_key_t *rhs) {
  if (lhs->size != rhs->size) return false;
  return 0 == ::memcmp(lhs->data, rhs->data, lhs->size);
}

// 这个真的需要吗？
static inline int ups_status_to_error(TABLE *table, const char *msg,
                                      ups_status_t st) {
  if (likely(st == 0)) {
    table->set_row_status_from_handler(0);
    return 0;
  }

  if (st == UPS_KEY_NOT_FOUND) {
    table->set_row_status_from_handler(STATUS_NOT_FOUND);
    return HA_ERR_END_OF_FILE;
  }
  if (st == UPS_DUPLICATE_KEY) {
    table->set_row_status_from_handler(STATUS_NOT_FOUND);
    return HA_ERR_FOUND_DUPP_KEY;
  }

  log_error(msg, st);
  return HA_ERR_GENERIC;
}

// 抽取出一个KEY，缓存在key_arena中，同时返回ups_key_t,keybuf中保存的必须是要抽取的key
static ups_key_t extract_key(const uint8_t *keybuf, KEY *key_info,
                             ByteVector &key_arena) {
  ups_key_t key = ups_make_key(nullptr, 0);
  KEY_PART_INFO *key_part = key_info->key_part;
  uint32_t key_parts = key_info->user_defined_key_parts;

  // if this is not a multi-part key AND it has fixed length then we can
  // simply use the existing |keybuf| pointer for the lookup
  // TODO 关于key_part的null_bit 我不太了解，先这样放着吧
  if (key_parts == 1 && encoded_length_bytes(key_part->type) == 0) {
    key.data = key_part->null_bit ? (void *)(keybuf + 1) : (void *)keybuf;
    key.size = key_part->length;
  }
  // otherwise we have to unpack the row and transform it into the
  // correct format
  else {
    const uint8_t *p = keybuf;
    key_arena.clear();

    for (uint32_t i = 0; i < key_parts; i++, key_part++) {
      // skip null byte, if it exists
      if (key_part->null_bit) p++;

      uint32_t length;
      switch (encoded_length_bytes(key_part->type)) {
        case 0:
          length = key_part->length;
          break;
        case 1:
          length = *(uint16_t *)p;
          // append the length if it's a multi-part key
          if (key_parts > 1) {
            key_arena.push_back((uint8_t)length);
            key.size += 1;
          }
          p += 2;
          break;
        case 2:
          length = *(uint16_t *)p;
          // append the length if it's a multi-part key
          if (key_parts > 1) {
            key_arena.insert(key_arena.end(), p, p + 2);
            key.size += 2;
          }
          p += 2;
          break;
      }

      // append the key data
      key_arena.insert(key_arena.end(), p, p + length);
      key.size += length;
      p += key_part->length;
    }

    key.data = key_arena.data();
  }
  return key;
}

// 这个函数和前面几乎唯一的区别就是key_info->user_defined_key_parts换成了
// table->reginfo.qep_tab->ref().key_parts
// 这个里面的QEP_TAB完全不知道是干什么用的
static ups_key_t extract_first_keys(const uint8_t *keybuf, TABLE *table,
                                    KEY *key_info, ByteVector &key_arena) {
  ups_key_t key = ups_make_key(nullptr, 0);
  KEY_PART_INFO *key_part = key_info->key_part;
  DBUG_ASSERT(key_info->user_defined_key_parts > 1);

  const uint8_t *p = keybuf;
  key_arena.clear();

  for (uint32_t i = 0; i < table->reginfo.qep_tab->ref().key_parts;
       i++, key_part++) {
    // skip null byte, if it exists
    if (key_part->null_bit) p++;

    uint32_t length;
    switch (encoded_length_bytes(key_part->type)) {
      case 0:
        length = key_part->length;
        break;
      case 1:
        length = *(uint16_t *)p;
        // always append the length for multi-part keys
        key_arena.push_back((uint8_t)length);
        p += 2;
        break;
      case 2:
        length = *(uint16_t *)p;
        // always append the length for multi-part keys
        key_arena.insert(key_arena.end(), p, p + 2);
        p += 2;
        break;
    }

    // append the key data
    key_arena.insert(key_arena.end(), p, p + length);
    p += length;
  }

  key.size = key_arena.size();
  key.data = key_arena.data();
  return key;
}

/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Example of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an sar of extracting all of the data as strings.
  ha_berekly.cc has an sar of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments. This case also applies to
  write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_sar::write_row(uchar *buf) {
  // table->s->reclength是一行（一个record）的最大长度，也就是说对于varchar等
  // 变长类型，会是其最长长度（而不是实际长度），直接用会有空间浪费，所以才需要自行提取
  DBUG_ENTER("ha_sar::write_row");

  ups_status_t st;
  // duplicate_error_index = (uint32_t)-1;

  // no index?
  DBUG_ASSERT(!current_table->indices.empty());

  //  // auto-incremented index? then get a new value
  //  if (table->next_number_field && buf == table->record[0]) {
  //    int rc = update_auto_increment();
  //    if (unlikely(rc)) DBUG_RETURN(rc);
  //  }

  // only one index? do not need transaction
  if (current_table->indices.size() == 1) {
    int rc = insert_primary_key(current_table->indices[0], table, buf, nullptr,
                                key_arena, record_arena);
    // if (unlikely(rc == HA_ERR_FOUND_DUPP_KEY)) duplicate_error_index = 0;
    DBUG_RETURN(rc);
  }

  // multiple indices? then create a new transaction and update
  // all indices
  TxnProxy txnp(Catalogue::env_manager->env);
  if (unlikely(!txnp.txn)) DBUG_RETURN(1);

  int rc = insert_multiple_indices(current_table.get(), table, buf, txnp.txn,
                                   key_arena, record_arena);
  // if (unlikely(rc == HA_ERR_FOUND_DUPP_KEY)) duplicate_error_index = 0;
  if (unlikely(rc)) DBUG_RETURN(rc);

  st = txnp.commit();
  DBUG_RETURN(st == 0 ? 0 : 1);
}

/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record. You can
  do this for sar by doing:

  @code

  if (table->next_number_field && record == table->record[0])
    update_auto_increment();

  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_sar::update_row(const uchar *old_buf, uchar *new_buf) {
  DBUG_ENTER("ha_sar::update_row");

  ups_status_t st;
  ups_record_t new_rec = record_from_row(table, new_buf, record_arena);

  DBUG_ASSERT(!current_table->indices.empty());

  std::vector<bool> changed(MAX_KEY, false);

  // fast code path: if there's just one primary key then try to overwrite
  // the record, or re-insert if the key was modified
  if (current_table->indices.size() == 1) {
    ups_key_t oldkey = key_from_row((uchar *)old_buf, get_key_info(table, current_table->indices[0].key_index),
                                    old_pk);
    ups_key_t newkey =
        key_from_row(new_buf, get_key_info(table, current_table->indices[0].key_index), key_arena);
    bool equal = are_keys_equal(&oldkey, &newkey);

    // if both keys are equal: simply overwrite the record of the
    // current key
    if (equal) {
      st = ups_db_insert(current_table->indices[0].db, nullptr, &oldkey,
                         &new_rec, UPS_OVERWRITE);
      if (unlikely(st != 0)) {
        log_error("ups_cursor_overwrite", st);
        DBUG_RETURN(1);
      }
      DBUG_RETURN(0);
    }

    // otherwise delete the old row, insert the new one. Inserting can fail if
    // the key is not unique, therefore both operations are wrapped
    // in a single transaction.
    TxnProxy txnp(Catalogue::env_manager->env);
    if (unlikely(!txnp.txn)) {
      DBUG_RETURN(1);
    }

    ups_db_t *db = current_table->indices[0].db;
    st = ups_db_erase(db, txnp.txn, &oldkey, 0);
    if (unlikely(st != 0)) {
      log_error("ups_db_erase", st);
      DBUG_RETURN(1);
    }

    st = ups_db_insert(db, txnp.txn, &newkey, &new_rec, 0);
    if (unlikely(st == UPS_DUPLICATE_KEY)) DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    if (unlikely(st != 0)) {
      log_error("ups_db_insert", st);
      DBUG_RETURN(1);
    }

    st = txnp.commit();
    DBUG_RETURN(st == 0 ? 0 : 1);
  }

  size_t i = 0;
  for (auto &idx : current_table->indices) {
    ups_key_t oldkey =
        key_from_row((uchar *)old_buf, get_key_info(table, idx.key_index), old_second_k);
    ups_key_t newkey = key_from_row(new_buf, get_key_info(table, idx.key_index), new_second_k);
    changed[i++] = !are_keys_equal(&oldkey, &newkey);
  }

  // More than one index? Update all indices that were changed.
  // Again wrap all of this in a single transaction, in case we fail
  // to insert the new row.
  TxnProxy txnp(Catalogue::env_manager->env);
  if (unlikely(!txnp.txn)) {
    DBUG_RETURN(1);
  }

  ups_key_t new_primary_key =
      key_from_row(new_buf, get_key_info(table, current_table->indices[0].key_index), key_arena);
  ups_key_t old_primary_key =
      key_from_row(old_buf, get_key_info(table, current_table->indices[0].key_index), old_pk);

  // Primary index:
  // 1. Was the key modified? then re-insert it
  // 2. Otherwise, overwrite the record
  if (changed[0]) {
    // ups_key_t oldkey = key_from_row((uchar *)old_buf,
    // current_table->indices[0].key_info, old_pk);
    st = ups_db_erase(current_table->indices[0].db, txnp.txn, &old_primary_key,
                      0);
    if (unlikely(st != 0)) {
      log_error("ups_db_erase", st);
      DBUG_RETURN(1);
    }
    st = ups_db_insert(current_table->indices[0].db, txnp.txn, &new_primary_key,
                       &new_rec, 0);
    if (unlikely(st == UPS_DUPLICATE_KEY)) DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    if (unlikely(st != 0)) {
      log_error("ups_db_insert", st);
      DBUG_RETURN(1);
    }
  } else {
    st = ups_db_insert(current_table->indices[0].db, txnp.txn, &new_primary_key,
                       &new_rec, UPS_OVERWRITE);
    if (unlikely(st != 0)) {
      log_error("ups_db_insert", st);
      DBUG_RETURN(1);
    }
  }


  // All secondary indices:
  // 1. if the primary key was changed then their new_rec has to be
  //    overwritten
  // 2. if the secondary key was changed then re-insert it

  // 新旧的record_arena record，给secondary key作为record
  ups_record_t new_primary_key_record =
      ups_make_record(new_primary_key.data, new_primary_key.size);
  ups_record_t old_primary_key_record =
      ups_make_record(old_primary_key.data, old_primary_key.size);
  for (i = 1; i < current_table->indices.size(); i++) {
    ups_key_t newkey = key_from_row(
        (uchar *)new_buf, get_key_info(table, current_table->indices[i].key_index), new_second_k);
    if (changed[i]) {
      // secondary index有修改，去旧存新
      int rc = delete_from_secondary(
          current_table->indices[i].db, get_key_info(table, current_table->indices[i].key_index),
          old_buf, txnp.txn, &old_primary_key, old_second_k);
      if (unlikely(rc != 0)) DBUG_RETURN(rc);
      uint32_t flags = 0;
      if (key_enable_duplicates(get_key_info(table, current_table->indices[i].key_index)))
        flags = UPS_DUPLICATE;
      st = ups_db_insert(current_table->indices[i].db, txnp.txn, &newkey,
                         &new_primary_key_record, flags);
      if (unlikely(st == UPS_DUPLICATE_KEY)) DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
      if (unlikely(st != 0)) {
        log_error("ups_db_insert", st);
        DBUG_RETURN(1);
      }
    } else if (changed[0]) {
      // secondary index没修改，但primary key有修改，只修改record的值
      ups_key_t oldkey = key_from_row(
          (uchar *)old_buf, get_key_info(table, current_table->indices[i].key_index), old_second_k);
      CursorProxy cp(locate_secondary_key(current_table->indices[i].db,
                                          txnp.txn, &oldkey,
                                          &old_primary_key_record));
      DBUG_ASSERT(cp.cursor != nullptr);
      st = ups_cursor_overwrite(cp.cursor, &new_primary_key_record, 0);
      if (unlikely(st != 0)) {
        log_error("ups_cursor_overwrite", st);
        DBUG_RETURN(1);
      }
    }
  }

  st = txnp.commit();
  if (unlikely(st != 0)) {
    log_error("ups_txn_commit", st);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_sar::delete_row(const uchar *buf) {
  DBUG_ENTER("ha_sar::delete_row");

  DBUG_ASSERT(!current_table->indices.empty());
  ups_status_t st;
  // fast code path: if there's just one index then use the cursor to
  // delete the record
  if (current_table->indices.size() == 1) {
    ups_key_t key =
        key_from_row(buf, get_key_info(table, current_table->indices[0].key_index), key_arena);
    st = ups_db_erase(current_table->indices[0].db, nullptr, &key, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_erase", st);
      DBUG_RETURN(1);
    }
    DBUG_RETURN(0);
  }

  // otherwise (if there are multiple indices) then delete the key from
  // each index
  int rc = delete_multiple_indices(table, current_table.get(), buf, key_arena);
  DBUG_RETURN(rc);
}

int ha_sar::index_init(uint idx, bool) {
  DBUG_ENTER("ha_sar::index_init");

  active_index = idx;
  DBUG_ASSERT(current_table != nullptr);
  DBUG_ASSERT(current_cursor == nullptr);
  DBUG_ASSERT(!current_table->indices.empty());
  DBUG_ASSERT(idx < current_table->indices.size());

  // from which index are we reading?
  ups_db_t *db = current_table->indices[idx].db;

  ups_txn_t *tx = get_tx_from_thd(ha_thd());
  ups_status_t st = ups_cursor_create(&current_cursor, db, tx, 0);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_create", st);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

int ha_sar::index_end() {
  DBUG_ENTER("ha_sar::index_end");

  DBUG_ASSERT(active_index != MAX_KEY);
  DBUG_ASSERT(current_table != nullptr);
  DBUG_ASSERT(current_cursor != nullptr);

  ups_status_t st = ups_cursor_close(current_cursor);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_close", st);
    DBUG_RETURN(1);
  }
  current_cursor = nullptr;
  active_index = MAX_KEY;
  DBUG_RETURN(0);
}

int ha_sar::index_read(uchar *buf, const uchar *key, uint,
                       enum ha_rkey_function find_flag) {
  DBUG_ENTER("ha_sar::index_read");
  DBUG_ASSERT(active_index != MAX_KEY);
  DBUG_ASSERT(current_table != nullptr);
  DBUG_ASSERT(current_cursor != nullptr);

  bool read_primary_index = (active_index == 0);

  // when reading from the primary index: directly fetch record into |buf|
  // if the row has fixed length
  ups_record_t record = ups_make_record(nullptr, 0);
  if (read_primary_index && row_is_fixed_length(table)) {
    record.data = buf;
    record.flags = UPS_RECORD_USER_ALLOC;
  }

  ups_status_t st;

  if (key == nullptr) {
    st = ups_cursor_move(current_cursor, nullptr, &record, UPS_CURSOR_FIRST);
  } else {
    ups_key_t key_ups =
        extract_key(key, &table->key_info[active_index], key_arena);

    switch (find_flag) {
      case HA_READ_KEY_EXACT:
        if (likely(table->key_info[active_index].user_defined_key_parts == 1))
          st = ups_cursor_find(current_cursor, &key_ups, &record, 0);
        else {
          st = ups_cursor_find(current_cursor, &key_ups, &record,
                               UPS_FIND_GEQ_MATCH);
          // if this was an approx. match: verify that the key part is really
          // identical!
          // 下面这段看不懂+不知道有没有必要
          if (ups_key_get_approximate_match_type(&key_ups) != 0) {
            // cursor找到的key不等于key_ups,这个时候key_ups的指针会指向一个
            // 临时地址并在后续的ups api中失效，所以key_arena又可用了
            ups_key_t first = extract_first_keys(
                key, table, &table->key_info[active_index], key_arena);
            if (::memcmp(key_ups.data, first.data, first.size) != 0)
              st = UPS_KEY_NOT_FOUND;
          }
        }
        break;
      // TODO 模糊查询可能有问题
      case HA_READ_KEY_OR_NEXT:
      case HA_READ_PREFIX:
        st = ups_cursor_find(current_cursor, &key_ups, &record,
                             UPS_FIND_GEQ_MATCH);
        break;
      case HA_READ_KEY_OR_PREV:
      case HA_READ_PREFIX_LAST_OR_PREV:
      case HA_READ_PREFIX_LAST:
        st = ups_cursor_find(current_cursor, &key_ups, &record,
                             UPS_FIND_LEQ_MATCH);
        break;
      case HA_READ_AFTER_KEY:
        st = ups_cursor_find(current_cursor, &key_ups, &record,
                             UPS_FIND_GT_MATCH);
        break;
      case HA_READ_BEFORE_KEY:
        st = ups_cursor_find(current_cursor, &key_ups, &record,
                             UPS_FIND_LT_MATCH);
        break;
      case HA_READ_INVALID:  // (last)
        st = ups_cursor_move(current_cursor, nullptr, &record, UPS_CURSOR_LAST);
        break;

      default:
        DBUG_ASSERT(!"shouldn't be here");
        st = UPS_INTERNAL_ERROR;
        break;
    }
  }

  if (likely(st == 0)) {
    // Did we fetch from the primary index? then we have to unpack the record
    if (read_primary_index && !row_is_fixed_length(table))
      record = unpack_record(table, &record, buf);

    // Or is this a secondary index? then use the primary key (in the record)
    // to fetch the row
    else if (!read_primary_index) {
      ups_key_t key_ups = ups_make_key(record.data, (uint16_t)record.size);
      ups_record_t rec = ups_make_record(nullptr, 0);
      if (row_is_fixed_length(table)) {
        rec.data = buf;
        rec.flags = UPS_RECORD_USER_ALLOC;
      }
      st = ups_db_find(current_table->indices[0].db, get_tx_from_thd(ha_thd()),
                       &key_ups, &rec, 0);
      if (likely(st == 0) && !row_is_fixed_length(table))
        rec = unpack_record(table, &rec, buf);
    }
  }

  int rc = ups_status_to_error(table, "ups_db_find", st);
  DBUG_RETURN(rc);
}

///**
//  @brief
//  Positions an index cursor to the index specified in the handle. Fetches the
//  row if available. If the key value is null, begin at the first key of the
//  index.
//*/
//
// int ha_sar::index_read_map(uchar *buf, const uchar *key,
//                           key_part_map keypart_map,
//                           enum ha_rkey_function find_flag) {
//  return handler::index_read_map(buf, key, keypart_map, find_flag);
//}

/**
  @brief
  Used to read forward through the index.
*/

// Helper function which moves the cursor in the direction specified in
// |flags|, and retrieves the row
// |flags| should not be 0
int ha_sar::index_operation(uchar *buf, uint32_t flags) {
  DBUG_ASSERT(active_index != MAX_KEY);
  DBUG_ASSERT(current_table != nullptr);
  DBUG_ASSERT(current_cursor != nullptr);
  ups_status_t st;

  // when reading from the primary index: directly fetch record into |buf|
  // if the row has fixed length
  ups_record_t record = ups_make_record(nullptr, 0);
  if ((active_index == 0) && row_is_fixed_length(table)) {
    record.data = buf;
    record.flags = UPS_RECORD_USER_ALLOC;
  }
  // move into the requested direction
  st = ups_cursor_move(current_cursor, nullptr, &record, flags);
  if (unlikely(st != 0))
    return ups_status_to_error(table, "ups_cursor_move", st);

  // if we fetched the record from a secondary index: lookup the actual row
  // from the primary index
  if ((active_index > 0 && active_index < MAX_KEY)) {
    ups_key_t key = ups_make_key(
        record.data,
        (uint16_t)record.size);  // 这个时候record.data是primary_key的值
    ups_record_t rec = ups_make_record(nullptr, 0);
    if (row_is_fixed_length(table)) {
      rec.data = buf;
      rec.flags = UPS_RECORD_USER_ALLOC;
    }
    st = ups_db_find(current_table->indices[0].db, get_tx_from_thd(ha_thd()),
                     &key, &rec, 0);
    if (unlikely(st != 0))
      return ups_status_to_error(table, "ups_cursor_move", st);

    record.data = rec.data;
    record.size = rec.size;
  }

  // if necessary then unpack the row
  if (!row_is_fixed_length(table)) unpack_record(table, &record, buf);

  return ups_status_to_error(table, "ups_cursor_move", 0);
}

int ha_sar::index_next(uchar *buf) {
  int rc;
  DBUG_ENTER("ha_sar::index_next");
  rc = index_operation(buf, UPS_CURSOR_NEXT);
  DBUG_RETURN(rc);
}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_sar::index_prev(uchar *buf) {
  int rc;
  DBUG_ENTER("ha_sar::index_prev");
  rc = index_operation(buf, UPS_CURSOR_PREVIOUS);
  DBUG_RETURN(rc);
}

/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_sar::index_first(uchar *buf) {
  int rc;
  DBUG_ENTER("ha_sar::index_first");
  rc = index_operation(buf, UPS_CURSOR_FIRST);
  DBUG_RETURN(rc);
}

/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_sar::index_last(uchar *buf) {
  int rc;
  DBUG_ENTER("ha_sar::index_last");
  rc = index_operation(buf, UPS_CURSOR_LAST);
  DBUG_RETURN(rc);
}

// int ha_sar::index_next_same(uchar *buf, const uchar *keybuf, uint keylen) {
//  DBUG_ENTER("ha_sar::index_next_same");
//
//  int rc = 0;
//
//  if (first_call_after_position) {
//    // locate the first key
//    rc = index_operation((uchar *)keybuf, keylen, buf, 0);
//    // and immediately try to move to the next key
//    if (likely(rc == 0))
//      rc = index_operation(nullptr, 0, buf,
//                           UPS_ONLY_DUPLICATES | UPS_CURSOR_NEXT);
//    first_call_after_position = false;
//  } else {
//    rc = index_operation((uchar *)keybuf, keylen, buf,
//                         UPS_ONLY_DUPLICATES | UPS_CURSOR_NEXT);
//  }
//  DBUG_RETURN(rc);
//}

/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the sar in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
int ha_sar::rnd_init(bool scan) {
  DBUG_ENTER("ha_sar::rnd_init");

  ups_status_t st;
  DBUG_ASSERT(current_table != nullptr);
  if (scan) {
    DBUG_ASSERT(current_cursor == nullptr);
    DBUG_ASSERT(active_index == MAX_KEY);
  }
  if (!scan) {
    DBUG_ASSERT(current_cursor != nullptr);
    DBUG_ASSERT(active_index == 0);
  }
  DBUG_ASSERT(!current_table->indices.empty());

  if (scan) {
    // 用primary key作为索引
    ups_db_t *db = current_table->indices[0].db;
    ups_txn_t *tx = get_tx_from_thd(ha_thd());
    st = ups_cursor_create(&current_cursor, db, tx, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_create", st);
      DBUG_RETURN(1);
    }
    active_index = 0;
  } else {
    st = ups_cursor_move(current_cursor, nullptr, nullptr, UPS_CURSOR_FIRST);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_create", st);
      DBUG_RETURN(1);
    }
  }

  DBUG_RETURN(0);
}

int ha_sar::rnd_end() {
  DBUG_ENTER("ha_sar::rnd_end");

  DBUG_ASSERT(current_table != nullptr);
  DBUG_ASSERT(current_cursor != nullptr);
  DBUG_ASSERT(active_index == 0);

  ups_cursor_close(current_cursor);
  current_cursor = nullptr;
  active_index = MAX_KEY;
  DBUG_RETURN(0);
}

/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
int ha_sar::rnd_next(uchar *buf) {
  int rc;
  DBUG_ENTER("ha_sar::rnd_next");
  rc = index_operation(buf, UPS_CURSOR_NEXT);
  DBUG_RETURN(rc);
}

/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_sar::position(const uchar *buf) {
  DBUG_ENTER("ha_sar::position");

  DBUG_ASSERT(active_index == 0);
  DBUG_ASSERT(current_table != nullptr);
  DBUG_ASSERT(current_cursor != nullptr);

  // Store the PRIMARY key as the reference in |ref|
  KEY *key_info = table->key_info;
  DBUG_ASSERT(
      ref_length ==
      key_info->key_length);  // table->key_info[0]，指向的是primary_key的info
  key_copy(ref, (uchar *)buf, key_info, ref_length);

  DBUG_VOID_RETURN;
}

/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and
  sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_sar::rnd_pos(uchar *buf, uchar *pos) {
  int rc;
  DBUG_ENTER("ha_sar::rnd_pos");
  rc = index_read(buf, pos, ref_length, HA_READ_KEY_EXACT);

  DBUG_RETURN(rc);
}

/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really
  needed. SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc and sql_update.cc
*/
int ha_sar::info(uint flag __attribute__((unused))) {
  DBUG_ENTER("ha_sar::info");

  /// if (flag & HA_STATUS_ERRKEY) errkey = duplicate_error_index;

  // TODO stats.records = ???
  stats.records = 2;

  DBUG_RETURN(0);
}

/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_sar::extra(enum ha_extra_function) {
  DBUG_ENTER("ha_sar::extra");
  DBUG_RETURN(0);
}

/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases
  where the optimizer realizes that all rows will be removed as a result of an
  SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
// TODO
// int ha_sar::delete_all_rows() {
//  DBUG_ENTER("ha_sar::delete_all_rows");
//
//  // close();  // closes the cursor
//
//  ups_status_t st = delete_mysql_table(catdb, cattbl, table);
//  if (unlikely(st)) {
//    log_error("delete_mysql_table", st);
//    DBUG_RETURN(1);
//  }
//
//  st = create_mysql_table(catdb, cattbl, table);
//  if (unlikely(st)) {
//    log_error("create_mysql_table", st);
//    DBUG_RETURN(1);
//  }
//
//  DBUG_RETURN(0);
//}

/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to
  understand this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
  TODO
*/
int ha_sar::external_lock(THD *, int) {
  DBUG_ENTER("ha_sar::external_lock");
  DBUG_RETURN(0);
}

// 这表明我们不需要Mysql的表锁
// uint ha_sar::lock_count() const { return 0; }

/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for sar, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_sar::store_lock(THD *thd __attribute__((unused)),
                                   THR_LOCK_DATA **to,
                                   enum thr_lock_type lock_type) {
  // TODO 这里用的是最基本的Mysql自带的表锁，可以参考MyRocks把他进行一定的转换，
  //  用我们自己的锁
  // store_lock是针对表的，给这张表加上Mysql表锁，锁的类型为lock_type，有好多个，
  // 可以点进去看，级别从低到高，也可以不用他的表锁，innobase就没有用，直接把to给
  // 返回来，也没有用lock_data，但是myrocks还是用了表锁，后面再改的时候要仔细考
  // 虑一下
  if (lock_type != TL_IGNORE && lock_data.type == TL_UNLOCK)
    lock_data.type = lock_type;
  *to++ = &lock_data;
  return to;

  //  if (lock_type != TL_IGNORE && lock_data.type == TL_UNLOCK) {
  //    /*
  //      Here is where we get into the guts of a row level lock.
  //      If TL_UNLOCK is set
  //      If we are not doing a LOCK TABLE or DISCARD/IMPORT
  //      TABLESPACE, then allow multiple writers
  //    */
  //
  //    //
  //    这里是直接copy过来的，不要改，虽然if语句里面的这些东西我不知道是在干什么，
  //    // 特别是thd_in_lock_tables(thd)，千万不要删
  //    if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE)
  //    &&
  //        !thd_in_lock_tables(thd))
  //      lock_type = TL_WRITE_ALLOW_WRITE;
  //
  //    /*
  //      In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
  //      MySQL would use the lock TL_READ_NO_INSERT on t2, and that
  //      would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
  //      to t2. Convert the lock to a normal read lock to allow
  //      concurrent inserts to t2.
  //    */
  //
  //    if (lock_type == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
  //      lock_type = TL_READ;
  //
  //    lock_data.type = lock_type;
  //  }
  //
  //  *to++ = &lock_data;
  //
  //  return to;
}

/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_sar::delete_table(const char *name, const dd::Table *) {
  DBUG_ENTER("ha_sar::delete_table");

  ups_status_t st = delete_mysql_table(name);
  if (unlikely(st != 0)) {
    log_error("delete table", st);
  }
  if (current_cursor) current_cursor = nullptr;
  active_index = MAX_KEY;
  if (current_table != nullptr) current_table = nullptr;

  DBUG_RETURN(0);
}

/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_sar::rename_table(const char *, const char *, const dd::Table *,
                         dd::Table *) {
  DBUG_ENTER("ha_sar::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_sar::records_in_range(uint, key_range *, key_range *) {
  DBUG_ENTER("ha_sar::records_in_range");
  DBUG_RETURN(10);  // low number to force index usage
}

struct st_mysql_storage_engine sar_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

// static ulong srv_enum_var = 0;
// static ulong srv_ulong_var = 0;
// static double srv_double_var = 0;
// static int srv_signed_int_var = 0;
// static long srv_signed_long_var = 0;
// static longlong srv_signed_longlong_var = 0;
//
// const char *enum_var_names[] = {"e1", "e2", NullS};
//
// TYPELIB enum_var_typelib = {array_elements(enum_var_names) - 1,
//                             "enum_var_typelib", enum_var_names, NULL};
//
// static MYSQL_SYSVAR_ENUM(enum_var,                        // name
//                          srv_enum_var,                    // varname
//                          PLUGIN_VAR_RQCMDARG,             // opt
//                          "Sample ENUM system variable.",  // comment
//                          NULL,                            // check
//                          NULL,                            // update
//                          0,                               // def
//                          &enum_var_typelib);              // typelib
//
// static MYSQL_SYSVAR_ULONG(ulong_var, srv_ulong_var, PLUGIN_VAR_RQCMDARG,
//                           "0..1000", NULL, NULL, 8, 0, 1000, 0);
//
// static MYSQL_SYSVAR_DOUBLE(double_var, srv_double_var, PLUGIN_VAR_RQCMDARG,
//                            "0.500000..1000.500000", NULL, NULL, 8.5, 0.5,
//                            1000.5,
//                            0);  // reserved always 0
//
// static MYSQL_THDVAR_DOUBLE(double_thdvar, PLUGIN_VAR_RQCMDARG,
//                            "0.500000..1000.500000", NULL, NULL, 8.5, 0.5,
//                            1000.5, 0);
//
// static MYSQL_SYSVAR_INT(signed_int_var, srv_signed_int_var,
// PLUGIN_VAR_RQCMDARG,
//                         "INT_MIN..INT_MAX", NULL, NULL, -10, INT_MIN,
//                         INT_MAX, 0);
//
// static MYSQL_THDVAR_INT(signed_int_thdvar, PLUGIN_VAR_RQCMDARG,
//                         "INT_MIN..INT_MAX", NULL, NULL, -10, INT_MIN,
//                         INT_MAX, 0);
//
// static MYSQL_SYSVAR_LONG(signed_long_var, srv_signed_long_var,
//                          PLUGIN_VAR_RQCMDARG, "LONG_MIN..LONG_MAX", NULL,
//                          NULL, -10, LONG_MIN, LONG_MAX, 0);
//
// static MYSQL_THDVAR_LONG(signed_long_thdvar, PLUGIN_VAR_RQCMDARG,
//                          "LONG_MIN..LONG_MAX", NULL, NULL, -10, LONG_MIN,
//                          LONG_MAX, 0);
//
// static MYSQL_SYSVAR_LONGLONG(signed_longlong_var, srv_signed_longlong_var,
//                              PLUGIN_VAR_RQCMDARG, "LLONG_MIN..LLONG_MAX",
//                              NULL, NULL, -10, LLONG_MIN, LLONG_MAX, 0);
//
// static MYSQL_THDVAR_LONGLONG(signed_longlong_thdvar, PLUGIN_VAR_RQCMDARG,
//                              "LLONG_MIN..LLONG_MAX", NULL, NULL, -10,
//                              LLONG_MIN, LLONG_MAX, 0);
//
// static SYS_VAR *sar_system_variables[] = {MYSQL_SYSVAR(enum_var),
//                                           MYSQL_SYSVAR(ulong_var),
//                                           MYSQL_SYSVAR(double_var),
//                                           MYSQL_SYSVAR(double_thdvar),
//                                           MYSQL_SYSVAR(last_create_thdvar),
//                                           MYSQL_SYSVAR(create_count_thdvar),
//                                           MYSQL_SYSVAR(signed_int_var),
//                                           MYSQL_SYSVAR(signed_int_thdvar),
//                                           MYSQL_SYSVAR(signed_long_var),
//                                           MYSQL_SYSVAR(signed_long_thdvar),
//                                           MYSQL_SYSVAR(signed_longlong_var),
//                                           MYSQL_SYSVAR(signed_longlong_thdvar),
//                                           NULL};
//
//// this is an sar of SHOW_FUNC
// static int show_func_sar(MYSQL_THD, SHOW_VAR *var, char *buf) {
//   var->type = SHOW_CHAR;
//   var->value = buf;  // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
//   snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
//            "enum_var is %lu, ulong_var is %lu, "
//            "double_var is %f, signed_int_var is %d, "
//            "signed_long_var is %ld, signed_longlong_var is %lld",
//            srv_enum_var, srv_ulong_var, srv_double_var, srv_signed_int_var,
//            srv_signed_long_var, srv_signed_longlong_var);
//   return 0;
// }
//
// struct sar_vars_t {
//   ulong var1;
//   double var2;
//   char var3[64];
//   bool var4;
//   bool var5;
//   ulong var6;
// };
//
// sar_vars_t sar_vars = {100, 20.01, "three hundred", true, 0, 8250};
//
// static SHOW_VAR show_status_sar[] = {
//     {"var1", (char *)&sar_vars.var1, SHOW_LONG, SHOW_SCOPE_GLOBAL},
//     {"var2", (char *)&sar_vars.var2, SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},
//     {0, 0, SHOW_UNDEF, SHOW_SCOPE_UNDEF}  // null terminator required
// };
//
// static SHOW_VAR show_array_sar[] = {
//     {"array", (char *)show_status_sar, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
//     {"var3", (char *)&sar_vars.var3, SHOW_CHAR, SHOW_SCOPE_GLOBAL},
//     {"var4", (char *)&sar_vars.var4, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
//     {0, 0, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};
//
// static SHOW_VAR func_status[] = {
//     {"sar_func_sar", (char *)show_func_sar, SHOW_FUNC, SHOW_SCOPE_GLOBAL},
//     {"sar_status_var5", (char *)&sar_vars.var5, SHOW_BOOL,
//     SHOW_SCOPE_GLOBAL},
//     {"sar_status_var6", (char *)&sar_vars.var6, SHOW_LONG,
//     SHOW_SCOPE_GLOBAL},
//     {"sar_status", (char *)show_array_sar, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
//     {0, 0, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

mysql_declare_plugin(sar){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &sar_storage_engine,
    "SAR",
    "Haitao",
    "SGX Enabled Remote storage engine",
    PLUGIN_LICENSE_GPL,
    sar_init_func, /* Plugin Init */
    nullptr,       /* Plugin check uninstall */
    sar_uninstall_func, /* Plugin Deinit */
    0x0001 /* 0.1 */,
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
} mysql_declare_plugin_end;
