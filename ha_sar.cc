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

// helper macros to improve CPU branch prediction
#if defined __GNUC__
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

#define CUSTOM_COMPARE_NAME "varlencmp"

struct KeyPart {
  KeyPart(uint32_t type_ = 0, uint32_t length_ = 0)
      : type(type_), length(length_) {}

  uint32_t type;
  uint32_t length;
};

// 提取key中的key_part信息，应该是包括key的类型和长度
struct CustomCompareState {
  CustomCompareState(KEY *key) {
    if (key) {
      for (uint32_t i = 0; i < key->user_defined_key_parts; i++) {
        KEY_PART_INFO *key_part = &key->key_part[i];
        parts.emplace_back(key_part->type, key_part->length);
      }
    }
  }

  std::vector<KeyPart> parts;
};

// 返回一个type类型的field的前缀长度
static inline int encoded_length_bytes(uint32_t type) {
  if (type == HA_KEYTYPE_VARTEXT1 || type == HA_KEYTYPE_VARBINARY1) return 1;
  if (type == HA_KEYTYPE_VARTEXT2 || type == HA_KEYTYPE_VARBINARY2) return 2;
  return 0;
}

static TABLE *recovery_table;  // TODO 干啥用的？

// 自定义的比较函数，按照key_part中的顺序依次比较各个key，按字面值进行比较，可以兼容变长字段
static int custom_compare_func(ups_db_t *db, const uint8_t *lhs,
                               uint32_t lhs_length, const uint8_t *rhs,
                               uint32_t rhs_length) {
  CustomCompareState *ccs = (CustomCompareState *)ups_get_context_data(db, 1);
  CustomCompareState tmp_ccs(
      ccs != nullptr ? nullptr
                     : &recovery_table->key_info[ups_db_get_name(db) - 2]);
  // during recovery, the context pointer was not yet installed. in this case
  // we use the temporary one.
  if (ccs == nullptr) ccs = &tmp_ccs;

  for (size_t i = 0; i < ccs->parts.size(); i++) {
    KeyPart *key_part = &ccs->parts[i];

    int cmp;

    // TODO use *real* comparison function for other columns like uint32,
    // float, ...?
    switch (encoded_length_bytes(key_part->type)) {
      case 0:  // fixed length columns
        cmp = ::memcmp(lhs, rhs, key_part->length);
        lhs_length = key_part->length;
        rhs_length = key_part->length;
        break;
      case 1:
        lhs_length = *lhs;
        lhs += 1;
        rhs_length = *rhs;
        rhs += 1;
        cmp = ::memcmp(lhs, rhs, std::min(lhs_length, rhs_length));
        break;
      case 2:
        lhs_length = *(uint16_t *)lhs;
        lhs += 2;
        rhs_length = *(uint16_t *)rhs;
        rhs += 2;
        cmp = ::memcmp(lhs, rhs, std::min(lhs_length, rhs_length));
        break;
      default:
        assert(!"shouldn't be here");
    }

    if (cmp != 0) return cmp;
    // cmp == 0
    if (lhs_length < rhs_length) return -1;
    if (lhs_length > rhs_length) return +1;

    // both keys are equal - continue with the next one
    lhs += lhs_length;
    rhs += rhs_length;
  }

  // still here? then both keys must be equal
  return 0;
}

static void log_error_impl(const char *file, int line, const char *function,
                           ups_status_t st) {
  sql_print_error("%s[%d] %s: failed with error %d (%s)", file, line, function,
                  st, ups_strerror(st));
}

#define log_error(f, s) log_error_impl(__FILE__, __LINE__, f, s)

// A class which aborts a transaction when it's destructed
struct TxnProxy {
  TxnProxy(ups_env_t *env) {
    ups_status_t st = ups_txn_begin(&txn, env, 0, 0, 0);
    if (unlikely(st != 0)) {
      log_error("ups_txn_begin", st);
      txn = nullptr;
    }
  }

  ~TxnProxy() {
    if (txn != nullptr) {
      ups_status_t st = ups_txn_abort(txn, 0);
      if (unlikely(st != 0)) log_error("ups_txn_abort", st);
    }
  }

  ups_status_t commit() {
    ups_status_t st = ups_txn_commit(txn, 0);
    if (likely(st == 0)) txn = nullptr;
    return st;
  }

  ups_status_t abort() {
    if (txn != nullptr) {
      ups_status_t st = ups_txn_abort(txn, 0);
      if (likely(st == 0)) txn = nullptr;
      return st;
    }
    return 0;
  }

  ups_txn_t *txn;
};

// A class which closes a cursor when going out of scope
struct CursorProxy {
  CursorProxy(ups_cursor_t *c = nullptr) : cursor(c) {}

  CursorProxy(ups_db_t *db, ups_txn_t *txn = nullptr) {
    ups_status_t st = ups_cursor_create(&cursor, db, txn, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_create", st);
      cursor = nullptr;
    }
  }

  ~CursorProxy() {
    if (cursor != nullptr) {
      ups_status_t st = ups_cursor_close(cursor);
      if (unlikely(st != 0)) log_error("ups_cursor_close", st);
    }
  }

  ups_cursor_t *detach() {
    ups_cursor_t *c = cursor;
    cursor = nullptr;
    return c;
  }

  ups_cursor_t *cursor;
};

static inline std::string format_environment_name(const char *name) {
  std::string n(name);
  return n + ".ups";
}

// 删除一个Mysql表，以及与之相关的Catalogue::Database
static inline void close_and_remove_share(const char *table_name,
                                          Catalogue::Database *catdb) {
  boost::mutex::scoped_lock lock(Catalogue::databases_mutex);
  ups_env_t *env = catdb ? catdb->env : nullptr;
  ups_srv_t *srv = catdb ? catdb->srv : nullptr;

  // also remove the environment from a server
  if (env && srv) (void)ups_srv_remove_env(srv, env);

  // close the environment
  if (env) {
    ups_status_t st = ups_env_close(env, UPS_AUTO_CLEANUP);
    if (unlikely(st != 0)) log_error("ups_env_close", st);
    catdb->env = nullptr;
  }

  // TODO TODO TODO
  std::string env_name = format_environment_name(table_name);
  if (Catalogue::databases.find(env_name) != Catalogue::databases.end())
    Catalogue::databases.erase(Catalogue::databases.find(env_name));
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

static handler *sar_create_handler(handlerton *hton, TABLE_SHARE *table, bool,
                                   MEM_ROOT *mem_root) {
  return new (mem_root) ha_sar(hton, table);
}

// handlerton *sar_hton; // TODO 是否需要？

/* Interface to mysqld, to check system tables supported by SE */
static bool sar_is_supported_system_table(const char *db,
                                          const char *table_name,
                                          bool is_sql_layer_system_table);

static int sar_init_func(void *p) {
  DBUG_ENTER("sar_init_func");

  handlerton *sar_hton = (handlerton *)p;
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

// 返回一个key_part(一个field)在upsdb中的类型，以及字节大小
static inline std::pair<uint32_t, uint32_t>  // <key, size>
table_field_info(KEY_PART_INFO *key_part) {
  Field *field = key_part->field;
  Field_temporal *f;

  // 我对引擎设置了不支持NULL，所以这里应该不需要了
  // if key can be null: use a variable-length key
  if (key_part->null_bit)
    return std::make_pair((uint32_t)UPS_TYPE_BINARY, UPS_KEY_SIZE_UNLIMITED);

  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_YEAR:
      return std::make_pair((uint32_t)UPS_TYPE_UINT8, 1u);
    case MYSQL_TYPE_SHORT:
      return std::make_pair((uint32_t)UPS_TYPE_UINT16, 2u);
    case MYSQL_TYPE_LONG:
      return std::make_pair((uint32_t)UPS_TYPE_UINT32, 4u);
    case MYSQL_TYPE_LONGLONG:
      return std::make_pair((uint32_t)UPS_TYPE_UINT64, 8u);
    case MYSQL_TYPE_FLOAT:
      return std::make_pair((uint32_t)UPS_TYPE_REAL32, 4u);
    case MYSQL_TYPE_DOUBLE:
      return std::make_pair((uint32_t)UPS_TYPE_REAL64, 8u);
    case MYSQL_TYPE_DATE:
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 3u);
    case MYSQL_TYPE_TIME:
      f = (Field_temporal *)field;
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 3u + f->decimals());
    case MYSQL_TYPE_DATETIME:
      f = (Field_temporal *)field;
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 5u + f->decimals());
    case MYSQL_TYPE_TIMESTAMP:
      f = (Field_temporal *)field;
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 4u + f->decimals());
    case MYSQL_TYPE_SET:
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, field->pack_length());
    default:
      break;
  }

  // includes ENUM, CHAR, BINARY...
  // For multi-part keys we return the length that is required to store the
  // full column. For single-part keys we only store the actually used
  // length.
  switch (key_part->type) {
    case HA_KEYTYPE_BINARY:
    case HA_KEYTYPE_TEXT:
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, key_part->length);
    case HA_KEYTYPE_VARTEXT1:
    case HA_KEYTYPE_VARTEXT2:
    case HA_KEYTYPE_VARBINARY1:
    case HA_KEYTYPE_VARBINARY2:
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 0);  // TODO 为什么是0?
  }

  return std::make_pair((uint32_t)UPS_TYPE_BINARY, 0u);
}

// 返回一个KEY在upsdb中的类型，以及字节大小，一个KEY可以包含多个key_part(field)，要全部累计起来
static inline std::pair<uint32_t, uint32_t>  // <key, size>
table_key_info(KEY *key_info) {
  // if this index is made from a single column: return type/size of this column
  if (key_info->user_defined_key_parts == 1)
    return table_field_info(key_info->key_part);

  // Otherwise accumulate the total size of all columns which form this
  // key.
  // If any key has variable length then we need to use our custom compare
  // function.
  uint32_t total_size = 0;
  bool need_custom_compare = false;
  for (uint32_t i = 0; i < key_info->user_defined_key_parts; i++) {
    std::pair<uint32_t, uint32_t> p = table_field_info(&key_info->key_part[i]);
    if (total_size == UPS_KEY_SIZE_UNLIMITED || p.second == 0 ||
        p.second == UPS_KEY_SIZE_UNLIMITED)
      total_size = UPS_KEY_SIZE_UNLIMITED;
    else
      total_size += p.second;

    switch (key_info->key_part[i].type) {
      case HA_KEYTYPE_VARTEXT1:
      case HA_KEYTYPE_VARTEXT2:
      case HA_KEYTYPE_VARBINARY1:
      case HA_KEYTYPE_VARBINARY2:
        need_custom_compare = true;
    }
  }

  return std::make_pair(need_custom_compare ? UPS_TYPE_CUSTOM : UPS_TYPE_BINARY,
                        total_size);
}

// 从一个行数据中提取出一个key_part(field)的数据，生成ups_key_t，TODO 改名
static inline ups_key_t key_from_single_row(TABLE *, const uchar *buf,
                                            KEY_PART_INFO *key_part) {
  uint16_t key_size = (uint16_t)key_part->length;
  uint32_t offset = key_part->offset;

  switch (encoded_length_bytes(key_part->type)) {
    case 1:
      key_size = buf[offset];
      offset++;
      key_size = std::min(key_size, key_part->length);
      break;
    case 2:
      key_size = *(uint16_t *)&buf[offset];
      offset += 2;
      key_size = std::min(key_size, key_part->length);
      break;
  }

  ups_key_t key = ups_make_key((uchar *)buf + offset, key_size);
  return key;
}

// buf指向一个行，提取其中第index个KEY(可能包含多个Field)的值，生成ups_key_t
static inline ups_key_t key_from_row(TABLE *table, const uchar *buf, int index,
                                     ByteVector &arena) {
  arena.clear();

  if (likely(table->key_info[index].user_defined_key_parts == 1))
    return key_from_single_row(table, buf, table->key_info[index].key_part);

  arena.clear();
  for (uint32_t i = 0; i < table->key_info[index].user_defined_key_parts; i++) {
    KEY_PART_INFO *key_part = &table->key_info[index].key_part[i];
    uint16_t key_size = (uint16_t)key_part->length;
    uint8_t *p = (uint8_t *)buf + key_part->offset;

    switch (encoded_length_bytes(key_part->type)) {
      case 1:
        key_size = *p;
        arena.insert(arena.end(), p, p + 1);
        p += 1;
        break;
      case 2:
        key_size = *(uint16_t *)p;
        arena.insert(arena.end(), p, p + 2);
        p += 2;
        break;
      case 0:  // fixed length columns
        arena.insert(arena.end(), p, p + key_size);
        continue;
      default:
        assert(!"shouldn't be here");
    }

    // append the data to our memory buffer
    // BLOB/TEXT data is encoded as a "pointer-to-pointer" in the stream
    if ((key_part->key_part_flag & HA_VAR_LENGTH_PART) == 0)
      p = *(uint8_t **)p;  // p原来指向的是一个指针，现在指向那个指针指向的值
    arena.insert(arena.end(), p, p + key_size);
  }

  ups_key_t key = ups_make_key(arena.data(), (uint16_t)arena.size());
  return key;
}

static inline bool row_is_fixed_length(TABLE *table) {
  // table->s里面保存的是一个Table不变的一些信息
  return table->s->blob_fields + table->s->varchar_fields == 0;
}

// 提取varchar字段的长度信息，保存在len_bytes里面
static inline void extract_varchar_field_info(Field *field, uint32_t *len_bytes,
                                              uint32_t *field_size,
                                              const uint8_t *src) {
  // see Field_blob::Field_blob() (in field.h) - need 1-4 bytes to
  // store the real size
  if (likely(field->field_length <= 255)) {
    *field_size = *src;
    *len_bytes = 1;
    return;
  }
  if (likely(field->field_length <= 65535)) {
    *field_size = *(uint16_t *)src;
    *len_bytes = 2;
    return;
  }
  if (likely(field->field_length <= 16777215)) {
    *field_size = (src[2] << 16) | (src[1] << 8) | (src[0] << 0);
    *len_bytes = 3;
    return;
  }
  *field_size = *(uint32_t *)src;
  *len_bytes = 4;
}

// 把upsdb的record转换成为Mysql的row，只有Row中包含变长字段的时候才需要
static inline ups_record_t unpack_record(TABLE *table, ups_record_t *record,
                                         uint8_t *buf) {
  assert(!row_is_fixed_length(table));

  uint8_t *src = (uint8_t *)record->data;
  uint8_t *dst = buf;

  // copy the "null bytes" descriptors
  for (uint32_t i = 0; i < table->s->null_bytes; i++) {
    *dst = *src;
    dst++;
    src++;
  }

  uint32_t i = 0;
  for (Field **field = table->field; *field != nullptr; field++, i++) {
    uint32_t size;
    uint32_t type = (*field)->type();

    if (type == MYSQL_TYPE_VARCHAR) {
      uint32_t len_bytes;
      extract_varchar_field_info(*field, &len_bytes, &size, src);
      ::memcpy(dst, src, size + len_bytes);
      if (likely((*field)->pack_length() > size + len_bytes))
        ::memset(dst + size + len_bytes, 0,
                 (*field)->pack_length() - (size + len_bytes));
      dst += (*field)->pack_length();
      src += size + len_bytes;
      continue;
    }

    if (type == MYSQL_TYPE_TINY_BLOB || type == MYSQL_TYPE_BLOB ||
        type == MYSQL_TYPE_MEDIUM_BLOB || type == MYSQL_TYPE_LONG_BLOB) {
      size = *(uint32_t *)src;
      src += sizeof(uint32_t);
      Field_blob *blob = *(Field_blob **)field;
      switch (blob->pack_length() - 8) {
        case 1:
          *dst = (uint8_t)size;
          break;
        case 2:
          *(uint16_t *)dst = (uint16_t)size;
          break;
        case 3:  // TODO not yet tested...
          dst[2] = (size & 0xff0000) >> 16;
          dst[1] = (size & 0xff00) >> 8;
          dst[0] = (size & 0xff);
          break;
        case 4:
          *(uint32_t *)dst = size;
          break;
        case 8:
          *(uint64_t *)dst = size;
          break;
        default:
          assert(!"not yet implemented");
          break;
      }

      dst += (*field)->pack_length() - 8;
      *(uint8_t **)dst = src;
      src += size;
      dst += sizeof(uint8_t *);
      continue;
    }

    size = (*field)->pack_length();
    ::memcpy(dst, src, size);
    src += size;
    dst += size;
  }

  ups_record_t r = ups_make_record(buf, (uint32_t)(dst - buf));
  return r;
}

// 把Mysql的row转换成为upsdb的record，只有Row中包含变长字段的时候才需要
static inline ups_record_t pack_record(TABLE *table, uint8_t *buf,
                                       ByteVector &arena) {
  assert(!row_is_fixed_length(table));

  uint8_t *src = buf;
  arena.clear();
  arena.reserve(1024);
  uint8_t *dst = arena.data();

  // copy the "null bytes" descriptors
  arena.resize(table->s->null_bytes);
  for (uint32_t i = 0; i < table->s->null_bytes; i++) {
    *dst = *src;
    dst++;
    src++;
  }

  for (Field **field = table->field; *field != nullptr; field++) {
    uint32_t size;
    uint32_t type = (*field)->type();

    if (type == MYSQL_TYPE_VARCHAR) {
      uint32_t len_bytes;
      extract_varchar_field_info(*field, &len_bytes, &size, src);

      // make sure we have sufficient space
      uint32_t pos = dst - arena.data();
      arena.resize(arena.size() + size + len_bytes);
      dst = &arena[pos];

      ::memcpy(dst, src, size + len_bytes);
      src += (*field)->pack_length();
      dst += size + len_bytes;
      continue;
    }

    if (type == MYSQL_TYPE_TINY_BLOB || type == MYSQL_TYPE_BLOB ||
        type == MYSQL_TYPE_MEDIUM_BLOB || type == MYSQL_TYPE_LONG_BLOB) {
      uint32_t packlength;
      uint32_t length;
      extract_varchar_field_info(*field, &packlength, &length, src);

      // make sure we have sufficient space
      uint32_t pos = dst - arena.data();
      arena.resize(arena.size() + sizeof(uint32_t) + length);
      dst = &arena[pos];

      *(uint32_t *)dst = length;
      dst += sizeof(uint32_t);
      ::memcpy(dst, *(char **)(src + packlength), length);
      dst += length;
      src += packlength + sizeof(void *);
      continue;
    }

    size = (*field)->key_length();

    // make sure we have sufficient space
    uint32_t pos = dst - arena.data();
    arena.resize(arena.size() + size);
    dst = &arena[pos];

    ::memcpy(dst, src, size);
    src += size;
    dst += size;
  }

  ups_record_t r = ups_make_record(arena.data(), (uint32_t)(dst - &arena[0]));
  return r;
}

// 把Mysql的row转换成为upsdb的record，会把buf中的内容copy到arena中
static inline ups_record_t record_from_row(TABLE *table, uint8_t *buf,
                                           ByteVector &arena) {
  // fixed length rows do not need any packing
  if (row_is_fixed_length(table)) {
    return ups_make_record(buf, (uint32_t)table->s->stored_rec_length);
  }

  // but if rows have variable length (i.e. due to a VARCHAR field) we
  // pack them to save space
  return pack_record(table, buf, arena);
}

static inline uint16_t dbname(int index) { return index + 2; }

// key是否存在于db中
static inline bool key_exists(ups_db_t *db, ups_key_t *key) {
  ups_record_t record = ups_make_record(nullptr, 0);
  ups_status_t st = ups_db_find(db, nullptr, key, &record, 0);
  return st == 0;
}

// 初始化自增Key，db是这个Key对应upsdb的db
static inline uint64_t initialize_autoinc(KEY_PART_INFO *key_part,
                                          ups_db_t *db) {
  // if the database is not empty: read the maximum value
  if (db) {
    CursorProxy cp(db, nullptr);
    if (unlikely(cp.cursor == nullptr)) return 0;

    ups_key_t key = ups_make_key(nullptr, 0);
    ups_status_t st =
        ups_cursor_move(cp.cursor, &key, nullptr, UPS_CURSOR_LAST);
    if (st == 0) {
      uint8_t *p = (uint8_t *)key.data;
      // the auto-increment key is always at the beginning of |p|
      if (key_part->length == 1) return *p;
      if (key_part->length == 2) return *(uint16_t *)p;
      if (key_part->length == 3) return (p[0] << 16) | (p[1] << 8) | p[0];
      if (key_part->length == 4) return *(uint32_t *)p;
      if (key_part->length == 8) return *(uint64_t *)p;
      assert(!"shouldn't be here");
      return 0;
    }
  }

  return 0;
}

// 删除一张Mysql表，删除upsdb中与这张表相关的全部内容
static ups_status_t delete_all_databases(Catalogue::Database *catdb,
                                         Catalogue::Table *cattbl,
                                         TABLE *) {
  ups_status_t st;
  uint16_t names[1024];
  uint32_t length = 1024;
  st = ups_env_get_database_names(catdb->env, names, &length);
  if (unlikely(st)) {
    log_error("ups_env_get_database_names", st);
    return 1;
  }

  if (cattbl->autoidx.db) {
    ups_db_close(cattbl->autoidx.db, 0);
    cattbl->autoidx.db = nullptr;
  }

  for (auto &indice : cattbl->indices) ups_db_close(indice.db, 0);
  cattbl->indices.clear();

  for (uint32_t i = 0; i < length; i++) {
    st = ups_env_erase_db(catdb->env, names[i], 0);
    if (unlikely(st)) {
      log_error("ups_env_erase_db", st);
      return 1;
    }
  }

  return 0;
}

// 创建一张Mysql表，创建upsdb中与这张表相关的全部内容
static ups_status_t create_all_databases(Catalogue::Database *catdb,
                                         Catalogue::Table *cattbl,
                                         TABLE *table) {
  ups_db_t *db;
  int num_indices = table->s->keys;
  bool has_primary_index = false;

  // key info for the primary key
  std::pair<uint32_t, uint32_t> primary_type_info;

  assert(cattbl ? cattbl->indices.empty() : true);

  ups_register_compare(CUSTOM_COMPARE_NAME, custom_compare_func);

  // foreach indexed field: create a database which stores this index
  KEY *key_info = table->key_info;
  for (int i = 0; i < num_indices; i++, key_info++) {
    Field *field = key_info->key_part->field;

    std::pair<uint32_t, uint32_t> type_info;
    type_info = table_key_info(key_info);
    uint32_t key_type = type_info.first;
    uint32_t key_size = type_info.second;

    bool enable_duplicates = true;
    bool is_primary_key = false;

    if (0 == ::strcmp("PRIMARY", key_info->name)) is_primary_key = true;

    if (key_info->actual_flags & HA_NOSAME) enable_duplicates = false;

    // If there is only one key then pretend that it's the primary key
    if (num_indices == 1) is_primary_key = true;

    if (is_primary_key) has_primary_index = true;

    // enable duplicates for all indices but the first
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

    // this index requires a custom compare callback function?
    if (key_type == UPS_TYPE_CUSTOM) {
      params[p].name = UPS_PARAM_CUSTOM_COMPARE_NAME;
      params[p].value = (uint64_t)CUSTOM_COMPARE_NAME;
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

    // is record compression enabled?
    if (is_primary_key && cattbl->record_compression) {
      params[p].name = UPS_PARAM_RECORD_COMPRESSION;
      params[p].value = cattbl->record_compression;
      p++;
    }

    ups_status_t st =
        ups_env_create_db(catdb->env, &db, dbname(i), flags, params);
    if (unlikely(st != 0)) {
      log_error("ups_env_create_db", st);
      return st;
    }

    if (cattbl)
      cattbl->indices.emplace_back(db, field, enable_duplicates, is_primary_key,
                                   key_type);
  }

  // no primary index? then create a record-number database
  if (!has_primary_index) {
    ups_parameter_t params[] = {{0, 0}, {0, 0}};

    if (cattbl->record_compression) {
      params[0].name = UPS_PARAM_RECORD_COMPRESSION;
      params[0].value = cattbl->record_compression;
    }

    ups_status_t st =
        ups_env_create_db(catdb->env, &db, 1, UPS_RECORD_NUMBER32, &params[0]);
    if (unlikely(st != 0)) {
      log_error("ups_env_create_db", st);
      return st;
    }
    if (cattbl)
      cattbl->autoidx = Catalogue::Index(db, 0, false, true, UPS_TYPE_UINT32);
  }

  return 0;
}

// 把一个Mysql表（一个upsdb的env，一个Catalogue::Databas）附加到ups
// server上，路径是path
static int attach_to_server(Catalogue::Database *catdb, const char *path) {
  ups_status_t st;
  assert(catdb->is_server_enabled);

  if (!catdb->srv) {
    ups_srv_config_t cfg;
    ::memset(&cfg, 0, sizeof(cfg));
    cfg.port = catdb->server_port;

    st = ups_srv_init(&cfg, &catdb->srv);
    if (unlikely(st != 0)) {
      log_error("ups_srv_init", st);
      return 1;
    }
  }

  st = ups_srv_add_env(catdb->srv, catdb->env, path);
  if (unlikely(st != 0)) {
    log_error("ups_srv_add_env", st);
    return 1;
  }

  return 0;
}

// 关闭一个MySQL表相关的全部upsdb（存了这个表的全部索引）
static void close_all_databases(Catalogue::Table *cattbl) {
  ups_status_t st;

  for (size_t i = 0; i < cattbl->indices.size(); i++) {
    st = ups_db_close(cattbl->indices[i].db, 0);
    if (unlikely(st != 0)) log_error("ups_db_close", st);
  }
  cattbl->indices.clear();

  if (cattbl->autoidx.db != nullptr) {
    st = ups_db_close(cattbl->autoidx.db, 0);
    if (unlikely(st != 0)) log_error("ups_db_close", st);
    cattbl->autoidx.db = nullptr;
  }
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

//static MYSQL_THDVAR_STR(last_create_thdvar, PLUGIN_VAR_MEMALLOC, NULL, NULL,
//                        NULL, NULL);
//
//static MYSQL_THDVAR_UINT(create_count_thdvar, 0, NULL, NULL, NULL, 0, 0, 1000,
//                         0);
//{
//    THD *thd = ha_thd();
//    THDVAR_SET(thd, last_create_thdvar, buf);
//    my_free(buf);
//
//    uint count = THDVAR(thd, create_count_thdvar) + 1;
//    THDVAR_SET(thd, create_count_thdvar, &count);
//}

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

int ha_sar::create(const char *name, TABLE *table, HA_CREATE_INFO *create_info, dd::Table *) {
    DBUG_ENTER("ha_sar::create");

    std::string env_name = format_environment_name(name);

    // See if the Catalogue::Database was already created; otherwise create
    // a new one
    boost::mutex::scoped_lock lock_tb(Catalogue::databases_mutex);
    std::string db_name = env_name; // TODO boost::filesystem::path(name).parent_path().string();
    catdb = Catalogue::databases[db_name];
    if (!catdb) {
      catdb = new Catalogue::Database(db_name);
      Catalogue::databases[db_name] = catdb;
    }

    // Now create the table information
    std::string tbl_name = boost::filesystem::path(name).filename().string();
    assert(catdb->tables.find(tbl_name) == catdb->tables.end());
    cattbl = catdb->tables[tbl_name];
    if (!cattbl) {
      cattbl = new Catalogue::Table(tbl_name);
      catdb->tables[tbl_name] = cattbl;
    }

    // parse the configuration settings from the configuration file (will
    // not do anything if the file does not yet exist)
    ParserStatus ps = parse_config_file(env_name + ".cnf", catdb, false);
    if (!ps.first) {
      sql_print_error("Invalid configuration file '%s.cnf': %s", env_name.c_str(),
                      ps.second.c_str());
      DBUG_RETURN(1);
    }

    // parse the configuration settings in the table's COMMENT
    if (create_info->comment.length > 0) {
      ps = parse_comment_list(create_info->comment.str, cattbl);
      if (!ps.first) {
        sql_print_error("Invalid COMMENT string: %s", ps.second.c_str());
        DBUG_RETURN(1);
      }
    }

    // write the settings to the configuration file; the user can then
    // modify them
//    if (!boost::filesystem::exists(env_name + ".cnf"))
//      write_configuration_settings(env_name, create_info->comment.str, catdb);

    ups_status_t st = 0;
    if (catdb->env == nullptr) {
      st = ups_env_create(&catdb->env, env_name.c_str(),
                          catdb->flags, 0644,
                          !catdb->params.empty() ? &catdb->params[0]
                              : nullptr);
      if (st != 0) {
        log_error("ups_env_create", st);
        DBUG_RETURN(1);
      }
    }

    // create a database for each index
    st = create_all_databases(catdb, cattbl, table);

    // persist the initial autoincrement-value
    cattbl->autoinc_value = create_info->auto_increment_value;

    // We have to clean up the database handles because cattbl->indices[].field
    // will be invalidated by the caller
    close_all_databases(cattbl);

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

  ups_register_compare(CUSTOM_COMPARE_NAME, custom_compare_func);
  ups_set_committed_flush_threshold(30);
  record_arena.resize(table->s->rec_buff_length);

  // TODO ref_length的值是自己设置的，不知道Mysql会不会依赖这个值，先不管它，因为我们的
  // 程序逻辑中是可以不要这个值的
  // ref_length = 0;

  std::string env_name = format_environment_name(name);

  // See if the Catalogue::Database was already created; otherwise create
  // a new one
  boost::mutex::scoped_lock lock_s(Catalogue::databases_mutex);
  std::string db_name =
      env_name;  // TODO boost::filesystem::path(name).parent_path().string();
  catdb = Catalogue::databases[db_name];
  if (!catdb) {
    catdb = new Catalogue::Database(db_name);
    Catalogue::databases[db_name] = catdb;
  }

  // Now create the table information
  std::string tbl_name = boost::filesystem::path(name).filename().string();
  cattbl = catdb->tables[tbl_name];
  bool already_initialized = false;
  if (!cattbl) {
    cattbl = new Catalogue::Table(tbl_name);
    catdb->tables[tbl_name] = cattbl;
  }

  if (!cattbl->indices.empty() || cattbl->autoidx.db != nullptr)
    already_initialized = true;

  // TODO 问题是不是出在这？
  Sar_share *table_share = get_share();
  thr_lock_data_init(&table_share->lock, &lock_data, nullptr);

  // parse the configuration settings in the table's .cnf file
  ParserStatus ps = parse_config_file(env_name + ".cnf", catdb, true);
  if (!ps.first) {
    sql_print_error("Invalid configuration file '%s.cnf': %s", env_name.c_str(),
                    ps.second.c_str());
    DBUG_RETURN(1);
  }

  if (already_initialized) DBUG_RETURN(0);

  // Temporary databases are opened in memory; read-only databases are
  // opened in read-only mode
  /*
  if (mode & O_RDONLY)
    catdb->config.flags |= UPS_READ_ONLY;
  if (mode & HA_OPEN_TMP_TABLE)
    catdb->flags |= UPS_ENABLE_TRANSACTIONS | UPS_IN_MEMORY;
*/

  // open the Environment
  recovery_table = table;
  ups_status_t st = 0;
  if (catdb->env == nullptr) {
    st = ups_env_open(&catdb->env, env_name.c_str(), catdb->flags,
                      !catdb->params.empty() ? &catdb->params[0] : nullptr);
    // UPS_NEED_RECOVERY
    if (unlikely(st != 0)) {
      log_error("ups_env_open", st);
      DBUG_RETURN(1);
    }
  }

  ups_db_t *db;

  int num_indices = table->s->keys;
  bool has_primary_index = false;

  // 与index相关的db已经创建好了
  if (!cattbl->indices.empty() || cattbl->autoidx.db != nullptr) DBUG_RETURN(0);

  // 下面这一堆东西应该是create table时候做的
  // foreach indexed field: open the database which stores this index
  KEY *key_info = table->key_info;
  for (int i = 0; i < num_indices; i++, key_info++) {
    Field *field = key_info->key_part->field;

    bool is_primary_key = false;
    bool enable_duplicates = true;
    if (0 == ::strcmp("PRIMARY", key_info->name)) is_primary_key = true;

    if (key_info->actual_flags & HA_NOSAME) enable_duplicates = false;

    // If there is only one key then pretend that it's the primary key!
    if (num_indices == 1) is_primary_key = true;

    if (is_primary_key) {
      has_primary_index = true;
      // cattbl->ref_length = ref_length = table->key_info[0].key_length;
      cattbl->ref_length = table->key_info[0].key_length;
    }

    st = ups_env_open_db(catdb->env, &db, dbname(i), 0, nullptr);
    if (st != 0) {
      log_error("ups_env_open_db", st);
      DBUG_RETURN(1);
    }

    // This leaks an object, but they're so tiny. Ignored for now.
    ups_set_context_data(db, new CustomCompareState(&table->key_info[i]));

    std::pair<uint32_t, uint32_t> type_info;
    type_info = table_key_info(key_info);
    uint32_t key_type = type_info.first;

    cattbl->indices.emplace_back(db, field, enable_duplicates, is_primary_key,
                                 key_type);

    // is this an auto-increment field? if the database is filled then read
    // the maximum value, otherwise initialize from the cached create_info
    if (field == table->found_next_number_field) {
      for (uint32_t k = 0; k < key_info->user_defined_key_parts; k++) {
        if (key_info->key_part[k].field == field) {
          cattbl->autoinc_value =
              initialize_autoinc(&key_info->key_part[k], db);
          cattbl->initial_autoinc_value = cattbl->autoinc_value + 1;
          break;
        }
      }
    }
  }

  // no primary index? then create a record-number database
  if (!has_primary_index) {
    st = ups_env_open_db(catdb->env, &db, 1, 0, nullptr);
    if (unlikely(st != 0)) {
      log_error("ups_env_open_db", st);
      DBUG_RETURN(1);
    }

    // cattbl->ref_length = ref_length = sizeof(uint32_t);
    cattbl->ref_length = sizeof(uint32_t);
    cattbl->autoidx =
        Catalogue::Index(db, nullptr, false, true, UPS_TYPE_UINT32);
  }

  if (catdb->is_server_enabled)
    DBUG_RETURN(attach_to_server(catdb, name + 1));  // skip leading "."

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

int ha_sar::close(void) {
  DBUG_ENTER("ha_sar::close");
  if (cursor) ups_cursor_close(cursor);

  DBUG_RETURN(0);
}

static inline int insert_auto_index(Catalogue::Table *cattbl, TABLE *table,
                                    uint8_t *buf, ups_txn_t *txn,
                                    ByteVector &key_arena,
                                    ByteVector &record_arena) {
  ups_key_t key = ups_make_key(nullptr, 0);
  ups_record_t record = record_from_row(table, buf, record_arena);

  ups_status_t st = ups_db_insert(cattbl->autoidx.db, txn, &key, &record, 0);
  if (unlikely(st != 0)) {
    log_error("ups_db_insert", st);
    return 1;
  }

  // Need to copy the key in the ByteVector - it will be required later
  key_arena.resize(key.size);
  ::memcpy(key_arena.data(), key.data, key.size);
  return 0;
}

static inline int insert_primary_key(Catalogue::Index *catidx, TABLE *table,
                                     uint8_t *buf, ups_txn_t *txn,
                                     ByteVector &key_arena,
                                     ByteVector &record_arena) {
  ups_key_t key = key_from_row(table, buf, 0, key_arena);
  ups_record_t record = record_from_row(table, buf, record_arena);

  //  // check if the key is greater than the current maximum. If yes then the
  //  // key is unique, and we can specify the flag UPS_OVERWRITE (which is much
  //  // faster)
  //  uint32_t flags = 0;
  //  if (likely(catidx->enable_duplicates))
  //    flags = UPS_DUPLICATE;

  ups_status_t st = ups_db_insert(catidx->db, txn, &key, &record, 0);

  if (unlikely(st == UPS_DUPLICATE_KEY)) return HA_ERR_FOUND_DUPP_KEY;
  if (unlikely(st != 0)) {
    log_error("ups_db_insert", st);
    return 1;
  }

  // Need to copy the key in the ByteVector - it will be required later
  // TODO it's copied *from* key_arena *to* key_arena - says valgrind
  // TODO looks like this is not required
  //  key_arena.resize(key.size);
  //  ::memmove(key_arena.data(), key.data, key.size);
  return 0;
}

static inline int insert_secondary_key(Catalogue::Index *catidx, TABLE *table,
                                       int index, uint8_t *buf, ups_txn_t *txn,
                                       ByteVector &key_arena,
                                       ByteVector &primary_key) {
  // The record of the secondary index is the primary key of the row
  ups_record_t record =
      ups_make_record(primary_key.data(), (uint16_t)primary_key.size());

  // The actual key is the column's value
  ups_key_t key = key_from_row(table, buf, index, key_arena);

  uint32_t flags = 0;
  if (likely(catidx->enable_duplicates)) flags = UPS_DUPLICATE;

  ups_status_t st = ups_db_insert(catidx->db, txn, &key, &record, flags);
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
  // is this an automatically generated index?
  if (cattbl->autoidx.db) {
    int rc =
        insert_auto_index(cattbl, table, buf, txn, key_arena, record_arena);
    if (unlikely(rc)) return rc;
  }

  for (int index = 0; index < (int)cattbl->indices.size(); index++) {
    // is this the primary index?
    if (cattbl->indices[index].is_primary_index) {
      assert(index == 0);
      int rc = insert_primary_key(&cattbl->indices[index], table, buf, txn,
                                  key_arena, record_arena);
      if (unlikely(rc)) return rc;
    }
    // is this a secondary index? the last parameter is a ByteVector with
    // the primary key.
    else {
      int rc = insert_secondary_key(&cattbl->indices[index], table, index, buf,
                                    txn, record_arena, key_arena);
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
static inline int delete_from_secondary(ups_db_t *db, TABLE *table, int index,
                                        const uint8_t *buf, ups_txn_t *txn,
                                        ups_key_t *primary_key,
                                        ByteVector &key_arena) {
  ups_key_t key = key_from_row(table, buf, index, key_arena);
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
static inline int delete_multiple_indices(ups_cursor_t *cursor,
                                          Catalogue::Database *catdb,
                                          Catalogue::Table *cattbl,
                                          TABLE *table, const uint8_t *buf,
                                          ByteVector &key_arena) {
  ups_status_t st;

  TxnProxy txnp(catdb->env);
  if (unlikely(!txnp.txn)) return 1;

  ups_key_t primary_key;

  // The cursor is positioned on the primary key. If the index was auto-
  // generated then fetch the key, otherwise extract the key from |buf|
  if (cattbl->autoidx.db) {
    st = ups_cursor_move(cursor, &primary_key, 0, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_erase", st);
      return 1;
    }
  } else {
    ups_key_t key = key_from_row(table, buf, 0, key_arena);
    primary_key = key;
  }

  for (int index = 0; index < (int)cattbl->indices.size(); index++) {
    Field *field = cattbl->indices[index].field;
    assert(field->m_indexed == true && field->stored_in_db != false);
    (void)field;

    // is this the primary index?
    if (cattbl->indices[index].is_primary_index) {
      // TODO can we use |cursor|? no, because it's not part of the txn :-/
      assert(index == 0);
      st = ups_db_erase(cattbl->indices[index].db, txnp.txn, &primary_key, 0);
      if (unlikely(st != 0)) {
        log_error("ups_db_erase", st);
        return 1;
      }
    }
    // is this a secondary index?
    else {
      int rc = delete_from_secondary(cattbl->indices[index].db, table, index,
                                     buf, txnp.txn, &primary_key, key_arena);
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
  assert(key_info->user_defined_key_parts > 1);

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
  DBUG_ENTER("ha_sar::write_row");

  ups_status_t st;
  duplicate_error_index = (uint32_t)-1;

  // no index? then use the one which was automatically generated
  if (cattbl->indices.empty())
    DBUG_RETURN(insert_auto_index(cattbl, table, buf, nullptr, key_arena,
                                  record_arena));

  // auto-incremented index? then get a new value
  if (table->next_number_field && buf == table->record[0]) {
    int rc = update_auto_increment();
    if (unlikely(rc)) DBUG_RETURN(rc);
  }

  // only one index? then use a temporary transaction
  if (cattbl->indices.size() == 1 && !cattbl->autoidx.db) {
    int rc = insert_primary_key(&cattbl->indices[0], table, buf, nullptr,
                                key_arena, record_arena);
    if (unlikely(rc == HA_ERR_FOUND_DUPP_KEY)) duplicate_error_index = 0;
    DBUG_RETURN(rc);
  }

  // multiple indices? then create a new transaction and update
  // all indices
  TxnProxy txnp(catdb->env);
  if (unlikely(!txnp.txn)) DBUG_RETURN(1);

  int rc = insert_multiple_indices(cattbl, table, buf, txnp.txn, key_arena,
                                   record_arena);
  if (unlikely(rc == HA_ERR_FOUND_DUPP_KEY)) duplicate_error_index = 0;
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
  ups_record_t record = record_from_row(table, new_buf, record_arena);

  // fastest code path: if there's no index then use the cursor to overwrite
  // the record
  if (cattbl->indices.empty()) {
    assert(cursor != nullptr);
    st = ups_cursor_overwrite(cursor, &record, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_overwrite", st);
      DBUG_RETURN(1);
    }
    DBUG_RETURN(0);
  }

  // build a map of the keys that are updated
  // TODO we're extracting keys over and over in the remaining code.
  // it might make sense to cache the old and new keys
  bool changed[MAX_KEY];
  ByteVector tmp1;
  ByteVector tmp2;
  for (size_t i = 0; i < cattbl->indices.size(); i++) {
    ups_key_t oldkey = key_from_row(table, (uchar *)old_buf, i, tmp1);
    ups_key_t newkey = key_from_row(table, new_buf, i, key_arena);
    changed[i] = !are_keys_equal(&oldkey, &newkey);
  }

  // fast code path: if there's just one primary key then try to overwrite
  // the record, or re-insert if the key was modified
  if (cattbl->indices.size() == 1 && cattbl->autoidx.db == nullptr) {
    // if both keys are equal: simply overwrite the record of the
    // current key
    if (!changed[0]) {
      if (likely(cursor != nullptr)) {
        assert(ups_cursor_get_database(cursor) == cattbl->indices[0].db);
        st = ups_cursor_overwrite(cursor, &record, 0);
      } else {
        ups_key_t key = key_from_row(table, (uchar *)old_buf, 0, tmp1);
        st = ups_db_insert(cattbl->indices[0].db, nullptr, &key, &record,
                           UPS_OVERWRITE);
      }
      if (unlikely(st != 0)) {
        log_error("ups_cursor_overwrite", st);
        DBUG_RETURN(1);
      }
      DBUG_RETURN(0);
    }

    ups_key_t oldkey = key_from_row(table, (uchar *)old_buf, 0, tmp1);
    ups_key_t newkey = key_from_row(table, new_buf, 0, key_arena);

    // otherwise delete the old row, insert the new one. Inserting can fail if
    // the key is not unique, therefore both operations are wrapped
    // in a single transaction.
    TxnProxy txnp(catdb->env);
    if (unlikely(!txnp.txn)) {
        DBUG_RETURN(1);
    }

    // TODO can we use the cursor?? no - not in the transaction :-/
    ups_db_t *db = cattbl->indices[0].db;
    st = ups_db_erase(db, txnp.txn, &oldkey, 0);
    if (unlikely(st != 0)) {
        log_error("ups_db_erase", st);
        DBUG_RETURN(1);
    }

    st = ups_db_insert(db, txnp.txn, &newkey, &record, 0);
    if (unlikely(st == UPS_DUPLICATE_KEY)) DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    if (unlikely(st != 0)) {
      log_error("ups_db_insert", st);
      DBUG_RETURN(1);
    }

    st = txnp.commit();
    DBUG_RETURN(st == 0 ? 0 : 1);
  }

  // More than one index? Update all indices that were changed.
  // Again wrap all of this in a single transaction, in case we fail
  // to insert the new row.
  TxnProxy txnp(catdb->env);
  if (unlikely(!txnp.txn)) {
    DBUG_RETURN(1);
  }

  ups_key_t new_primary_key = key_from_row(table, new_buf, 0, record_arena);

  // Are we overwriting an auto-generated index?
  if (cattbl->autoidx.db) {
    ups_key_t k = ups_make_key(ref, sizeof(uint32_t));
    st =
        ups_db_insert(cattbl->autoidx.db, txnp.txn, &k, &record, UPS_OVERWRITE);
    if (unlikely(st != 0)) {
        log_error("ups_db_insert", st);
        DBUG_RETURN(1);
    }
  }

  // Primary index:
  // 1. Was the key modified? then re-insert it
  // 2. Otherwise overwrite the record
  // TODO can we use the cursor? -> no, it's not in the txn :-/
  if (changed[0]) {
    ups_key_t oldkey = key_from_row(table, (uchar *)old_buf, 0, key_arena);
    st = ups_db_erase(cattbl->indices[0].db, txnp.txn, &oldkey, 0);
    if (unlikely(st != 0)) {
      log_error("ups_db_erase", st);
      DBUG_RETURN(1);
    }
  }
  st = ups_db_insert(cattbl->indices[0].db, txnp.txn, &new_primary_key, &record,
                     UPS_OVERWRITE);
  if (unlikely(st != 0)) {
    log_error("ups_db_insert", st);
    DBUG_RETURN(1);
  }

  // All secondary indices:
  // 1. if the primary key was changed then their record has to be
  //    overwritten
  // 2. if the secondary key was changed then re-insert it
  for (size_t i = 1; i < cattbl->indices.size(); i++) {
    ups_record_t new_primary_record =
        ups_make_record(new_primary_key.data, new_primary_key.size);
    ups_key_t old_primary_key = key_from_row(table, old_buf, 0, tmp1);
    ups_key_t newkey = key_from_row(table, (uchar *)new_buf, i, tmp2);

    if (changed[i]) {
      // secondary index有修改，去旧存新
      int rc = delete_from_secondary(cattbl->indices[i].db, table, i, old_buf,
                                     txnp.txn, &old_primary_key, key_arena);
      if (unlikely(rc != 0)) DBUG_RETURN(rc);
      uint32_t flags = 0;
      if (likely(cattbl->indices[i].enable_duplicates)) flags = UPS_DUPLICATE;
      st = ups_db_insert(cattbl->indices[i].db, txnp.txn, &newkey,
                         &new_primary_record, flags);
      if (unlikely(st == UPS_DUPLICATE_KEY)) DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
      if (unlikely(st != 0)) {
        log_error("ups_db_insert", st);
        DBUG_RETURN(1);
      }
    } else if (changed[0]) {
      // secondary index没修改，但primary key有修改，只修改record的值
      ups_key_t oldkey = key_from_row(table, (uchar *)old_buf, i, tmp1);
      ups_record_t old_primary_record =
          ups_make_record(old_primary_key.data, old_primary_key.size);
      CursorProxy cp(locate_secondary_key(cattbl->indices[i].db, txnp.txn,
                                          &oldkey, &old_primary_record));
      assert(cp.cursor != nullptr);
      st = ups_cursor_overwrite(cp.cursor, &new_primary_record, 0);
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

  ups_status_t st;
  // fast code path: if there's just one index then use the cursor to
  // delete the record
  if (cattbl->indices.size() <= 1) {
    if (likely(cursor != nullptr))
      st = ups_cursor_erase(cursor, 0);
    else {
      assert(cattbl->indices.size() == 1);
      ups_key_t key = key_from_row(table, buf, 0, key_arena);
      st = ups_db_erase(cattbl->indices[0].db, nullptr, &key, 0);
    }

    if (unlikely(st != 0)) {
      log_error("ups_cursor_erase", st);
      DBUG_RETURN(1);
    }
    DBUG_RETURN(0);
  }

  // otherwise (if there are multiple indices) then delete the key from
  // each index
  int rc =
      delete_multiple_indices(cursor, catdb, cattbl, table, buf, key_arena);
  DBUG_RETURN(rc);
}

int ha_sar::index_init(uint idx, bool) {
  DBUG_ENTER("ha_sar::index_init");

  active_index = idx;

  assert(cattbl != nullptr);

  // from which index are we reading?
  ups_db_t *db = cattbl->indices[idx].db;

  // if there's a cursor then make sure it's for the correct index database
  if (cursor && ups_cursor_get_database(cursor) == db) DBUG_RETURN(0);

  if (cursor) ups_cursor_close(cursor);

  ups_status_t st = ups_cursor_create(&cursor, db, nullptr, 0);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_create", st);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}
int ha_sar::index_end() {
  active_index = MAX_KEY;
  return rnd_end();
}

int ha_sar::index_read(uchar *buf, const uchar *key, uint,
                       enum ha_rkey_function find_flag) {
  DBUG_ENTER("ha_sar::index_read");
  // MYSQL_INDEX_READ_ROW_START(table_share->db.str,
  // table_share->table_name.str);

  bool read_primary_index = (active_index == 0 || active_index == MAX_KEY) &&
                            cattbl->autoidx.db == nullptr;

  // when reading from the primary index: directly fetch record into |buf|
  // if the row has fixed length
  ups_record_t record = ups_make_record(nullptr, 0);
  if (read_primary_index && row_is_fixed_length(table)) {
    record.data = buf;
    record.flags = UPS_RECORD_USER_ALLOC;
  }

  ups_status_t st;

  if (key == nullptr) {
    st = ups_cursor_move(cursor, nullptr, &record, UPS_CURSOR_FIRST);
  } else {
    ups_key_t key_ups =
        extract_key(key, &table->key_info[active_index], key_arena);

    switch (find_flag) {
      case HA_READ_KEY_EXACT:
        if (likely(table->key_info[active_index].user_defined_key_parts == 1))
          st = ups_cursor_find(cursor, &key_ups, &record, 0);
        else {
          st = ups_cursor_find(cursor, &key_ups, &record, UPS_FIND_GEQ_MATCH);
          // if this was an approx. match: verify that the key part is really
          // identical!
          // 下面这段看不懂+不知道有没有必要
          if (ups_key_get_approximate_match_type(&key_ups) != 0) {
            ups_key_t first = extract_first_keys(
                key, table, &table->key_info[active_index], key_arena);
            if (::memcmp(key_ups.data, first.data, first.size) != 0)
              st = UPS_KEY_NOT_FOUND;
          }
        }
        break;
      case HA_READ_KEY_OR_NEXT:
      case HA_READ_PREFIX:
        st = ups_cursor_find(cursor, &key_ups, &record, UPS_FIND_GEQ_MATCH);
        break;
      case HA_READ_KEY_OR_PREV:
      case HA_READ_PREFIX_LAST_OR_PREV:
        st = ups_cursor_find(cursor, &key_ups, &record, UPS_FIND_LEQ_MATCH);
        break;
      case HA_READ_AFTER_KEY:
        st = ups_cursor_find(cursor, &key_ups, &record, UPS_FIND_GT_MATCH);
        break;
      case HA_READ_BEFORE_KEY:
        st = ups_cursor_find(cursor, &key_ups, &record, UPS_FIND_LT_MATCH);
        break;
      case HA_READ_INVALID:  // (last)
        st = ups_cursor_move(cursor, nullptr, &record, UPS_CURSOR_LAST);
        break;
      default:
        assert(!"shouldn't be here");
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
      // Auto-generated index? Then store the internal row-id (which is a 32bit
      // record number)
      if (cattbl->autoidx.db != nullptr) {
        // assert(ref_length == sizeof(uint32_t));
        assert(record.size == sizeof(uint32_t));
        *(uint32_t *)ref = *(uint32_t *)record.data;
      }

      ups_key_t key_ups = ups_make_key(record.data, (uint16_t)record.size);
      ups_record_t rec = ups_make_record(nullptr, 0);
      if (row_is_fixed_length(table)) {
        rec.data = buf;
        rec.flags = UPS_RECORD_USER_ALLOC;
      }
      st = ups_db_find(
          cattbl->autoidx.db ? cattbl->autoidx.db : cattbl->indices[0].db,
          nullptr, &key_ups, &rec, 0);
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
// If flags is 0 then will perform a lookup for |keybuf|, otherwise
// |keybuf| and |keylen| are ignored
int ha_sar::index_operation(uchar *keybuf, uint32_t, uchar *buf,
                            uint32_t flags) {
  ups_status_t st;

  // when reading from the primary index: directly fetch record into |buf|
  // if the row has fixed length
  ups_record_t record = ups_make_record(nullptr, 0);
  if ((active_index == 0 || active_index == MAX_KEY) &&
      row_is_fixed_length(table)) {
    record.data = buf;
    record.flags = UPS_RECORD_USER_ALLOC;
  }

  // if flags are 0: lookup the current key, but do not move the cursor!
  if (unlikely(flags == 0)) {
    assert(first_call_after_position == true);
    /*
      // skip the null-byte
      KEY_PART_INFO *key_part = table->key_info[active_index].key_part;
      if (key_part->null_bit) {
        keybuf++;
        keylen--;
      }
      ups_key_t key = ups_make_key((void *)keybuf, (uint16_t)keylen);
      */
    ups_key_t key = ups_make_key((void *)last_position_key.data(),
                                 (uint16_t)last_position_key.size());
    st = ups_cursor_find(cursor, &key, &record, 0);
  }
  // otherwise move forward or backwards (or whatever the caller requested)
  else {
    // if we fetched the record from an auto-generated index then store the
    // row id; it will be required in ::position()
    if (cattbl->autoidx.db != nullptr) {
      ups_key_t key = ups_make_key(&recno_row_id, sizeof(recno_row_id));
      key.flags = UPS_KEY_USER_ALLOC;
      st = ups_cursor_move(cursor, &key, &record, flags);
    }
    // if we move to the next duplicate of a multipart index then check if the
    // first (!) key of the multipart key is still identical to the previous
    // one.
    else if ((flags & UPS_ONLY_DUPLICATES) &&
             table->key_info[active_index].user_defined_key_parts > 1) {
      ups_key_t key = ups_make_key(nullptr, 0);
      st = ups_cursor_move(cursor, &key, &record, flags & ~UPS_ONLY_DUPLICATES);
      if (likely(st == 0 && keybuf != nullptr)) {
        ups_key_t first = extract_first_keys(
            keybuf, table, &table->key_info[active_index], key_arena);
        if (::memcmp(key.data, first.data, first.size) != 0)
          st = UPS_KEY_NOT_FOUND;
      }
    }
    // otherwise simply move into the requested direction
    else {
      st = ups_cursor_move(cursor, nullptr, &record, flags);
    }
  }

  if (unlikely(st != 0))
    return ups_status_to_error(table, "ups_cursor_move", st);

  // if we fetched the record from a secondary index: lookup the actual row
  // from the primary index
  if (!cattbl->indices.empty() &&
      (active_index > 0 && active_index < MAX_KEY)) {
    ups_key_t key = ups_make_key(
        record.data,
        (uint16_t)record.size);  // 这个时候record.data是primary_key的值
    ups_record_t rec = ups_make_record(nullptr, 0);
    if (row_is_fixed_length(table)) {
      rec.data = buf;
      rec.flags = UPS_RECORD_USER_ALLOC;
    }
    st = ups_db_find(
        cattbl->autoidx.db ? cattbl->autoidx.db : cattbl->indices[0].db,
        nullptr, &key, &rec, 0);  // or autoidx.db??
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
  rc = index_operation(nullptr, 0, buf, UPS_CURSOR_NEXT);
  DBUG_RETURN(rc);
}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_sar::index_prev(uchar *buf) {
  int rc;
  DBUG_ENTER("ha_sar::index_prev");
  rc = index_operation(nullptr, 0, buf, UPS_CURSOR_PREVIOUS);
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
  rc = index_operation(nullptr, 0, buf, UPS_CURSOR_FIRST);
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
  rc = index_operation(nullptr, 0, buf, UPS_CURSOR_LAST);
  DBUG_RETURN(rc);
}

// 也许是不需要的
int ha_sar::index_next_same(uchar *buf, const uchar *keybuf, uint keylen) {
  DBUG_ENTER("ha_sar::index_next_same");

  int rc = 0;

  if (first_call_after_position) {
    // locate the first key
    rc = index_operation((uchar *)keybuf, keylen, buf, 0);
    // and immediately try to move to the next key
    if (likely(rc == 0))
      rc = index_operation(nullptr, 0, buf,
                           UPS_ONLY_DUPLICATES | UPS_CURSOR_NEXT);
    first_call_after_position = false;
  } else {
    rc = index_operation((uchar *)keybuf, keylen, buf,
                         UPS_ONLY_DUPLICATES | UPS_CURSOR_NEXT);
  }
  DBUG_RETURN(rc);
}

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
int ha_sar::rnd_init(bool) {
  DBUG_ENTER("ha_sar::rnd_init");

  assert(cattbl != nullptr);

  if (cursor) ups_cursor_close(cursor);

  ups_db_t *db = cattbl->autoidx.db;
  if (!db) db = cattbl->indices[0].db;

  ups_status_t st = ups_cursor_create(&cursor, db, nullptr, 0);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_create", st);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

int ha_sar::rnd_end() {
  DBUG_ENTER("ha_sar::rnd_end");

  assert(cursor != 0);
  ups_cursor_close(cursor);
  cursor = 0;

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
  rc = index_operation(nullptr, 0, buf, UPS_CURSOR_NEXT);
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

  // Auto-generated index? Then store the internal row-id (which is a 32bit
  // record number)
  if (cattbl->autoidx.db != nullptr) {
    // assert(ref_length == sizeof(uint32_t));
    *(uint32_t *)ref = recno_row_id;
    DBUG_VOID_RETURN;
  }

  // Store the PRIMARY key as the reference in |ref|
  KEY *key_info = table->key_info;
//  assert(
//      ref_length ==
//      key_info->key_length);  // table->key_info[0]，指向的是primary_key的info
  key_copy(ref, (uchar *)buf, key_info, key_info->key_length);

  // Same (index) key as in the last call? then return immediately (otherwise
  // ups_cursor_find would reset the cursor to the first duplicate, and the
  // following call to ha_sar::index_next_same() would always
  // return the same row)
  ups_key_t key = key_from_row(
      table, buf, active_index == MAX_KEY ? 0 : active_index, key_arena);
  if (key.size == last_position_key.size() &&
      ::memcmp(key.data, last_position_key.data(), key.size) == 0) {
    DBUG_VOID_RETURN;
  }

    // otherwise store a copy of the last key
    last_position_key.resize(key.size);
    ::memcpy(last_position_key.data(), key.data, key.size);

    first_call_after_position = true;

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
  ups_status_t st;

  DBUG_ENTER("ha_sar::rnd_pos");

  assert(cattbl != nullptr);
  assert(active_index == MAX_KEY);

  bool is_fixed_row_length = row_is_fixed_length(table);
  ups_record_t rec = ups_make_record(nullptr, 0);
  ups_key_t key = ups_make_key(nullptr, 0);

  ups_db_t *db = cattbl->autoidx.db; // 如果有primary_key是不会有autoidx的
  if (!db) db = cattbl->indices[0].db;

  // 要么是主键table->key_info[0].key_length，要么是自动生成的索引sizeof(uint32_t)
  key.data = pos;
  key.size = table->key_info ? (uint16_t)table->key_info[0].key_length
                             : (uint16_t)sizeof(uint32_t);  // recno

  // when reading from the primary index: directly fetch record into |buf|
  // if the row has fixed length
  if (is_fixed_row_length) {
    rec.data = buf;
    rec.flags = UPS_RECORD_USER_ALLOC;
  }

  st = ups_cursor_find(cursor, &key, &rec, 0);

  // did we fetch from the primary index? then we have to unpack the record
  if (st == 0 && !is_fixed_row_length) rec = unpack_record(table, &rec, buf);

  rc = ups_status_to_error(table, "ups_cursor_find", st);

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
int ha_sar::info(uint flag) {
  DBUG_ENTER("ha_sar::info");

  if (flag & HA_STATUS_AUTO)
    stats.auto_increment_value = cattbl->initial_autoinc_value;

  if (flag & HA_STATUS_ERRKEY)
    errkey = duplicate_error_index;

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
int ha_sar::delete_all_rows() {
  DBUG_ENTER("ha_sar::delete_all_rows");

  close(); // closes the cursor

  ups_status_t st = delete_all_databases(catdb, cattbl, table);
  if (unlikely(st)) {
    log_error("delete_all_databases", st);
    DBUG_RETURN(1);
  }

  st = create_all_databases(catdb, cattbl, table);
  if (unlikely(st)) {
    log_error("create_all_databases", st);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}


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
  TODO
*/
THR_LOCK_DATA **ha_sar::store_lock(THD *thd, THR_LOCK_DATA **to,
                                   enum thr_lock_type lock_type) {
    if (lock_type != TL_IGNORE && lock_data.type == TL_UNLOCK) {
      /*
        Here is where we get into the guts of a row level lock.
        If TL_UNLOCK is set
        If we are not doing a LOCK TABLE or DISCARD/IMPORT
        TABLESPACE, then allow multiple writers
      */

      if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE) &&
          !thd_in_lock_tables(thd))
        lock_type = TL_WRITE_ALLOW_WRITE;

      /*
        In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
        MySQL would use the lock TL_READ_NO_INSERT on t2, and that
        would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
        to t2. Convert the lock to a normal read lock to allow
        concurrent inserts to t2.
      */

      if (lock_type == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
        lock_type = TL_READ;

      lock_data.type = lock_type;
    }

    *to++ = &lock_data;

    return to;
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

  std::string env_name = format_environment_name(name);
  if (!catdb)
    catdb = Catalogue::databases[env_name];

  // remove the environment from the global cache
  close_and_remove_share(name, catdb);

//  // delete all files
  (void)boost::filesystem::remove(env_name);
  (void)boost::filesystem::remove(env_name + ".jrn0");
  (void)boost::filesystem::remove(env_name + ".jrn1");
  (void)boost::filesystem::remove(env_name + ".cnf");

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

//static ulong srv_enum_var = 0;
//static ulong srv_ulong_var = 0;
//static double srv_double_var = 0;
//static int srv_signed_int_var = 0;
//static long srv_signed_long_var = 0;
//static longlong srv_signed_longlong_var = 0;
//
//const char *enum_var_names[] = {"e1", "e2", NullS};
//
//TYPELIB enum_var_typelib = {array_elements(enum_var_names) - 1,
//                            "enum_var_typelib", enum_var_names, NULL};
//
//static MYSQL_SYSVAR_ENUM(enum_var,                        // name
//                         srv_enum_var,                    // varname
//                         PLUGIN_VAR_RQCMDARG,             // opt
//                         "Sample ENUM system variable.",  // comment
//                         NULL,                            // check
//                         NULL,                            // update
//                         0,                               // def
//                         &enum_var_typelib);              // typelib
//
//static MYSQL_SYSVAR_ULONG(ulong_var, srv_ulong_var, PLUGIN_VAR_RQCMDARG,
//                          "0..1000", NULL, NULL, 8, 0, 1000, 0);
//
//static MYSQL_SYSVAR_DOUBLE(double_var, srv_double_var, PLUGIN_VAR_RQCMDARG,
//                           "0.500000..1000.500000", NULL, NULL, 8.5, 0.5,
//                           1000.5,
//                           0);  // reserved always 0
//
//static MYSQL_THDVAR_DOUBLE(double_thdvar, PLUGIN_VAR_RQCMDARG,
//                           "0.500000..1000.500000", NULL, NULL, 8.5, 0.5,
//                           1000.5, 0);
//
//static MYSQL_SYSVAR_INT(signed_int_var, srv_signed_int_var, PLUGIN_VAR_RQCMDARG,
//                        "INT_MIN..INT_MAX", NULL, NULL, -10, INT_MIN, INT_MAX,
//                        0);
//
//static MYSQL_THDVAR_INT(signed_int_thdvar, PLUGIN_VAR_RQCMDARG,
//                        "INT_MIN..INT_MAX", NULL, NULL, -10, INT_MIN, INT_MAX,
//                        0);
//
//static MYSQL_SYSVAR_LONG(signed_long_var, srv_signed_long_var,
//                         PLUGIN_VAR_RQCMDARG, "LONG_MIN..LONG_MAX", NULL, NULL,
//                         -10, LONG_MIN, LONG_MAX, 0);
//
//static MYSQL_THDVAR_LONG(signed_long_thdvar, PLUGIN_VAR_RQCMDARG,
//                         "LONG_MIN..LONG_MAX", NULL, NULL, -10, LONG_MIN,
//                         LONG_MAX, 0);
//
//static MYSQL_SYSVAR_LONGLONG(signed_longlong_var, srv_signed_longlong_var,
//                             PLUGIN_VAR_RQCMDARG, "LLONG_MIN..LLONG_MAX", NULL,
//                             NULL, -10, LLONG_MIN, LLONG_MAX, 0);
//
//static MYSQL_THDVAR_LONGLONG(signed_longlong_thdvar, PLUGIN_VAR_RQCMDARG,
//                             "LLONG_MIN..LLONG_MAX", NULL, NULL, -10, LLONG_MIN,
//                             LLONG_MAX, 0);
//
//static SYS_VAR *sar_system_variables[] = {MYSQL_SYSVAR(enum_var),
//                                          MYSQL_SYSVAR(ulong_var),
//                                          MYSQL_SYSVAR(double_var),
//                                          MYSQL_SYSVAR(double_thdvar),
//                                          MYSQL_SYSVAR(last_create_thdvar),
//                                          MYSQL_SYSVAR(create_count_thdvar),
//                                          MYSQL_SYSVAR(signed_int_var),
//                                          MYSQL_SYSVAR(signed_int_thdvar),
//                                          MYSQL_SYSVAR(signed_long_var),
//                                          MYSQL_SYSVAR(signed_long_thdvar),
//                                          MYSQL_SYSVAR(signed_longlong_var),
//                                          MYSQL_SYSVAR(signed_longlong_thdvar),
//                                          NULL};
//
//// this is an sar of SHOW_FUNC
//static int show_func_sar(MYSQL_THD, SHOW_VAR *var, char *buf) {
//  var->type = SHOW_CHAR;
//  var->value = buf;  // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
//  snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
//           "enum_var is %lu, ulong_var is %lu, "
//           "double_var is %f, signed_int_var is %d, "
//           "signed_long_var is %ld, signed_longlong_var is %lld",
//           srv_enum_var, srv_ulong_var, srv_double_var, srv_signed_int_var,
//           srv_signed_long_var, srv_signed_longlong_var);
//  return 0;
//}
//
//struct sar_vars_t {
//  ulong var1;
//  double var2;
//  char var3[64];
//  bool var4;
//  bool var5;
//  ulong var6;
//};
//
//sar_vars_t sar_vars = {100, 20.01, "three hundred", true, 0, 8250};
//
//static SHOW_VAR show_status_sar[] = {
//    {"var1", (char *)&sar_vars.var1, SHOW_LONG, SHOW_SCOPE_GLOBAL},
//    {"var2", (char *)&sar_vars.var2, SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},
//    {0, 0, SHOW_UNDEF, SHOW_SCOPE_UNDEF}  // null terminator required
//};
//
//static SHOW_VAR show_array_sar[] = {
//    {"array", (char *)show_status_sar, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
//    {"var3", (char *)&sar_vars.var3, SHOW_CHAR, SHOW_SCOPE_GLOBAL},
//    {"var4", (char *)&sar_vars.var4, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
//    {0, 0, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};
//
//static SHOW_VAR func_status[] = {
//    {"sar_func_sar", (char *)show_func_sar, SHOW_FUNC, SHOW_SCOPE_GLOBAL},
//    {"sar_status_var5", (char *)&sar_vars.var5, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
//    {"sar_status_var6", (char *)&sar_vars.var6, SHOW_LONG, SHOW_SCOPE_GLOBAL},
//    {"sar_status", (char *)show_array_sar, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
//    {0, 0, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

mysql_declare_plugin(sar){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &sar_storage_engine,
    "SAR",
    "Haitao",
    "SGX Enabled Remote storage engine",
    PLUGIN_LICENSE_GPL,
    sar_init_func, /* Plugin Init */
    nullptr,          /* Plugin check uninstall */
    NULL,          /* Plugin Deinit */ // TODO
    0x0001 /* 0.1 */,
    nullptr,          /* status variables */
    nullptr, /* system variables */
    nullptr,                 /* config options */
    0,                    /* flags */
} mysql_declare_plugin_end;
