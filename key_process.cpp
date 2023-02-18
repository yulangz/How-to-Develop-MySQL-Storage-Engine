//
// Created by yulan on 22-7-8.
//

#include "key_process.h"

static uint32_t key_sort_length(KEY *key_info) {
  uint32_t sz = 0;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    auto field = key_info->key_part[i].field;
    DBUG_ASSERT((field->flags & BLOB_FLAG) == 0);
    sz += field->sort_length();
    if (field->real_maybe_null()) {
      sz++;
    }
  }
  return sz;
}

int mysql_key_info_to_internal_key_info(KEY *mysql_key_info, uint32_t &key,
                                        uint32_t &size) {
  // accumulate the total size of all columns which form this key.
  key = UPS_TYPE_BINARY;
  size = 0;
  for (uint32_t i = 0; i < mysql_key_info->user_defined_key_parts; i++) {
    auto field = mysql_key_info->key_part[i].field;
    if (field->flags & BLOB_FLAG) {
      // blob 类型不允许做 key
      sql_print_error("bolb type index not supported");
      return 1;
    }
    size += field->sort_length();
    if (field->real_maybe_null()) {
      size++;
    }
  }
  return 0;
}
/**
 *
 * @param to
 * @param row_buf
 * @param mysql_key_part
 * @param tbl
 * @param is_key_tuple_format true,则表示 buf 的格式是
 * KeyTupleFormat，且指向的是当前 key，false,则表示格式是
 * TableRecordFormat，且指向这个 record 的起始位置
 * @param key_part_real_length[out] 只有 KeyTupleFormat 下有用，返回这个
 * key_part 在 key_buf 中实际占用的长度
 * @return 这个 key_part 存储后的长度（也就是在 to 中占用的空间）
 */
static uint32_t convert_to_sort_key(uchar *to, const uchar *buf,
                                    KEY_PART_INFO *mysql_key_part, TABLE *tbl,
                                    bool is_key_tuple_format,
                                    uint32 *key_part_real_length) {
  uint32 store_length = 0;
  auto field = mysql_key_part->field;
  uint field_offset = field->offset(tbl->record[0]);
  uint null_offset = field->null_offset(tbl->record[0]);
  bool maybe_null = field->real_maybe_null();

  if (is_key_tuple_format) {
    field->move_field(const_cast<uchar *>(buf) + (maybe_null ? 1 : 0),
                      maybe_null ? const_cast<uchar *>(buf) : nullptr,
                      field->null_bit);
    DBUG_ASSERT(key_part_real_length != nullptr);
    *key_part_real_length =
        field->data_length() + (maybe_null ? 1 : 0);
    if (field->type() == MYSQL_TYPE_VARCHAR) {
      *key_part_real_length += field->get_length_bytes();
    }
  } else {
    field->move_field(
        const_cast<uchar *>(buf) + field_offset,
        maybe_null ? const_cast<uchar *>(buf) + null_offset : nullptr,
        field->null_bit);
  }
  // WARNING! Don't return without restoring field->ptr and field->null_ptr

  if (field->real_maybe_null()) {
    store_length++;
    if (field->is_real_null()) {
      to[0] = 0;  // 让 null 的 key 排在最前面
    } else {
      to[0] = 1;
    }
  }
  bzero(to + store_length, field->sort_length());
  auto used_length = field->make_sort_key(to + store_length, field->sort_length());
  (void)used_length; // for debug
  store_length += field->sort_length();

  // Restore field->ptr and field->null_ptr
  field->move_field(tbl->record[0] + field_offset,
                    maybe_null ? tbl->record[0] + null_offset : nullptr,
                    field->null_bit);
  return store_length;
}

ups_key_t key_from_row(const uchar *row_buf, KEY *mysql_key_info,
                       TABLE *mysql_table,  Catalogue::Table& cattbl, ByteVector &arena) {
  if (mysql_key_info == nullptr) {
    // this is a hidden key
    DBUG_ASSERT(mysql_table->s->is_missing_primary_key());
    arena.resize(8);
    *((uint64*)arena.data()) = cattbl.hidden_index_value++;
    return ups_make_key(arena.data(), (uint16_t)arena.size());
  }

  auto internal_key_size = key_sort_length(mysql_key_info);
  arena.resize(internal_key_size, 0);

  uint32 offset = 0;
  for (uint32_t i = 0; i < mysql_key_info->user_defined_key_parts; i++) {
    KEY_PART_INFO *key_part = &mysql_key_info->key_part[i];
    offset += convert_to_sort_key(arena.data() + offset, row_buf, key_part,
                                  mysql_table, false, nullptr);
  }

  return ups_make_key(arena.data(), (uint16_t)arena.size());
}

ups_key_t key_from_key_buf(const uchar *mysql_key_buf, key_part_map mysql_key_part_map,
                           KEY *mysql_key_info, TABLE *mysql_table,
                           ByteVector &arena, uint fill_val, uint32 &exact_key_length) {
  DBUG_ASSERT(((mysql_key_part_map + 1) & mysql_key_part_map) == 0);
  auto internal_key_size = key_sort_length(mysql_key_info);
  arena.resize(internal_key_size, fill_val);

  uint32 key_offset = 0;
  uint32 to_offset = 0;
  for (uint32_t i = 0; i < mysql_key_info->user_defined_key_parts && mysql_key_part_map; i++) {
    KEY_PART_INFO *key_part = &mysql_key_info->key_part[i];
    uint32 key_part_real_length;
    to_offset += convert_to_sort_key(
        arena.data() + to_offset, mysql_key_buf + key_offset, key_part,
        mysql_table, true, &key_part_real_length);
    key_offset += key_part_real_length;
    mysql_key_part_map >>= 1;
  }

  exact_key_length = to_offset;
  ups_key_t key = ups_make_key(arena.data(), (uint16_t)arena.size());
  return key;
}
