//
// Created by yulan on 22-7-8.
//

#include "key_process.h"

int table_field_info(KEY_PART_INFO *key_part, uint32_t &key, uint32_t &size) {
  // TODO 可不可以为存储引擎设置可以支持的key类型

  // Field 和 KEY_PART 基本对等，也许有区别但是我不知道
  // field->type() 是从field中提取信息，key_part->type是从key_part中提取信息
  Field *field = key_part->field;

  switch (field->type()) {
      // TODO 无法支持负数整形，只能支持非负数，等修改完upsdb再来改这里
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_YEAR:
      key = UPS_TYPE_UINT8;
      size = 1u;
      return 0;
    case MYSQL_TYPE_SHORT:
      key = UPS_TYPE_UINT16;
      size = 2u;
      return 0;
    case MYSQL_TYPE_LONG:
      key = UPS_TYPE_UINT32;
      size = 4u;
      return 0;
    case MYSQL_TYPE_LONGLONG:
      key = UPS_TYPE_UINT64;
      size = 8u;
      return 0;
    case MYSQL_TYPE_FLOAT:
      key = UPS_TYPE_REAL32;
      size = 4u;
      return 0;
    case MYSQL_TYPE_DOUBLE:
      key = UPS_TYPE_REAL64;
      size = 8u;
      return 0;

    // 参考
    // https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html，
    // DATE是3字节小端序，不能直接用binary，暂时不支持
    case MYSQL_TYPE_DATE:
      //      key = UPS_TYPE_BINARY;
      //      size = 3u;
      return 1;

    // Mysql中TIME、DATETIME、TIMESTAMP系列的数据在内存中是以大端序存储的，是mem_comparable的，所以
    // 在upsDB中可以直接用UPS_TYPE_BINARY
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP:
      key = UPS_TYPE_BINARY;
      size = field->pack_length();
      return 0;

      // blob 和 text 类型不能作为索引
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
      sql_print_error("BLOB, TEXT type index not supported");
      return 1;

      // char 和 binary类型可以直接用
      // TODO 字符集问题？？？
    case MYSQL_TYPE_STRING:
      key = UPS_TYPE_BINARY;
      size = field->pack_length();
      return 0;
      // TODO 暂时不支持varchar，等改好upsdb之后再支持
      // 在type为varchar时，field->pack_length()是可能的最长长度，即 1+m*k ，
      // 其中m是varchar(m)，k是字符集中单个字符最大的长度
    case MYSQL_TYPE_VARCHAR:
      sql_print_error("currently varchar type index not supported");
      return 1;

      // 先不支持这些特殊类型
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_JSON:
    case MYSQL_TYPE_NULL:
    default:
      sql_print_error("unsupported index type");
      return 1;
  }

  return 1;
}

int table_key_info(KEY *key_info, uint32_t &key, uint32_t &size,
                   __attribute__((unused))
                   std::vector<std::pair<uint32_t, uint32_t>> *key_parts) {
  // if this index is made from a single column: return type/size of this column
  if (key_info->user_defined_key_parts == 1)
    return table_field_info(key_info->key_part, key, size);

  return 1;
  // Otherwise accumulate the total size of all columns which form this
  // key.
  // If any key has variable length then we need to use our custom compare
  // function.
  // TODO 如果index包含多个字段，会返回复合键类型(Complex)，暂时不支持，返回1,
  //  下面的也不是正确的写法，回头再改
  //  uint32_t total_size = 0;
  //  bool need_custom_compare = false;
  //  for (uint32_t i = 0; i < key_info->user_defined_key_parts; i++) {
  //    std::pair<uint32_t, uint32_t> p =
  //    table_field_info(&key_info->key_part[i]); if (total_size ==
  //    UPS_KEY_SIZE_UNLIMITED || p.second == 0 ||
  //        p.second == UPS_KEY_SIZE_UNLIMITED)
  //      total_size = UPS_KEY_SIZE_UNLIMITED;
  //    else
  //      total_size += p.second;
  //
  //    switch (key_info->key_part[i].type) {
  //      case HA_KEYTYPE_VARTEXT1:
  //      case HA_KEYTYPE_VARTEXT2:
  //      case HA_KEYTYPE_VARBINARY1:
  //      case HA_KEYTYPE_VARBINARY2:
  //        need_custom_compare = true;
  //    }
  //  }
  //
  //  return std::make_pair(need_custom_compare ? UPS_TYPE_CUSTOM :
  //  UPS_TYPE_BINARY,
  //                        total_size);
}

ups_key_t field_from_row(const uchar *buf, KEY_PART_INFO *key_part) {
  uint16_t key_size = key_part->field->pack_length();
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

ups_key_t key_from_row(const uchar *buf, KEY* key_info,
                       ByteVector &arena) {
  arena.clear();

  if (likely(key_info->user_defined_key_parts == 1))
    return field_from_row(buf, key_info->key_part);

  DBUG_ASSERT(!"shouldn't be here! currently not support multi part index!");  // 现在不支持多列索引

  arena.clear();
  for (uint32_t i = 0; i < key_info->user_defined_key_parts; i++) {
    KEY_PART_INFO *key_part = &key_info->key_part[i];
    auto key_size = (uint16_t)key_part->length;
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
        DBUG_ASSERT(!"shouldn't be here");
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
